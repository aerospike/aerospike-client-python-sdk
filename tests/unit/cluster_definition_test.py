# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Unit tests for ClusterDefinition auth mode support."""

from typing import Any

import pytest
from aerospike_sdk import AuthMode

from aerospike_sdk.aio.cluster_definition import ClusterDefinition, Host
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sync.client import SyncClient
from aerospike_sdk.sync.cluster import Cluster as SyncCluster
from aerospike_sdk.sync.cluster_definition import (
    ClusterDefinition as SyncClusterDefinition,
    Host as SyncHost,
)


class TestAuthMode:
    def test_default_auth_mode_is_none(self):
        cd = ClusterDefinition("localhost", 3000)
        assert cd.auth_mode == AuthMode.NONE

    def test_native_credentials_sets_internal(self):
        cd = ClusterDefinition("localhost", 3000).with_native_credentials("admin", "pass")
        assert cd.auth_mode == AuthMode.INTERNAL
        assert cd._user_name == "admin"
        assert cd._password == "pass"

    def test_native_credentials_empty_user_resets_to_none(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_native_credentials("admin", "pass")
            .with_native_credentials("", "")
        )
        assert cd.auth_mode == AuthMode.NONE
        assert cd._user_name is None
        assert cd._password is None

    def test_external_credentials_sets_external(self):
        cd = ClusterDefinition("localhost", 3000).with_external_credentials("ldap_user", "ldap_pass")
        assert cd.auth_mode == AuthMode.EXTERNAL
        assert cd._user_name == "ldap_user"
        assert cd._password == "ldap_pass"

    def test_external_credentials_empty_user_resets_to_none(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_external_credentials("user", "pass")
            .with_external_credentials("", "")
        )
        assert cd.auth_mode == AuthMode.NONE
        assert cd._user_name is None

    def test_certificate_credentials_sets_pki(self):
        cd = ClusterDefinition("localhost", 3000).with_certificate_credentials()
        assert cd.auth_mode == AuthMode.PKI
        assert cd._user_name is None
        assert cd._password is None

    def test_certificate_credentials_auto_enables_tls(self):
        cd = ClusterDefinition("localhost", 3000)
        assert cd._tls_builder is None
        cd.with_certificate_credentials()
        assert cd._tls_builder is not None
        assert cd._tls_builder.is_tls_enabled()

    def test_certificate_credentials_preserves_existing_tls(self):
        cd = ClusterDefinition("localhost", 3000)
        cd.with_tls_config_of().tls_name("myTls").done()
        original_builder = cd._tls_builder
        cd.with_certificate_credentials()
        assert cd._tls_builder is original_builder

    def test_switching_auth_modes(self):
        cd = ClusterDefinition("localhost", 3000)
        cd.with_native_credentials("u", "p")
        assert cd.auth_mode == AuthMode.INTERNAL
        cd.with_external_credentials("e", "p")
        assert cd.auth_mode == AuthMode.EXTERNAL
        cd.with_certificate_credentials()
        assert cd.auth_mode == AuthMode.PKI
        assert cd._user_name is None
        cd.with_native_credentials("u2", "p2")
        assert cd.auth_mode == AuthMode.INTERNAL
        assert cd._user_name == "u2"


class TestAuthModePolicy:
    def test_native_credentials_policy(self):
        cd = ClusterDefinition("localhost", 3000).with_native_credentials("admin", "pass")
        policy = cd._get_policy()
        assert policy.auth_mode == AuthMode.INTERNAL

    def test_external_credentials_policy(self):
        cd = ClusterDefinition("localhost", 3000).with_external_credentials("user", "pass")
        policy = cd._get_policy()
        assert policy.auth_mode == AuthMode.EXTERNAL

    def test_pki_credentials_policy(self):
        cd = ClusterDefinition("localhost", 3000).with_certificate_credentials()
        policy = cd._get_policy()
        assert policy.auth_mode == AuthMode.PKI

    def test_no_credentials_policy(self):
        cd = ClusterDefinition("localhost", 3000)
        policy = cd._get_policy()
        assert policy.auth_mode == AuthMode.NONE


class TestPkiValidation:
    def test_pki_without_tls_names_raises(self):
        cd = ClusterDefinition("localhost", 3000).with_certificate_credentials()
        with pytest.raises(ValueError, match="Missing TLS name"):
            cd._validate()

    def test_pki_with_tls_names_passes(self):
        cd = ClusterDefinition(
            hosts=[Host("localhost", 3000, tls_name="myTls")]
        ).with_certificate_credentials()
        cd._validate()

    def test_pki_with_tls_builder_name_passes(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_tls_config_of()
            .tls_name("myTls")
            .done()
            .with_certificate_credentials()
        )
        cd._validate()

    def test_non_pki_without_tls_names_passes(self):
        cd = ClusterDefinition("localhost", 3000).with_native_credentials("u", "p")
        cd._validate()


class TestClusterDefinitionChaining:
    def test_full_chain_with_native_credentials(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_native_credentials("admin", "password")
            .using_services_alternate()
            .preferring_racks(1, 2)
            .validate_cluster_name_is("my-cluster")
        )
        assert cd.auth_mode == AuthMode.INTERNAL
        assert cd._use_services_alternate is True
        assert cd._preferred_racks == [1, 2]
        assert cd._cluster_name == "my-cluster"

    def test_full_chain_with_external_credentials(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_external_credentials("ldap_user", "ldap_pass")
            .using_services_alternate()
        )
        assert cd.auth_mode == AuthMode.EXTERNAL

    def test_full_chain_with_certificate_credentials(self):
        cd = (
            ClusterDefinition(hosts=[Host("localhost", 3000, tls_name="myTls")])
            .with_certificate_credentials()
            .using_services_alternate()
        )
        assert cd.auth_mode == AuthMode.PKI
        assert cd._tls_builder is not None


class TestIpMap:
    def test_default_ip_map_is_none(self):
        cd = ClusterDefinition("localhost", 3000)
        assert cd._ip_map is None

    def test_set_ip_map(self):
        mapping = {"10.0.0.1": "3.72.54.187", "10.0.0.2": "3.72.54.188"}
        cd = ClusterDefinition("localhost", 3000).with_ip_map(mapping)
        assert cd._ip_map == mapping

    def test_empty_dict_clears_ip_map(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_ip_map({"10.0.0.1": "1.2.3.4"})
            .with_ip_map({})
        )
        assert cd._ip_map is None

    def test_ip_map_propagates_to_policy(self):
        mapping = {"10.0.0.1": "3.72.54.187"}
        cd = ClusterDefinition("localhost", 3000).with_ip_map(mapping)
        policy = cd._get_policy()
        assert policy.ip_map == mapping

    def test_no_ip_map_policy_is_none(self):
        cd = ClusterDefinition("localhost", 3000)
        policy = cd._get_policy()
        assert policy.ip_map is None

    def test_ip_map_chaining(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_native_credentials("admin", "pass")
            .with_ip_map({"10.0.0.1": "1.2.3.4"})
            .using_services_alternate()
        )
        assert cd._ip_map == {"10.0.0.1": "1.2.3.4"}
        assert cd.auth_mode == AuthMode.INTERNAL
        assert cd._use_services_alternate is True


class TestServicesAlternate:
    """The setting defaults from the environment, so both directions must be
    expressible on the builder — otherwise a caller running with
    ``AEROSPIKE_USE_SERVICES_ALTERNATE`` set cannot turn it back off, and
    discovery routes through alternate addresses the cluster may not publish.
    """

    def test_no_arg_enables(self):
        cd = ClusterDefinition("localhost", 3000).using_services_alternate()
        assert cd._use_services_alternate is True
        assert cd._get_policy().use_services_alternate is True

    def test_explicit_false_disables(self):
        cd = ClusterDefinition("localhost", 3000).using_services_alternate(False)
        assert cd._use_services_alternate is False
        assert cd._get_policy().use_services_alternate is False

    def test_explicit_false_overrides_env_default(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("AEROSPIKE_USE_SERVICES_ALTERNATE", "true")
        cd = ClusterDefinition("localhost", 3000)
        assert cd._use_services_alternate is True, "env var should seed the default"
        assert cd.using_services_alternate(False)._get_policy().use_services_alternate is False

    def test_env_default_reaches_policy(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("AEROSPIKE_USE_SERVICES_ALTERNATE", "true")
        assert ClusterDefinition("localhost", 3000)._get_policy().use_services_alternate is True

    def test_sync_tree_disables_too(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("AEROSPIKE_USE_SERVICES_ALTERNATE", "true")
        cd = SyncClusterDefinition("localhost", 3000).using_services_alternate(False)
        assert cd._get_policy().use_services_alternate is False


class TestFailIfNotConnected:
    def test_default_is_true(self):
        cd = ClusterDefinition("localhost", 3000)
        assert cd._fail_if_not_connected is True

    def test_set_false(self):
        cd = ClusterDefinition("localhost", 3000).fail_if_not_connected(False)
        assert cd._fail_if_not_connected is False

    def test_set_true_explicit(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .fail_if_not_connected(False)
            .fail_if_not_connected(True)
        )
        assert cd._fail_if_not_connected is True

    def test_propagates_to_policy_true(self):
        cd = ClusterDefinition("localhost", 3000)
        policy = cd._get_policy()
        assert policy.fail_if_not_connected is True

    def test_propagates_to_policy_false(self):
        cd = ClusterDefinition("localhost", 3000).fail_if_not_connected(False)
        policy = cd._get_policy()
        assert policy.fail_if_not_connected is False

    def test_chaining(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_native_credentials("admin", "pass")
            .fail_if_not_connected(False)
            .using_services_alternate()
        )
        assert cd._fail_if_not_connected is False
        assert cd.auth_mode == AuthMode.INTERNAL
        assert cd._use_services_alternate is True


class TestClientId:
    """The built policy stamps PSDK's own user-agent id, overriding the
    underlying async client's, so PSDK usage is distinguishable on the wire."""

    def test_aio_overrides_client_id(self):
        policy = ClusterDefinition("localhost", 3000)._get_policy()
        assert policy.custom_client_id.startswith("python-sdk-")

    def test_sync_overrides_client_id(self):
        policy = SyncClusterDefinition("localhost", 3000)._get_policy()
        assert policy.custom_client_id.startswith("python-sdk-")


class TestSyncBuilderSmoke:
    """Sync builder-chain + Host smoke, mirroring the async coverage above.

    The async ``ClusterDefinition`` builders are exercised throughout this file;
    the sync equivalents had no unit coverage. Since Phase 3 hoists these pure
    state-mutation builders onto a shared base wholesale, this smoke pins the
    sync side first so the hoist lands against green coverage on both trees.
    """

    def test_sync_full_builder_chain(self):
        cd = (
            SyncClusterDefinition("localhost", 3000)
            .with_external_credentials("ldap_user", "ldap_pass")
            .using_services_alternate()
            .preferring_racks(1, 2)
            .validate_cluster_name_is("my-cluster")
            .fail_if_not_connected(False)
        )
        assert cd.auth_mode == AuthMode.EXTERNAL
        assert cd._user_name == "ldap_user"
        assert cd._use_services_alternate is True
        assert cd._preferred_racks == [1, 2]
        assert cd._cluster_name == "my-cluster"
        assert cd._fail_if_not_connected is False

    def test_sync_with_tls_config_of(self):
        cd = (
            SyncClusterDefinition("localhost", 3000)
            .with_tls_config_of()
            .tls_name("myTls")
            .done()
        )
        assert cd._tls_builder is not None
        assert cd._tls_builder.is_tls_enabled()

    def test_sync_host_of_and_parse_hosts(self):
        h = SyncHost.of("node-a", 3000)
        assert (h.name, h.port) == ("node-a", 3000)

        hosts = SyncHost.parse_hosts("node-a:3001,node-b", 3000)
        assert [(x.name, x.port) for x in hosts] == [("node-a", 3001), ("node-b", 3000)]


@pytest.fixture
def captured_connect(monkeypatch):
    """Run the real sync ``connect()``, capturing rather than opening a connection.

    Returns a callable that connects the given definition and hands back what
    reached ``Cluster._create`` — the assembled ``ClientPolicy`` plus the
    keyword arguments — so the policy assembly is exercised rather than
    re-implemented in the test.
    """
    seen: dict[str, Any] = {}

    def _fake_create(policy, seeds, **kwargs):
        seen.clear()
        seen.update(kwargs)
        seen["policy"] = policy
        seen["seeds"] = seeds
        return "cluster"

    monkeypatch.setattr(SyncCluster, "_create", staticmethod(_fake_create))

    def _connect(cd: SyncClusterDefinition) -> dict[str, Any]:
        cd.connect()
        return seen

    return _connect


class TestSyncConnectionPoolDefaults:
    """Every sync entry point leaves ``conn_pools_per_node`` at 4.

    A 32-thread sync A/B on a 3-node cluster measured 4 and 8 as
    indistinguishable on both throughput and p99, so the sync surface holds
    the underlying default rather than diverging from it. Pinned so the value
    stays a deliberate choice rather than an accident.
    """

    def test_defaults_to_the_underlying_value(self, captured_connect):
        seen = captured_connect(SyncClusterDefinition("localhost", 3000))
        assert seen["policy"].conn_pools_per_node == 4

    def test_direct_client_construction_does_not_diverge(self):
        assert SyncClient("localhost:3000")._policy.conn_pools_per_node == 4

    def test_explicit_system_setting_wins(self, captured_connect):
        seen = captured_connect(
            SyncClusterDefinition("localhost", 3000)
            .with_system_settings(SystemSettings(conn_pools_per_node=3))
        )
        assert seen["policy"].conn_pools_per_node == 3
