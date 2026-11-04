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

"""Tests for seed discovery and address translation settings (no server needed)."""

import logging
import os
from datetime import timedelta

import pytest
from aerospike_native import Host as PncHost

from aerospike_sdk import (
    AerospikeError,
    AwsAddressField,
    AwsEc2TagSeedCandidateProvider,
    ClusterDefinition,
    DiscoveryExecutionMode,
    DnsRecordType,
    DnsSeedCandidateProvider,
    PassThroughAddressTranslator,
    PortMapAddressTranslator,
    ResultCode,
    SeedCandidateProvider,
    SeedMergePolicy,
    StaticMapAddressTranslator,
    StaticSeedCandidateProvider,
    TranslationMissMode,
)
from aerospike_sdk.policy import DiscoverySettings, SystemSettings, TranslationSettings
from aerospike_sdk.policy.discovery_settings import FileAddressTranslator, FileSeedProvider
from aerospike_sdk.policy.sdk_config_loader import (
    _collect_drops,
    load_and_resolve,
    parse_sdk_config,
    resolve_settings,
)
from aerospike_sdk.sdk_config_monitor import SdkConfigSource, _SdkConfigPoller


class RegistryProvider(SeedCandidateProvider):
    def refresh_seed_candidates(self, ctx):
        return ["10.0.0.1:3000"]


def _profile(text: str):
    return parse_sdk_config(text)["DEFAULT"]


def _policy(file_text=None, cd=None):
    cd = cd or ClusterDefinition("localhost", 3000)
    file_layer = _profile(file_text) if file_text else None
    return cd._get_policy(resolve_settings(file_layer, cd._programmatic_settings()))


class TestWithSeedDiscovery:
    """The builder carries every setting onto the client policy."""

    def test_every_setting_reaches_the_policy(self):
        provider = DnsSeedCandidateProvider(["seeds.example.com"])
        cd = ClusterDefinition("localhost", 3000).with_seed_discovery(
            provider,
            merge_policy=SeedMergePolicy.DISCOVERY_ONLY,
            execution_mode=DiscoveryExecutionMode.TEND,
            refresh_interval=timedelta(seconds=5),
            refresh_timeout=timedelta(seconds=2),
            tend_refresh_deadline=timedelta(milliseconds=50),
        )
        policy = cd._get_policy()
        assert policy.seed_candidate_provider == provider
        assert policy.discovery_merge_policy == SeedMergePolicy.DISCOVERY_ONLY
        assert policy.discovery_refresh_interval == 5000
        assert policy.discovery_refresh_timeout == 2000
        assert policy.discovery_tend_refresh_deadline == 50

    def test_python_provider_in_thread_mode_is_accepted(self):
        provider = RegistryProvider()
        cd = ClusterDefinition("localhost", 3000).with_seed_discovery(
            provider, execution_mode="thread",
        )
        assert cd._get_policy().seed_candidate_provider is provider

    @pytest.mark.parametrize("provider, mode", [
        (RegistryProvider(), DiscoveryExecutionMode.TEND),
        (DnsSeedCandidateProvider(["seeds.example.com"]), DiscoveryExecutionMode.THREAD),
        (
            AwsEc2TagSeedCandidateProvider("us-west-2", {"tag:role": ["aerospike"]}),
            DiscoveryExecutionMode.THREAD,
        ),
    ])
    def test_a_mode_the_provider_does_not_run_in_is_refused(self, provider, mode):
        with pytest.raises(AerospikeError) as exc_info:
            ClusterDefinition("localhost", 3000).with_seed_discovery(
                provider, execution_mode=mode,
            )
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR

    def test_static_provider_takes_either_mode(self):
        provider = StaticSeedCandidateProvider([PncHost("10.0.0.1", 3000)])
        for mode in DiscoveryExecutionMode:
            ClusterDefinition("localhost", 3000).with_seed_discovery(
                provider, execution_mode=mode,
            )

    def test_non_provider_is_a_type_error(self):
        with pytest.raises(TypeError, match="seed candidate provider"):
            ClusterDefinition("localhost", 3000).with_seed_discovery(["10.0.0.1:3000"])

    def test_non_positive_duration_is_a_value_error(self):
        with pytest.raises(ValueError, match="refresh_interval"):
            ClusterDefinition("localhost", 3000).with_seed_discovery(
                RegistryProvider(), refresh_interval=timedelta(0),
            )

    def test_builder_overrides_system_settings_field_by_field(self):
        settings = SystemSettings(
            discovery=DiscoverySettings(
                merge_policy=SeedMergePolicy.REPLACE, refresh_interval=timedelta(seconds=9),
            ),
        )
        cd = (
            ClusterDefinition("localhost", 3000)
            .with_system_settings(settings)
            .with_seed_discovery(RegistryProvider(), merge_policy=SeedMergePolicy.MERGE)
        )
        policy = cd._get_policy()
        assert policy.discovery_merge_policy == SeedMergePolicy.MERGE
        assert policy.discovery_refresh_interval == 9000


class TestWithAddressTranslation:
    """The builder sets the translator and miss mode."""

    def test_translator_and_on_miss_reach_the_policy(self):
        translator = PortMapAddressTranslator({"10.0.0.1:3000": "lb.example.com:30001"})
        cd = ClusterDefinition("localhost", 3000).with_address_translation(
            translator, on_miss=TranslationMissMode.STRICT,
        )
        policy = cd._get_policy()
        assert policy.address_translator == translator
        assert policy.translation_on_miss == TranslationMissMode.STRICT

    def test_on_miss_alone_keeps_ip_map(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .ip_map({"10.0.0.1": "192.168.1.1"})
            .with_address_translation(on_miss=TranslationMissMode.STRICT)
        )
        policy = cd._get_policy()
        assert policy.address_translator is None
        assert policy.ip_map == {"10.0.0.1": "192.168.1.1"}
        assert policy.translation_on_miss == TranslationMissMode.STRICT

    def test_non_translator_is_a_type_error(self):
        with pytest.raises(TypeError, match="address translator"):
            ClusterDefinition("localhost", 3000).with_address_translation(
                {"10.0.0.1": "192.168.1.1"},
            )


_DNS_FILE = """
system:
  DEFAULT:
    discovery:
      provider: dns
      merge_policy: discovery_only
      execution_mode: tend
      refresh_interval_ms: 60000
      refresh_timeout_ms: 5000
      tend_refresh_deadline_ms: 80
      dns:
        names: [_aerospike._tcp.prod.example.com]
        record_type: SRV
        port: 3100
        tls_name: prod
        timeout_ms: 40
        servers: ["10.0.0.53:53"]
"""

_AWS_FILE = """
system:
  DEFAULT:
    discovery:
      provider: aws-tags
      aws:
        region: us-west-2
        filters:
          - {name: "tag:role", values: [aerospike]}
          - {name: "tag:env", values: [prod, staging]}
        address_field: public_dns
        port: 3100
        timeout_ms: 200
        init_timeout_ms: 5000
        endpoint: http://127.0.0.1:9999
"""


class TestDiscoveryBlock:
    """``system.<cluster>.discovery`` parses to the provider and settings."""

    def test_dns_block(self):
        policy = _policy(_DNS_FILE)
        assert policy.seed_candidate_provider == DnsSeedCandidateProvider(
            ["_aerospike._tcp.prod.example.com"], record_type=DnsRecordType.SRV,
            port=3100, tls_name="prod", timeout=40, servers=["10.0.0.53:53"],
        )
        assert policy.discovery_merge_policy == SeedMergePolicy.DISCOVERY_ONLY
        assert policy.discovery_refresh_interval == 60000
        assert policy.discovery_refresh_timeout == 5000
        assert policy.discovery_tend_refresh_deadline == 80

    def test_aws_block(self):
        provider = _policy(_AWS_FILE).seed_candidate_provider
        assert isinstance(provider, AwsEc2TagSeedCandidateProvider)
        assert provider.region == "us-west-2"
        assert provider.filters == {
            "tag:role": ["aerospike"], "tag:env": ["prod", "staging"],
        }
        assert provider.address_field == AwsAddressField.PUBLIC_DNS
        assert (provider.port, provider.timeout, provider.init_timeout) == (3100, 200, 5000)
        assert provider.endpoint == "http://127.0.0.1:9999"

    def test_a_reloaded_provider_compares_by_value(self):
        assert _profile(_DNS_FILE) == _profile(_DNS_FILE)

    def test_file_overrides_code_field_by_field(self):
        cd = ClusterDefinition("localhost", 3000).with_seed_discovery(
            RegistryProvider(), refresh_interval=timedelta(seconds=7),
        )
        policy = _policy(
            "system:\n  DEFAULT:\n    discovery:\n      merge_policy: replace\n", cd,
        )
        assert isinstance(policy.seed_candidate_provider, RegistryProvider)
        assert policy.discovery_merge_policy == SeedMergePolicy.REPLACE
        assert policy.discovery_refresh_interval == 7000

    def test_provider_static_clears_the_code_provider(self):
        cd = ClusterDefinition("localhost", 3000).with_seed_discovery(RegistryProvider())
        policy = _policy("system:\n  DEFAULT:\n    discovery:\n      provider: static\n", cd)
        assert policy.seed_candidate_provider is None

    @pytest.mark.parametrize("block, reason", [
        ("provider: consul", "must be one of static, dns, aws-tags"),
        ("provider: dns", "needs a DEFAULT.discovery.dns mapping"),
        ("provider: dns\n      dns: {record_type: a}", "needs names"),
        ("provider: dns\n      dns: {names: [x.example.com], record_type: mx}",
         "must be one of a, srv"),
        ("provider: dns\n      dns: {names: [x.example.com], port: 70000}",
         "between 1 and 65535"),
        ("provider: static\n      dns: {names: [x.example.com]}", "applies only to provider dns"),
        ("provider: aws-tags\n      aws: {region: us-west-2, filters: [{name: x}]}",
         "entry must be {name, values}"),
        ("provider: aws-tags\n      aws: {region: us-west-2, filters: "
         "[{name: instance-state-name, values: [stopped]}]}",
         "the provider adds instance-state-name=running"),
    ])
    def test_a_bad_provider_is_skipped_whole_and_the_rest_applies(self, block, reason, caplog):
        text = (
            "system:\n  DEFAULT:\n    discovery:\n"
            f"      merge_policy: replace\n      {block}\n"
        )
        with caplog.at_level(logging.WARNING):
            discovery = _profile(text).discovery
        assert discovery.provider is None
        assert discovery.merge_policy == SeedMergePolicy.REPLACE
        assert "skipping DEFAULT.discovery.provider" in caplog.text
        assert reason in caplog.text

    def test_a_bad_scalar_is_skipped_alone(self, caplog):
        text = (
            "system:\n  DEFAULT:\n    discovery:\n      provider: static\n"
            "      refresh_interval_ms: 0\n      merge_policy: sometimes\n"
        )
        with caplog.at_level(logging.WARNING):
            discovery = _profile(text).discovery
        assert discovery == DiscoverySettings(provider=FileSeedProvider("static"))
        assert "refresh_interval_ms must be an integer >= 1" in caplog.text
        assert "merge_policy must be one of merge, replace, discovery_only" in caplog.text

    def test_unknown_keys_are_drops(self):
        text = (
            "system:\n  DEFAULT:\n    discovery:\n      refresh_ms: 10\n"
            "      provider: dns\n      dns: {names: [x.example.com], ttl: 5}\n"
        )
        with _collect_drops() as dropped:
            discovery = _profile(text).discovery
        assert sorted(dropped) == [
            "key DEFAULT.discovery.dns.ttl", "key DEFAULT.discovery.refresh_ms",
        ]
        assert discovery.provider == FileSeedProvider("dns", {"names": ["x.example.com"]})

    def test_a_mode_the_file_provider_does_not_run_in_is_skipped(self, caplog):
        text = "system:\n  DEFAULT:\n    discovery:\n      execution_mode: thread\n" + (
            "      provider: dns\n      dns: {names: [x.example.com]}\n"
        )
        with caplog.at_level(logging.WARNING):
            discovery = _profile(text).discovery
        assert discovery.execution_mode is None
        assert discovery.provider is not None
        assert "skipping DEFAULT.discovery.execution_mode" in caplog.text

    def test_a_file_mode_the_code_provider_does_not_run_in_is_skipped(self, caplog):
        cd = ClusterDefinition("localhost", 3000).with_seed_discovery(RegistryProvider())
        with caplog.at_level(logging.WARNING):
            policy = _policy(
                "system:\n  DEFAULT:\n    discovery:\n      execution_mode: tend\n", cd,
            )
        assert isinstance(policy.seed_candidate_provider, RegistryProvider)
        assert "skipping discovery.execution_mode tend" in caplog.text


class TestTranslationBlock:
    """``system.<cluster>.translation`` parses to the translator and miss mode."""

    def test_static_map(self):
        policy = _policy(
            "system:\n  DEFAULT:\n    translation:\n      on_miss: strict\n"
            "      static_map: {10.0.0.1: 203.0.113.1}\n"
        )
        assert policy.address_translator == StaticMapAddressTranslator(
            {"10.0.0.1": "203.0.113.1"},
        )
        assert policy.translation_on_miss == TranslationMissMode.STRICT

    def test_empty_static_map_is_a_translator_over_no_entries(self):
        cd = ClusterDefinition("localhost", 3000).ip_map({"10.0.0.1": "192.168.1.1"})
        policy = _policy("system:\n  DEFAULT:\n    translation:\n      static_map: {}\n", cd)
        assert policy.address_translator == StaticMapAddressTranslator({})

    def test_port_map(self):
        policy = _policy(
            "system:\n  DEFAULT:\n    translation:\n      provider: port-map\n"
            "      port_map: {\"10.0.0.1:3000\": \"lb.example.com:30001\"}\n"
        )
        assert policy.address_translator == PortMapAddressTranslator(
            {"10.0.0.1:3000": "lb.example.com:30001"},
        )

    def test_pass_through(self):
        policy = _policy("system:\n  DEFAULT:\n    translation:\n      provider: pass-through\n")
        assert isinstance(policy.address_translator, PassThroughAddressTranslator)

    def test_on_miss_alone_keeps_the_code_translator(self):
        translator = PassThroughAddressTranslator()
        cd = ClusterDefinition("localhost", 3000).with_address_translation(translator)
        policy = _policy("system:\n  DEFAULT:\n    translation:\n      on_miss: strict\n", cd)
        assert policy.address_translator == translator
        assert policy.translation_on_miss == TranslationMissMode.STRICT

    def test_static_map_without_a_map_restores_ip_map(self):
        cd = (
            ClusterDefinition("localhost", 3000)
            .ip_map({"10.0.0.1": "192.168.1.1"})
            .with_address_translation(PassThroughAddressTranslator())
        )
        policy = _policy("system:\n  DEFAULT:\n    translation:\n      provider: static-map\n", cd)
        assert policy.address_translator is None
        assert policy.ip_map == {"10.0.0.1": "192.168.1.1"}

    @pytest.mark.parametrize("block, reason", [
        ("provider: port-map", "needs DEFAULT.translation.port_map"),
        ("port_map: {\"10.0.0.1:3000\": \"lb:1\"}", "applies only to provider port-map"),
        ("provider: pass-through\n      static_map: {a: b}", "applies only to provider static-map"),
        ("provider: port-map\n      port_map: {\"10.0.0.1:3000\": lb}", "lb is not host:port"),
        ("provider: identity", "must be one of static-map, port-map, pass-through"),
    ])
    def test_a_bad_translator_is_skipped_whole_and_on_miss_applies(self, block, reason, caplog):
        text = (
            "system:\n  DEFAULT:\n    translation:\n"
            f"      on_miss: strict\n      {block}\n"
        )
        with caplog.at_level(logging.WARNING):
            translation = _profile(text).translation
        assert translation == TranslationSettings(on_miss=TranslationMissMode.STRICT)
        assert reason in caplog.text

    def test_file_translator_is_kept_as_data(self):
        translation = _profile(
            "system:\n  DEFAULT:\n    translation:\n      static_map: {a: b}\n"
        ).translation
        assert translation.translator == FileAddressTranslator("static-map", {"a": "b"})


class TestReload:
    """A reload reports discovery and translation changes rather than applying them."""

    def test_discovery_change_warns_and_keeps_the_connect_value(self, tmp_path, caplog):
        path = tmp_path / "sdk.yaml"
        path.write_text(_DNS_FILE)
        current = load_and_resolve(str(path), None, None)
        applied = []
        poller = _SdkConfigPoller(SdkConfigSource(str(path), None, None), current, applied.append)
        path.write_text(
            _DNS_FILE.replace("merge_policy: discovery_only", "merge_policy: merge")
            + "    transactions:\n      number_of_attempts: 3\n"
        )
        stat = os.stat(path)
        os.utime(path, (stat.st_atime, stat.st_mtime + 2))
        with caplog.at_level(logging.WARNING):
            poller.poll_once()
        assert "discovery changed" in caplog.text
        assert "takes effect when the client reconnects" in caplog.text
        assert applied[0].discovery == current.discovery
        assert applied[0].transactions.number_of_attempts == 3
