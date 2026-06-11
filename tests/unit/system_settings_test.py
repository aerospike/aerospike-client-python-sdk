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

"""Tests for SystemSettings: apply_to, ClusterDefinition integration."""

from datetime import timedelta

import pytest
from aerospike_async import ClientPolicy

from aerospike_sdk.aio.cluster_definition import ClusterDefinition as AsyncClusterDefinition
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sync.cluster_definition import ClusterDefinition as SyncClusterDefinition


class TestSystemSettingsApplyTo:
    """Verify apply_to() maps each SystemSettings field to the correct
    ClientPolicy attribute, converts timedeltas to milliseconds, and
    leaves fields untouched when they are None."""
    def test_max_connections(self):
        ss = SystemSettings(max_connections_per_node=200)
        p = ClientPolicy()
        ss.apply_to(p)
        assert p.max_conns_per_node == 200

    def test_conn_pools_per_node(self):
        ss = SystemSettings(conn_pools_per_node=4)
        p = ClientPolicy()
        ss.apply_to(p)
        assert p.conn_pools_per_node == 4

    def test_idle_timeout(self):
        ss = SystemSettings(max_socket_idle_time=timedelta(seconds=30))
        p = ClientPolicy()
        ss.apply_to(p)
        assert p.idle_timeout == 30_000

    def test_tend_interval(self):
        ss = SystemSettings(tend_interval=timedelta(seconds=2))
        p = ClientPolicy()
        ss.apply_to(p)
        assert p.tend_interval == 2_000

    def test_none_fields_not_applied(self):
        ss = SystemSettings()
        p = ClientPolicy()
        original_max = p.max_conns_per_node
        ss.apply_to(p)
        assert p.max_conns_per_node == original_max

    def test_multiple_fields(self):
        ss = SystemSettings(
            max_connections_per_node=500,
            conn_pools_per_node=8,
            max_socket_idle_time=timedelta(seconds=45),
            tend_interval=timedelta(milliseconds=500),
        )
        p = ClientPolicy()
        ss.apply_to(p)
        assert p.max_conns_per_node == 500
        assert p.conn_pools_per_node == 8
        assert p.idle_timeout == 45_000
        assert p.tend_interval == 500


class TestSystemSettingsImmutability:
    """Verify the frozen dataclass rejects field mutation after creation."""
    def test_frozen(self):
        ss = SystemSettings(max_connections_per_node=100)
        # `setattr` instead of direct `ss.max_connections_per_node = ...` to
        # bypass static analyzers (PyCharm `PyDataclass`, mypy `misc`) that
        # flag the intentional frozen-dataclass mutation. Runtime behavior is
        # identical: any attribute assignment raises `FrozenInstanceError`
        # (an `AttributeError` subclass).
        with pytest.raises(AttributeError):
            setattr(ss, "max_connections_per_node", 200)


class TestClusterDefinitionWithSystemSettings:
    """Verify SystemSettings integrates with both async and sync
    ClusterDefinition builders, correctly populating the ClientPolicy
    produced by _get_policy() and supporting method chaining."""
    def test_async_cluster_definition_applies_settings(self):
        cd = AsyncClusterDefinition("localhost", 3000)
        ss = SystemSettings(
            max_connections_per_node=300,
            tend_interval=timedelta(seconds=3),
        )
        cd.with_system_settings(ss)
        policy = cd._get_policy()
        assert policy.max_conns_per_node == 300
        assert policy.tend_interval == 3_000

    def test_sync_cluster_definition_applies_settings(self):
        cd = SyncClusterDefinition("localhost", 3000)
        ss = SystemSettings(
            max_connections_per_node=300,
            tend_interval=timedelta(seconds=3),
        )
        cd.with_system_settings(ss)
        policy = cd._get_policy()
        assert policy.max_conns_per_node == 300
        assert policy.tend_interval == 3_000

    def test_chaining(self):
        cd = AsyncClusterDefinition("localhost", 3000) \
            .with_system_settings(SystemSettings(max_connections_per_node=400)) \
            .validate_cluster_name_is("test")
        policy = cd._get_policy()
        assert policy.max_conns_per_node == 400
        assert policy.cluster_name == "test"
