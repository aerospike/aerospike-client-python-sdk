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

"""Tests for ClusterDefinition and Cluster."""

import pytest

from aerospike_sdk import Behavior, ClusterDefinition, DataSet, Host
from tests.integration.general_auth import apply_general_auth


@pytest.fixture
async def cluster(aerospike_host):
    """Setup cluster for testing."""
    # Parse host:port from aerospike_host
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = apply_general_auth(ClusterDefinition(hostname, port))
    cluster = await cluster_def.connect()
    yield cluster
    await cluster.close()


async def test_cluster_definition_basic_connection(cluster):
    """Test basic ClusterDefinition connection."""
    assert cluster.is_connected()

    # Create a session
    session = cluster.create_session(Behavior.DEFAULT)
    assert session is not None
    assert session.behavior.name == "DEFAULT"


async def test_cluster_definition_with_hosts(aerospike_host):
    """Test ClusterDefinition with Host objects."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    hosts = [Host(hostname, port)]
    cluster_def = apply_general_auth(ClusterDefinition(hosts=hosts))
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
        session = cluster.create_session()
        assert session is not None
    finally:
        await cluster.close()


async def test_cluster_definition_with_credentials(aerospike_host):
    """Test ClusterDefinition with credentials (if auth is enabled)."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    # Test with empty credentials (should work if no auth). The general-auth
    # wrap goes outside the chain: on an auth-required leg the env credentials
    # must land last, or the empty pair would overwrite them.
    cluster_def = apply_general_auth(
        ClusterDefinition(hostname, port).with_native_credentials("", ""))
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()


async def test_cluster_definition_services_alternate(aerospike_host):
    """Test ClusterDefinition with services alternate."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = apply_general_auth(ClusterDefinition(hostname, port)).using_services_alternate()
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()


async def test_cluster_definition_with_ip_map(aerospike_host):
    """Test ClusterDefinition with an IP translation map."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    # The mapping targets an address the local cluster never advertises, so a
    # connection still succeeds — translation only rewrites addresses it matches.
    cluster_def = (
        apply_general_auth(ClusterDefinition(hostname, port))
        .using_services_alternate()
        .with_ip_map({"10.0.0.1": "3.72.54.187"})
    )
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()


async def test_cluster_definition_preferring_racks(aerospike_host, enterprise):
    """Test ClusterDefinition with preferred racks."""
    if not enterprise:
        pytest.skip("Rack awareness requires Enterprise Edition")

    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = apply_general_auth(ClusterDefinition(hostname, port)).preferring_racks(1, 2)
    cluster = await cluster_def.connect()

    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()


async def test_cluster_definition_context_manager(aerospike_host):
    """Test ClusterDefinition with async context manager."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cluster_def = apply_general_auth(ClusterDefinition(hostname, port))
    async with await cluster_def.connect() as cluster:
        assert cluster.is_connected()
        session = cluster.create_session()
        assert session is not None


async def test_cluster_create_session(cluster):
    """Test creating sessions from cluster."""
    # Create session with default behavior
    session1 = cluster.create_session()
    assert session1 is not None

    # Create session with explicit behavior
    session2 = cluster.create_session(Behavior.DEFAULT)
    assert session2 is not None

    # Create session with custom behavior
    custom_behavior = Behavior.DEFAULT.derive_with_changes(name="test", max_retries=3)
    session3 = cluster.create_session(custom_behavior)
    assert session3 is not None
    assert session3.behavior.name == "test"


async def test_cluster_transaction(cluster):
    """Test creating transactional session from cluster."""
    tx_session = cluster.transaction()
    assert tx_session is not None


async def test_host_parse_hosts():
    """Test Host.parse_hosts() method."""
    hosts = Host.parse_hosts("host1:3000,host2:3001", 3000)
    assert len(hosts) == 2
    assert hosts[0].name == "host1"
    assert hosts[0].port == 3000
    assert hosts[1].name == "host2"
    assert hosts[1].port == 3001

    # Test with default port
    hosts2 = Host.parse_hosts("host1,host2", 3000)
    assert len(hosts2) == 2
    assert hosts2[0].port == 3000
    assert hosts2[1].port == 3000


async def test_host_of():
    """Test Host.of() static method."""
    host = Host.of("localhost", 3000)
    assert host.name == "localhost"
    assert host.port == 3000


async def test_fail_if_not_connected_default_bad_host():
    """Default fail_if_not_connected=True raises on unreachable host."""
    cd = apply_general_auth(ClusterDefinition("127.0.0.1", 19999))
    with pytest.raises(Exception):
        await cd.connect()


async def test_fail_if_not_connected_explicit_true(aerospike_host):
    """Explicit fail_if_not_connected(True) still connects to a live cluster."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cd = apply_general_auth(ClusterDefinition(hostname, port)).fail_if_not_connected(True)
    cluster = await cd.connect()
    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()



class TestRestrictingClusterToSeeds:
    """Pinning the cluster view to the seeds, against a live cluster."""

    async def test_seeds_only_view_excludes_discovered_peers(
        self, aerospike_host_sc, make_cluster_definition
    ):
        """Discovery normally finds the peers; restricting keeps only the seed.

        Needs more than one node to mean anything: on a single-node cluster the
        seed *is* the whole cluster, so both modes look identical and the test
        would pass without the setting doing anything.
        """
        discovered = await make_cluster_definition(aerospike_host_sc, auth=True).connect()
        try:
            info = discovered.create_session().info()
            namespaces = await info.namespaces()
            namespace = next(iter(namespaces))
            peer_count = len(await info.namespace_details_per_node(namespace))
        finally:
            await discovered.close()

        if peer_count < 2:
            pytest.skip(
                f"seed-only is only observable on a multi-node cluster; "
                f"{aerospike_host_sc} reports {peer_count}"
            )

        restricted = await (
            make_cluster_definition(aerospike_host_sc, auth=True)
            .restricting_cluster_to_seeds()
            .connect()
        )
        try:
            seen = len(
                await restricted.create_session()
                .info()
                .namespace_details_per_node(namespace)
            )
        finally:
            await restricted.close()

        assert seen == 1, f"expected only the seed, saw {seen} of {peer_count} nodes"

    async def test_restricted_cluster_still_serves_reads_and_writes(
        self, aerospike_host_sc, make_cluster_definition
    ):
        """A pinned view is still a working client, not just a narrower one."""
        cluster = await (
            make_cluster_definition(aerospike_host_sc, auth=True)
            .restricting_cluster_to_seeds()
            .connect()
        )
        try:
            session = cluster.create_session()
            namespace = next(iter(await session.info().namespaces()))
            key = DataSet.of(namespace, "seed_only").id("k1")
            await session.upsert(key).put({"v": 1}).execute()
            result = await session.query(key).first_or_raise()
            assert result.record_or_raise().bins["v"] == 1
            await session.delete(key).execute()
        finally:
            await cluster.close()
