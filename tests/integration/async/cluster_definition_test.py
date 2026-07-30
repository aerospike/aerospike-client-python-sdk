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

from aerospike_sdk import ClusterDefinition, Host, Behavior


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

    cluster_def = ClusterDefinition(hostname, port)
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
    cluster_def = ClusterDefinition(hosts=hosts)
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

    # Test with empty credentials (should work if no auth)
    cluster_def = ClusterDefinition(hostname, port).with_native_credentials("", "")
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

    cluster_def = ClusterDefinition(hostname, port).using_services_alternate()
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

    cluster_def = ClusterDefinition(hostname, port).preferring_racks(1, 2)
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

    cluster_def = ClusterDefinition(hostname, port)
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
    cd = ClusterDefinition("127.0.0.1", 19999)
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

    cd = ClusterDefinition(hostname, port).fail_if_not_connected(True)
    cluster = await cd.connect()
    try:
        assert cluster.is_connected()
    finally:
        await cluster.close()


async def test_with_index_refresh_interval_threads_through(aerospike_host):
    """``with_index_refresh_interval`` reaches the connected cluster's monitor."""
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname = aerospike_host
        port = 3000

    cd = ClusterDefinition(hostname, port).with_index_refresh_interval(2.5)
    assert cd._index_refresh_interval == 2.5
    cluster = await cd.connect()
    try:
        assert cluster._sdk_client._indexes_monitor._refresh_interval == 2.5
    finally:
        await cluster.close()


