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

"""Cluster membership and per-node access on a connected cluster."""

import pytest
import pytest_asyncio

from aerospike_sdk import InvalidNodeError, Node

pytestmark = pytest.mark.asyncio


def _version_tuple(v):
    return (v.major, v.minor, v.patch, v.build)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    async with make_cluster_definition(aerospike_host).connect() as c:
        yield c


class TestClusterNodes:

    async def test_nodes_match_node_names(self, cluster):
        nodes = cluster.nodes()
        assert nodes and all(isinstance(n, Node) and n.is_active for n in nodes)
        assert sorted(n.name for n in nodes) == sorted(cluster.node_names())

    async def test_server_version_is_the_node_minimum(self, cluster):
        versions = [_version_tuple(n.version) for n in cluster.nodes()]
        assert _version_tuple(cluster.server_version()) == min(versions)

    async def test_get_node_info_runs_on_that_node(self, cluster):
        for name in cluster.node_names():
            node = cluster.get_node(name)
            assert (await node.info("node"))["node"] == name

    async def test_get_node_unknown_name_raises_sdk_error(self, cluster):
        with pytest.raises(InvalidNodeError):
            cluster.get_node("no-such-node")

    async def test_aliases_include_connected_host(self, cluster):
        node = cluster.nodes()[0]
        assert node.host in node.aliases()

    async def test_cluster_name_matches_server_config(self, cluster):
        node = cluster.nodes()[0]
        configured = (await node.info("cluster-name"))["cluster-name"]
        assert cluster.cluster_name == (None if configured == "null" else configured)
