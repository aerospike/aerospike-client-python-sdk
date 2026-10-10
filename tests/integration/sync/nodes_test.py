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

"""Sync cluster membership and per-node access.

The sync node dispatches through PNC's ``*_blocking`` node entries rather than
the awaitables the async suite covers.
"""

import pytest

from aerospike_sdk import InvalidNodeError
from aerospike_sdk.sync import Node


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


class TestSyncClusterNodes:

    def test_nodes_match_node_names(self, cluster):
        nodes = cluster.nodes()
        assert nodes and all(isinstance(n, Node) and n.is_active for n in nodes)
        assert sorted(n.name for n in nodes) == sorted(cluster.node_names())

    def test_get_node_info_runs_on_that_node(self, cluster):
        for name in cluster.node_names():
            assert cluster.get_node(name).info("node")["node"] == name

    def test_get_node_unknown_name_raises_sdk_error(self, cluster):
        with pytest.raises(InvalidNodeError):
            cluster.get_node("no-such-node")

    def test_aliases_include_connected_host(self, cluster):
        node = cluster.nodes()[0]
        assert node.host in node.aliases()

    def test_cluster_name_matches_server_config(self, cluster):
        configured = cluster.nodes()[0].info("cluster-name")["cluster-name"]
        assert cluster.cluster_name == (None if configured == "null" else configured)
