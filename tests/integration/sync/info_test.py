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

"""Tests for SyncInfoCommands."""

import pytest
from aerospike_sdk import Behavior


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    """Setup sync SDK cluster for testing."""
    with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
        yield cluster


@pytest.fixture
def session(cluster):
    """Setup session with default behavior for testing."""
    return cluster.create_session(Behavior.DEFAULT)


def test_info_creation(session):
    """Test creating a SyncInfoCommands instance."""
    info = session.info()
    assert info is not None
    # SyncInfoCommands holds a PAC client directly.
    assert info._pac is not None


def test_namespace_sc_status_and_is_namespace_sc(session):
    """Exercise the sync ``namespace_sc_status`` / ``is_namespace_sc`` info path.

    Gates the Phase-3 hoist of these methods onto a shared session base: the
    async tree already exercises this path in integration, the sync tree had no
    coverage, so a one-sided hoist could silently break the blocking info
    dispatch. Namespace-agnostic (no SC cluster required).
    """
    namespaces = session.info().namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")
    ns = next(iter(namespaces))

    status = session.namespace_sc_status(ns)
    is_sc = session.is_namespace_sc(ns)
    assert isinstance(is_sc, bool)
    assert is_sc == status.is_sc

    # A namespace that does not exist is reported as non-SC, with a detail.
    missing = session.namespace_sc_status("nonexistent_namespace_xyz")
    assert missing.is_sc is False
    assert missing.detail
    assert session.is_namespace_sc("nonexistent_namespace_xyz") is False


def test_build(session):
    """Test getting build information."""
    info = session.info()
    build_info = info.build()

    assert isinstance(build_info, set)
    assert len(build_info) > 0, "Should have at least one build string"
    assert all(isinstance(b, str) for b in build_info), "All build strings should be strings"


def test_namespaces(session):
    """Test getting list of namespaces."""
    info = session.info()
    namespaces = info.namespaces()

    assert isinstance(namespaces, set)
    assert len(namespaces) > 0, "Should have at least one namespace"
    assert all(isinstance(ns, str) for ns in namespaces), "All namespaces should be strings"


def test_namespace_details(session):
    """Test getting namespace details."""
    info = session.info()

    # First get the list of namespaces
    namespaces = info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    # Test getting details for the first namespace
    test_namespace = list(namespaces)[0]
    details = info.namespace_details(test_namespace)

    assert details is not None
    assert isinstance(details, dict), "stays a mapping, so raw-key access keeps working"
    # The body is parsed into its fields -- not returned as one blob under the
    # command key, which is what this call used to answer.
    assert f"namespace/{test_namespace}" not in details
    assert "nsup-period" in details
    assert len(details) > 50, "a namespace reports hundreds of keys"
    # Typed reads coerce; raw reads still resolve.
    assert isinstance(details.strong_consistency, bool)
    assert isinstance(details.nsup_period, int)
    assert details["replication-factor"] == details.get("replication-factor")


def test_namespace_details_nonexistent(session):
    """Test getting details for a non-existent namespace."""
    info = session.info()
    details = info.namespace_details("nonexistent_namespace_xyz")

    assert details is None


def test_sets(session):
    """Test getting list of sets in a namespace."""
    info = session.info()

    # First get the list of namespaces
    namespaces = info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    # Test getting sets for the first namespace
    test_namespace = list(namespaces)[0]
    sets = info.sets(test_namespace)

    assert isinstance(sets, list)
    assert all(isinstance(s, str) for s in sets), "All sets should be strings"


def test_secondary_indexes(session):
    """Test getting list of secondary indexes."""
    info = session.info()
    indexes = info.secondary_indexes()

    assert isinstance(indexes, list)
    # Each index should be a dictionary with at least namespace, set, bin, name
    for idx in indexes:
        assert isinstance(idx, dict)
        assert "namespace" in idx
        assert "set" in idx
        assert "bin" in idx
        assert "name" in idx


def test_secondary_indexes_with_namespace_filter(session):
    """Test getting secondary indexes filtered by namespace."""
    info = session.info()

    # First get the list of namespaces
    namespaces = info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    # Test filtering by the first namespace
    test_namespace = list(namespaces)[0]
    indexes = info.secondary_indexes(namespace=test_namespace)

    assert isinstance(indexes, list)
    # All indexes should be from the specified namespace
    for idx in indexes:
        assert idx["namespace"] == test_namespace


def test_secondary_index_details(session):
    """Test getting details for a specific secondary index."""
    info = session.info()

    # First get the list of indexes
    indexes = info.secondary_indexes()
    if not indexes:
        pytest.skip("No secondary indexes found to test")

    # Test getting details for the first index
    test_index = indexes[0]
    details = info.secondary_index_details(test_index["namespace"], test_index["name"])

    # Details might be None if the index doesn't support detailed info
    if details is not None:
        assert isinstance(details, dict)


def test_secondary_index_details_nonexistent(session):
    """Test getting details for a non-existent secondary index."""
    info = session.info()

    # First get the list of namespaces
    namespaces = info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    test_namespace = list(namespaces)[0]
    details = info.secondary_index_details(test_namespace, "nonexistent_index_xyz")

    assert details is None


def test_get_cluster_size(session):
    """Test getting cluster size."""
    info = session.info()
    cluster_size = info.get_cluster_size()

    assert isinstance(cluster_size, int)
    assert cluster_size > 0, "Should have at least one node"


def test_is_cluster_stable(session):
    """Test checking if cluster is stable."""
    info = session.info()
    is_stable = info.is_cluster_stable()

    assert isinstance(is_stable, bool)


def test_info_build(session):
    """Test executing raw info command for build information (InfoCommands style)."""
    info = session.info()
    response = info.info("build")

    assert isinstance(response, dict)
    assert len(response) > 0, "Build info should contain data"


def test_info_statistics(session):
    """Test executing raw info command for statistics (InfoCommands style)."""
    info = session.info()
    response = info.info("statistics")

    assert isinstance(response, dict)
    assert len(response) > 0, "Statistics should contain data"


def test_info_direct_build(session):
    """Test session.info(command) style for build (new style, no .info().info())."""
    response = session.info("build")

    assert isinstance(response, dict)
    assert len(response) > 0, "Build info should contain data"


def test_info_direct_statistics(session):
    """Test session.info(command) style for statistics (new style)."""
    response = session.info("statistics")

    assert isinstance(response, dict)
    assert len(response) > 0, "Statistics should contain data"


def test_info_direct_sindex_list(session):
    """Test session.info(command) style for sindex-list (new style)."""
    response = session.info("sindex-list")

    assert isinstance(response, dict)
    assert len(response) > 0, "sindex-list should return data from at least one node"


def test_info_both_styles_equivalent(session):
    """Test that session.info(cmd) and session.info().info(cmd) return equivalent results."""
    direct = session.info("build")
    via_commands = session.info().info("build")

    assert isinstance(direct, dict)
    assert isinstance(via_commands, dict)
    assert len(direct) == len(via_commands), "Both styles should return same number of node entries"
    assert set(direct.keys()) == set(via_commands.keys()), "Same node keys"
    for key in direct:
        assert direct[key] == via_commands[key], f"Same content for node {key}"


def test_info_on_all_nodes_build(session):
    """Test executing info command on all nodes for build information."""
    info = session.info()
    response = info.info_on_all_nodes("build")

    assert isinstance(response, dict)
    assert len(response) > 0, "Should have responses from at least one node"

    # Each value should be a dict (the info response from that node)
    for node_name, node_response in response.items():
        assert isinstance(node_name, str), "Node names should be strings"
        assert isinstance(node_response, dict), "Node responses should be dictionaries"
        assert len(node_response) > 0, "Node response should contain data"


def test_info_on_all_nodes_statistics(session):
    """Test executing info command on all nodes for statistics."""
    info = session.info()
    response = info.info_on_all_nodes("statistics")

    assert isinstance(response, dict)
    assert len(response) > 0, "Should have responses from at least one node"

    # Statistics should have many keys from each node
    for node_name, node_response in response.items():
        assert isinstance(node_response, dict), "Node responses should be dictionaries"
        assert len(node_response) > 0, "Statistics should contain data"

