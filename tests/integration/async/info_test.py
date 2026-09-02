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

"""Tests for InfoCommands."""

import pytest
from aerospike_sdk import Behavior


@pytest.fixture
async def session(cluster):
    """Setup session with default behavior for testing."""
    return cluster.create_session(Behavior.DEFAULT)


async def test_info_creation(session):
    """Test creating an InfoCommands instance."""
    info = session.info()
    assert info is not None
    assert info._session is session


async def test_build(session):
    """Test getting build information."""
    info = session.info()
    build_info = await info.build()

    assert isinstance(build_info, set)
    assert len(build_info) > 0, "Should have at least one build string"
    assert all(isinstance(b, str) for b in build_info), "All build strings should be strings"


async def test_namespaces(session):
    """Test getting list of namespaces."""
    info = session.info()
    namespaces = await info.namespaces()

    assert isinstance(namespaces, set)
    assert len(namespaces) > 0, "Should have at least one namespace"
    assert all(isinstance(ns, str) for ns in namespaces), "All namespaces should be strings"


async def test_namespace_details(session):
    """Test getting namespace details."""
    info = session.info()

    # First get the list of namespaces
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    # Test getting details for the first namespace
    test_namespace = list(namespaces)[0]
    details = await info.namespace_details(test_namespace)

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


async def test_namespace_details_nonexistent(session):
    """Test getting details for a non-existent namespace."""
    info = session.info()
    details = await info.namespace_details("nonexistent_namespace_xyz")

    assert details is None


async def test_sets(session):
    """Test getting list of sets in a namespace."""
    info = session.info()

    # First get the list of namespaces
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    # Test getting sets for the first namespace
    test_namespace = list(namespaces)[0]
    sets = await info.sets(test_namespace)

    from aerospike_sdk import SetDetail

    assert isinstance(sets, list)
    assert all(isinstance(s, SetDetail) for s in sets)
    # A shape-only check passes when the whole raw body arrives as one element,
    # which is what this returned before: names carry no info-protocol
    # delimiters, so their presence means the body was never split.
    # Names carry no info-protocol delimiters; their presence would mean the
    # body was returned unsplit, which a shape-only check cannot tell apart.
    names = [d.name for d in sets]
    for name in names:
        assert ";" not in name and ":" not in name and "=" not in name, (
            f"set name still contains raw info-protocol delimiters: {name[:60]!r}"
        )
    assert names == sorted(names), "sets should be ordered by name"


async def test_sets_returns_detail(session):
    """Per-set detail, typed."""
    from aerospike_sdk import SetDetail

    info = session.info()
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")
    namespace = list(namespaces)[0]

    details = await info.sets(namespace)
    if not details:
        pytest.skip(f"No sets in namespace {namespace!r}")

    assert all(isinstance(d, SetDetail) for d in details)
    first = details[0]
    assert first.name and ":" not in first.name
    assert first.namespace == namespace
    # Coerced on access rather than left as wire strings.
    assert isinstance(first.objects, int)
    assert isinstance(first.truncating, bool)
    # Still a mapping, so any key the server reports stays reachable.
    assert first["set"] == first.name
    assert [d.name for d in details] == sorted(d.name for d in details)


async def test_secondary_indexes(session):
    """Test getting list of secondary indexes."""
    info = session.info()
    indexes = await info.secondary_indexes()

    assert isinstance(indexes, list)
    # Each index should be a dictionary with at least namespace, set, bin, name
    for idx in indexes:
        assert isinstance(idx, dict)
        assert "namespace" in idx
        assert "set" in idx
        assert "bin" in idx
        assert "name" in idx


async def test_secondary_indexes_with_namespace_filter(session):
    """Test getting secondary indexes filtered by namespace."""
    info = session.info()

    # First get the list of namespaces
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    # Test filtering by the first namespace
    test_namespace = list(namespaces)[0]
    indexes = await info.secondary_indexes(namespace=test_namespace)

    assert isinstance(indexes, list)
    # All indexes should be from the specified namespace
    for idx in indexes:
        assert idx["namespace"] == test_namespace


async def test_secondary_index_details(session):
    """Test getting details for a specific secondary index."""
    info = session.info()

    # First get the list of indexes
    indexes = await info.secondary_indexes()
    if not indexes:
        pytest.skip("No secondary indexes found to test")

    # Test getting details for the first index
    test_index = indexes[0]
    details = await info.secondary_index_details(test_index["namespace"], test_index["name"])

    # Details might be None if the index doesn't support detailed info
    if details is not None:
        from aerospike_sdk import SindexDetail

        assert isinstance(details, SindexDetail)
        # Parsed counters, not the raw {command: body} envelope -- the envelope
        # also satisfies isinstance(dict), which is how that shape survived.
        assert f"sindex/{test_index['namespace']}" not in details
        assert isinstance(details.entries, int)
        assert isinstance(details.used_bytes, int)
        assert 0 <= details.load_pct <= 100


async def test_secondary_index_details_nonexistent(session):
    """Test getting details for a non-existent secondary index."""
    info = session.info()

    # First get the list of namespaces
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    test_namespace = list(namespaces)[0]
    details = await info.secondary_index_details(test_namespace, "nonexistent_index_xyz")

    assert details is None


async def test_get_cluster_size(session):
    """Test getting cluster size."""
    info = session.info()
    cluster_size = await info.get_cluster_size()

    assert isinstance(cluster_size, int)
    assert cluster_size > 0, "Should have at least one node"


async def test_is_cluster_stable(session):
    """Test checking if cluster is stable."""
    info = session.info()
    is_stable = await info.is_cluster_stable()

    assert isinstance(is_stable, bool)


async def test_info_build(session):
    """Test executing raw info command for build information (InfoCommands style)."""
    info = session.info()
    response = await info.info("build")

    assert isinstance(response, dict)
    assert len(response) > 0, "Build info should contain data"


async def test_info_statistics(session):
    """Test executing raw info command for statistics (InfoCommands style)."""
    info = session.info()
    response = await info.info("statistics")

    assert isinstance(response, dict)
    assert len(response) > 0, "Statistics should contain data"


async def test_info_direct_build(session):
    """Test session.info(command) style for build (new style, no .info().info())."""
    response = await session.info("build")

    assert isinstance(response, dict)
    assert len(response) > 0, "Build info should contain data"


async def test_info_direct_statistics(session):
    """Test session.info(command) style for statistics (new style)."""
    response = await session.info("statistics")

    assert isinstance(response, dict)
    assert len(response) > 0, "Statistics should contain data"


async def test_info_direct_sindex_list(session):
    """Test session.info(command) style for sindex-list (new style)."""
    response = await session.info("sindex-list")

    assert isinstance(response, dict)
    assert len(response) > 0, "sindex-list should return data from at least one node"


async def test_info_both_styles_equivalent(session):
    """Test that session.info(cmd) and session.info().info(cmd) return equivalent results."""
    direct = await session.info("build")
    via_commands = await session.info().info("build")

    assert isinstance(direct, dict)
    assert isinstance(via_commands, dict)
    assert len(direct) == len(via_commands), "Both styles should return same number of node entries"
    assert set(direct.keys()) == set(via_commands.keys()), "Same node keys"
    for key in direct:
        assert direct[key] == via_commands[key], f"Same content for node {key}"


async def test_info_on_all_nodes_build(session):
    """Test executing info command on all nodes for build information."""
    info = session.info()
    response = await info.info_on_all_nodes("build")

    assert isinstance(response, dict)
    assert len(response) > 0, "Should have responses from at least one node"

    # Each value should be a dict (the info response from that node)
    for node_name, node_response in response.items():
        assert isinstance(node_name, str), "Node names should be strings"
        assert isinstance(node_response, dict), "Node responses should be dictionaries"
        assert len(node_response) > 0, "Node response should contain data"


async def test_info_on_all_nodes_statistics(session):
    """Test executing info command on all nodes for statistics."""
    info = session.info()
    response = await info.info_on_all_nodes("statistics")

    assert isinstance(response, dict)
    assert len(response) > 0, "Should have responses from at least one node"

    # Statistics should have many keys from each node
    for node_name, node_response in response.items():
        assert isinstance(node_response, dict), "Node responses should be dictionaries"
        assert len(node_response) > 0, "Statistics should contain data"



async def test_per_node_views_agree_with_the_merged_ones(session):
    """Per-node variants answer per node; the merged views collapse that.

    On a single-node cluster the two agree, which is the useful invariant to
    assert here -- the divergence they exist to expose only appears with more
    than one node, and this suite does not require one.
    """
    from aerospike_sdk import NamespaceDetail, SetDetail, Sindex

    info = session.info()
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")
    namespace = list(namespaces)[0]

    per_node_ns = await info.namespace_details_per_node(namespace)
    assert per_node_ns, "no node reported the namespace"
    assert all(isinstance(d, NamespaceDetail) for d in per_node_ns.values())

    per_node_sets = await info.sets_per_node(namespace)
    assert set(per_node_sets) == set(per_node_ns), "same nodes should answer both"
    for details in per_node_sets.values():
        assert all(isinstance(d, SetDetail) for d in details)
        names = [d.name for d in details]
        assert names == sorted(names)

    per_node_idx = await info.secondary_indexes_per_node(namespace)
    for indexes in per_node_idx.values():
        assert all(isinstance(i, Sindex) for i in indexes)
        assert all(i.namespace == namespace for i in indexes)

    # Every set the merged view reports is reported by at least one node.
    merged = {d.name for d in await info.sets(namespace)}
    from_nodes = {d.name for details in per_node_sets.values() for d in details}
    assert merged == from_nodes


async def test_secondary_index_details_per_node(session):
    """Build progress is per node, so this is the view that shows it."""
    from aerospike_sdk import SindexDetail

    info = session.info()
    indexes = await info.secondary_indexes()
    if not indexes:
        pytest.skip("No secondary indexes found to test")

    index = indexes[0]
    per_node = await info.secondary_index_details_per_node(
        index.namespace, index.name
    )
    if not per_node:
        pytest.skip("no node reported details for the index")
    assert all(isinstance(d, SindexDetail) for d in per_node.values())
    assert all(d.load_pct <= 100 for d in per_node.values())


async def test_storage_engine_view(session):
    """The storage-engine section, lifted out of the namespace response."""
    info = session.info()
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    detail = await info.namespace_details(list(namespaces)[0])
    assert detail is not None
    engine = detail.storage_engine
    assert engine.engine in {"memory", "device"}
    assert engine.is_memory != engine.is_device
    # Grouped per location: the path travels with its own counters, rather
    # than the counters appearing as separate entries alongside it.
    for storage_file in engine.files:
        assert storage_file.path and not storage_file.path.isdigit()
        assert isinstance(storage_file.used_bytes, int)
    # The section is a subset: unrelated namespace keys stay out of it.
    assert "replication-factor" not in engine


async def test_set_lookup_by_name(session):
    """One set by name, matching the reference client's ``set(name)``."""
    info = session.info()
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")
    namespace = list(namespaces)[0]

    all_sets = await info.sets(namespace)
    if not all_sets:
        pytest.skip(f"No sets in namespace {namespace!r}")

    found = await info.set(namespace, all_sets[0].name)
    assert found is not None
    assert found.name == all_sets[0].name
    assert await info.set(namespace, "no_such_set_xyz") is None


async def test_sets_without_a_namespace_spans_all_of_them(session):
    """The bare ``sets`` command answers for every namespace, as the reference does."""
    info = session.info()
    namespaces = await info.namespaces()
    if not namespaces:
        pytest.skip("No namespaces found to test")

    everything = await info.sets()
    if not everything:
        pytest.skip("No sets on the cluster")

    reported = {d.namespace for d in everything}
    assert reported <= set(namespaces)
    # Filtering must agree with the unfiltered call for the namespace asked.
    one = list(namespaces)[0]
    filtered = {(d.namespace, d.name) for d in await info.sets(one)}
    assert filtered == {(d.namespace, d.name) for d in everything if d.namespace == one}
