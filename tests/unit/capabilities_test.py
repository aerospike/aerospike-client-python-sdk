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

"""Unit tests for server-capability resolution (minimum version across nodes).

The folds are pure functions over per-node version objects, so a lightweight
stand-in for PAC's ``Version`` (four version fields + the ``supports_*``
predicate methods) exercises every rule without a cluster. The load-bearing
property: a single lagging node makes the cluster answer unsupported.
"""

from aerospike_sdk import capabilities


class _FakeVersion:
    """Stand-in for PAC ``Version``: version fields + predicate methods."""

    def __init__(self, major, minor, patch, build=0, *,
                 ael=None, query_ops=None, string_ops=None, query_selection=None):
        self.major, self.minor, self.patch, self.build = major, minor, patch, build
        # Default each predicate to "supported iff >= 8.1.3", the real floor,
        # unless the test pins it explicitly.
        ge813 = (major, minor, patch) >= (8, 1, 3)
        self._ael = ge813 if ael is None else ael
        self._query_ops = ((major, minor, patch) >= (8, 1, 2)
                           if query_ops is None else query_ops)
        self._string_ops = ge813 if string_ops is None else string_ops
        self._query_selection = ge813 if query_selection is None else query_selection

    def supports_server_compiled_ael(self):
        return self._ael

    def supports_query_ops_projection_ext(self):
        return self._query_ops

    def supports_string_operations(self):
        return self._string_ops

    def supports_query_selection(self):
        return self._query_selection


class TestVersionKeyAndMin:

    def test_version_key_orders_by_all_four_fields(self):
        assert capabilities.version_key(_FakeVersion(8, 1, 3, 63)) == (8, 1, 3, 63)

    def test_min_version_picks_least_capable_node(self):
        nodes = [_FakeVersion(8, 1, 3, 75), _FakeVersion(8, 1, 2, 9),
                 _FakeVersion(8, 1, 3, 63)]
        floor = capabilities.min_version(nodes)
        assert capabilities.version_key(floor) == (8, 1, 2, 9)

    def test_min_version_empty_is_none(self):
        assert capabilities.min_version([]) is None

    def test_min_version_tie_breaks_on_build(self):
        floor = capabilities.min_version(
            [_FakeVersion(8, 1, 3, 75), _FakeVersion(8, 1, 3, 60)])
        assert capabilities.version_key(floor)[3] == 60


class TestAllNodesFolds:
    """A single lagging node makes each predicate report unsupported."""

    def test_all_capable_supports_everything(self):
        vs = [_FakeVersion(8, 1, 3), _FakeVersion(8, 1, 3)]
        assert capabilities.supports_ael(vs)
        assert capabilities.supports_string_operations(vs)
        assert capabilities.supports_query_operations(vs)
        assert capabilities.supports_query_selection(vs)

    def test_one_lagging_node_downgrades_813_features(self):
        # One node at 8.1.2 fails the >= 8.1.3 features but keeps 8.1.2 ones.
        vs = [_FakeVersion(8, 1, 3), _FakeVersion(8, 1, 2)]
        assert not capabilities.supports_ael(vs)
        assert not capabilities.supports_string_operations(vs)
        assert not capabilities.supports_query_selection(vs)
        assert capabilities.supports_query_operations(vs)  # 8.1.2 floor

    def test_one_lagging_node_downgrades_812_feature(self):
        vs = [_FakeVersion(8, 1, 2), _FakeVersion(8, 1, 1)]
        assert not capabilities.supports_query_operations(vs)

    def test_empty_cluster_supports_nothing(self):
        assert not capabilities.supports_ael([])
        assert not capabilities.supports_query_operations([])
        assert not capabilities.supports_string_operations([])
        assert not capabilities.supports_query_selection([])

    def test_predicates_delegate_to_pac_not_version_floor(self):
        # Every predicate reads PAC's Version.supports_*(), not a hardcoded
        # version check: a node reporting the predicate False despite a high
        # version is honored.
        vs = [_FakeVersion(9, 0, 0, ael=False, string_ops=False, query_ops=False,
                           query_selection=False)]
        assert not capabilities.supports_ael(vs)
        assert not capabilities.supports_string_operations(vs)
        assert not capabilities.supports_query_operations(vs)
        assert not capabilities.supports_query_selection(vs)
