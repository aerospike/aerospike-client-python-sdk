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

"""Unit tests for CDT write options (keyword-only flag parameters).

Covers:
- _resolve_list_policy helper
- _resolve_map_policy helper
- Keyword arg wiring on WriteBinBuilder and CdtWriteBuilder methods
"""

import pytest

from aerospike_async import (
    Key,
    ListOrderType,
    ListPolicy,
    ListWriteFlags,
    MapOrder,
    MapPolicy,
    MapWriteFlags,
)

from aerospike_sdk.aio.operations.cdt_write import (
    _resolve_list_policy,
    _resolve_map_policy,
    _UNORDERED_LIST_POLICY,
    _ORDERED_LIST_POLICY,
)
from aerospike_sdk.aio.operations.query import WriteBinBuilder, WriteSegmentBuilder


# ---------------------------------------------------------------------------
# _resolve_list_policy
# ---------------------------------------------------------------------------

class TestResolveListPolicy:

    def test_no_flags_unordered_returns_default(self):
        policy = _resolve_list_policy(None)
        assert policy is _UNORDERED_LIST_POLICY

    def test_no_flags_ordered_returns_default(self):
        policy = _resolve_list_policy(ListOrderType.ORDERED)
        assert policy is _ORDERED_LIST_POLICY

    def test_unique_flag(self):
        policy = _resolve_list_policy(None, unique=True)
        assert isinstance(policy, ListPolicy)
        assert policy.write_flags == ListWriteFlags.ADD_UNIQUE

    def test_bounded_flag(self):
        policy = _resolve_list_policy(None, bounded=True)
        assert policy.write_flags == ListWriteFlags.INSERT_BOUNDED

    def test_no_fail_flag(self):
        policy = _resolve_list_policy(None, no_fail=True)
        assert policy.write_flags == ListWriteFlags.NO_FAIL

    def test_partial_flag(self):
        policy = _resolve_list_policy(None, partial=True)
        assert policy.write_flags == ListWriteFlags.PARTIAL

    def test_unique_preserves_order(self):
        policy = _resolve_list_policy(ListOrderType.ORDERED, unique=True)
        assert policy.write_flags == ListWriteFlags.ADD_UNIQUE
        assert policy.order == ListOrderType.ORDERED

    def test_combined_flags_bitmask(self):
        policy = _resolve_list_policy(None, unique=True, no_fail=True)
        assert policy.write_flags == (ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL)

    def test_three_combined_flags_bitmask(self):
        policy = _resolve_list_policy(
            None, unique=True, no_fail=True, partial=True,
        )
        assert policy.write_flags == (
            ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL
        )


# ---------------------------------------------------------------------------
# _resolve_map_policy
# ---------------------------------------------------------------------------

class TestResolveMapPolicy:

    def test_no_flags_returns_default(self):
        policy = _resolve_map_policy(MapWriteFlags.DEFAULT)
        assert isinstance(policy, MapPolicy)

    def test_base_flags_preserved(self):
        policy = _resolve_map_policy(MapWriteFlags.CREATE_ONLY)
        assert isinstance(policy, MapPolicy)

    def test_no_fail_combined(self):
        policy = _resolve_map_policy(
            MapWriteFlags.CREATE_ONLY, no_fail=True,
        )
        assert isinstance(policy, MapPolicy)

    def test_partial_combined(self):
        policy = _resolve_map_policy(
            MapWriteFlags.CREATE_ONLY, no_fail=True, partial=True,
        )
        assert isinstance(policy, MapPolicy)

    def test_order_forwarded(self):
        policy = _resolve_map_policy(
            MapWriteFlags.DEFAULT, order=MapOrder.KEY_ORDERED,
        )
        assert isinstance(policy, MapPolicy)

    def test_persist_index(self):
        policy = _resolve_map_policy(
            MapWriteFlags.DEFAULT, persist_index=True,
        )
        assert isinstance(policy, MapPolicy)

    def test_persist_index_with_flags(self):
        policy = _resolve_map_policy(
            MapWriteFlags.CREATE_ONLY,
            persist_index=True, no_fail=True,
        )
        assert isinstance(policy, MapPolicy)


# ---------------------------------------------------------------------------
# WriteBinBuilder keyword wiring (smoke tests)
# ---------------------------------------------------------------------------

class _OpCollector:
    """Minimal parent satisfying the add_operation(op) protocol."""

    def __init__(self):
        self.operations: list = []

    def _add_op(self, op):
        self.operations.append(op)
        return self


def _make_key() -> Key:
    return Key("test", "unit", 1)


class TestWriteBinBuilderKeywords:

    @pytest.fixture()
    def builder(self):
        collector = _OpCollector()
        seg = WriteSegmentBuilder.__new__(WriteSegmentBuilder)
        seg._operations = []
        seg._add_op = collector._add_op
        wb = WriteBinBuilder(seg, "b")
        return wb, collector

    def test_list_append_unique(self, builder):
        wb, collector = builder
        wb.list_append("v", unique=True)
        assert len(collector.operations) == 1

    def test_list_add_no_fail(self, builder):
        wb, collector = builder
        wb.list_add("v", no_fail=True)
        assert len(collector.operations) == 1

    def test_list_append_items_partial(self, builder):
        wb, collector = builder
        wb.list_append_items([1, 2], partial=True)
        assert len(collector.operations) == 1

    def test_list_insert_bounded(self, builder):
        wb, collector = builder
        wb.list_insert(0, "v", bounded=True)
        assert len(collector.operations) == 1

    def test_list_insert_items_unique(self, builder):
        wb, collector = builder
        wb.list_insert_items(0, [1, 2], unique=True)
        assert len(collector.operations) == 1

    def test_map_upsert_items_no_fail(self, builder):
        wb, collector = builder
        wb.map_upsert_items({"a": 1}, no_fail=True)
        assert len(collector.operations) == 1

    def test_map_insert_items_partial(self, builder):
        wb, collector = builder
        wb.map_insert_items({"a": 1}, no_fail=True, partial=True)
        assert len(collector.operations) == 1

    def test_map_update_items_order(self, builder):
        wb, collector = builder
        wb.map_update_items({"a": 1}, order=MapOrder.KEY_ORDERED)
        assert len(collector.operations) == 1

    def test_map_upsert_items_persist_index(self, builder):
        wb, collector = builder
        wb.map_upsert_items({"a": 1}, persist_index=True)
        assert len(collector.operations) == 1
