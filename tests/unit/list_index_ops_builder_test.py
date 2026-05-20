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

"""Unit tests for index-based list builders (whole-bin and nested CDT)."""

from unittest.mock import MagicMock

from aerospike_async import Key, ListOperation, ListReturnType

from aerospike_sdk.aio.operations.cdt_read import CdtReadBuilder
from aerospike_sdk.aio.operations.cdt_write import CdtWriteBuilder
from aerospike_sdk.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
)
from aerospike_sdk.sync.operations.query import SyncWriteBinBuilder, SyncWriteSegmentBuilder


class _OpCollector:
    def __init__(self):
        self.operations: list = []

    def add_operation(self, op):
        self.operations.append(op)


def _make_qb() -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="unit")


def _make_key(digest: int = 1) -> Key:
    return Key("test", "unit", digest)


class TestWriteBinBuilderIndexListOps:

    def _build(self, bin_name: str = "L"):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        return WriteBinBuilder(segment, bin_name), segment

    def test_list_insert_returns_segment(self):
        wbb, segment = self._build()
        assert wbb.list_insert(0, "a") is segment

    def test_list_insert_produces_list_operation(self):
        wbb, segment = self._build()
        wbb.list_insert(1, "x")
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_insert_items(self):
        wbb, segment = self._build()
        wbb.list_insert_items(0, [1, 2])
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_set(self):
        wbb, segment = self._build()
        wbb.list_set(2, 9)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_increment_one_uses_by_one(self):
        wbb, segment = self._build()
        wbb.list_increment(0, 1)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_increment_other_value(self):
        wbb, segment = self._build()
        wbb.list_increment(0, 5)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_remove(self):
        wbb, segment = self._build()
        wbb.list_remove(-1)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_remove_range_with_count(self):
        wbb, segment = self._build()
        wbb.list_remove_range(0, 2)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_remove_range_open(self):
        wbb, segment = self._build()
        wbb.list_remove_range(1, None)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_pop(self):
        wbb, segment = self._build()
        wbb.list_pop(0)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_pop_range(self):
        wbb, segment = self._build()
        wbb.list_pop_range(0, 2)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_pop_range_from(self):
        wbb, segment = self._build()
        wbb.list_pop_range(1, None)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_list_trim(self):
        wbb, segment = self._build()
        wbb.list_trim(0, 2)
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)


class TestQueryBinBuilderIndexListReads:

    def _build(self):
        parent = _OpCollector()
        return QueryBinBuilder(parent, "L"), parent

    def test_list_get(self):
        qbb, parent = self._build()
        assert qbb.list_get(0) is parent
        assert len(parent.operations) == 1
        assert isinstance(parent.operations[0], ListOperation)

    def test_list_get_range_count(self):
        qbb, parent = self._build()
        assert qbb.list_get_range(0, 2) is parent
        assert len(parent.operations) == 1
        assert isinstance(parent.operations[0], ListOperation)

    def test_list_get_range_from(self):
        qbb, parent = self._build()
        assert qbb.list_get_range(1, None) is parent
        assert len(parent.operations) == 1
        assert isinstance(parent.operations[0], ListOperation)


class TestSyncWriteBinBuilderIndexListOps:

    def _build(self, bin_name: str = "L"):
        from aerospike_sdk.sync.operations.query import SyncQueryBuilder
        qb = SyncQueryBuilder(client=MagicMock(), namespace="test", set_name="t")
        qb._op_type = "upsert"
        qb._single_key = _make_key()
        sync_seg = SyncWriteSegmentBuilder(qb)
        return SyncWriteBinBuilder(sync_seg, bin_name), qb

    def test_list_insert_appends_operation_to_segment_state(self):
        swb, qb = self._build()
        result = swb.list_insert(0, 1)
        # SyncWriteBinBuilder is just WriteBinBuilder; methods return the
        # parent segment for chaining.
        assert result is swb._segment
        assert len(qb._operations) == 1
        assert isinstance(qb._operations[0], ListOperation)


class TestCdtNestedIndexListOps:

    def _write_build(self):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        wbb = WriteBinBuilder(segment, "root")
        return wbb, segment

    def _read_build(self):
        parent = _OpCollector()
        qbb = QueryBinBuilder(parent, "root")
        return qbb, parent

    def test_nested_list_insert(self):
        wbb, segment = self._write_build()
        wbb.on_map_key("items").list_insert(0, "a")
        assert len(segment._qb._operations) == 1
        assert isinstance(segment._qb._operations[0], ListOperation)

    def test_nested_list_get(self):
        qbb, parent = self._read_build()
        qbb.on_map_key("items").list_get(0)
        assert len(parent.operations) == 1
        assert isinstance(parent.operations[0], ListOperation)

    def test_nested_list_get_range_open(self):
        qbb, parent = self._read_build()
        qbb.on_map_key("items").list_get_range(0, None)
        assert len(parent.operations) == 1
        assert isinstance(parent.operations[0], ListOperation)

    def test_cdt_write_builder_list_increment_default(self):
        parent = _OpCollector()
        b = CdtWriteBuilder(
            parent,
            lambda rt: f"g_{rt}",
            lambda rt: f"r_{rt}",
            ListReturnType,
            is_map=False,
        )
        b.list_increment(2)
        assert len(parent.operations) == 1
        assert isinstance(parent.operations[0], ListOperation)

    def test_cdt_read_builder_list_get(self):
        parent = _OpCollector()
        b = CdtReadBuilder(
            parent,
            lambda rt: f"g_{rt}",
            ListReturnType,
            is_map=False,
        )
        b.list_get(1)
        assert len(parent.operations) == 1
        assert isinstance(parent.operations[0], ListOperation)
