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

"""Unit tests for query bin-level read operations, CDT builders, and stacking.

Covers:
- CdtReadBuilder / CdtReadInvertableBuilder terminal methods and return types
- QueryBinBuilder navigation and operation registration
- _OperationSpec dataclass
- QueryBuilder.query() stacking and _finalize_current_spec()
- SyncQueryBuilder delegation for bin/query/stacking
"""

import pytest
from unittest.mock import MagicMock

from aerospike_async import Key, ListReturnType, MapReturnType
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.aio.operations.cdt_read import (
    CdtReadBuilder,
    CdtReadInvertableBuilder,
)
from aerospike_sdk.aio.operations.query import (
    _OperationSpec,
    QueryBinBuilder,
    QueryBuilder,
)
from aerospike_sdk.sync.operations.query import SyncQueryBuilder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _OpCollector:
    """Minimal parent mock that satisfies add_operation(op) protocol."""

    def __init__(self):
        self.operations: list = []

    def add_operation(self, op):
        self.operations.append(op)


def _make_builder(**overrides) -> QueryBuilder:
    """Create a QueryBuilder with a fake client for unit testing."""
    return QueryBuilder(client=object(), namespace="test", set_name="unit", **overrides)


def _make_key(digest: int = 1) -> Key:
    return Key("test", "unit", digest)


# ===================================================================
# CdtReadBuilder
# ===================================================================

class TestCdtReadBuilder:
    """Verify each terminal method passes the correct return type to op_factory."""

    def _build(self, *, is_map: bool = True):
        parent = _OpCollector()
        captured = []
        factory = lambda rt: (captured.append(rt), f"op_{rt}")[1]
        rt_cls = MapReturnType if is_map else ListReturnType
        builder = CdtReadBuilder(parent, factory, rt_cls, is_map=is_map)
        return builder, parent, captured

    def test_get_values(self):
        b, parent, cap = self._build()
        result = b.get_values()
        assert result is parent
        assert cap == [MapReturnType.VALUE]
        assert len(parent.operations) == 1

    def test_get_keys(self):
        b, parent, cap = self._build(is_map=True)
        b.get_keys()
        assert cap == [MapReturnType.KEY]

    def test_get_keys_and_values(self):
        b, parent, cap = self._build(is_map=True)
        b.get_keys_and_values()
        assert cap == [MapReturnType.KEY_VALUE]

    def test_count(self):
        b, parent, cap = self._build()
        b.count()
        assert cap == [MapReturnType.COUNT]

    def test_get_indexes(self):
        b, parent, cap = self._build()
        b.get_indexes()
        assert cap == [MapReturnType.INDEX]

    def test_get_reverse_indexes(self):
        b, parent, cap = self._build()
        b.get_reverse_indexes()
        assert cap == [MapReturnType.REVERSE_INDEX]

    def test_get_ranks(self):
        b, parent, cap = self._build()
        b.get_ranks()
        assert cap == [MapReturnType.RANK]

    def test_get_reverse_ranks(self):
        b, parent, cap = self._build()
        b.get_reverse_ranks()
        assert cap == [MapReturnType.REVERSE_RANK]

    def test_list_get_values(self):
        b, parent, cap = self._build(is_map=False)
        b.get_values()
        assert cap == [ListReturnType.VALUE]

    def test_list_count(self):
        b, parent, cap = self._build(is_map=False)
        b.count()
        assert cap == [ListReturnType.COUNT]

    def test_get_keys_raises_for_list(self):
        b, _, _ = self._build(is_map=False)
        with pytest.raises(TypeError, match="only supported for map"):
            b.get_keys()

    def test_get_keys_and_values_raises_for_list(self):
        b, _, _ = self._build(is_map=False)
        with pytest.raises(TypeError, match="only supported for map"):
            b.get_keys_and_values()


# ===================================================================
# CdtReadInvertableBuilder
# ===================================================================

class TestCdtReadInvertableBuilder:
    """Verify inverted terminals combine INVERTED flag correctly."""

    def _build(self, *, is_map: bool = True):
        parent = _OpCollector()
        captured = []
        factory = lambda rt: (captured.append(rt), f"op_{rt}")[1]
        rt_cls = MapReturnType if is_map else ListReturnType
        builder = CdtReadInvertableBuilder(parent, factory, rt_cls, is_map=is_map)
        return builder, parent, captured

    def test_get_all_other_values(self):
        b, parent, cap = self._build()
        result = b.get_all_other_values()
        assert result is parent
        assert cap == [MapReturnType.VALUE | MapReturnType.INVERTED]

    def test_get_all_other_keys(self):
        b, _, cap = self._build(is_map=True)
        b.get_all_other_keys()
        assert cap == [MapReturnType.KEY | MapReturnType.INVERTED]

    def test_get_all_other_keys_and_values(self):
        b, _, cap = self._build(is_map=True)
        b.get_all_other_keys_and_values()
        assert cap == [MapReturnType.KEY_VALUE | MapReturnType.INVERTED]

    def test_count_all_others(self):
        b, _, cap = self._build()
        b.count_all_others()
        assert cap == [MapReturnType.COUNT | MapReturnType.INVERTED]

    def test_get_all_other_indexes(self):
        b, _, cap = self._build()
        b.get_all_other_indexes()
        assert cap == [MapReturnType.INDEX | MapReturnType.INVERTED]

    def test_get_all_other_reverse_indexes(self):
        b, _, cap = self._build()
        b.get_all_other_reverse_indexes()
        assert cap == [MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED]

    def test_get_all_other_ranks(self):
        b, _, cap = self._build()
        b.get_all_other_ranks()
        assert cap == [MapReturnType.RANK | MapReturnType.INVERTED]

    def test_get_all_other_reverse_ranks(self):
        b, _, cap = self._build()
        b.get_all_other_reverse_ranks()
        assert cap == [MapReturnType.REVERSE_RANK | MapReturnType.INVERTED]

    def test_list_get_all_other_values(self):
        b, _, cap = self._build(is_map=False)
        b.get_all_other_values()
        assert cap == [ListReturnType.VALUE | ListReturnType.INVERTED]

    def test_list_count_all_others(self):
        b, _, cap = self._build(is_map=False)
        b.count_all_others()
        assert cap == [ListReturnType.COUNT | ListReturnType.INVERTED]

    def test_get_all_other_keys_raises_for_list(self):
        b, _, _ = self._build(is_map=False)
        with pytest.raises(TypeError, match="only supported for map"):
            b.get_all_other_keys()

    def test_get_all_other_keys_and_values_raises_for_list(self):
        b, _, _ = self._build(is_map=False)
        with pytest.raises(TypeError, match="only supported for map"):
            b.get_all_other_keys_and_values()

    def test_inherits_non_inverted_terminals(self):
        """Invertable builder should also expose the non-inverted methods."""
        b, _, cap = self._build()
        b.get_values()
        assert cap == [MapReturnType.VALUE]


# ===================================================================
# QueryBinBuilder
# ===================================================================

class TestQueryBinBuilder:
    """Verify QueryBinBuilder produces correct operations and builder types."""

    def _build(self):
        parent = _OpCollector()
        return QueryBinBuilder(parent, "mybin"), parent

    def test_get_returns_parent_and_adds_operation(self):
        qbb, parent = self._build()
        result = qbb.get()
        assert result is parent
        assert len(parent.operations) == 1

    def test_map_size_returns_parent(self):
        qbb, parent = self._build()
        result = qbb.map_size()
        assert result is parent
        assert len(parent.operations) == 1

    def test_list_size_returns_parent(self):
        qbb, parent = self._build()
        result = qbb.list_size()
        assert result is parent
        assert len(parent.operations) == 1

    def test_on_map_key_returns_cdt_read_builder(self):
        qbb, _ = self._build()
        result = qbb.on_map_key("some_key")
        assert isinstance(result, CdtReadBuilder)
        assert not isinstance(result, CdtReadInvertableBuilder)

    def test_on_map_index_returns_cdt_read_builder(self):
        qbb, _ = self._build()
        result = qbb.on_map_index(0)
        assert isinstance(result, CdtReadBuilder)
        assert not isinstance(result, CdtReadInvertableBuilder)

    def test_on_map_rank_returns_cdt_read_builder(self):
        qbb, _ = self._build()
        result = qbb.on_map_rank(0)
        assert isinstance(result, CdtReadBuilder)
        assert not isinstance(result, CdtReadInvertableBuilder)

    def test_on_map_value_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_map_value("v"), CdtReadInvertableBuilder)

    def test_on_map_key_range_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_map_key_range("a", "z"), CdtReadInvertableBuilder)

    def test_on_map_index_range_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_map_index_range(0, 5), CdtReadInvertableBuilder)

    def test_on_map_index_range_no_count_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_map_index_range(0), CdtReadInvertableBuilder)

    def test_on_map_rank_range_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_map_rank_range(0, 3), CdtReadInvertableBuilder)

    def test_on_map_value_range_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_map_value_range(1, 100), CdtReadInvertableBuilder)

    def test_on_map_key_list_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_map_key_list(["a", "b"]), CdtReadInvertableBuilder)

    def test_on_map_value_list_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_map_value_list([1, 2]), CdtReadInvertableBuilder)

    def test_on_list_index_returns_cdt_read_builder(self):
        qbb, _ = self._build()
        result = qbb.on_list_index(0)
        assert isinstance(result, CdtReadBuilder)
        assert not isinstance(result, CdtReadInvertableBuilder)

    def test_on_list_rank_returns_cdt_read_builder(self):
        qbb, _ = self._build()
        result = qbb.on_list_rank(0)
        assert isinstance(result, CdtReadBuilder)
        assert not isinstance(result, CdtReadInvertableBuilder)

    def test_on_list_value_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_list_value(42), CdtReadInvertableBuilder)

    def test_on_list_index_range_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_list_index_range(0, 5), CdtReadInvertableBuilder)

    def test_on_list_rank_range_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_list_rank_range(0, 3), CdtReadInvertableBuilder)

    def test_on_list_value_range_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_list_value_range(1, 100), CdtReadInvertableBuilder)

    def test_on_list_value_list_returns_invertable_builder(self):
        qbb, _ = self._build()
        assert isinstance(qbb.on_list_value_list([1, 2, 3]), CdtReadInvertableBuilder)

    def test_generic_parent_type(self):
        """QueryBinBuilder should work with any parent type that has add_operation."""
        parent = _OpCollector()
        qbb = QueryBinBuilder(parent, "b")
        result = qbb.get()
        assert result is parent
        assert len(parent.operations) == 1


# ===================================================================
# _OperationSpec
# ===================================================================

class TestOperationSpec:

    def test_fields(self):
        k = _make_key()
        spec = _OperationSpec(keys=[k], operations=["op1"])
        assert spec.keys == [k]
        assert spec.operations == ["op1"]
        assert spec.bins is None
        assert spec.filter_expression is None

    def test_defaults(self):
        k = _make_key()
        spec = _OperationSpec(keys=[k])
        assert spec.operations == []
        assert spec.bins is None


# ===================================================================
# _should_include_result
# ===================================================================

class TestShouldIncludeResult:

    def test_ok_always_included(self):
        assert QueryBuilder._should_include_result(
            ResultCode.OK, False, False) is True

    def test_key_not_found_excluded_by_default(self):
        assert QueryBuilder._should_include_result(
            ResultCode.KEY_NOT_FOUND_ERROR, False, False) is False

    def test_key_not_found_included_with_respond_all_keys(self):
        assert QueryBuilder._should_include_result(
            ResultCode.KEY_NOT_FOUND_ERROR, True, False) is True

    def test_filtered_out_excluded_by_default(self):
        assert QueryBuilder._should_include_result(
            ResultCode.FILTERED_OUT, False, False) is False

    def test_filtered_out_included_with_fail_on_filtered(self):
        assert QueryBuilder._should_include_result(
            ResultCode.FILTERED_OUT, False, True) is True

    def test_filtered_out_included_with_respond_all_keys(self):
        assert QueryBuilder._should_include_result(
            ResultCode.FILTERED_OUT, True, False) is True

    def test_other_errors_always_included(self):
        assert QueryBuilder._should_include_result(
            ResultCode.TIMEOUT, False, False) is True


# ===================================================================
# QueryBuilder stacking
# ===================================================================

class TestQueryBuilderStacking:

    def test_finalize_packages_single_key(self):
        b = _make_builder()
        k = _make_key()
        b._single_key = k
        b._operations = ["op1", "op2"]
        b._bins = ["age"]
        b._finalize_current_spec()
        assert len(b._specs) == 1
        assert b._specs[0].keys == [k]
        assert b._specs[0].operations == ["op1", "op2"]
        assert b._specs[0].bins == ["age"]

    def test_finalize_packages_batch_keys(self):
        b = _make_builder()
        keys = [_make_key(1), _make_key(2)]
        b._keys = keys
        b._finalize_current_spec()
        assert len(b._specs) == 1
        assert b._specs[0].keys is keys

    def test_finalize_noop_for_dataset_query(self):
        b = _make_builder()
        b._finalize_current_spec()
        assert len(b._specs) == 0

    def test_finalize_resets_per_spec_state(self):
        b = _make_builder()
        b._single_key = _make_key()
        b._operations = ["op"]
        b._bins = ["name"]
        b._with_no_bins = True
        b._filter_expression = object()
        b._finalize_current_spec()
        assert b._single_key is None
        assert b._keys is None
        assert b._operations == []
        assert b._bins is None
        assert b._with_no_bins is False
        assert b._filter_expression is None

    def test_query_chain_creates_specs(self):
        b = _make_builder()
        k1, k2 = _make_key(1), _make_key(2)
        b._single_key = k1
        b._operations = ["op_a"]
        b.query(k2)
        assert len(b._specs) == 1
        assert b._specs[0].keys == [k1]
        assert b._specs[0].operations == ["op_a"]
        assert b._single_key == k2
        assert b._operations == []

    def test_query_chain_with_list(self):
        b = _make_builder()
        k1 = _make_key(1)
        keys2 = [_make_key(2), _make_key(3)]
        b._single_key = k1
        b.query(keys2)
        assert len(b._specs) == 1
        assert b._keys == keys2

    def test_query_chain_varargs(self):
        b = _make_builder()
        k1, k2, k3 = _make_key(1), _make_key(2), _make_key(3)
        b._single_key = k1
        b.query(k2, k3)
        assert len(b._specs) == 1
        assert b._keys == [k2, k3]

    def test_query_on_dataset_raises(self):
        b = _make_builder()
        with pytest.raises(ValueError, match="Dataset.*cannot be stacked"):
            b.query(_make_key())

    def test_query_empty_list_raises(self):
        b = _make_builder()
        b._single_key = _make_key()
        with pytest.raises(ValueError, match="cannot be empty"):
            b.query([])

    def test_query_wrong_type_raises(self):
        b = _make_builder()
        b._single_key = _make_key()
        with pytest.raises(TypeError, match="requires a Key or List"):
            b.query("not_a_key")

    def test_multiple_stacks(self):
        b = _make_builder()
        k1, k2, k3 = _make_key(1), _make_key(2), _make_key(3)
        b._single_key = k1
        b._operations = ["op1"]
        b.query(k2)
        b._operations = ["op2"]
        b.query(k3)
        b._operations = ["op3"]
        b._finalize_current_spec()
        assert len(b._specs) == 3
        assert b._specs[0].operations == ["op1"]
        assert b._specs[1].operations == ["op2"]
        assert b._specs[2].operations == ["op3"]

    def test_bins_captured_per_spec(self):
        b = _make_builder()
        k1, k2 = _make_key(1), _make_key(2)
        b._single_key = k1
        b._bins = ["age"]
        b.query(k2)
        b._bins = ["name"]
        b._finalize_current_spec()
        assert b._specs[0].bins == ["age"]
        assert b._specs[1].bins == ["name"]

    def test_bin_method_returns_query_bin_builder(self):
        b = _make_builder()
        qbb = b.bin("mybin")
        assert isinstance(qbb, QueryBinBuilder)

    def test_add_operation_accumulates(self):
        b = _make_builder()
        b.add_operation("op1")
        b.add_operation("op2")
        assert b._operations == ["op1", "op2"]


# ===================================================================
# SyncQueryBuilder delegation
# ===================================================================

class TestSyncQueryBuilderDelegation:
    """SyncQueryBuilder is a native subclass of ``_QueryBuilderBase``; state
    lives directly on the instance (no ``_qb`` wrapper)."""

    def _sync_builder(self, **qb_overrides) -> SyncQueryBuilder:
        return SyncQueryBuilder(
            client=object(),
            namespace="test",
            set_name="unit",
        )

    def test_bin_returns_query_bin_builder(self):
        sb = self._sync_builder()
        qbb = sb.bin("mybin")
        assert isinstance(qbb, QueryBinBuilder)

    def test_add_operation_mutates_state(self):
        sb = self._sync_builder()
        sb.add_operation("sentinel_op")
        assert "sentinel_op" in sb._operations

    def test_bins_mutates_state(self):
        sb = self._sync_builder()
        sb.bins(["a", "b"])
        assert sb._bins == ["a", "b"]

    def test_with_no_bins_mutates_state(self):
        sb = self._sync_builder()
        sb.with_no_bins()
        assert sb._with_no_bins is True

    def test_query_stacking_mutates_state(self):
        sb = self._sync_builder()
        k1, k2 = _make_key(1), _make_key(2)
        sb._single_key = k1
        sb._operations = ["op1"]
        result = sb.query(k2)
        assert result is sb
        assert len(sb._specs) == 1
        assert sb._single_key == k2

    def test_query_on_dataset_raises(self):
        sb = self._sync_builder()
        with pytest.raises(ValueError, match="Dataset.*cannot be stacked"):
            sb.query(_make_key())
