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

"""Unit tests for complex batch (mixed read + write chains).

Covers:
- _OperationSpec with op_type
- WriteSegmentBuilder / WriteBinBuilder operation accumulation
- Cross-builder transitions (QueryBuilder -> WriteSegmentBuilder, etc.)
- Per-spec settings (where, TTL, generation)
- Chain-level defaults (default_where, default_expire)
- BinBuilder -> QueryBuilder transitions
"""

import pytest
from unittest.mock import MagicMock

from aerospike_async import (
    FilterExpression as Exp,
    Key,
    Operation,
    RecordExistsAction,
    WritePolicy,
)

from aerospike_sdk.aio.operations.query import (
    _OperationSpec,
    _OP_TYPE_TO_REA,
    QueryBuilder,
    WriteSegmentBuilder,
    WriteBinBuilder,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_builder(**overrides) -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="unit", **overrides)

def _make_key(i: int = 1) -> Key:
    return Key("test", "unit", f"key{i}")


# ---------------------------------------------------------------------------
# _OperationSpec
# ---------------------------------------------------------------------------

class TestOperationSpec:
    def test_defaults(self):
        spec = _OperationSpec(keys=[_make_key()])
        assert spec.op_type is None
        assert spec.generation is None
        assert spec.ttl_seconds is None
        assert spec.durable_delete is None

    def test_write_op_type(self):
        spec = _OperationSpec(keys=[_make_key()], op_type="upsert")
        assert spec.op_type == "upsert"

    def test_delete_op_type(self):
        spec = _OperationSpec(keys=[_make_key()], op_type="delete")
        assert spec.op_type == "delete"

    def test_per_spec_settings(self):
        spec = _OperationSpec(
            keys=[_make_key()],
            op_type="insert",
            generation=42,
            ttl_seconds=300,
            durable_delete=True,
        )
        assert spec.generation == 42
        assert spec.ttl_seconds == 300
        assert spec.durable_delete is True


class TestOpTypeToRecordExistsAction:
    def test_insert(self):
        assert _OP_TYPE_TO_REA["insert"] == RecordExistsAction.CREATE_ONLY

    def test_update(self):
        assert _OP_TYPE_TO_REA["update"] == RecordExistsAction.UPDATE_ONLY

    def test_replace(self):
        assert _OP_TYPE_TO_REA["replace"] == RecordExistsAction.REPLACE

    def test_replace_if_exists(self):
        assert _OP_TYPE_TO_REA["replace_if_exists"] == RecordExistsAction.REPLACE_ONLY

    def test_upsert_not_in_map(self):
        assert "upsert" not in _OP_TYPE_TO_REA


# ---------------------------------------------------------------------------
# WriteSegmentBuilder
# ---------------------------------------------------------------------------

class TestWriteSegmentBuilder:
    def test_put_accumulates_operations(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)

        wsb.put({"name": "Alice", "age": 25})
        assert len(qb._operations) == 2

    def test_set_bins_alias(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)

        result = wsb.set_bins({"x": 1})
        assert result is wsb
        assert len(qb._operations) == 1

    def test_transition_to_query(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)

        wsb.put({"name": "Alice"})
        result = wsb.query(_make_key(2))

        assert result is qb
        assert len(qb._specs) == 1
        assert qb._specs[0].op_type == "upsert"
        assert qb._op_type is None
        assert qb._single_key == _make_key(2)

    def test_transition_to_another_write(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)

        wsb.put({"name": "Alice"})
        result = wsb.insert(_make_key(2))

        assert result is wsb
        assert len(qb._specs) == 1
        assert qb._specs[0].op_type == "upsert"
        assert qb._op_type == "insert"

    def test_transition_to_delete(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)
        wsb.delete(_make_key(2))

        assert len(qb._specs) == 1
        assert qb._op_type == "delete"

    def test_where_setting(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)

        result = wsb.where("$.age > 21")
        assert result is wsb
        assert qb._filter_expression is not None

    def test_ttl_setting(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)

        wsb.expire_record_after_seconds(600)
        assert qb._ttl_seconds == 600

    def test_generation_setting(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)

        wsb.ensure_generation_is(5)
        assert qb._generation == 5

    def test_durable_delete_setting(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "delete"
        wsb = WriteSegmentBuilder(qb)

        wsb.with_durable_delete()
        assert qb._durable_delete is True

    def test_direct_set_to(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)
        result = wsb.set_to("name", "Alice")
        assert result is wsb
        assert len(qb._operations) == 1

    def test_direct_add(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)
        wsb.add("counter", 5)
        assert len(qb._operations) == 1

    def test_direct_get(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)
        wsb.get("counter")
        assert len(qb._operations) == 1

    def test_direct_append(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)
        wsb.append("log", " entry")
        assert len(qb._operations) == 1

    def test_direct_prepend(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)
        wsb.prepend("log", "prefix ")
        assert len(qb._operations) == 1

    def test_direct_remove_bin(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)
        wsb.remove_bin("temp")
        assert len(qb._operations) == 1

    def test_direct_chained_add_get(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        wsb = WriteSegmentBuilder(qb)
        wsb.add("counter", 30).get("counter")
        assert len(qb._operations) == 2

    def test_both_styles_equivalent(self):
        """Direct segment ops and bin-first ops produce the same operations."""
        qb_direct = _make_builder()
        qb_direct._single_key = _make_key()
        qb_direct._op_type = "upsert"
        wsb_direct = WriteSegmentBuilder(qb_direct)
        wsb_direct.set_to("name", "Alice").add("counter", 5).get("counter")

        qb_bin = _make_builder()
        qb_bin._single_key = _make_key()
        qb_bin._op_type = "upsert"
        wsb_bin = WriteSegmentBuilder(qb_bin)
        wsb_bin.bin("name").set_to("Alice").bin("counter").add(5).bin("counter").get()

        assert len(qb_direct._operations) == len(qb_bin._operations) == 3
        for op_d, op_b in zip(qb_direct._operations, qb_bin._operations):
            assert type(op_d) is type(op_b)


# ---------------------------------------------------------------------------
# WriteBinBuilder
# ---------------------------------------------------------------------------

class TestWriteBinBuilder:
    def _make_wsb(self):
        qb = _make_builder()
        qb._single_key = _make_key()
        qb._op_type = "upsert"
        return WriteSegmentBuilder(qb), qb

    def test_set_to(self):
        wsb, qb = self._make_wsb()
        result = wsb.bin("name").set_to("Alice")
        assert result is wsb
        assert len(qb._operations) == 1

    def test_add(self):
        wsb, qb = self._make_wsb()
        wsb.bin("counter").add(5)
        assert len(qb._operations) == 1

    def test_append(self):
        wsb, qb = self._make_wsb()
        wsb.bin("log").append(" entry")
        assert len(qb._operations) == 1

    def test_prepend(self):
        wsb, qb = self._make_wsb()
        wsb.bin("log").prepend("prefix ")
        assert len(qb._operations) == 1

    def test_remove(self):
        wsb, qb = self._make_wsb()
        wsb.bin("temp").remove()
        assert len(qb._operations) == 1

    def test_chained_bins(self):
        wsb, qb = self._make_wsb()
        wsb.bin("name").set_to("Alice").bin("age").set_to(25).bin("score").add(10)
        assert len(qb._operations) == 3

    def test_bin_to_bin_transition(self):
        wsb, qb = self._make_wsb()
        wbb = wsb.bin("first")
        result = wbb.bin("second")
        assert isinstance(result, WriteBinBuilder)

    def test_expression_select_from(self):
        wsb, qb = self._make_wsb()
        wsb.bin("computed").select_from("$.a + $.b")
        assert len(qb._operations) == 1

    def test_expression_upsert_from(self):
        wsb, qb = self._make_wsb()
        wsb.bin("derived").upsert_from("$.x * 2")
        assert len(qb._operations) == 1

    def test_transition_from_bin_to_query(self):
        wsb, qb = self._make_wsb()
        result = wsb.bin("name").set_to("Alice").query(_make_key(2))
        assert result is qb
        assert len(qb._specs) == 1
        assert qb._specs[0].op_type == "upsert"

    def test_transition_from_bin_to_upsert(self):
        wsb, qb = self._make_wsb()
        result = wsb.bin("name").set_to("Alice").upsert(_make_key(2))
        assert result is wsb
        assert len(qb._specs) == 1


# ---------------------------------------------------------------------------
# QueryBuilder write transitions
# ---------------------------------------------------------------------------

class TestQueryBuilderWriteTransitions:
    def test_query_to_upsert(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        wsb = qb.upsert(_make_key(2))

        assert isinstance(wsb, WriteSegmentBuilder)
        assert len(qb._specs) == 1
        assert qb._specs[0].op_type is None
        assert qb._op_type == "upsert"

    def test_query_to_insert(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        wsb = qb.insert(_make_key(2))

        assert isinstance(wsb, WriteSegmentBuilder)
        assert qb._op_type == "insert"

    def test_query_to_delete(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        wsb = qb.delete(_make_key(2))

        assert isinstance(wsb, WriteSegmentBuilder)
        assert qb._op_type == "delete"

    def test_roundtrip_query_upsert_query(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)

        wsb = qb.upsert(_make_key(2))
        wsb.bin("name").set_to("Alice")
        result = wsb.query(_make_key(3))

        assert result is qb
        assert len(qb._specs) == 2
        assert qb._specs[0].op_type is None
        assert qb._specs[1].op_type == "upsert"
        assert len(qb._specs[1].operations) == 1

    def test_multi_segment_chain(self):
        """session.query(k1).bin("x").get().upsert(k2).bin("y").set_to(1).delete(k3)"""
        qb = _make_builder()
        qb._single_key = _make_key(1)

        qb.add_operation(object())  # simulated read op

        wsb = qb.upsert(_make_key(2))
        wsb.bin("y").set_to(1)
        wsb.delete(_make_key(3))

        qb._finalize_current_spec()
        assert len(qb._specs) == 3
        assert qb._specs[0].op_type is None
        assert qb._specs[1].op_type == "upsert"
        assert qb._specs[2].op_type == "delete"

    def test_multiple_keys_in_write(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)

        k2, k3 = _make_key(2), _make_key(3)
        wsb = qb.upsert([k2, k3])
        wsb.bin("name").set_to("bulk")

        qb._finalize_current_spec()
        assert len(qb._specs) == 2
        assert len(qb._specs[1].keys) == 2


# ---------------------------------------------------------------------------
# Per-spec & chain-level settings
# ---------------------------------------------------------------------------

class TestPerSpecSettings:
    def test_where_captured_in_spec(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        wsb = qb.upsert(_make_key(2))
        wsb.bin("x").set_to(1)
        wsb.where("$.status == 'ACTIVE'")
        wsb.delete(_make_key(3))

        assert len(qb._specs) == 2
        assert qb._specs[1].filter_expression is not None

    def test_ttl_captured_in_spec(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        wsb = qb.upsert(_make_key(2))
        wsb.expire_record_after_seconds(300)
        wsb.delete(_make_key(3))

        assert qb._specs[1].ttl_seconds == 300

    def test_generation_captured_in_spec(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        wsb = qb.upsert(_make_key(2))
        wsb.ensure_generation_is(7)
        wsb.delete(_make_key(3))

        assert qb._specs[1].generation == 7

    def test_durable_delete_captured_in_spec(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        wsb = qb.delete(_make_key(2))
        wsb.with_durable_delete()
        qb._finalize_current_spec()

        assert qb._specs[1].durable_delete is True

    def test_settings_reset_between_specs(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        wsb = qb.upsert(_make_key(2))
        wsb.expire_record_after_seconds(300)
        wsb.ensure_generation_is(5)
        wsb.upsert(_make_key(3))
        qb._finalize_current_spec()

        assert qb._specs[1].ttl_seconds == 300
        assert qb._specs[1].generation == 5
        assert qb._specs[2].ttl_seconds is None
        assert qb._specs[2].generation is None


class TestChainLevelDefaults:
    def test_default_where_applied(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        qb.default_where("$.active == true")

        wsb = qb.upsert(_make_key(2))
        wsb.bin("x").set_to(1)
        qb._finalize_current_spec()

        assert qb._specs[0].filter_expression is not None
        assert qb._specs[1].filter_expression is not None

    def test_per_spec_where_overrides_default(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        qb.default_where("$.active == true")

        wsb = qb.upsert(_make_key(2))
        wsb.where("$.status == 'VIP'")
        qb._finalize_current_spec()

        spec0_filter = qb._specs[0].filter_expression
        spec1_filter = qb._specs[1].filter_expression
        assert spec0_filter is not None
        assert spec1_filter is not None
        assert spec0_filter is not spec1_filter

    def test_default_ttl_applied(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        qb.default_expire_record_after_seconds(600)

        wsb = qb.upsert(_make_key(2))
        qb._finalize_current_spec()

        assert qb._specs[1].ttl_seconds == 600

    def test_per_spec_ttl_overrides_default(self):
        qb = _make_builder()
        qb._single_key = _make_key(1)
        qb.default_expire_record_after_seconds(600)

        wsb = qb.upsert(_make_key(2))
        wsb.expire_record_after_seconds(120)
        qb._finalize_current_spec()

        assert qb._specs[1].ttl_seconds == 120


# ---------------------------------------------------------------------------
# _make_write_policy integration
# ---------------------------------------------------------------------------

class TestMakeWritePolicy:
    def test_upsert_has_no_rea(self):
        qb = _make_builder()
        spec = _OperationSpec(keys=[_make_key()], op_type="upsert")
        wp = qb._make_write_policy(spec)
        assert isinstance(wp, WritePolicy)

    def test_insert_has_create_only(self):
        qb = _make_builder()
        spec = _OperationSpec(keys=[_make_key()], op_type="insert")
        wp = qb._make_write_policy(spec)
        assert wp.record_exists_action == RecordExistsAction.CREATE_ONLY

    def test_filter_expression_applied(self):
        qb = _make_builder()
        filt = Exp.gt(Exp.int_bin("a"), Exp.int_val(10))
        spec = _OperationSpec(
            keys=[_make_key()], op_type="upsert",
            filter_expression=filt,
        )
        wp = qb._make_write_policy(spec)
        assert wp.filter_expression is not None

    def test_ttl_applied(self):
        qb = _make_builder()
        spec = _OperationSpec(
            keys=[_make_key()], op_type="upsert", ttl_seconds=300,
        )
        wp = qb._make_write_policy(spec)
        assert wp.expiration is not None

    def test_durable_delete_applied(self):
        qb = _make_builder()
        spec = _OperationSpec(
            keys=[_make_key()], op_type="delete", durable_delete=True,
        )
        wp = qb._make_write_policy(spec)
        assert wp.durable_delete is True
