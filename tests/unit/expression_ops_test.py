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

"""Unit tests for expression bin operations.

Covers:
- _build_exp_write_flags bitmask construction
- parse_ael -> FilterExpression conversion
- QueryBinBuilder.select_from
- OP_NOT_APPLICABLE guard on dataset queries with expression ops
- BatchBinBuilder expression methods
"""

import pytest
from unittest.mock import MagicMock

from aerospike_async import (
    ExpReadFlags,
    ExpWriteFlags,
    FilterExpression,
    Key,
)
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.aio.operations.batch import (
    BatchBinBuilder,
    BatchKeyOperationBuilder,
    BatchOperationBuilder,
)
from aerospike_sdk.aio.operations.query import QueryBinBuilder, QueryBuilder
from aerospike_sdk.ael.parser import parse_ael
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.operations_shared import BatchOpType, _build_exp_write_flags

from tests.pac_compat import xfail_if_server_compiled_ael_factory_exposed

_EXP_READ_DEFAULT = ExpReadFlags.DEFAULT
_EXP_READ_EVAL_NO_FAIL = ExpReadFlags.EVAL_NO_FAIL
_EXP_WRITE_DEFAULT = ExpWriteFlags.DEFAULT
_EXP_WRITE_CREATE_ONLY = ExpWriteFlags.CREATE_ONLY
_EXP_WRITE_UPDATE_ONLY = ExpWriteFlags.UPDATE_ONLY
_EXP_WRITE_ALLOW_DELETE = ExpWriteFlags.ALLOW_DELETE
_EXP_WRITE_POLICY_NO_FAIL = ExpWriteFlags.POLICY_NO_FAIL
_EXP_WRITE_EVAL_NO_FAIL = ExpWriteFlags.EVAL_NO_FAIL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _OpCollector:
    """Minimal parent that satisfies the add_operation(op) protocol."""

    def __init__(self):
        self.operations: list = []

    def add_operation(self, op):
        self.operations.append(op)


# ===================================================================
# _build_exp_write_flags
# ===================================================================

class TestBuildWriteFlags:

    def test_default_no_options(self):
        flags = _build_exp_write_flags(_EXP_WRITE_DEFAULT, False, False, False)
        assert flags == _EXP_WRITE_DEFAULT

    def test_create_only_base(self):
        flags = _build_exp_write_flags(_EXP_WRITE_CREATE_ONLY, False, False, False)
        assert flags == _EXP_WRITE_CREATE_ONLY

    def test_update_only_base(self):
        flags = _build_exp_write_flags(_EXP_WRITE_UPDATE_ONLY, False, False, False)
        assert flags == _EXP_WRITE_UPDATE_ONLY

    def test_ignore_op_failure(self):
        flags = _build_exp_write_flags(_EXP_WRITE_DEFAULT, True, False, False)
        assert flags & _EXP_WRITE_POLICY_NO_FAIL

    def test_ignore_eval_failure(self):
        flags = _build_exp_write_flags(_EXP_WRITE_DEFAULT, False, True, False)
        assert flags & _EXP_WRITE_EVAL_NO_FAIL

    def test_delete_if_null(self):
        flags = _build_exp_write_flags(_EXP_WRITE_DEFAULT, False, False, True)
        assert flags & _EXP_WRITE_ALLOW_DELETE

    def test_all_options_combined(self):
        flags = _build_exp_write_flags(
            _EXP_WRITE_CREATE_ONLY, True, True, True,
        )
        assert flags & _EXP_WRITE_CREATE_ONLY
        assert flags & _EXP_WRITE_POLICY_NO_FAIL
        assert flags & _EXP_WRITE_EVAL_NO_FAIL
        assert flags & _EXP_WRITE_ALLOW_DELETE

    def test_flags_are_ints(self):
        flags = _build_exp_write_flags(_EXP_WRITE_UPDATE_ONLY, True, True, True)
        assert isinstance(flags, int)


# ===================================================================
# parse_ael
# ===================================================================

class TestParseAel:

    def test_string_converted_via_parse_ael(self):
        result = parse_ael("$.age + 1")
        assert isinstance(result, FilterExpression)


# ===================================================================
# QueryBinBuilder.select_from
# ===================================================================

class TestQueryBinBuilderSelectFrom:

    def test_select_from_string(self):
        xfail_if_server_compiled_ael_factory_exposed()
        collector = _OpCollector()
        qbb = QueryBinBuilder(collector, "ev")
        result = qbb.select_from("$.A + 4")
        assert result is collector
        assert len(collector.operations) == 1

    def test_select_from_filter_expression(self):
        expr = parse_ael("$.A + 4")
        collector = _OpCollector()
        qbb = QueryBinBuilder(collector, "ev")
        qbb.select_from(expr)
        assert len(collector.operations) == 1

    def test_select_from_ignore_eval_failure(self):
        xfail_if_server_compiled_ael_factory_exposed()
        collector = _OpCollector()
        qbb = QueryBinBuilder(collector, "ev")
        qbb.select_from("$.A + 4", ignore_eval_failure=True)
        assert len(collector.operations) == 1

    def test_multiple_select_from(self):
        xfail_if_server_compiled_ael_factory_exposed()
        collector = _OpCollector()
        QueryBinBuilder(collector, "r1").select_from("$.A == 0 and $.D == 2")
        QueryBinBuilder(collector, "r2").select_from("$.A == 0 or $.D == 2")
        assert len(collector.operations) == 2


# ===================================================================
# Dataset query guard
# ===================================================================

class TestDatasetQueryGuard:

    async def test_select_from_on_dataset_query_raises(self):
        qb = QueryBuilder(client=MagicMock(), namespace="test", set_name="s")
        qb.bin("ev").select_from("$.A + 4")
        with pytest.raises(AerospikeError) as exc_info:
            await qb.execute()
        assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE


# ===================================================================
# BatchBinBuilder expression methods
# ===================================================================

class TestBatchBinBuilderExpression:

    def _make_batch_builder(self, bin_name: str = "ev"):
        batch = BatchOperationBuilder(client=MagicMock())
        key_op = BatchKeyOperationBuilder(batch, Key("test", "s", "k1"), BatchOpType.UPDATE)
        return BatchBinBuilder(key_op, bin_name), key_op

    def test_select_from_string(self):
        bb, key_op = self._make_batch_builder("ev")
        result = bb.select_from("$.A + 4")
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_insert_from_string(self):
        bb, key_op = self._make_batch_builder("c")
        result = bb.insert_from("$.A + 4")
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_update_from_string(self):
        bb, key_op = self._make_batch_builder("c")
        result = bb.update_from("$.A + 4")
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_upsert_from_string(self):
        bb, key_op = self._make_batch_builder("c")
        result = bb.upsert_from("$.A + 4")
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_upsert_from_with_flags(self):
        bb, key_op = self._make_batch_builder("c")
        bb.upsert_from("$.A + 4", ignore_op_failure=True, ignore_eval_failure=True, delete_if_null=True)
        assert len(key_op._operations) == 1

    def test_chaining_set_to_and_expression(self):
        batch = BatchOperationBuilder(client=MagicMock())
        key_op = BatchKeyOperationBuilder(batch, Key("test", "s", "k1"), BatchOpType.UPSERT)
        key_op.bin("name").set_to("Alice").bin("computed").upsert_from("$.age * 2")
        assert len(key_op._operations) == 2
