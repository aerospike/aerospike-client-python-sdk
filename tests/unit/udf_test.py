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
# License for the specific language governing permissions and limitations
# under the License.

"""Unit tests for foreground UDF chainable builders."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from aerospike_async import FilterExpression

from aerospike_sdk import Key

from aerospike_sdk.aio.operations.query import QueryBuilder, _OperationSpec
from aerospike_sdk.aio.operations.udf import UdfFunctionBuilder
from aerospike_sdk.policy.behavior import Behavior


def _connected_qb() -> QueryBuilder:
    client = MagicMock()
    client.execute_udf = AsyncMock(return_value="rv")
    client.batch_apply = AsyncMock(return_value=[])
    return QueryBuilder(
        client,
        "test",
        "set",
        Behavior.DEFAULT,
        supports_server_compiled_ael=True,
    )


def test_function_builder_has_no_execute():
    qb = _connected_qb()
    qb._set_current_keys_from_varargs((Key("test", "set", 1),))
    fb = UdfFunctionBuilder(qb)
    assert not hasattr(fb, "execute")


def test_rejects_empty_package():
    qb = _connected_qb()
    qb._set_current_keys_from_varargs((Key("test", "set", 1),))
    fb = UdfFunctionBuilder(qb)
    with pytest.raises(ValueError, match="package"):
        fb.function("", "fn")


def test_rejects_empty_function():
    qb = _connected_qb()
    qb._set_current_keys_from_varargs((Key("test", "set", 1),))
    fb = UdfFunctionBuilder(qb)
    with pytest.raises(ValueError, match="function_name"):
        fb.function("pkg", "")


async def test_passing_accumulates_args():
    qb = _connected_qb()
    qb._set_current_keys_from_varargs((Key("test", "set", 1),))
    b = UdfFunctionBuilder(qb).function("pkg", "fn").passing("a", 1)
    await b.execute()
    qb._client.execute_udf.assert_awaited()
    call = qb._client.execute_udf.await_args
    # call.args is (key, server_path, function_name, args)
    assert call[0][3] == ["a", 1]


async def test_single_key_routing():
    qb = _connected_qb()
    k = Key("test", "set", 1)
    qb._set_current_keys_from_varargs((k,))
    await UdfFunctionBuilder(qb).function("record_example", "readBin").passing("b").execute()
    qb._client.execute_udf.assert_awaited_once()
    qb._client.batch_apply.assert_not_called()


async def test_multi_key_routing():
    qb = _connected_qb()
    keys = (Key("test", "set", 1), Key("test", "set", 2))
    qb._set_current_keys_from_varargs(keys)
    qb._client.batch_apply = AsyncMock(return_value=[])
    await (
        UdfFunctionBuilder(qb)
        .function("record_example", "writeBin")
        .passing("B", 1)
        .execute()
    )
    qb._client.batch_apply.assert_awaited_once()
    qb._client.execute_udf.assert_not_called()


def test_udf_spec_type_in_operation_spec():
    s = _OperationSpec(
        keys=[Key("test", "set", 1)],
        op_type="udf",
        udf_package="p",
        udf_function="f",
        udf_args=[1],
    )
    assert s.op_type == "udf"
    assert s.udf_package == "p"


async def test_where_sets_filter_on_builder():
    qb = _connected_qb()
    qb._set_current_keys_from_varargs((Key("test", "set", 1),))
    with patch(
        "aerospike_sdk.query_shared.filter_expression_from_ael_string",
        side_effect=lambda ael, *, supports_server_compiled_ael=True: (
            FilterExpression.from_server_compiled_ael(ael)
        ),
    ):
        await (
            UdfFunctionBuilder(qb)
            .function("pkg", "fn")
            .where("$.x == 1")
            .execute()
        )
    wp = qb._client.execute_udf.await_args.kwargs["policy"]
    assert wp.filter_expression is not None


class TestChainToUdfTransition:
    """Chain-shape tests for the query/write -> UDF forward transition."""

    def test_query_chain_returns_function_builder(self):
        qb = _connected_qb()
        k1, k2 = Key("test", "set", 1), Key("test", "set", 2)
        qb._set_current_keys(k1)
        fb = qb.execute_udf(k2)
        assert type(fb) is UdfFunctionBuilder
        assert fb._qb is qb

    def test_query_chain_finalizes_read_spec_and_targets_new_key(self):
        qb = _connected_qb()
        k1, k2 = Key("test", "set", 1), Key("test", "set", 2)
        qb._set_current_keys(k1)
        qb.execute_udf(k2)
        assert len(qb._specs) == 1
        assert qb._specs[0].op_type is None
        assert qb._specs[0].keys == [k1]
        assert qb._single_key == k2

    def test_write_segment_transition_finalizes_write_spec(self):
        qb = _connected_qb()
        k1, k2 = Key("test", "set", 1), Key("test", "set", 2)
        seg = qb._start_write_verb("upsert", k1)
        seg.put({"a": 1})
        fb = seg.execute_udf(k2)
        assert type(fb) is UdfFunctionBuilder
        assert len(qb._specs) == 1
        assert qb._specs[0].op_type == "upsert"
        assert len(qb._specs[0].operations) == 1
        assert qb._single_key == k2

    def test_bin_builder_transition(self):
        qb = _connected_qb()
        k1, k2 = Key("test", "set", 1), Key("test", "set", 2)
        fb = (
            qb._start_write_verb("upsert", k1)
            .bin("count").set_to(5)
            .execute_udf(k2)
        )
        assert type(fb) is UdfFunctionBuilder
        assert qb._specs[0].op_type == "upsert"

    def test_single_key_fast_segment_promotes_then_transitions(self):
        from aerospike_sdk.aio.operations.query import _SingleKeyWriteSegment

        k1, k2 = Key("test", "set", 1), Key("test", "set", 2)
        seg = _SingleKeyWriteSegment(
            MagicMock(), k1, "upsert", Behavior.DEFAULT, None,
        )
        seg.put({"a": 1})
        fb = seg.execute_udf(k2)
        assert type(fb) is UdfFunctionBuilder
        assert seg._qb is not None
        assert seg._qb._specs[0].op_type == "upsert"
        assert seg._qb._single_key == k2

    def test_multi_key_transition_sets_batch_keys(self):
        qb = _connected_qb()
        k1 = Key("test", "set", 1)
        batch = (Key("test", "set", 2), Key("test", "set", 3))
        qb._set_current_keys(k1)
        qb.execute_udf(*batch)
        assert qb._keys == list(batch)
        assert qb._single_key is None

    def test_requires_at_least_one_key(self):
        qb = _connected_qb()
        qb._set_current_keys(Key("test", "set", 1))
        with pytest.raises(ValueError, match="At least one key"):
            qb.execute_udf()

    def test_dataset_query_cannot_transition(self):
        qb = _connected_qb()
        with pytest.raises(ValueError, match="Dataset"):
            qb.execute_udf(Key("test", "set", 1))

    def test_function_then_execute_udf_round_trip_specs(self):
        qb = _connected_qb()
        k1, k2 = Key("test", "set", 1), Key("test", "set", 2)
        qb._set_current_keys(k1)
        ub = qb.execute_udf(k2).function("pkg", "fn").passing(7)
        ub._qb._finalize_udf_spec()
        assert [s.op_type for s in qb._specs] == [None, "udf"]
        assert qb._specs[1].udf_package == "pkg"
        assert qb._specs[1].udf_args == [7]

    def test_sync_chain_returns_sync_builder(self):
        # Cross-surface invariant: the ClassVar hook must hand back the
        # sync leaf type when the chain started on the sync surface.
        from aerospike_sdk.sync.operations.query import QueryBuilder as SyncQB
        from aerospike_sdk.sync.operations.udf import (
            UdfFunctionBuilder as SyncUFB,
        )

        qb = SyncQB(MagicMock(), "test", "set", Behavior.DEFAULT)
        qb._set_current_keys(Key("test", "set", 1))
        fb = qb.execute_udf(Key("test", "set", 2))
        assert type(fb) is SyncUFB
