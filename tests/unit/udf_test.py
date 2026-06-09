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

from unittest.mock import AsyncMock, MagicMock

import pytest
from aerospike_async import Key

from aerospike_sdk.aio.operations.query import QueryBuilder, _OperationSpec
from aerospike_sdk.aio.operations.udf import UdfFunctionBuilder
from aerospike_sdk.policy.behavior import Behavior


def _connected_qb() -> QueryBuilder:
    client = MagicMock()
    client.execute_udf = AsyncMock(return_value="rv")
    client.batch_apply = AsyncMock(return_value=[])
    return QueryBuilder(client, "test", "set", Behavior.DEFAULT)


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
    await (
        UdfFunctionBuilder(qb)
        .function("pkg", "fn")
        .where("$.x == 1")
        .execute()
    )
    wp = qb._client.execute_udf.await_args.kwargs["policy"]
    assert wp.filter_expression is not None
