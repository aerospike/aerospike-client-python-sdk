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

"""Unit tests for session.background_task() builders."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from aerospike_async import Expiration, Filter, Key, MapOperation, MapPolicy, Operation

from aerospike_sdk.aio.background import (
    BackgroundOperationBuilder,
    BackgroundTaskSession,
    BackgroundUdfFunctionBuilder,
    _OpType,
)
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.background_shared import make_background_write_policy
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.policy.behavior import Behavior


def _session_mock() -> MagicMock:
    from aerospike_sdk.policy.behavior_settings import Mode

    s = MagicMock()
    s.behavior = Behavior.DEFAULT
    fc = MagicMock()
    fc._client = MagicMock()
    s.client = fc
    s._resolve_namespace_mode = AsyncMock(return_value=Mode.AP)
    return s


def test_update_builder_produces_put_operation():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    b.bin("x").set_to(1)
    assert len(b._operations) == 1
    assert b._operations[0] is not None


def test_update_builder_produces_add_operation():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    b.bin("x").add(10)
    assert len(b._operations) == 1


async def test_delete_auto_adds_delete_op():
    s = _session_mock()
    s.client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    await BackgroundOperationBuilder(s, ds, _OpType.DELETE).execute()
    _stmt, ops = s.client._client.query_operate.call_args[0]
    assert len(ops) == 1
    assert ops[0] is not None


async def test_touch_auto_adds_touch_op():
    s = _session_mock()
    s.client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    await BackgroundOperationBuilder(s, ds, _OpType.TOUCH).execute()
    _stmt, ops = s.client._client.query_operate.call_args[0]
    assert len(ops) == 1


async def test_update_with_no_ops_raises():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    with pytest.raises(ValueError, match="at least one bin operation"):
        await BackgroundOperationBuilder(s, ds, _OpType.UPDATE).execute()


def test_where_sets_filter_expression():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    b.where("$.age > 30")
    assert b._filter_expression is not None


def test_index_filters_stores_filters():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    b.index_filters(Filter.range("bgval", 9, 10))
    assert len(b._index_filters) == 1


def test_index_filters_empty_raises():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    with pytest.raises(ValueError, match="at least one"):
        b.index_filters()


def test_where_mutex_with_index_filters():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    b.index_filters(Filter.range("bgval", 9, 10))
    with pytest.raises(ValueError, match="index_filters"):
        b.where("$.bgval > 8")


def test_index_filters_mutex_with_where():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    b.where("$.bgval > 8")
    with pytest.raises(ValueError, match="where"):
        b.index_filters(Filter.range("bgval", 9, 10))


async def test_delete_execute_passes_statement_index_filters():
    s = _session_mock()
    s.client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    b.index_filters(Filter.range("n", 1, 3))
    await b.execute()
    stmt, ops = s.client._client.query_operate.call_args[0]
    assert stmt.filters is not None
    assert len(ops) == 1


def test_expire_record_after_seconds_wired():
    wp = make_background_write_policy(
        Behavior.DEFAULT,
        None,
        3600,
        None,
    )
    assert wp.expiration == Expiration.seconds(3600)


def test_records_per_second_stored_on_builder():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = (
        BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
        .records_per_second(5000)
    )
    assert b._records_per_second == 5000


async def test_rejects_cdt_operations():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    mp = MapPolicy(None, None)
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    b.bin("k").set_to(1)
    b._operations.append(MapOperation.put("m", "k", 1, mp))
    with pytest.raises(AerospikeError):
        await b.execute()


def test_udf_function_builder_has_no_execute():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    fb = BackgroundUdfFunctionBuilder(s, ds)
    assert not hasattr(fb, "execute")


def test_udf_rejects_empty_package():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    fb = BackgroundUdfFunctionBuilder(s, ds)
    with pytest.raises(ValueError, match="package_name"):
        fb.function("", "fn")


def test_udf_rejects_empty_function():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    fb = BackgroundUdfFunctionBuilder(s, ds)
    with pytest.raises(ValueError, match="function_name"):
        fb.function("pkg", "")


def test_fail_on_filtered_out_raises():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundTaskSession(s).update(ds)
    with pytest.raises(TypeError, match="fail_on_filtered_out"):
        b.fail_on_filtered_out()


async def test_execute_background_task_requires_write_ops():
    client = MagicMock()
    qb = QueryBuilder(client, "test", "bgset")
    with pytest.raises(ValueError, match="At least one write operation"):
        await qb.execute_background_task()


async def test_execute_background_task_rejects_key_chain():
    client = MagicMock()
    qb = QueryBuilder(client, "test", "bgset")
    qb._set_current_keys(Key("test", "bgset", 1))
    qb.with_write_operations([Operation.put("x", 1)])
    with pytest.raises(ValueError, match="dataset queries"):
        await qb.execute_background_task()


async def test_execute_background_task_rejects_map_operation():
    client = MagicMock()
    qb = QueryBuilder(client, "test", "bgset")
    mp = MapPolicy(None, None)
    qb.with_write_operations([MapOperation.put("m", "k", 1, mp)])
    with pytest.raises(AerospikeError) as ei:
        await qb.execute_background_task()
    assert ei.value.result_code is not None


async def test_execute_udf_background_task_rejects_with_write_ops():
    client = MagicMock()
    qb = QueryBuilder(client, "test", "bgset")
    qb.with_write_operations([Operation.put("x", 1)])
    with pytest.raises(ValueError, match="Do not combine"):
        await qb.execute_udf_background_task("pkg", "fn")


async def test_execute_udf_background_task_rejects_key_chain():
    client = MagicMock()
    qb = QueryBuilder(client, "test", "bgset")
    qb._set_current_keys(Key("test", "bgset", 1))
    with pytest.raises(ValueError, match="dataset queries"):
        await qb.execute_udf_background_task("pkg", "fn")
