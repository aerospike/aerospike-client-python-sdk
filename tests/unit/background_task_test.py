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
from types import SimpleNamespace

import pytest
from aerospike_sdk import Filter, HllConfig, Txn
from aerospike_async import (
    ExpOperation,
    Expiration,
    HllOperation,
    ListOperation,
    MapOperation,
    MapPolicy,
    Operation,
)

from aerospike_sdk.aio.background import (
    BackgroundOperationBuilder,
    BackgroundTaskSession,
    BackgroundUdfFunctionBuilder,
    _OpType,
)
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.background_shared import make_background_write_policy
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode
from aerospike_sdk.metrics import usage
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.sync.background import (
    BackgroundOperationBuilder as SyncBackgroundOperationBuilder,
    BackgroundUdfBuilder as SyncBackgroundUdfBuilder,
)
from aerospike_sdk.sync.operations.query import QueryBuilder as SyncQueryBuilder


def _session_mock() -> MagicMock:
    s = MagicMock()
    s.behavior = Behavior.DEFAULT
    s.current_transaction = None
    fc = MagicMock()
    fc._client = MagicMock()
    s._client = fc
    s._resolve_namespace_mode = AsyncMock(return_value=Mode.AP)
    return s


def _usage_sdk():
    """SDK-client stand-in that captures each flushed feature set.

    ``sdk.counted`` collects one entry per counted call, so a test can assert
    the command count and the usage flush agree about what a call is.
    """
    calls = []
    counted = []
    sdk = SimpleNamespace(
        _record_on=True,
        _cmd_count_on=True,
        _usage_on=True,
        _usage_counters=SimpleNamespace(add=lambda features: calls.append(list(features))),
        _command_counts=SimpleNamespace(add=counted.append),
        supports_server_compiled_ael=False,
        supports_query_selection=False,
    )
    sdk.counted = counted
    return sdk, calls


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
    s._client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    await BackgroundOperationBuilder(s, ds, _OpType.DELETE).execute()
    _stmt, ops = s._client._client.query_operate.call_args[0]
    assert len(ops) == 1
    assert ops[0] is not None


async def test_touch_auto_adds_touch_op():
    s = _session_mock()
    s._client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    await BackgroundOperationBuilder(s, ds, _OpType.TOUCH).execute()
    _stmt, ops = s._client._client.query_operate.call_args[0]
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


def test_filter_stores_filter():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    f = Filter.range("bgval", 9, 10)
    assert b.filter(f) is b
    assert b._filter is f


def test_filter_rejects_a_second_call():
    """The server accepts one index range per query, so a second filter is an error."""
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    b.filter(Filter.range("bgval", 9, 10))
    with pytest.raises(ValueError, match="once"):
        b.filter(Filter.equal("state", "CO"))


def test_filter_rejects_none():
    """A missing filter would widen the job to the whole set."""
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    with pytest.raises(TypeError):
        b.filter(None)


def test_where_after_filter_keeps_both():
    """The index narrows through the index; the predicate filters what it returns."""
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    b.filter(Filter.range("bgval", 9, 10))
    b.where("$.bgval > 8")
    assert b._filter is not None
    assert b._filter_expression is not None


def test_filter_after_where_keeps_both():
    """Declaration order must not matter."""
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    b.where("$.bgval > 8")
    b.filter(Filter.range("bgval", 9, 10))
    assert b._filter is not None
    assert b._filter_expression is not None


async def test_execute_sends_filter_and_filter_expression_together():
    """Both channels reach PAC: the statement filter and the write policy's expression."""
    s = _session_mock()
    s._client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    b.bin("x").set_to(1)
    await b.filter(Filter.range("n", 1, 3)).where("$.n > 1").execute()
    stmt, _ops = s._client._client.query_operate.call_args[0]
    wp = s._client._client.query_operate.call_args.kwargs["write_policy"]
    assert len(stmt.filters) == 1
    assert wp.filter_expression is not None


def test_blocking_execute_sends_filter_and_filter_expression_together():
    """The sync tree drives this terminal, so it needs its own arm."""
    s = _session_mock()
    s._client._client.query_operate_blocking = MagicMock(return_value=MagicMock())
    s._resolve_namespace_mode_blocking = MagicMock(return_value=Mode.AP)
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    b.bin("x").set_to(1)
    b.filter(Filter.range("n", 1, 3)).where("$.n > 1")._execute_blocking()
    call = s._client._client.query_operate_blocking.call_args
    stmt, _ops = call[0]
    assert len(stmt.filters) == 1
    assert call.kwargs["write_policy"].filter_expression is not None


def _udf_builder():
    s = _session_mock()
    s._resolve_namespace_mode_blocking = MagicMock(return_value=Mode.AP)
    return BackgroundUdfFunctionBuilder(s, DataSet.of("test", "bgset")).function("pkg", "fn")


def test_udf_filter_rejects_a_second_call():
    b = _udf_builder().filter(Filter.range("bgval", 9, 10))
    with pytest.raises(ValueError, match="once"):
        b.filter(Filter.equal("state", "CO"))


def test_udf_filter_rejects_none():
    with pytest.raises(TypeError):
        _udf_builder().filter(None)


async def test_udf_execute_sends_filter_and_filter_expression_together():
    b = _udf_builder()
    pac = b._session._client._client
    pac.query_execute_udf = AsyncMock(return_value=MagicMock())
    await b.filter(Filter.range("n", 1, 3)).where("$.n > 1").execute()
    call = pac.query_execute_udf.call_args
    assert len(call[0][0].filters) == 1
    assert call.kwargs["write_policy"].filter_expression is not None


def test_sync_udf_filter_reaches_the_blocking_terminal():
    inner = _udf_builder()
    pac = inner._session._client._client
    pac.query_execute_udf_blocking = MagicMock(return_value=MagicMock())
    b = SyncBackgroundUdfBuilder(inner)
    assert b.filter(Filter.range("n", 1, 3)) is b
    b.execute()
    assert len(pac.query_execute_udf_blocking.call_args[0][0].filters) == 1


async def test_delete_execute_passes_statement_filter():
    s = _session_mock()
    s._client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.DELETE)
    b.filter(Filter.range("n", 1, 3))
    await b.execute()
    stmt, ops = s._client._client.query_operate.call_args[0]
    assert len(stmt.filters) == 1
    assert len(ops) == 1


def test_expire_record_after_seconds_wired():
    wp = make_background_write_policy(
        Behavior.DEFAULT,
        None,
        3600,
        None,
    )
    assert wp.expiration == Expiration.seconds(3600)


_EXPIRY_VERBS = [
    ("never_expire", -1, Expiration.NEVER_EXPIRE),
    ("with_no_change_in_expiration", -2, Expiration.DONT_UPDATE),
    ("expiry_from_server_default", 0, Expiration.NAMESPACE_DEFAULT),
]


@pytest.mark.parametrize("verb, _ttl, expected", _EXPIRY_VERBS)
async def test_expiry_verbs_reach_the_policy_pac_receives(verb, _ttl, expected):
    s = _session_mock()
    s._client._client.query_operate = AsyncMock(return_value=MagicMock())
    b = BackgroundOperationBuilder(s, DataSet.of("test", "bgset"), _OpType.TOUCH)
    assert getattr(b, verb)() is b
    await b.execute()
    wp = s._client._client.query_operate.call_args.kwargs["write_policy"]
    assert wp.expiration == expected


@pytest.mark.parametrize("verb, ttl, _expected", _EXPIRY_VERBS)
def test_sync_expiry_verbs_forward_to_the_async_builder(verb, ttl, _expected):
    inner = BackgroundOperationBuilder(
        _session_mock(), DataSet.of("test", "bgset"), _OpType.TOUCH)
    b = SyncBackgroundOperationBuilder(inner)
    assert getattr(b, verb)() is b
    assert inner._ttl_seconds == ttl


def test_background_task_session_refuses_an_active_transaction():
    s = _session_mock()
    s.current_transaction = Txn()
    with pytest.raises(RuntimeError, match="inside a transaction"):
        BackgroundTaskSession(s)


def _string_where_builder(kind, *, server_ael):
    s = _session_mock()
    s._client.supports_server_compiled_ael = server_ael
    ds = DataSet.of("test", "bgset")
    if kind == "operation":
        return BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    return BackgroundUdfFunctionBuilder(s, ds).function("pkg", "fn")


@pytest.mark.parametrize("kind", ["operation", "udf"])
def test_background_task_applies_a_string_where(kind):
    b = _string_where_builder(kind, server_ael=True)
    b.where("$.age > 20")
    assert b._filter_expression is not None


@pytest.mark.parametrize("kind", ["operation", "udf"])
def test_background_task_refuses_a_string_where_the_cluster_cannot_compile(kind):
    """The refusal comes before anything is sent, so no job is ever started."""
    b = _string_where_builder(kind, server_ael=False)
    with pytest.raises(AerospikeError) as exc_info:
        b.where("$.age > 20")
    assert exc_info.value.result_code == ResultCode.OP_NOT_APPLICABLE
    assert b._filter_expression is None


@pytest.mark.parametrize("qb_cls", [QueryBuilder, SyncQueryBuilder], ids=["async", "sync"])
@pytest.mark.parametrize(
    "name", ["with_write_operations", "execute_background_task", "execute_udf_background_task"],
)
def test_query_builder_has_no_background_task_methods(qb_cls, name):
    """session.background_task() is the one entry point for background jobs."""
    assert not hasattr(qb_cls(MagicMock(), "test", "bgset"), name)


def test_records_per_second_reaches_the_write_policy():
    """Storing the value on the builder is not enough; it has to be applied.

    Asserting only that the builder remembers it is what let this sit as an
    accept-and-drop knob: the setter looked wired while nothing ever read it.
    """
    wp = make_background_write_policy(
        Behavior.DEFAULT,
        None,
        None,
        None,
        records_per_second=5000,
    )
    assert wp.records_per_second == 5000


def test_records_per_second_defaults_to_unthrottled():
    wp = make_background_write_policy(Behavior.DEFAULT, None, None, None)
    assert wp.records_per_second == 0


async def test_records_per_second_reaches_the_policy_pac_receives():
    """End of the chain: the value the builder took is on the policy PAC gets."""
    s = _session_mock()
    s._client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    b.bin("tier").set_to("gold")
    await b.records_per_second(2500).execute()
    wp = s._client._client.query_operate.call_args.kwargs["write_policy"]
    assert wp.records_per_second == 2500


async def test_update_sends_collection_steps_to_pac():
    s = _session_mock()
    s._client._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    parent = b.bin("segments").on_map_value_range(None, [1704067200]).remove()
    assert parent is b
    b.bin("prefs").on_map_key("tags").list_append_items(["sports"])
    b.bin("visitors").hll_add(["u1"], config=HllConfig.of(8))
    await b.execute()
    _stmt, ops = s._client._client.query_operate.call_args[0]
    assert [type(op) for op in ops] == [MapOperation, ListOperation, HllOperation]


def test_update_expression_step_appends_exp_operation():
    s = _session_mock()
    s._client.supports_server_compiled_ael = True
    ds = DataSet.of("test", "bgset")
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    assert b.bin("tier").update_from("$.score * 2") is b
    assert type(b._operations[0]) is ExpOperation


def test_add_operation_appends_prebuilt_operation():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    op = MapOperation.put("m", "k", 1, MapPolicy(None, None))
    b = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    assert b.add_operation(op) is b
    assert b._operations == [op]


def test_sync_bin_steps_return_the_sync_builder():
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    inner = BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
    b = SyncBackgroundOperationBuilder(inner)
    assert b.bin("prefs").on_map_key("tags").list_append_items(["sports"]) is b
    assert b.bin("score").add(1) is b
    assert [type(op) for op in inner._operations] == [ListOperation, Operation]


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


@pytest.mark.parametrize("builder", ["operation", "udf"])
@pytest.mark.parametrize("verb", ["fail_on_filtered_out", "include_missing_keys"])
def test_foreground_read_verbs_are_rejected_on_background_tasks(builder, verb):
    """Both verbs shape a foreground read's result rows; a background job has none.

    All four combinations are live code, so all four are pinned here.
    """
    s = _session_mock()
    ds = DataSet.of("test", "bgset")
    b = (
        BackgroundTaskSession(s).update(ds) if builder == "operation"
        else BackgroundUdfFunctionBuilder(s, ds).function("pkg", "fn")
    )
    with pytest.raises(TypeError, match="background tasks"):
        getattr(b, verb)()


async def test_background_operation_execute_records_usage():
    s = _session_mock()
    sdk, calls = _usage_sdk()
    s._client = sdk
    sdk._client = MagicMock()
    sdk._client.query_operate = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    await (
        BackgroundOperationBuilder(s, ds, _OpType.UPDATE)
        .bin("x").set_to(1)
        .execute()
    )
    features = calls[0]
    assert usage.API_BACKGROUND in features
    assert usage.SHAPE_QUERY in features
    assert usage.BACKGROUND_OPERATE in features


def test_background_operation_blocking_records_usage():
    s = _session_mock()
    sdk, calls = _usage_sdk()
    s._client = sdk
    sdk._client = MagicMock()
    sdk._client.query_operate_blocking = MagicMock(return_value=MagicMock())
    s._resolve_namespace_mode_blocking = MagicMock(return_value=Mode.AP)
    ds = DataSet.of("test", "bgset")
    BackgroundOperationBuilder(s, ds, _OpType.UPDATE).bin("x").set_to(1)._execute_blocking()
    assert usage.API_BACKGROUND in calls[0]
    assert usage.BACKGROUND_OPERATE in calls[0]


async def test_background_udf_execute_records_usage():
    s = _session_mock()
    sdk, calls = _usage_sdk()
    s._client = sdk
    sdk._client = MagicMock()
    sdk._client.query_execute_udf = AsyncMock(return_value=MagicMock())
    ds = DataSet.of("test", "bgset")
    await (
        BackgroundUdfFunctionBuilder(s, ds)
        .function("pkg", "fn")
        .execute()
    )
    features = calls[0]
    assert usage.API_BACKGROUND in features
    assert usage.BACKGROUND_UDF in features


def _usage_session():
    s = _session_mock()
    sdk, calls = _usage_sdk()
    s._client = sdk
    sdk._client = MagicMock()
    sdk._client.query_operate = AsyncMock(return_value=MagicMock())
    return BackgroundOperationBuilder(s, DataSet.of("test", "bgset"), _OpType.UPDATE), sdk, calls


async def test_background_task_counts_the_call_once():
    """Registering a job is one call, however many records it goes on to touch."""
    b, sdk, calls = _usage_session()
    await b.bin("x").set_to(1).execute()
    assert len(sdk.counted) == 1
    assert len(calls) == 1


async def test_background_task_is_counted_without_the_usage_group():
    """The command count is the wider gate: no usage, still one call."""
    b, sdk, calls = _usage_session()
    sdk._usage_on = False
    await b.bin("x").set_to(1).execute()
    assert len(sdk.counted) == 1
    assert calls == []


async def test_rejected_background_task_does_not_record_usage():
    b, sdk, calls = _usage_session()
    with pytest.raises(ValueError, match="at least one bin operation"):
        await b.execute()
    assert calls == []
    assert sdk.counted == []


class TestSyncDurableDeleteForwarding:
    """The sync background wrappers forward the durable-delete quartet.

    These four verbs existed only on the async builders until the background
    pairs were added to the drift guard, so sync callers could not express
    durable delete on a background job at all. The wrappers hold no state of
    their own — each verb must land on the wrapped async builder and return
    the sync builder so the chain stays sync-typed.
    """

    def _sync_op_builder(self) -> SyncBackgroundOperationBuilder:
        s = _session_mock()
        ds = DataSet.of("test", "bgset")
        return SyncBackgroundOperationBuilder(
            BackgroundOperationBuilder(s, ds, _OpType.DELETE))

    def _sync_udf_builder(self) -> SyncBackgroundUdfBuilder:
        s = _session_mock()
        ds = DataSet.of("test", "bgset")
        return SyncBackgroundUdfBuilder(
            BackgroundUdfFunctionBuilder(s, ds).function("pkg", "fn"))

    @pytest.mark.parametrize(
        "verb, attr, expected",
        [
            ("with_durable_delete", "_durable_delete_override", True),
            ("without_durable_delete", "_durable_delete_override", False),
            ("default_with_durable_delete", "_durable_delete_command_default", True),
            ("default_without_durable_delete", "_durable_delete_command_default", False),
        ],
    )
    def test_operation_builder_forwards(self, verb, attr, expected):
        b = self._sync_op_builder()
        assert getattr(b._inner, attr) is None
        assert getattr(b, verb)() is b
        assert getattr(b._inner, attr) is expected

    @pytest.mark.parametrize(
        "verb, attr, expected",
        [
            ("with_durable_delete", "_durable_delete_override", True),
            ("without_durable_delete", "_durable_delete_override", False),
            ("default_with_durable_delete", "_durable_delete_command_default", True),
            ("default_without_durable_delete", "_durable_delete_command_default", False),
        ],
    )
    def test_udf_builder_forwards(self, verb, attr, expected):
        b = self._sync_udf_builder()
        assert getattr(b._inner, attr) is None
        assert getattr(b, verb)() is b
        assert getattr(b._inner, attr) is expected

    def test_durable_delete_reaches_the_write_policy(self):
        """The forwarded override survives into the policy the job submits."""
        s = _session_mock()
        s._resolve_namespace_mode_blocking = MagicMock(return_value=Mode.AP)
        s._client._client.query_operate_blocking = MagicMock(return_value=MagicMock())
        ds = DataSet.of("test", "bgset")
        b = SyncBackgroundOperationBuilder(
            BackgroundOperationBuilder(s, ds, _OpType.DELETE))
        b.with_durable_delete().execute()
        write_policy = (
            s._client._client.query_operate_blocking.call_args.kwargs["write_policy"])
        assert write_policy.durable_delete is True
