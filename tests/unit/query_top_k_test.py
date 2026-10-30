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

"""Unit tests for Top-K ranking on the dataset query builder.

The builder's own checks must fire before either dispatch branch, the plain
``query`` branch and the explain-then-plan branch, because the guards PNC runs
differ between them.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aerospike_native import FilterExpression

from aerospike_sdk import BinDataType, Behavior, Key, Order, ResultCode
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.sync.operations.query import QueryBuilder as SyncQueryBuilder


def _builder(
    client: Any = None,
    *,
    sync: bool = False,
    supports_query_selection: bool = False,
    supports_query_top_k: bool | None = None,
) -> QueryBuilder:
    sdk_client = None
    if supports_query_top_k is not None:
        sdk_client = SimpleNamespace(
            _usage_on=False, _record_on=False, supports_query_top_k=supports_query_top_k,
        )
    cls = SyncQueryBuilder if sync else QueryBuilder
    qb = cls(
        client=client if client is not None else MagicMock(),
        namespace="test",
        set_name="products",
        behavior=Behavior.DEFAULT,
        sdk_client=sdk_client,
        supports_server_compiled_ael=True,
        supports_query_selection=supports_query_selection,
    )
    qb._namespace_mode = Mode.AP
    return qb


def _keys(statement) -> list[tuple[str, BinDataType, Order]]:
    return [(k.bin_name, k.data_type, k.order) for k in statement.order_by]


def _assert_parameter_error(exc_info: pytest.ExceptionInfo, fragment: str) -> None:
    exc = exc_info.value
    assert exc.result_code == ResultCode.PARAMETER_ERROR
    assert fragment in str(exc)
    # A client-side refusal must not send the reader to a server setting.
    assert exc.hint is None


class TestOrderKeys:

    def test_order_by_replaces_and_then_order_by_appends(self):
        qb = (
            _builder()
            .order_by("score", BinDataType.INTEGER, Order.DESC)
            .then_order_by("price", BinDataType.DOUBLE, Order.ASC)
            .order_by("rank", BinDataType.INTEGER, Order.ASC)
            .then_order_by("price", BinDataType.DOUBLE, Order.DESC)
            .top_k(5)
        )
        statement = qb._build_dataset_read_statement()
        assert _keys(statement) == [
            ("rank", BinDataType.INTEGER, Order.ASC),
            ("price", BinDataType.DOUBLE, Order.DESC),
        ]
        assert statement.top_k == 5

    def test_then_order_by_alone_becomes_the_first_key(self):
        qb = _builder().then_order_by("price", BinDataType.DOUBLE, Order.ASC).top_k(3)
        statement = qb._build_dataset_read_statement()
        assert _keys(statement) == [("price", BinDataType.DOUBLE, Order.ASC)]

    def test_unranked_query_sets_nothing(self):
        statement = _builder()._build_dataset_read_statement()
        assert statement.order_by == []
        assert statement.top_k is None


class TestBuilderChecks:

    def test_order_by_without_top_k(self):
        qb = _builder().order_by("price", BinDataType.DOUBLE, Order.ASC)
        with pytest.raises(AerospikeError) as exc_info:
            qb._build_dataset_read_statement()
        _assert_parameter_error(exc_info, "got order_by=True, top_k=False")

    @pytest.mark.parametrize("k", [0, -1, 1001])
    def test_k_out_of_range_is_refused_at_chain_time(self, k):
        with pytest.raises(AerospikeError) as exc_info:
            _builder().top_k(k)
        _assert_parameter_error(exc_info, f"top_k(k) must be in [1, 1000]; got {k}")

    @pytest.mark.parametrize("k", [1, 1000])
    def test_k_at_the_bounds_is_accepted(self, k):
        qb = _builder().order_by("price", BinDataType.DOUBLE, Order.ASC).top_k(k)
        assert qb._build_dataset_read_statement().top_k == k

    def test_top_k_without_order_by(self):
        with pytest.raises(AerospikeError) as exc_info:
            _builder().top_k(5)._build_dataset_read_statement()
        _assert_parameter_error(exc_info, "got order_by=False, top_k=True")

    @pytest.mark.parametrize(
        "knob, fragment",
        [
            (lambda qb: qb.limit(10), "incompatible with limit(...)"),
            (lambda qb: qb.chunk_size(10), "incompatible with chunk_size(...)"),
            (lambda qb: qb.with_no_bins(), "incompatible with with_no_bins()"),
        ],
        ids=["limit", "chunk_size", "with_no_bins"],
    )
    def test_knobs_that_conflict_with_ranking(self, knob, fragment):
        qb = _builder().order_by("price", BinDataType.DOUBLE, Order.ASC).top_k(5)
        knob(qb)
        with pytest.raises(AerospikeError) as exc_info:
            qb._build_dataset_read_statement()
        _assert_parameter_error(exc_info, fragment)

    @pytest.mark.parametrize("batch", [False, True], ids=["point", "batch"])
    def test_key_query_is_refused_at_chain_time(self, batch):
        qb = _builder()
        key = Key("test", "products", 1)
        if batch:
            qb._keys = [key, Key("test", "products", 2)]
        else:
            qb._single_key = key
        with pytest.raises(AerospikeError) as exc_info:
            qb.order_by("price", BinDataType.DOUBLE, Order.ASC)
        _assert_parameter_error(exc_info, "not a key query")
        with pytest.raises(AerospikeError):
            qb.top_k(5)

    def test_cluster_without_top_k_is_refused(self):
        qb = (
            _builder(supports_query_top_k=False)
            .order_by("price", BinDataType.DOUBLE, Order.ASC)
            .top_k(5)
        )
        with pytest.raises(AerospikeError) as exc_info:
            qb._build_dataset_read_statement()
        _assert_parameter_error(exc_info, "not supported by all nodes")

    def test_capable_cluster_passes_the_gate(self):
        qb = (
            _builder(supports_query_top_k=True)
            .order_by("price", BinDataType.DOUBLE, Order.ASC)
            .top_k(5)
        )
        assert qb._build_dataset_read_statement().top_k == 5


def _plan_client(*, blocking: bool) -> MagicMock:
    client = MagicMock()
    plan = MagicMock()
    plan.is_filtered_out = False
    if blocking:
        client.query_explain_blocking.return_value = plan
    else:
        client.query_explain = AsyncMock(return_value=plan)
        client.query_with_plan = AsyncMock(return_value=MagicMock())
        client.query = AsyncMock(return_value=MagicMock())
    return client


def _server_compiled(ael, *, supports_server_compiled_ael=True):
    return FilterExpression.from_server_compiled_ael(ael)


@pytest.mark.parametrize("via_plan", [False, True], ids=["query", "explain_then_plan"])
class TestBothDispatchBranches:

    @pytest.mark.asyncio
    async def test_async_ranked_statement_reaches_the_query(self, via_plan):
        client = _plan_client(blocking=False)
        qb = (
            _builder(client, supports_query_selection=via_plan)
            .where("$.category == 'shoes'")
            .order_by("price", BinDataType.DOUBLE, Order.ASC)
            .top_k(5)
        )
        with patch("aerospike_sdk.query_shared.filter_expression_from_ael_string", _server_compiled):
            await qb._execute_dataset_query()
        call = client.query_with_plan if via_plan else client.query
        statement = call.await_args.args[0]
        assert statement.top_k == 5

    @pytest.mark.asyncio
    async def test_async_conflict_is_refused_before_any_request(self, via_plan):
        client = _plan_client(blocking=False)
        qb = (
            _builder(client, supports_query_selection=via_plan)
            .where("$.category == 'shoes'")
            .order_by("price", BinDataType.DOUBLE, Order.ASC)
            .top_k(5)
            .limit(10)
        )
        with patch("aerospike_sdk.query_shared.filter_expression_from_ael_string", _server_compiled):
            with pytest.raises(AerospikeError) as exc_info:
                await qb._execute_dataset_query()
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
        client.query.assert_not_awaited()
        client.query_explain.assert_not_awaited()
        client.query_with_plan.assert_not_awaited()

    def test_sync_ranked_statement_reaches_the_query(self, via_plan):
        client = _plan_client(blocking=True)
        qb = (
            _builder(client, sync=True, supports_query_selection=via_plan)
            .where("$.category == 'shoes'")
            .order_by("price", BinDataType.DOUBLE, Order.ASC)
            .top_k(5)
        )
        with patch("aerospike_sdk.query_shared.filter_expression_from_ael_string", _server_compiled):
            qb._execute_dataset_query_blocking()
        call = client.query_with_plan_blocking if via_plan else client.query_blocking
        assert call.call_args.args[0].top_k == 5

    def test_sync_conflict_is_refused_before_any_request(self, via_plan):
        client = _plan_client(blocking=True)
        qb = (
            _builder(client, sync=True, supports_query_selection=via_plan)
            .where("$.category == 'shoes'")
            .order_by("price", BinDataType.DOUBLE, Order.ASC)
            .top_k(5)
            .chunk_size(10)
        )
        with patch("aerospike_sdk.query_shared.filter_expression_from_ael_string", _server_compiled):
            with pytest.raises(AerospikeError) as exc_info:
                qb._execute_dataset_query_blocking()
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
        client.query_blocking.assert_not_called()
        client.query_explain_blocking.assert_not_called()
        client.query_with_plan_blocking.assert_not_called()
