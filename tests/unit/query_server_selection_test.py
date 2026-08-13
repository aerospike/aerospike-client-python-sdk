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

"""Unit tests for server-led query selection routing in QueryBuilder."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aerospike_async import QueryPolicy, FilterExpression

from aerospike_sdk import Filter, QueryHint, ResultCode
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.sync.operations.query import SyncQueryBuilder

try:
    from aerospike_async import QueryWhereFlags
except ImportError:
    QueryWhereFlags = None


class _ClientSupportsSelection:
    """PAC client stub — capability is threaded via QueryBuilder kwarg."""


class _ClientNoSelection:
    """PAC client stub — capability is threaded via QueryBuilder kwarg."""


def _async_builder(
    client: object,
    *,
    supports_query_selection: bool = True,
    supports_server_compiled_ael: bool = False,
) -> QueryBuilder:
    return QueryBuilder(
        client=client,
        namespace="test",
        set_name="s",
        supports_query_selection=supports_query_selection,
        supports_server_compiled_ael=supports_server_compiled_ael,
    )


def _sync_builder(
    client: object,
    *,
    supports_query_selection: bool = True,
    supports_server_compiled_ael: bool = False,
) -> SyncQueryBuilder:
    return SyncQueryBuilder(
        client=client,
        namespace="test",
        set_name="s",
        supports_query_selection=supports_query_selection,
        supports_server_compiled_ael=supports_server_compiled_ael,
    )


class TestUseServerQuerySelection:
    def test_true_with_string_ael_and_support(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        assert qb._use_server_query_selection(None) is True

    def test_false_without_where_ael(self):
        qb = _async_builder(_ClientSupportsSelection())
        assert qb._use_server_query_selection(None) is False

    def test_false_with_explicit_filter(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        qb.filter(Filter.equal("age", 30))
        assert qb._use_server_query_selection(None) is False

    def test_false_when_capability_off(self):
        qb = _async_builder(
            _ClientNoSelection(),
            supports_query_selection=False,
        ).where("$.age > 30")
        assert qb._use_server_query_selection(None) is False

    def test_false_when_capability_not_enabled_on_builder(self):
        qb = _async_builder(object(), supports_query_selection=False).where("$.age > 30")
        assert qb._use_server_query_selection(None) is False

    def test_index_name_hint_still_uses_server_path(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        hint = QueryHint(index_name="age_idx")
        assert qb._use_server_query_selection(hint) is True

    def test_sync_builder_inherits_routing(self):
        qb = _sync_builder(_ClientSupportsSelection()).where("$.score >= 10")
        assert qb._use_server_query_selection(None) is True


@pytest.mark.skipif(QueryWhereFlags is None, reason="PAC lacks QueryWhereFlags")
class TestExplainWhereFlags:
    def test_default_none(self):
        qb = _async_builder(_ClientSupportsSelection())
        assert qb._query_explain_where_flags(None) is None

    def test_require_index(self):
        qb = _async_builder(_ClientSupportsSelection())
        hint = QueryHint(require_index=True)
        flags = qb._query_explain_where_flags(hint)
        assert flags == (QueryWhereFlags.EXPLAIN | QueryWhereFlags.REQUIRE_INDEX)

    def test_hard_hint_with_index_name(self):
        qb = _async_builder(_ClientSupportsSelection())
        hint = QueryHint(index_name="age_idx", hard_hint=True)
        flags = qb._query_explain_where_flags(hint)
        assert flags == (QueryWhereFlags.EXPLAIN | QueryWhereFlags.HARD_HINT)


class TestApplyDatasetQueryPolicyFilter:
    def test_skips_filter_expression_on_server_path(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        policy = QueryPolicy()
        qb._apply_dataset_query_policy_filter(
            policy, use_server_query_selection=True,
        )
        assert policy.filter_expression is None

    def test_sets_filter_expression_on_legacy_path(self):
        qb = _async_builder(
            _ClientNoSelection(),
            supports_query_selection=False,
            supports_server_compiled_ael=True,
        ).where("$.age > 30")
        policy = QueryPolicy()
        with patch(
            "aerospike_sdk.query_shared.filter_expression_from_ael_string",
            side_effect=lambda ael, *, supports_server_compiled_ael=True: (
                FilterExpression.from_server_compiled_ael(ael)
            ),
        ):
            qb._apply_dataset_query_policy_filter(
                policy, use_server_query_selection=False,
            )
        assert policy.filter_expression is not None


@pytest.mark.asyncio
class TestExecuteDatasetQueryRouting:
    async def test_server_path_calls_explain_and_with_plan(self):
        client = MagicMock()
        plan = MagicMock()
        plan.is_filtered_out = False
        recordset = MagicMock()
        client.query_explain = AsyncMock(return_value=plan)
        client.query_with_plan = AsyncMock(return_value=recordset)
        client.query = AsyncMock()

        qb = _async_builder(client, supports_query_selection=True).where("$.age > 30")
        await qb._execute_dataset_query()

        client.query_explain.assert_awaited_once()
        client.query_with_plan.assert_awaited_once()
        client.query.assert_not_awaited()

    async def test_legacy_path_calls_query_only(self):
        client = MagicMock()
        recordset = MagicMock()
        client.query = AsyncMock(return_value=recordset)
        client.query_explain = AsyncMock()
        client.query_with_plan = AsyncMock()

        qb = _async_builder(
            client,
            supports_query_selection=False,
            supports_server_compiled_ael=True,
        ).where("$.age > 30")
        with patch(
            "aerospike_sdk.query_shared.filter_expression_from_ael_string",
            side_effect=lambda ael, *, supports_server_compiled_ael=True: (
                FilterExpression.from_server_compiled_ael(ael)
            ),
        ):
            await qb._execute_dataset_query()

        client.query.assert_awaited_once()
        client.query_explain.assert_not_awaited()
        client.query_with_plan.assert_not_awaited()

    async def test_filtered_out_plan_skips_execute(self):
        client = MagicMock()
        plan = MagicMock()
        plan.is_filtered_out = True
        client.query_explain = AsyncMock(return_value=plan)
        client.query_with_plan = AsyncMock()

        qb = _async_builder(client, supports_query_selection=True).where(
            "$.age > 100 and $.age < 10",
        )
        with pytest.raises(AerospikeError) as exc_info:
            await qb._execute_dataset_query()

        assert exc_info.value.result_code == ResultCode.FILTERED_OUT
        assert str(exc_info.value) == "Query plan filtered out by server"
        client.query_explain.assert_awaited_once()
        client.query_with_plan.assert_not_awaited()


class TestExecuteDatasetQueryBlockingRouting:
    def test_server_path_calls_explain_and_with_plan_blocking(self):
        client = MagicMock()
        plan = MagicMock()
        plan.is_filtered_out = False
        recordset = MagicMock()
        client.query_explain_blocking.return_value = plan
        client.query_with_plan_blocking.return_value = recordset
        client.query_blocking = MagicMock()

        qb = _sync_builder(client, supports_query_selection=True).where("$.age > 30")
        qb._execute_dataset_query_blocking()

        client.query_explain_blocking.assert_called_once()
        client.query_with_plan_blocking.assert_called_once()
        client.query_blocking.assert_not_called()

    def test_legacy_path_calls_query_blocking_only(self):
        client = MagicMock()
        recordset = MagicMock()
        client.query_blocking.return_value = recordset
        client.query_explain_blocking = MagicMock()
        client.query_with_plan_blocking = MagicMock()

        qb = _sync_builder(
            client,
            supports_query_selection=False,
            supports_server_compiled_ael=True,
        ).where("$.age > 30")
        with patch(
            "aerospike_sdk.query_shared.filter_expression_from_ael_string",
            side_effect=lambda ael, *, supports_server_compiled_ael=True: (
                FilterExpression.from_server_compiled_ael(ael)
            ),
        ):
            qb._execute_dataset_query_blocking()

        client.query_blocking.assert_called_once()
        client.query_explain_blocking.assert_not_called()
        client.query_with_plan_blocking.assert_not_called()

    def test_filtered_out_plan_skips_execute_blocking(self):
        client = MagicMock()
        plan = MagicMock()
        plan.is_filtered_out = True
        client.query_explain_blocking.return_value = plan
        client.query_with_plan_blocking = MagicMock()

        qb = _sync_builder(client, supports_query_selection=True).where(
            "$.age > 100 and $.age < 10",
        )
        with pytest.raises(AerospikeError) as exc_info:
            qb._execute_dataset_query_blocking()

        assert exc_info.value.result_code == ResultCode.FILTERED_OUT
        assert str(exc_info.value) == "Query plan filtered out by server"
        client.query_explain_blocking.assert_called_once()
        client.query_with_plan_blocking.assert_not_called()


class TestServerCompiledAelWhere:
    def test_where_uses_server_filter_helper_when_gate_on(self):
        from unittest.mock import patch

        sentinel = object()
        with patch(
            "aerospike_sdk.query_shared.filter_expression_from_ael_string",
            return_value=sentinel,
        ) as factory:
            qb = _async_builder(
                _ClientSupportsSelection(),
                supports_server_compiled_ael=True,
            ).where("$.age > 30")
            qb._resolve_where_filter_expression()
        factory.assert_called_once_with(
            "$.age > 30",
            supports_server_compiled_ael=True,
        )
        assert qb._filter_expression is sentinel

    def test_selection_takes_precedence_over_legacy_filter_on_dataset(self):
        qb = _async_builder(
            _ClientSupportsSelection(),
            supports_query_selection=True,
            supports_server_compiled_ael=True,
        ).where("$.age > 30")
        policy = QueryPolicy()
        qb._apply_dataset_query_policy_filter(
            policy, use_server_query_selection=True,
        )
        assert policy.filter_expression is None
        assert qb._use_server_query_selection(None) is True


class TestAsyncSessionSingleKeyCapabilityFlags:
    def test_fast_path_inherits_server_compiled_ael(self):
        from unittest.mock import MagicMock

        from aerospike_async import ClientPolicy
        from aerospike_sdk import Key

        from aerospike_sdk.aio.client import Client
        from aerospike_sdk.aio.session import Session
        from aerospike_sdk.policy.behavior import Behavior

        sdk_client = Client("127.0.0.1:3000", policy=ClientPolicy())
        sdk_client._client = MagicMock()
        sdk_client._connected = True
        sdk_client._cached_supports_query_selection = True
        sdk_client._cached_supports_server_compiled_ael = True
        session = Session(client=sdk_client, behavior=Behavior.DEFAULT)
        builder = session.query(Key("test", "users", 1))
        assert builder._supports_server_compiled_ael is True
        assert builder._supports_query_selection is True

