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

from unittest.mock import AsyncMock, MagicMock

import pytest
from aerospike_async import Filter, QueryPolicy
from aerospike_async.exceptions import ResultCode

from aerospike_sdk import QueryHint
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.sync.operations.query import SyncQueryBuilder


class _ClientSupportsSelection:
    def supports_query_selection(self) -> bool:
        return True


class _ClientNoSelection:
    def supports_query_selection(self) -> bool:
        return False


def _async_builder(client: object) -> QueryBuilder:
    return QueryBuilder(client=client, namespace="test", set_name="s")


def _sync_builder(client: object) -> SyncQueryBuilder:
    return SyncQueryBuilder(client=client, namespace="test", set_name="s")


class TestUseServerQuerySelection:
    def test_true_with_string_ael_and_support(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        assert qb._use_server_query_selection(None) is True

    def test_false_without_where_ael(self):
        qb = _async_builder(_ClientSupportsSelection())
        assert qb._use_server_query_selection(None) is False

    def test_false_with_bin_name_hint(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        hint = QueryHint(bin_name="alt")
        assert qb._use_server_query_selection(hint) is False

    def test_false_with_explicit_filter(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        qb.filter(Filter.equal("age", 30))
        assert qb._use_server_query_selection(None) is False

    def test_false_when_client_lacks_support(self):
        qb = _async_builder(_ClientNoSelection()).where("$.age > 30")
        assert qb._use_server_query_selection(None) is False

    def test_false_when_client_has_no_method(self):
        qb = _async_builder(object()).where("$.age > 30")
        assert qb._use_server_query_selection(None) is False

    def test_index_name_hint_still_uses_server_path(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        hint = QueryHint(index_name="age_idx")
        assert qb._use_server_query_selection(hint) is True

    def test_sync_builder_inherits_routing(self):
        qb = _sync_builder(_ClientSupportsSelection()).where("$.score >= 10")
        assert qb._use_server_query_selection(None) is True


class TestApplyDatasetQueryPolicyFilter:
    def test_skips_filter_expression_on_server_path(self):
        qb = _async_builder(_ClientSupportsSelection()).where("$.age > 30")
        policy = QueryPolicy()
        qb._apply_dataset_query_policy_filter(policy, None)
        assert policy.filter_expression is None

    def test_sets_filter_expression_on_legacy_path(self):
        qb = _async_builder(_ClientNoSelection()).where("$.age > 30")
        policy = QueryPolicy()
        qb._apply_dataset_query_policy_filter(policy, None)
        assert policy.filter_expression is not None


@pytest.mark.asyncio
class TestExecuteDatasetQueryRouting:
    async def test_server_path_calls_explain_and_with_plan(self):
        client = MagicMock()
        client.supports_query_selection.return_value = True
        plan = MagicMock()
        plan.is_filtered_out = False
        recordset = MagicMock()
        client.query_explain = AsyncMock(return_value=plan)
        client.query_with_plan = AsyncMock(return_value=recordset)
        client.query = AsyncMock()

        qb = _async_builder(client).where("$.age > 30")
        await qb._execute_dataset_query()

        client.query_explain.assert_awaited_once()
        client.query_with_plan.assert_awaited_once()
        client.query.assert_not_awaited()

    async def test_legacy_path_calls_query_only(self):
        client = MagicMock()
        client.supports_query_selection.return_value = False
        recordset = MagicMock()
        client.query = AsyncMock(return_value=recordset)
        client.query_explain = AsyncMock()
        client.query_with_plan = AsyncMock()

        qb = _async_builder(client).where("$.age > 30")
        await qb._execute_dataset_query()

        client.query.assert_awaited_once()
        client.query_explain.assert_not_awaited()
        client.query_with_plan.assert_not_awaited()

    async def test_filtered_out_plan_skips_execute(self):
        client = MagicMock()
        client.supports_query_selection.return_value = True
        plan = MagicMock()
        plan.is_filtered_out = True
        client.query_explain = AsyncMock(return_value=plan)
        client.query_with_plan = AsyncMock()

        qb = _async_builder(client).where("$.age > 100 and $.age < 10")
        with pytest.raises(AerospikeError) as exc_info:
            await qb._execute_dataset_query()

        assert exc_info.value.result_code == ResultCode.FILTERED_OUT
        client.query_explain.assert_awaited_once()
        client.query_with_plan.assert_not_awaited()


class TestExecuteDatasetQueryBlockingRouting:
    def test_server_path_calls_explain_and_with_plan_blocking(self):
        client = MagicMock()
        client.supports_query_selection.return_value = True
        plan = MagicMock()
        plan.is_filtered_out = False
        recordset = MagicMock()
        client.query_explain_blocking.return_value = plan
        client.query_with_plan_blocking.return_value = recordset
        client.query_blocking = MagicMock()

        qb = _sync_builder(client).where("$.age > 30")
        qb._execute_dataset_query_blocking()

        client.query_explain_blocking.assert_called_once()
        client.query_with_plan_blocking.assert_called_once()
        client.query_blocking.assert_not_called()

    def test_legacy_path_calls_query_blocking_only(self):
        client = MagicMock()
        client.supports_query_selection.return_value = False
        recordset = MagicMock()
        client.query_blocking.return_value = recordset
        client.query_explain_blocking = MagicMock()
        client.query_with_plan_blocking = MagicMock()

        qb = _sync_builder(client).where("$.age > 30")
        qb._execute_dataset_query_blocking()

        client.query_blocking.assert_called_once()
        client.query_explain_blocking.assert_not_called()
        client.query_with_plan_blocking.assert_not_called()

    def test_filtered_out_plan_skips_execute_blocking(self):
        client = MagicMock()
        client.supports_query_selection.return_value = True
        plan = MagicMock()
        plan.is_filtered_out = True
        client.query_explain_blocking.return_value = plan
        client.query_with_plan_blocking = MagicMock()

        qb = _sync_builder(client).where("$.age > 100 and $.age < 10")
        with pytest.raises(AerospikeError) as exc_info:
            qb._execute_dataset_query_blocking()

        assert exc_info.value.result_code == ResultCode.FILTERED_OUT
        client.query_explain_blocking.assert_called_once()
        client.query_with_plan_blocking.assert_not_called()
