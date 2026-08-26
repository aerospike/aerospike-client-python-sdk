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

"""Unit tests for the secondary-index builder chain (expression-based creation)."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from aerospike_sdk import CollectionIndexType, CTX, Exp
from aerospike_sdk.exceptions import AerospikeError, ResultCode
from aerospike_async import FilterExpression, IndexType

from aerospike_sdk.aio.operations.index import IndexBuilder


def _async_builder(supports_server_compiled_ael: bool = True) -> IndexBuilder:
    client = MagicMock()
    client.supports_server_compiled_ael = supports_server_compiled_ael
    client._async_client.create_index = AsyncMock(return_value=None)
    client._async_client.create_index_using_expression = AsyncMock(
        return_value=MagicMock(),
    )
    return IndexBuilder(client, "test", "users")


class TestOnExpressionChaining:

    def test_stores_prebuilt_filter_expression(self):
        exp = Exp.int_bin("age")
        b = _async_builder()
        assert b.on_expression(exp) is b
        assert b._expression is exp

    def test_on_bin_first_raises(self):
        b = _async_builder().on_bin("age")
        with pytest.raises(ValueError, match="mutually exclusive"):
            b.on_expression(Exp.int_bin("age"))

    def test_stores_ael_string(self):
        b = _async_builder()
        assert b.on_expression("$.age") is b
        assert b._expression == "$.age"


class TestExpressionCreateAsync:

    async def test_routes_to_expression_entry(self):
        exp = Exp.int_bin("age")
        b = (
            _async_builder()
            .on_expression(exp)
            .named("users_age_exp_idx")
            .numeric()
        )
        await b.create()
        b._client._async_client.create_index_using_expression.assert_awaited_once_with(
            "test", "users", "users_age_exp_idx", IndexType.NUMERIC, exp, None,
        )
        b._client._async_client.create_index.assert_not_called()

    async def test_forwards_collection_index_type(self):
        exp = Exp.list_bin("tags")
        b = (
            _async_builder()
            .on_expression(exp)
            .named("users_tags_exp_idx")
            .string()
            .collection(CollectionIndexType.LIST)
        )
        await b.create()
        args = b._client._async_client.create_index_using_expression.await_args[0]
        assert args[5] == CollectionIndexType.LIST

    async def test_on_expression_then_on_bin_raises_at_create(self):
        b = (
            _async_builder()
            .on_expression(Exp.int_bin("age"))
            .on_bin("age")
            .named("idx")
            .numeric()
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            await b.create()

    async def test_context_rejected(self):
        b = (
            _async_builder()
            .on_expression(Exp.int_bin("age"))
            .named("idx")
            .numeric()
            .context([CTX.map_key("meta")])
        )
        with pytest.raises(ValueError, match="context"):
            await b.create()

    async def test_missing_name_raises(self):
        b = _async_builder().on_expression(Exp.int_bin("age")).numeric()
        with pytest.raises(ValueError, match="index_name"):
            await b.create()

    async def test_missing_index_type_raises(self):
        b = _async_builder().on_expression(Exp.int_bin("age")).named("idx")
        with pytest.raises(ValueError, match="index_type"):
            await b.create()

    async def test_bin_path_unchanged(self):
        b = _async_builder().on_bin("age").named("idx").numeric()
        await b.create()
        b._client._async_client.create_index.assert_awaited_once()
        b._client._async_client.create_index_using_expression.assert_not_called()


class TestExpressionCreateSync:

    def test_routes_to_blocking_expression_entry(self):
        # Sync-specific dispatch: the blocking terminal must hit PAC's
        # `*_blocking` sibling, not the async entry.
        from aerospike_sdk.sync.operations.index import IndexBuilder as SyncIB

        sync_client = MagicMock()
        exp = Exp.int_bin("age")
        b = (
            SyncIB(sync_client, "test", "users")
            .on_expression(exp)
            .named("users_age_exp_idx")
            .numeric()
        )
        b.create()
        pac = sync_client._async_client
        pac.create_index_using_expression_blocking.assert_called_once_with(
            "test", "users", "users_age_exp_idx", IndexType.NUMERIC, exp, None,
        )
        pac.create_index_blocking.assert_not_called()


class TestAelStringCreate:

    async def test_string_resolves_to_server_compiled_expression(self):
        b = (
            _async_builder(supports_server_compiled_ael=True)
            .on_expression("$.age")
            .named("users_age_ael_idx")
            .numeric()
        )
        await b.create()
        pac = b._client._async_client
        pac.create_index_using_expression.assert_awaited_once()
        args = pac.create_index_using_expression.await_args[0]
        assert isinstance(args[4], FilterExpression)

    async def test_string_without_server_support_raises(self):
        b = (
            _async_builder(supports_server_compiled_ael=False)
            .on_expression("$.age")
            .named("users_age_ael_idx")
            .numeric()
        )
        with pytest.raises(AerospikeError) as excinfo:
            await b.create()
        assert excinfo.value.result_code == ResultCode.OP_NOT_APPLICABLE
        b._client._async_client.create_index_using_expression.assert_not_called()

    async def test_prebuilt_expression_skips_capability_gate(self):
        # A prebuilt expression must never pay for the capability probe —
        # only string chains read the gate.
        class GateSpyClient:
            def __init__(self):
                self.gate_reads = 0
                self._async_client = MagicMock()
                self._async_client.create_index_using_expression = AsyncMock(
                    return_value=MagicMock(),
                )

            @property
            def supports_server_compiled_ael(self):
                self.gate_reads += 1
                return True

        client = GateSpyClient()
        b = IndexBuilder(client, "test", "users")
        b.on_expression(Exp.int_bin("age")).named("idx").numeric()
        await b.create()
        assert client.gate_reads == 0

    def test_sync_string_routes_to_blocking_expression_entry(self):
        from aerospike_sdk.sync.operations.index import IndexBuilder as SyncIB

        sync_client = MagicMock()
        sync_client.supports_server_compiled_ael = True
        b = (
            SyncIB(sync_client, "test", "users")
            .on_expression("$.age")
            .named("users_age_ael_idx")
            .numeric()
        )
        b.create()
        pac = sync_client._async_client
        pac.create_index_using_expression_blocking.assert_called_once()
        args = pac.create_index_using_expression_blocking.call_args[0]
        assert isinstance(args[4], FilterExpression)


class TestIndexTypeSetters:

    def test_blob_sets_index_type(self):
        b = _async_builder().on_bin("payload").named("payload_idx").blob()
        assert b._index_type is IndexType.BLOB

    def test_last_type_setter_wins(self):
        b = _async_builder().on_bin("payload").numeric().blob()
        assert b._index_type is IndexType.BLOB

    async def test_blob_passes_through_to_create(self):
        b = _async_builder().on_bin("payload").named("payload_idx").blob()
        await b.create()
        b._client._async_client.create_index.assert_awaited_once_with(
            "test", "users", "payload", "payload_idx", IndexType.BLOB, None, None,
        )
