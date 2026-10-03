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

    def test_rejects_non_expression_input(self):
        # Without the shape check, None would fall through to the bin path and
        # surface as a misleading "bin_name is required" at create().
        b = _async_builder()
        with pytest.raises(TypeError, match="AEL string or a FilterExpression"):
            b.on_expression(None)
        with pytest.raises(TypeError, match="got int"):
            b.on_expression(42)


class TestExpressionCreateAsync:

    async def test_routes_to_expression_entry(self):
        exp = Exp.int_bin("age")
        b = (
            _async_builder()
            .on_expression(exp)
            .named("users_age_exp_idx")
            .integer()
        )
        await b.create()
        b._client._async_client.create_index_using_expression.assert_awaited_once_with(
            "test", "users", "users_age_exp_idx", IndexType.INTEGER, exp, None,
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
            .integer()
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            await b.create()

    async def test_context_rejected(self):
        b = (
            _async_builder()
            .on_expression(Exp.int_bin("age"))
            .named("idx")
            .integer()
            .context([CTX.map_key("meta")])
        )
        with pytest.raises(ValueError, match="context"):
            await b.create()

    async def test_missing_name_raises(self):
        b = _async_builder().on_expression(Exp.int_bin("age")).integer()
        with pytest.raises(ValueError, match="index_name"):
            await b.create()

    async def test_missing_index_type_raises(self):
        b = _async_builder().on_expression(Exp.int_bin("age")).named("idx")
        with pytest.raises(ValueError, match="index_type"):
            await b.create()

    async def test_bin_path_unchanged(self):
        b = _async_builder().on_bin("age").named("idx").integer()
        await b.create()
        b._client._async_client.create_index.assert_awaited_once()
        b._client._async_client.create_index_using_expression.assert_not_called()

    async def test_bin_path_missing_index_type_raises(self):
        b = _async_builder().on_bin("age").named("idx")
        with pytest.raises(ValueError, match=r"index_type is required\. Call integer\(\)"):
            await b.create()


class TestSetIndexChaining:

    def test_on_set_marks_builder(self):
        b = _async_builder()
        assert b.on_set() is b
        assert b._on_set is True

    def test_on_set_after_on_bin_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _async_builder().on_bin("age").on_set()

    def test_on_set_after_on_expression_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _async_builder().on_expression(Exp.int_bin("age")).on_set()

    def test_on_bin_after_on_set_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _async_builder().on_set().on_bin("age")

    def test_on_expression_after_on_set_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _async_builder().on_set().on_expression("$.age")


class TestSetIndexCreateAsync:

    async def test_routes_to_set_index_entry(self):
        b = _async_builder()
        b._client._async_client.create_set_index = AsyncMock(return_value=MagicMock())
        b.on_set().named("users_set_idx")
        task = await b.create()
        pac = b._client._async_client
        pac.create_set_index.assert_awaited_once_with("test", "users", "users_set_idx")
        pac.create_index.assert_not_called()
        pac.create_index_using_expression.assert_not_called()
        assert task is pac.create_set_index.return_value

    async def test_missing_name_raises(self):
        with pytest.raises(ValueError, match="index_name"):
            await _async_builder().on_set().create()

    async def test_index_type_rejected(self):
        with pytest.raises(ValueError, match="no index type"):
            await _async_builder().on_set().named("idx").integer().create()

    async def test_collection_type_rejected(self):
        b = _async_builder().on_set().named("idx").collection(CollectionIndexType.LIST)
        with pytest.raises(ValueError, match="no index type or collection type"):
            await b.create()

    async def test_context_rejected(self):
        b = _async_builder().on_set().named("idx").context([CTX.map_key("meta")])
        with pytest.raises(ValueError, match="context"):
            await b.create()

    async def test_namespace_wide_rejected(self):
        # A set index with no set covers nothing; refuse before the wire.
        client = MagicMock()
        client._async_client.create_set_index = AsyncMock()
        b = IndexBuilder(client, "test", "").on_set().named("idx")
        with pytest.raises(ValueError, match="requires a set"):
            await b.create()
        client._async_client.create_set_index.assert_not_called()

    async def test_converts_pac_failure(self):
        b = _async_builder().on_set().named("idx")
        b._client._async_client.create_set_index = AsyncMock(side_effect=RuntimeError("boom"))
        with pytest.raises(AerospikeError):
            await b.create()


class TestConfigureFromArguments:
    """The one-call ``create_index`` arguments map onto the chain exactly."""

    def test_bin_index(self):
        b = _async_builder()._configure(
            "city_idx", "city", IndexType.STRING, CollectionIndexType.LIST,
            [CTX.map_key("meta")], None,
        )
        assert b._index_name == "city_idx"
        assert b._bin_name == "city"
        assert b._index_type == IndexType.STRING
        assert b._collection_index_type == CollectionIndexType.LIST
        assert b._ctx == [CTX.map_key("meta")]
        assert b._on_set is False
        assert b._expression is None

    def test_expression_index(self):
        b = _async_builder()._configure("age_idx", None, IndexType.INTEGER, None, None, "$.age + 1")
        assert b._expression == "$.age + 1"
        assert b._bin_name is None
        assert b._on_set is False

    def test_name_alone_is_a_set_index(self):
        b = _async_builder()._configure("set_idx", None, None, None, None, None)
        assert b._on_set is True
        assert b._bin_name is None
        assert b._expression is None

    def test_bin_and_expression_together_rejected(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _async_builder()._configure("idx", "age", IndexType.INTEGER, None, None, "$.age")

    async def test_bin_without_type_rejected_at_create(self):
        b = _async_builder()._configure("idx", "age", None, None, None, None)
        with pytest.raises(ValueError, match="index_type is required"):
            await b.create()

    async def test_set_index_with_type_rejected_at_create(self):
        b = _async_builder()._configure("idx", None, IndexType.INTEGER, None, None, None)
        with pytest.raises(ValueError, match="no index type"):
            await b.create()


class TestSetIndexCreateSync:

    def test_routes_to_blocking_set_index_entry(self):
        from aerospike_sdk.sync.operations.index import IndexBuilder as SyncIB

        sync_client = MagicMock()
        b = SyncIB(sync_client, "test", "users").on_set().named("users_set_idx")
        task = b.create()
        pac = sync_client._async_client
        pac.create_set_index_blocking.assert_called_once_with("test", "users", "users_set_idx")
        pac.create_index_blocking.assert_not_called()
        pac.create_index_using_expression_blocking.assert_not_called()
        assert task is pac.create_set_index_blocking.return_value

    def test_sync_index_type_rejected(self):
        from aerospike_sdk.sync.operations.index import IndexBuilder as SyncIB

        b = SyncIB(MagicMock(), "test", "users").on_set().named("idx").string()
        with pytest.raises(ValueError, match="no index type"):
            b.create()


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
            .integer()
        )
        b.create()
        pac = sync_client._async_client
        pac.create_index_using_expression_blocking.assert_called_once_with(
            "test", "users", "users_age_exp_idx", IndexType.INTEGER, exp, None,
        )
        pac.create_index_blocking.assert_not_called()


class TestAelStringCreate:

    async def test_string_resolves_to_server_compiled_expression(self):
        b = (
            _async_builder(supports_server_compiled_ael=True)
            .on_expression("$.age")
            .named("users_age_ael_idx")
            .integer()
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
            .integer()
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
                self._usage_on = False
                self._record_on = False
                self._cmd_count_on = False
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
        b.on_expression(Exp.int_bin("age")).named("idx").integer()
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
            .integer()
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
        b = _async_builder().on_bin("payload").integer().blob()
        assert b._index_type is IndexType.BLOB

    async def test_blob_passes_through_to_create(self):
        b = _async_builder().on_bin("payload").named("payload_idx").blob()
        await b.create()
        b._client._async_client.create_index.assert_awaited_once_with(
            "test", "users", "payload", "payload_idx", IndexType.BLOB, None, None,
        )


class TestSyncBinPathValidation:
    """Bin-path create()/drop() validation and error conversion (sync)."""

    def _sync_builder(self):
        from aerospike_sdk.sync.operations.index import IndexBuilder as SyncIB
        client = MagicMock()
        client._usage_on = False
        return client, SyncIB(client, "test", "users")

    def test_create_without_bin_raises(self):
        _, b = self._sync_builder()
        with pytest.raises(ValueError, match="bin_name is required"):
            b.create()

    def test_create_without_name_raises(self):
        _, b = self._sync_builder()
        with pytest.raises(ValueError, match="index_name is required"):
            b.on_bin("age").create()

    def test_create_without_index_type_raises(self):
        _, b = self._sync_builder()
        with pytest.raises(ValueError, match=r"index_type is required\. Call integer\(\)"):
            b.on_bin("age").named("idx_age").create()

    def test_create_converts_pac_failure(self):
        client, b = self._sync_builder()
        client._async_client.create_index_blocking.side_effect = RuntimeError("boom")
        with pytest.raises(AerospikeError):
            b.on_bin("age").named("idx_age").integer().create()

    def test_drop_without_name_raises(self):
        _, b = self._sync_builder()
        with pytest.raises(ValueError, match="index_name is required"):
            b.drop()

    def test_drop_routes_to_blocking_entry(self):
        client, b = self._sync_builder()
        task = b.named("idx_age").drop()
        client._async_client.drop_index_blocking.assert_called_once_with(
            "test", "users", "idx_age",
        )
        assert task is client._async_client.drop_index_blocking.return_value

    def test_drop_converts_pac_failure(self):
        client, b = self._sync_builder()
        client._async_client.drop_index_blocking.side_effect = RuntimeError("boom")
        with pytest.raises(AerospikeError):
            b.named("idx_age").drop()
