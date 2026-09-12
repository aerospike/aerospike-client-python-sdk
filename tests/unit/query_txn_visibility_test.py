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

"""Dataset queries inside a transaction log a visibility warning (no server needed)."""

from __future__ import annotations

import logging

from unittest.mock import AsyncMock, MagicMock

import pytest

from aerospike_sdk import Txn
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.sync.operations.query import SyncQueryBuilder

_EXPECTED_WARNING = (
    "Query executed inside a transaction will not take part in it. The server "
    "has no multi-record transaction support on the query path, so this query "
    "reads the state as it was before the transaction began: it will not see "
    "the transaction's own writes, and the rows it returns are not protected "
    "against concurrent modification at commit. Call with_txn(None) on this "
    "query to confirm that is intended and silence this warning."
)


def _async_builder(txn=None) -> QueryBuilder:
    client = MagicMock()
    client.query = AsyncMock(return_value=MagicMock())
    return QueryBuilder(
        client=client,
        namespace="test",
        set_name="s",
        txn=txn,
        supports_query_selection=False,
        supports_server_compiled_ael=False,
    )


def _sync_builder(txn=None) -> SyncQueryBuilder:
    client = MagicMock()
    client.query_blocking.return_value = MagicMock()
    return SyncQueryBuilder(
        client=client,
        namespace="test",
        set_name="s",
        txn=txn,
        supports_query_selection=False,
        supports_server_compiled_ael=False,
    )


def _query_warnings(caplog) -> list[str]:
    return [
        r.message for r in caplog.records
        if r.name == SdkLoggers.QUERY and r.levelno == logging.WARNING
    ]


@pytest.mark.asyncio
class TestAsyncQueryInTxnWarning:
    async def test_ambient_txn_warns_once(self, caplog):
        qb = _async_builder(txn=Txn())
        with caplog.at_level(logging.WARNING, logger=SdkLoggers.QUERY):
            await qb._execute_dataset_query()
        assert _query_warnings(caplog) == [_EXPECTED_WARNING]

    async def test_explicit_with_txn_warns(self, caplog):
        qb = _async_builder().with_txn(Txn())
        with caplog.at_level(logging.WARNING, logger=SdkLoggers.QUERY):
            await qb._execute_dataset_query()
        assert _query_warnings(caplog) == [_EXPECTED_WARNING]

    async def test_no_txn_does_not_warn(self, caplog):
        qb = _async_builder()
        with caplog.at_level(logging.WARNING, logger=SdkLoggers.QUERY):
            await qb._execute_dataset_query()
        assert _query_warnings(caplog) == []

    async def test_with_txn_none_silences(self, caplog):
        qb = _async_builder(txn=Txn()).with_txn(None)
        with caplog.at_level(logging.WARNING, logger=SdkLoggers.QUERY):
            await qb._execute_dataset_query()
        assert _query_warnings(caplog) == []


class TestSyncQueryInTxnWarning:
    def test_ambient_txn_warns_once(self, caplog):
        qb = _sync_builder(txn=Txn())
        with caplog.at_level(logging.WARNING, logger=SdkLoggers.QUERY):
            qb._execute_dataset_query_blocking()
        assert _query_warnings(caplog) == [_EXPECTED_WARNING]

    def test_explicit_with_txn_warns(self, caplog):
        qb = _sync_builder().with_txn(Txn())
        with caplog.at_level(logging.WARNING, logger=SdkLoggers.QUERY):
            qb._execute_dataset_query_blocking()
        assert _query_warnings(caplog) == [_EXPECTED_WARNING]

    def test_no_txn_does_not_warn(self, caplog):
        qb = _sync_builder()
        with caplog.at_level(logging.WARNING, logger=SdkLoggers.QUERY):
            qb._execute_dataset_query_blocking()
        assert _query_warnings(caplog) == []

    def test_with_txn_none_silences(self, caplog):
        qb = _sync_builder(txn=Txn()).with_txn(None)
        with caplog.at_level(logging.WARNING, logger=SdkLoggers.QUERY):
            qb._execute_dataset_query_blocking()
        assert _query_warnings(caplog) == []
