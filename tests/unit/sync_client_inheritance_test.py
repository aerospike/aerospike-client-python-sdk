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

"""SyncClient / SyncSession factory invariants.

These tests catch regressions where a sync factory accidentally returns
an :class:`aerospike_sdk.aio.operations.query.QueryBuilder` (async type)
instead of :class:`SyncQueryBuilder`. The original example: an inheritance
attempt left the single-key fast path returning an aio.QueryBuilder, so
``session.query(key).execute()`` returned a coroutine and the bench/users
got :class:`AttributeError` on the next method call.
"""

from unittest.mock import MagicMock

from aerospike_async import ClientPolicy, Key

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.client import SyncClient
from aerospike_sdk.sync.operations.query import SyncQueryBuilder
from aerospike_sdk.sync.session import SyncSession


def _make_offline_sync_client() -> SyncClient:
    """SyncClient with a mock PAC connection — never opens a socket."""
    client = SyncClient("127.0.0.1:3000", policy=ClientPolicy())
    client._client = MagicMock()
    client._connected = True
    return client


def _make_offline_sync_session() -> SyncSession:
    """SyncSession bound to an offline SyncClient."""
    return SyncSession(
        client=_make_offline_sync_client(), behavior=Behavior.DEFAULT,
    )


class TestSyncClientFactoryReturnTypes:
    """SyncClient factories must return :class:`SyncQueryBuilder` regardless of arg shape."""

    def test_query_single_key(self):
        client = _make_offline_sync_client()
        builder = client.query(Key("test", "users", 1))
        assert isinstance(builder, SyncQueryBuilder)

    def test_query_multi_key(self):
        client = _make_offline_sync_client()
        keys = [Key("test", "users", i) for i in range(3)]
        builder = client.query(keys)
        assert isinstance(builder, SyncQueryBuilder)

    def test_query_dataset(self):
        client = _make_offline_sync_client()
        builder = client.query(DataSet.of("test", "users"))
        assert isinstance(builder, SyncQueryBuilder)

    def test_query_namespace_set(self):
        client = _make_offline_sync_client()
        builder = client.query(namespace="test", set_name="users")
        assert isinstance(builder, SyncQueryBuilder)


class TestSyncSessionFactoryReturnTypes:
    """SyncSession factories must return :class:`SyncQueryBuilder` regardless of arg shape.

    Covers the bug class that bit Phase 5: a sync factory returning an
    async type when the inherited base body took an unexpected code path
    (single-key fast path constructed aio.QueryBuilder directly).
    """

    def test_query_single_key(self):
        session = _make_offline_sync_session()
        builder = session.query(Key("test", "users", 1))
        assert isinstance(builder, SyncQueryBuilder), (
            f"single-key SyncSession.query must return SyncQueryBuilder, "
            f"got {type(builder).__name__}"
        )

    def test_query_multi_key(self):
        session = _make_offline_sync_session()
        keys = [Key("test", "users", i) for i in range(3)]
        builder = session.query(keys)
        assert isinstance(builder, SyncQueryBuilder)

    def test_query_dataset(self):
        session = _make_offline_sync_session()
        ds = DataSet.of("test", "users")
        builder = session.query(ds)
        assert isinstance(builder, SyncQueryBuilder)

    def test_query_namespace_set(self):
        session = _make_offline_sync_session()
        builder = session.query(namespace="test", set_name="users")
        assert isinstance(builder, SyncQueryBuilder)
