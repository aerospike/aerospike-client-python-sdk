# Copyright 2025-2026 Aerospike, Inc.
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

"""One-time seed/teardown for query-selection integration datasets (four Java suites)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from aerospike_async import IndexType

from aerospike_sdk import CollectionIndexType, DataSet, Filter

from tests.integration.query_selection_helpers import (
    BIN_AGE,
    BIN_COUNTRY,
    BIN_SCORE,
    CDT_LIST_BIN,
    CDT_LIST_BLOB_BYTES,
    CDT_LIST_INDEX,
    CDT_MAP_BIN,
    CDT_MAP_INDEX,
    CDT_MAP_KEY,
    CDT_SET_NAME,
    CDT_SIZE,
    HINT_INDEX_NAME,
    HINT_SCORE_INDEX_NAME,
    HINT_SET_NAME,
    INDEX_NAME,
    NS,
    SCORE_INDEX_NAME,
    SET_NAME,
    SIZE,
    SCOPE_AGE_BIN,
    SCOPE_BLOB_BIN,
    SCOPE_BLOB_BYTES,
    SCOPE_BLOB_INDEX,
    SCOPE_COUNTRY_BIN,
    SCOPE_INT_INDEX,
    SCOPE_MAP_BIN,
    SCOPE_MAP_INDEX,
    SCOPE_MAP_KEY,
    SCOPE_SET_NAME,
    cdt_key_name,
    create_index_quiet_async,
    create_index_quiet_blocking,
    hint_key_name,
    key_name,
    long_bytes_be,
)


@dataclass(frozen=True)
class QuerySelectionClusterState:
    """Connected SDK client + session shared by query-selection module fixtures."""

    client: Any
    session: Any


async def seed_query_selection_async(
    client: Any,
    session: Any,
    wait_for_index: Callable[..., Awaitable[None]],
    wait_for_set_visible: Callable[..., Awaitable[None]],
) -> None:
    """Seed all four query-selection sets and wait for SI readiness (async)."""
    pac = client.underlying_client
    await _seed_qsel_async(client, session, wait_for_index, wait_for_set_visible)
    await _seed_qscexp_async(client, session, pac, wait_for_index, wait_for_set_visible)
    await _seed_qselhint_async(client, session, wait_for_index, wait_for_set_visible)
    await _seed_qp_cdt_async(client, session, pac, wait_for_set_visible)


def seed_query_selection_sync(
    client: Any,
    session: Any,
    sync_wait_for_index: Callable[..., None],
) -> None:
    """Seed all four query-selection sets and wait for SI readiness (sync)."""
    pac = client.underlying_client
    _seed_qsel_sync(client, session, sync_wait_for_index)
    _seed_qscexp_sync(client, session, pac, sync_wait_for_index)
    _seed_qselhint_sync(client, session, sync_wait_for_index)
    _seed_qp_cdt_sync(client, session, pac)


async def teardown_query_selection_async(client: Any, session: Any) -> None:
    await _teardown_qsel_async(client, session)
    await _teardown_qscexp_async(client, session)
    await _teardown_qselhint_async(client, session)
    await _teardown_qp_cdt_async(client, session)


def teardown_query_selection_sync(client: Any, session: Any) -> None:
    _teardown_qsel_sync(client, session)
    _teardown_qscexp_sync(client, session)
    _teardown_qselhint_sync(client, session)
    _teardown_qp_cdt_sync(client, session)


async def _seed_qsel_async(client, session, wait_for_index, wait_for_set_visible) -> None:
    ds = DataSet.of(NS, SET_NAME)
    for i in range(1, SIZE + 1):
        try:
            await session.delete(ds.id(key_name(i))).execute()
        except Exception:
            pass
    for i in range(1, SIZE + 1):
        country = "US" if i % 2 == 0 else "CA"
        await (
            session.upsert(ds.id(key_name(i)))
            .put({BIN_AGE: i, BIN_SCORE: i, BIN_COUNTRY: country})
            .execute()
        )
    await wait_for_set_visible(session, NS, SET_NAME, SIZE)
    for index_name, bin_name in ((INDEX_NAME, BIN_AGE), (SCORE_INDEX_NAME, BIN_SCORE)):
        try:
            await (
                client.index(NS, SET_NAME)
                .on_bin(bin_name)
                .named(index_name)
                .numeric()
                .create()
            )
        except Exception:
            pass
    await wait_for_index(client, NS, SET_NAME, Filter.range(BIN_AGE, 1, SIZE))
    await wait_for_index(client, NS, SET_NAME, Filter.range(BIN_SCORE, 1, SIZE))


def _seed_qsel_sync(client, session, sync_wait_for_index) -> None:
    ds = DataSet.of(NS, SET_NAME)
    for i in range(1, SIZE + 1):
        try:
            session.delete(ds.id(key_name(i))).execute()
        except Exception:
            pass
    for i in range(1, SIZE + 1):
        country = "US" if i % 2 == 0 else "CA"
        session.upsert(ds.id(key_name(i))).put(
            {BIN_AGE: i, BIN_SCORE: i, BIN_COUNTRY: country},
        ).execute()
    for index_name, bin_name in ((INDEX_NAME, BIN_AGE), (SCORE_INDEX_NAME, BIN_SCORE)):
        try:
            client.index(NS, SET_NAME).on_bin(bin_name).named(index_name).numeric().create()
        except Exception:
            pass
    sync_wait_for_index(client, NS, SET_NAME, Filter.range(BIN_AGE, 1, SIZE))
    sync_wait_for_index(client, NS, SET_NAME, Filter.range(BIN_SCORE, 1, SIZE))


async def _seed_qscexp_async(client, session, pac, wait_for_index, wait_for_set_visible) -> None:
    ds = DataSet.of(NS, SCOPE_SET_NAME)
    for key_id in ("k1", "k2"):
        try:
            await session.delete(ds.id(key_id)).execute()
        except Exception:
            pass
    await create_index_quiet_async(
        pac, set_name=SCOPE_SET_NAME, bin_name=SCOPE_AGE_BIN,
        index_name=SCOPE_INT_INDEX, index_type=IndexType.NUMERIC,
    )
    await create_index_quiet_async(
        pac, set_name=SCOPE_SET_NAME, bin_name=SCOPE_BLOB_BIN,
        index_name=SCOPE_BLOB_INDEX, index_type=IndexType.BLOB,
    )
    await create_index_quiet_async(
        pac, set_name=SCOPE_SET_NAME, bin_name=SCOPE_MAP_BIN,
        index_name=SCOPE_MAP_INDEX, index_type=IndexType.STRING,
        collection_type=CollectionIndexType.MAP_KEYS,
    )
    await (
        session.upsert(ds.id("k1"))
        .put({
            SCOPE_AGE_BIN: 25,
            SCOPE_COUNTRY_BIN: "US",
            SCOPE_BLOB_BIN: SCOPE_BLOB_BYTES,
            SCOPE_MAP_BIN: {SCOPE_MAP_KEY: "v1"},
        })
        .execute()
    )
    await (
        session.upsert(ds.id("k2"))
        .put({SCOPE_AGE_BIN: 30, SCOPE_COUNTRY_BIN: "CA"})
        .execute()
    )
    await wait_for_set_visible(session, NS, SCOPE_SET_NAME, 2)
    await wait_for_index(client, NS, SCOPE_SET_NAME, Filter.equal(SCOPE_AGE_BIN, 25))
    await wait_for_index(
        client, NS, SCOPE_SET_NAME, Filter.equal(SCOPE_BLOB_BIN, SCOPE_BLOB_BYTES),
    )


def _seed_qscexp_sync(client, session, pac, sync_wait_for_index) -> None:
    ds = DataSet.of(NS, SCOPE_SET_NAME)
    for key_id in ("k1", "k2"):
        try:
            session.delete(ds.id(key_id)).execute()
        except Exception:
            pass
    create_index_quiet_blocking(
        pac, set_name=SCOPE_SET_NAME, bin_name=SCOPE_AGE_BIN,
        index_name=SCOPE_INT_INDEX, index_type=IndexType.NUMERIC,
    )
    create_index_quiet_blocking(
        pac, set_name=SCOPE_SET_NAME, bin_name=SCOPE_BLOB_BIN,
        index_name=SCOPE_BLOB_INDEX, index_type=IndexType.BLOB,
    )
    create_index_quiet_blocking(
        pac, set_name=SCOPE_SET_NAME, bin_name=SCOPE_MAP_BIN,
        index_name=SCOPE_MAP_INDEX, index_type=IndexType.STRING,
        collection_type=CollectionIndexType.MAP_KEYS,
    )
    session.upsert(ds.id("k1")).put({
        SCOPE_AGE_BIN: 25,
        SCOPE_COUNTRY_BIN: "US",
        SCOPE_BLOB_BIN: SCOPE_BLOB_BYTES,
        SCOPE_MAP_BIN: {SCOPE_MAP_KEY: "v1"},
    }).execute()
    session.upsert(ds.id("k2")).put(
        {SCOPE_AGE_BIN: 30, SCOPE_COUNTRY_BIN: "CA"},
    ).execute()
    sync_wait_for_index(client, NS, SCOPE_SET_NAME, Filter.equal(SCOPE_AGE_BIN, 25))
    sync_wait_for_index(
        client, NS, SCOPE_SET_NAME, Filter.equal(SCOPE_BLOB_BIN, SCOPE_BLOB_BYTES),
    )


async def _seed_qselhint_async(client, session, wait_for_index, wait_for_set_visible) -> None:
    ds = DataSet.of(NS, HINT_SET_NAME)
    for suffix in ("1", "2"):
        try:
            await session.delete(ds.id(hint_key_name(suffix))).execute()
        except Exception:
            pass
    for index_name, bin_name in (
        (HINT_INDEX_NAME, BIN_AGE),
        (HINT_SCORE_INDEX_NAME, BIN_SCORE),
    ):
        try:
            await (
                client.index(NS, HINT_SET_NAME)
                .on_bin(bin_name)
                .named(index_name)
                .numeric()
                .create()
            )
        except Exception:
            pass
    await (
        session.upsert(ds.id(hint_key_name("1")))
        .put({BIN_AGE: 25, BIN_SCORE: 25, BIN_COUNTRY: "US"})
        .execute()
    )
    await (
        session.upsert(ds.id(hint_key_name("2")))
        .put({BIN_AGE: 30, BIN_SCORE: 30, BIN_COUNTRY: "CA"})
        .execute()
    )
    await wait_for_set_visible(session, NS, HINT_SET_NAME, 2)
    await wait_for_index(client, NS, HINT_SET_NAME, Filter.range(BIN_AGE, 25, 30))
    await wait_for_index(client, NS, HINT_SET_NAME, Filter.range(BIN_SCORE, 25, 30))


def _seed_qselhint_sync(client, session, sync_wait_for_index) -> None:
    ds = DataSet.of(NS, HINT_SET_NAME)
    for suffix in ("1", "2"):
        try:
            session.delete(ds.id(hint_key_name(suffix))).execute()
        except Exception:
            pass
    for index_name, bin_name in (
        (HINT_INDEX_NAME, BIN_AGE),
        (HINT_SCORE_INDEX_NAME, BIN_SCORE),
    ):
        try:
            client.index(NS, HINT_SET_NAME).on_bin(bin_name).named(index_name).numeric().create()
        except Exception:
            pass
    session.upsert(ds.id(hint_key_name("1"))).put(
        {BIN_AGE: 25, BIN_SCORE: 25, BIN_COUNTRY: "US"},
    ).execute()
    session.upsert(ds.id(hint_key_name("2"))).put(
        {BIN_AGE: 30, BIN_SCORE: 30, BIN_COUNTRY: "CA"},
    ).execute()
    sync_wait_for_index(client, NS, HINT_SET_NAME, Filter.range(BIN_AGE, 25, 30))
    sync_wait_for_index(client, NS, HINT_SET_NAME, Filter.range(BIN_SCORE, 25, 30))


async def _seed_qp_cdt_async(client, session, pac, wait_for_set_visible) -> None:
    ds = DataSet.of(NS, CDT_SET_NAME)
    for i in range(1, CDT_SIZE + 1):
        try:
            await session.delete(ds.id(cdt_key_name(i))).execute()
        except Exception:
            pass
    await create_index_quiet_async(
        pac, set_name=CDT_SET_NAME, bin_name=CDT_MAP_BIN,
        index_name=CDT_MAP_INDEX, index_type=IndexType.STRING,
        collection_type=CollectionIndexType.MAP_KEYS,
    )
    await create_index_quiet_async(
        pac, set_name=CDT_SET_NAME, bin_name=CDT_LIST_BIN,
        index_name=CDT_LIST_INDEX, index_type=IndexType.BLOB,
        collection_type=CollectionIndexType.LIST,
    )
    for i in range(1, CDT_SIZE + 1):
        map_data = {"mkey1": f"v{i}"}
        if i % 2 == 0:
            map_data[CDT_MAP_KEY] = f"v{i}"
        list_data = (
            [CDT_LIST_BLOB_BYTES] if i == 3 else [long_bytes_be(50000 + i)]
        )
        await (
            session.upsert(ds.id(cdt_key_name(i)))
            .put({CDT_MAP_BIN: map_data, CDT_LIST_BIN: list_data})
            .execute()
        )
    await wait_for_set_visible(session, NS, CDT_SET_NAME, CDT_SIZE)


def _seed_qp_cdt_sync(client, session, pac) -> None:
    ds = DataSet.of(NS, CDT_SET_NAME)
    for i in range(1, CDT_SIZE + 1):
        try:
            session.delete(ds.id(cdt_key_name(i))).execute()
        except Exception:
            pass
    create_index_quiet_blocking(
        pac, set_name=CDT_SET_NAME, bin_name=CDT_MAP_BIN,
        index_name=CDT_MAP_INDEX, index_type=IndexType.STRING,
        collection_type=CollectionIndexType.MAP_KEYS,
    )
    create_index_quiet_blocking(
        pac, set_name=CDT_SET_NAME, bin_name=CDT_LIST_BIN,
        index_name=CDT_LIST_INDEX, index_type=IndexType.BLOB,
        collection_type=CollectionIndexType.LIST,
    )
    for i in range(1, CDT_SIZE + 1):
        map_data = {"mkey1": f"v{i}"}
        if i % 2 == 0:
            map_data[CDT_MAP_KEY] = f"v{i}"
        list_data = (
            [CDT_LIST_BLOB_BYTES] if i == 3 else [long_bytes_be(50000 + i)]
        )
        session.upsert(ds.id(cdt_key_name(i))).put(
            {CDT_MAP_BIN: map_data, CDT_LIST_BIN: list_data},
        ).execute()


async def _teardown_qsel_async(client, session) -> None:
    ds = DataSet.of(NS, SET_NAME)
    for i in range(1, SIZE + 1):
        try:
            await session.delete(ds.id(key_name(i))).execute()
        except Exception:
            pass
    for index_name in (INDEX_NAME, SCORE_INDEX_NAME):
        try:
            await client.index(NS, SET_NAME).named(index_name).drop()
        except Exception:
            pass


def _teardown_qsel_sync(client, session) -> None:
    ds = DataSet.of(NS, SET_NAME)
    for i in range(1, SIZE + 1):
        try:
            session.delete(ds.id(key_name(i))).execute()
        except Exception:
            pass
    for index_name in (INDEX_NAME, SCORE_INDEX_NAME):
        try:
            client.index(NS, SET_NAME).named(index_name).drop()
        except Exception:
            pass


async def _teardown_qscexp_async(client, session) -> None:
    ds = DataSet.of(NS, SCOPE_SET_NAME)
    for key_id in ("k1", "k2"):
        try:
            await session.delete(ds.id(key_id)).execute()
        except Exception:
            pass
    for index_name in (SCOPE_INT_INDEX, SCOPE_BLOB_INDEX, SCOPE_MAP_INDEX):
        try:
            await client.index(NS, SCOPE_SET_NAME).named(index_name).drop()
        except Exception:
            pass


def _teardown_qscexp_sync(client, session) -> None:
    ds = DataSet.of(NS, SCOPE_SET_NAME)
    for key_id in ("k1", "k2"):
        try:
            session.delete(ds.id(key_id)).execute()
        except Exception:
            pass
    for index_name in (SCOPE_INT_INDEX, SCOPE_BLOB_INDEX, SCOPE_MAP_INDEX):
        try:
            client.index(NS, SCOPE_SET_NAME).named(index_name).drop()
        except Exception:
            pass


async def _teardown_qselhint_async(client, session) -> None:
    ds = DataSet.of(NS, HINT_SET_NAME)
    for suffix in ("1", "2"):
        try:
            await session.delete(ds.id(hint_key_name(suffix))).execute()
        except Exception:
            pass
    for index_name in (HINT_INDEX_NAME, HINT_SCORE_INDEX_NAME):
        try:
            await client.index(NS, HINT_SET_NAME).named(index_name).drop()
        except Exception:
            pass


def _teardown_qselhint_sync(client, session) -> None:
    ds = DataSet.of(NS, HINT_SET_NAME)
    for suffix in ("1", "2"):
        try:
            session.delete(ds.id(hint_key_name(suffix))).execute()
        except Exception:
            pass
    for index_name in (HINT_INDEX_NAME, HINT_SCORE_INDEX_NAME):
        try:
            client.index(NS, HINT_SET_NAME).named(index_name).drop()
        except Exception:
            pass


async def _teardown_qp_cdt_async(client, session) -> None:
    ds = DataSet.of(NS, CDT_SET_NAME)
    for i in range(1, CDT_SIZE + 1):
        try:
            await session.delete(ds.id(cdt_key_name(i))).execute()
        except Exception:
            pass
    for index_name in (CDT_MAP_INDEX, CDT_LIST_INDEX):
        try:
            await client.index(NS, CDT_SET_NAME).named(index_name).drop()
        except Exception:
            pass


def _teardown_qp_cdt_sync(client, session) -> None:
    ds = DataSet.of(NS, CDT_SET_NAME)
    for i in range(1, CDT_SIZE + 1):
        try:
            session.delete(ds.id(cdt_key_name(i))).execute()
        except Exception:
            pass
    for index_name in (CDT_MAP_INDEX, CDT_LIST_INDEX):
        try:
            client.index(NS, CDT_SET_NAME).named(index_name).drop()
        except Exception:
            pass
