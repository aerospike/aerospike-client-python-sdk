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

"""One-time seed/teardown for query-selection integration datasets (four suites).

The datasets are declared as data — rows and index definitions — and walked by
one small driver per runtime. Only the driver is written twice, so the thing
that must not drift between the async and blocking suites (what gets seeded)
exists once.

Index readiness needs no polling here: ``create_index_quiet_*`` waits on the
server's build task, so an index is queryable by the time the call returns.
Row visibility is a separate concern and still uses ``wait_for_set_visible``,
because a scan can lag the writes that produced the rows.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from aerospike_async import IndexType

from aerospike_sdk import CollectionIndexType, DataSet

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
    drop_index_quiet_async,
    drop_index_quiet_blocking,
    hint_key_name,
    key_name,
    long_bytes_be,
)

_QUERY_SELECTION_SETS = (SET_NAME, SCOPE_SET_NAME, HINT_SET_NAME, CDT_SET_NAME)


# ---------------------------------------------------------------------------
# What the four datasets contain
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _IndexSpec:
    bin_name: str
    index_name: str
    index_type: IndexType
    collection_type: CollectionIndexType | None = None


@dataclass(frozen=True)
class _SetSeed:
    """One set's rows and indexes.

    ``indexes_before_rows`` preserves each dataset's original order. It is not
    cosmetic: an index created first indexes each row as it is written, while an
    index created afterwards has to build against rows that already exist.
    """

    set_name: str
    indexes: tuple[_IndexSpec, ...]
    rows: tuple[tuple[str, dict[str, Any]], ...]
    indexes_before_rows: bool = True
    expected_visible: int | None = field(default=None)

    def visible_count(self) -> int:
        return len(self.rows) if self.expected_visible is None else self.expected_visible


_QSEL = _SetSeed(
    set_name=SET_NAME,
    indexes=(
        _IndexSpec(BIN_AGE, INDEX_NAME, IndexType.NUMERIC),
        _IndexSpec(BIN_SCORE, SCORE_INDEX_NAME, IndexType.NUMERIC),
    ),
    rows=tuple(
        (
            key_name(i),
            {BIN_AGE: i, BIN_SCORE: i, BIN_COUNTRY: "US" if i % 2 == 0 else "CA"},
        )
        for i in range(1, SIZE + 1)
    ),
    indexes_before_rows=False,
)

_QSCEXP = _SetSeed(
    set_name=SCOPE_SET_NAME,
    indexes=(
        _IndexSpec(SCOPE_AGE_BIN, SCOPE_INT_INDEX, IndexType.NUMERIC),
        _IndexSpec(SCOPE_BLOB_BIN, SCOPE_BLOB_INDEX, IndexType.BLOB),
        _IndexSpec(
            SCOPE_MAP_BIN, SCOPE_MAP_INDEX, IndexType.STRING,
            CollectionIndexType.MAP_KEYS,
        ),
    ),
    rows=(
        ("k1", {
            SCOPE_AGE_BIN: 25,
            SCOPE_COUNTRY_BIN: "US",
            SCOPE_BLOB_BIN: SCOPE_BLOB_BYTES,
            SCOPE_MAP_BIN: {SCOPE_MAP_KEY: "v1"},
        }),
        ("k2", {SCOPE_AGE_BIN: 30, SCOPE_COUNTRY_BIN: "CA"}),
    ),
)

_QSELHINT = _SetSeed(
    set_name=HINT_SET_NAME,
    indexes=(
        _IndexSpec(BIN_AGE, HINT_INDEX_NAME, IndexType.NUMERIC),
        _IndexSpec(BIN_SCORE, HINT_SCORE_INDEX_NAME, IndexType.NUMERIC),
    ),
    rows=(
        (hint_key_name("1"), {BIN_AGE: 25, BIN_SCORE: 25, BIN_COUNTRY: "US"}),
        (hint_key_name("2"), {BIN_AGE: 30, BIN_SCORE: 30, BIN_COUNTRY: "CA"}),
    ),
)


def _cdt_rows() -> tuple[tuple[str, dict[str, Any]], ...]:
    rows = []
    for i in range(1, CDT_SIZE + 1):
        map_data: dict[str, Any] = {"mkey1": f"v{i}"}
        if i % 2 == 0:
            map_data[CDT_MAP_KEY] = f"v{i}"
        list_data = [CDT_LIST_BLOB_BYTES] if i == 3 else [long_bytes_be(50000 + i)]
        rows.append((cdt_key_name(i), {CDT_MAP_BIN: map_data, CDT_LIST_BIN: list_data}))
    return tuple(rows)


_QP_CDT = _SetSeed(
    set_name=CDT_SET_NAME,
    indexes=(
        _IndexSpec(
            CDT_MAP_BIN, CDT_MAP_INDEX, IndexType.STRING,
            CollectionIndexType.MAP_KEYS,
        ),
        _IndexSpec(
            CDT_LIST_BIN, CDT_LIST_INDEX, IndexType.BLOB,
            CollectionIndexType.LIST,
        ),
    ),
    rows=_cdt_rows(),
)

_ALL_SEEDS = (_QSEL, _QSCEXP, _QSELHINT, _QP_CDT)

_QUERY_SELECTION_INDEX_DROPS = tuple(
    (seed.set_name, tuple(ix.index_name for ix in seed.indexes)) for seed in _ALL_SEEDS
)


@dataclass(frozen=True)
class QuerySelectionClusterState:
    """Connected SDK client + session shared by query-selection module fixtures."""

    client: Any
    session: Any


# ---------------------------------------------------------------------------
# Drivers — one per runtime, walking the same declarations
# ---------------------------------------------------------------------------

async def seed_query_selection_async(
    client: Any,
    session: Any,
    wait_for_set_visible: Callable[..., Any],
) -> None:
    """Seed all four query-selection sets (async)."""
    for set_name in _QUERY_SELECTION_SETS:
        await session.truncate(DataSet.of(NS, set_name))

    pac = client.underlying_client
    for seed in _ALL_SEEDS:
        ds = DataSet.of(NS, seed.set_name)

        async def make_indexes(seed=seed):
            for ix in seed.indexes:
                await create_index_quiet_async(
                    pac,
                    set_name=seed.set_name,
                    bin_name=ix.bin_name,
                    index_name=ix.index_name,
                    index_type=ix.index_type,
                    collection_type=ix.collection_type,
                )

        if seed.indexes_before_rows:
            await make_indexes()
        for key_id, bins in seed.rows:
            await session.upsert(ds.id(key_id)).put(bins).execute()
        if not seed.indexes_before_rows:
            await make_indexes()
        if seed.rows:
            await wait_for_set_visible(session, NS, seed.set_name, seed.visible_count())


def seed_query_selection_sync(
    client: Any,
    session: Any,
    sync_wait_for_set_visible: Callable[..., Any],
) -> None:
    """Seed all four query-selection sets (blocking)."""
    for set_name in _QUERY_SELECTION_SETS:
        session.truncate(DataSet.of(NS, set_name))

    pac = client.underlying_client
    for seed in _ALL_SEEDS:
        ds = DataSet.of(NS, seed.set_name)

        def make_indexes(seed=seed):
            for ix in seed.indexes:
                create_index_quiet_blocking(
                    pac,
                    set_name=seed.set_name,
                    bin_name=ix.bin_name,
                    index_name=ix.index_name,
                    index_type=ix.index_type,
                    collection_type=ix.collection_type,
                )

        if seed.indexes_before_rows:
            make_indexes()
        for key_id, bins in seed.rows:
            session.upsert(ds.id(key_id)).put(bins).execute()
        if not seed.indexes_before_rows:
            make_indexes()
        if seed.rows:
            sync_wait_for_set_visible(session, NS, seed.set_name, seed.visible_count())


async def teardown_query_selection_async(client: Any, session: Any) -> None:
    for set_name in _QUERY_SELECTION_SETS:
        await session.truncate(DataSet.of(NS, set_name))
    for set_name, index_names in _QUERY_SELECTION_INDEX_DROPS:
        for index_name in index_names:
            await drop_index_quiet_async(client, NS, set_name, index_name)


def teardown_query_selection_sync(client: Any, session: Any) -> None:
    for set_name in _QUERY_SELECTION_SETS:
        session.truncate(DataSet.of(NS, set_name))
    for set_name, index_names in _QUERY_SELECTION_INDEX_DROPS:
        for index_name in index_names:
            drop_index_quiet_blocking(client, NS, set_name, index_name)
