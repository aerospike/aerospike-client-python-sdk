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

from aerospike_native import CTX, FilterExpression, GeoJSON, IndexType

from aerospike_sdk import CollectionIndexType, DataSet, Exp, StringWriteFlags

from tests.integration.query_selection_helpers import (
    BIN_AGE,
    BIN_COUNTRY,
    BIN_SCORE,
    CDT_INT_LIST_BIN,
    CDT_INT_LIST_INDEX,
    CDT_INT_MAP_BIN,
    CDT_INT_MAP_KEYS_INDEX,
    CDT_INT_MAP_VALUES_INDEX,
    CDT_LIST_BIN,
    CDT_LIST_BLOB_BYTES,
    CDT_LIST_INDEX,
    CDT_LIST_STR_BIN,
    CDT_LIST_STR_INDEX,
    CDT_LIST_STR_TARGET,
    CDT_MAP_BIN,
    CDT_MAP_INDEX,
    CDT_MAP_KEY,
    CDT_MAP_VALUE_TARGET,
    CDT_MAP_VALUES_INDEX,
    CDT_NESTED_BIN,
    CDT_NESTED_INDEX,
    CDT_NESTED_KEY,
    CDT_NESTED_TARGET,
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
    SCOPE_AGE_PLUS_INDEX,
    SCOPE_BLOB_BIN,
    SCOPE_BLOB_BYTES,
    SCOPE_BLOB_INDEX,
    SCOPE_COUNTRY_BIN,
    SCOPE_FAR_LAT,
    SCOPE_FAR_LNG,
    SCOPE_INT_INDEX,
    SCOPE_LOC_BIN,
    SCOPE_LOC_INDEX,
    SCOPE_MAP_BIN,
    SCOPE_MAP_INDEX,
    SCOPE_MAP_KEY,
    SCOPE_MATCH_LAT,
    SCOPE_MATCH_LNG,
    SCOPE_MATCH_POINT,
    SCOPE_NAME_BIN,
    SCOPE_PT_BIN,
    SCOPE_PT_INDEX,
    SCOPE_SCORE_LIST_BIN,
    SCOPE_SCORE_LIST_INDEX,
    SCOPE_SCORE_LIST_MATCH,
    SCOPE_SCORE_LIST_POSITION,
    SCOPE_SET_NAME,
    SCOPE_TAG_BIN,
    SCOPE_TAG_INDEX,
    SCOPE_TAG_MATCH,
    SCOPE_UPPER_INDEX,
    SCOPE_VENUE_BIN,
    SCOPE_VENUE_INDEX,
    SCOPE_VENUE_KEY,
    cdt_key_name,
    circle_geo_json,
    create_index_quiet_async,
    create_index_quiet_blocking,
    drop_index_quiet_async,
    drop_index_quiet_blocking,
    hint_key_name,
    key_name,
    long_bytes_be,
    point_geo_json,
)

_QUERY_SELECTION_SETS = (SET_NAME, SCOPE_SET_NAME, HINT_SET_NAME, CDT_SET_NAME)


# ---------------------------------------------------------------------------
# What the four datasets contain
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _IndexSpec:
    """One index; an ``expression`` index has no bin, so it leaves ``bin_name`` as ``None``."""

    bin_name: str | None
    index_name: str
    index_type: IndexType
    collection_type: CollectionIndexType | None = None
    ctx: tuple[CTX, ...] | None = None
    expression: FilterExpression | None = None


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
        _IndexSpec(BIN_AGE, INDEX_NAME, IndexType.INTEGER),
        _IndexSpec(BIN_SCORE, SCORE_INDEX_NAME, IndexType.INTEGER),
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

# A geo index on a region bin answers "which regions contain this point", so
# ``loc`` and ``venue.location`` hold circles around the probe point, while
# ``pt`` holds points for the large-region probe that runs the other way.
_K1_REGION = GeoJSON(circle_geo_json(SCOPE_MATCH_LNG, SCOPE_MATCH_LAT, 3000.0))
_K2_REGION = GeoJSON(circle_geo_json(SCOPE_FAR_LNG, SCOPE_FAR_LAT, 3000.0))

_QSCEXP = _SetSeed(
    set_name=SCOPE_SET_NAME,
    indexes=(
        _IndexSpec(SCOPE_AGE_BIN, SCOPE_INT_INDEX, IndexType.INTEGER),
        _IndexSpec(SCOPE_BLOB_BIN, SCOPE_BLOB_INDEX, IndexType.BLOB),
        _IndexSpec(SCOPE_TAG_BIN, SCOPE_TAG_INDEX, IndexType.STRING),
        _IndexSpec(
            SCOPE_MAP_BIN, SCOPE_MAP_INDEX, IndexType.STRING,
            CollectionIndexType.MAP_KEYS,
        ),
        _IndexSpec(SCOPE_LOC_BIN, SCOPE_LOC_INDEX, IndexType.GEO2D_SPHERE),
        _IndexSpec(SCOPE_PT_BIN, SCOPE_PT_INDEX, IndexType.GEO2D_SPHERE),
        _IndexSpec(
            SCOPE_SCORE_LIST_BIN, SCOPE_SCORE_LIST_INDEX, IndexType.INTEGER,
            ctx=(CTX.list_index(SCOPE_SCORE_LIST_POSITION),),
        ),
        _IndexSpec(
            SCOPE_VENUE_BIN, SCOPE_VENUE_INDEX, IndexType.GEO2D_SPHERE,
            ctx=(CTX.map_key(SCOPE_VENUE_KEY),),
        ),
        _IndexSpec(
            None, SCOPE_UPPER_INDEX, IndexType.STRING,
            expression=Exp.string_upper(
                int(StringWriteFlags.DEFAULT), Exp.string_bin(SCOPE_NAME_BIN),
            ),
        ),
        _IndexSpec(
            None, SCOPE_AGE_PLUS_INDEX, IndexType.INTEGER,
            expression=Exp.num_add([Exp.int_bin(SCOPE_AGE_BIN), Exp.int_val(1)]),
        ),
    ),
    rows=(
        ("k1", {
            SCOPE_AGE_BIN: 25,
            SCOPE_COUNTRY_BIN: "US",
            SCOPE_TAG_BIN: SCOPE_TAG_MATCH,
            SCOPE_BLOB_BIN: SCOPE_BLOB_BYTES,
            SCOPE_MAP_BIN: {SCOPE_MAP_KEY: "v1"},
            SCOPE_SCORE_LIST_BIN: [10, 20, SCOPE_SCORE_LIST_MATCH, 30],
            SCOPE_NAME_BIN: "alice",
            SCOPE_VENUE_BIN: {SCOPE_VENUE_KEY: _K1_REGION},
            SCOPE_LOC_BIN: _K1_REGION,
            SCOPE_PT_BIN: GeoJSON(SCOPE_MATCH_POINT),
        }),
        ("k2", {
            SCOPE_AGE_BIN: 30,
            SCOPE_COUNTRY_BIN: "CA",
            SCOPE_TAG_BIN: "ordinary",
            SCOPE_SCORE_LIST_BIN: [1, 2, 3, 4],
            SCOPE_NAME_BIN: "bob",
            SCOPE_VENUE_BIN: {SCOPE_VENUE_KEY: _K2_REGION},
            SCOPE_LOC_BIN: _K2_REGION,
            SCOPE_PT_BIN: GeoJSON(point_geo_json(SCOPE_FAR_LNG, SCOPE_FAR_LAT)),
        }),
    ),
)

_QSELHINT = _SetSeed(
    set_name=HINT_SET_NAME,
    indexes=(
        _IndexSpec(BIN_AGE, HINT_INDEX_NAME, IndexType.INTEGER),
        _IndexSpec(BIN_SCORE, HINT_SCORE_INDEX_NAME, IndexType.INTEGER),
    ),
    rows=(
        (hint_key_name("1"), {BIN_AGE: 25, BIN_SCORE: 25, BIN_COUNTRY: "US"}),
        (hint_key_name("2"), {BIN_AGE: 30, BIN_SCORE: 30, BIN_COUNTRY: "CA"}),
    ),
)


def _cdt_rows() -> tuple[tuple[str, dict[str, Any]], ...]:
    """Every probe splits the rows by parity, so each match count is ``CDT_SIZE / 2``.

    Positional ``[0]`` existence is the exception: every row has one list
    element, so it matches all of them. The string-range entries sit inside, below, and above the probed
    ``CDT_STR_MAP_*_RANGE`` intervals, and the integer collections inside or
    outside the integer ranges, so a mis-evaluated bound changes the count.
    """
    rows = []
    for i in range(1, CDT_SIZE + 1):
        even = i % 2 == 0
        map_data: dict[str, Any] = {"mkey1": f"v{i}"}
        if even:
            map_data.update({
                CDT_MAP_KEY: CDT_MAP_VALUE_TARGET,
                "mkey10": "inRangeKey",
                "mkey15": "inRangeKeyMid",
                "mkey40": "outOfRangeKey",
                "slotA": "mv10",
                "slotB": "mv15",
                "slotD": "mv35",
            })
        else:
            map_data["mkey05"] = "belowRangeKey"
        rows.append((cdt_key_name(i), {
            CDT_MAP_BIN: map_data,
            CDT_LIST_BIN: [CDT_LIST_BLOB_BYTES] if i == 3 else [long_bytes_be(50000 + i)],
            CDT_LIST_STR_BIN: [CDT_LIST_STR_TARGET if even else f"other{i}"],
            CDT_INT_LIST_BIN: [5, 15, 25] if even else [1, 2, 3],
            CDT_INT_MAP_BIN: {10: 100, 15: 150, 25: 250} if even else {1: 1, 2: 2},
            CDT_NESTED_BIN: {CDT_NESTED_KEY: [CDT_NESTED_TARGET if even else f"nested_other{i}"]},
        }))
    return tuple(rows)


_QP_CDT = _SetSeed(
    set_name=CDT_SET_NAME,
    indexes=(
        _IndexSpec(
            CDT_MAP_BIN, CDT_MAP_INDEX, IndexType.STRING,
            CollectionIndexType.MAP_KEYS,
        ),
        _IndexSpec(
            CDT_MAP_BIN, CDT_MAP_VALUES_INDEX, IndexType.STRING,
            CollectionIndexType.MAP_VALUES,
        ),
        _IndexSpec(
            CDT_LIST_BIN, CDT_LIST_INDEX, IndexType.BLOB,
            CollectionIndexType.LIST,
        ),
        _IndexSpec(
            CDT_LIST_STR_BIN, CDT_LIST_STR_INDEX, IndexType.STRING,
            CollectionIndexType.LIST,
        ),
        _IndexSpec(
            CDT_INT_LIST_BIN, CDT_INT_LIST_INDEX, IndexType.INTEGER,
            CollectionIndexType.LIST,
        ),
        _IndexSpec(
            CDT_INT_MAP_BIN, CDT_INT_MAP_KEYS_INDEX, IndexType.INTEGER,
            CollectionIndexType.MAP_KEYS,
        ),
        _IndexSpec(
            CDT_INT_MAP_BIN, CDT_INT_MAP_VALUES_INDEX, IndexType.INTEGER,
            CollectionIndexType.MAP_VALUES,
        ),
        _IndexSpec(
            CDT_NESTED_BIN, CDT_NESTED_INDEX, IndexType.STRING,
            CollectionIndexType.LIST, ctx=(CTX.map_key(CDT_NESTED_KEY),),
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

    pnc = client.underlying_client
    for seed in _ALL_SEEDS:
        ds = DataSet.of(NS, seed.set_name)

        async def make_indexes(seed=seed):
            for ix in seed.indexes:
                await create_index_quiet_async(
                    pnc,
                    set_name=seed.set_name,
                    bin_name=ix.bin_name,
                    index_name=ix.index_name,
                    index_type=ix.index_type,
                    collection_type=ix.collection_type,
                    ctx=ix.ctx,
                    expression=ix.expression,
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

    pnc = client.underlying_client
    for seed in _ALL_SEEDS:
        ds = DataSet.of(NS, seed.set_name)

        def make_indexes(seed=seed):
            for ix in seed.indexes:
                create_index_quiet_blocking(
                    pnc,
                    set_name=seed.set_name,
                    bin_name=ix.bin_name,
                    index_name=ix.index_name,
                    index_type=ix.index_type,
                    collection_type=ix.collection_type,
                    ctx=ix.ctx,
                    expression=ix.expression,
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
