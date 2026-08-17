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

"""One-time seed/teardown for query-selection integration datasets (four suites)."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Awaitable

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
    drop_index_quiet_async,
    drop_index_quiet_blocking,
    hint_key_name,
    key_name,
    long_bytes_be,
)

_QUERY_SELECTION_SETS = (SET_NAME, SCOPE_SET_NAME, HINT_SET_NAME, CDT_SET_NAME)


@dataclass(frozen=True)
class _IndexDropSpec:
    set_name: str
    index_names: tuple[str, ...]


_QUERY_SELECTION_INDEX_DROPS: tuple[_IndexDropSpec, ...] = (
    _IndexDropSpec(SET_NAME, (INDEX_NAME, SCORE_INDEX_NAME)),
    _IndexDropSpec(SCOPE_SET_NAME, (SCOPE_INT_INDEX, SCOPE_BLOB_INDEX, SCOPE_MAP_INDEX)),
    _IndexDropSpec(HINT_SET_NAME, (HINT_INDEX_NAME, HINT_SCORE_INDEX_NAME)),
    _IndexDropSpec(CDT_SET_NAME, (CDT_MAP_INDEX, CDT_LIST_INDEX)),
)


@dataclass(frozen=True)
class _SeedOps:
    """Async/sync seed helpers selected by ``async_mode``."""

    client: Any
    session: Any
    pac: Any
    wait_for_index: Callable[..., Any]
    wait_for_set_visible: Callable[..., Any]
    create_index: Callable[..., Any]
    async_mode: bool


def _truncate_steps(session: Any, *, async_mode: bool) -> Iterator[Awaitable[Any] | None]:
    for set_name in _QUERY_SELECTION_SETS:
        if async_mode:
            yield session.truncate(DataSet.of(NS, set_name))
        else:
            session.truncate(DataSet.of(NS, set_name))
            yield None


def _drop_index_steps(client: Any, *, async_mode: bool) -> Iterator[Awaitable[Any] | None]:
    for spec in _QUERY_SELECTION_INDEX_DROPS:
        for index_name in spec.index_names:
            if async_mode:
                yield drop_index_quiet_async(client, NS, spec.set_name, index_name)
            else:
                drop_index_quiet_blocking(client, NS, spec.set_name, index_name)
                yield None


def _upsert(
    ops: _SeedOps, ds: DataSet, key_id: str, bins: dict[str, Any],
) -> Awaitable[Any] | None:
    chain = ops.session.upsert(ds.id(key_id)).put(bins)
    if ops.async_mode:
        return chain.execute()
    chain.execute()
    return None


def _create_idx(
    ops: _SeedOps,
    *,
    set_name: str,
    bin_name: str,
    index_name: str,
    index_type: IndexType,
    collection_type: CollectionIndexType | None = None,
) -> Awaitable[Any] | None:
    kwargs: dict[str, Any] = {
        "set_name": set_name,
        "bin_name": bin_name,
        "index_name": index_name,
        "index_type": index_type,
    }
    if collection_type is not None:
        kwargs["collection_type"] = collection_type
    if ops.async_mode:
        return ops.create_index(ops.pac, **kwargs)
    ops.create_index(ops.pac, **kwargs)
    return None


def _wait_set_visible(
    ops: _SeedOps, set_name: str, expected: int,
) -> Awaitable[Any] | None:
    if ops.async_mode:
        return ops.wait_for_set_visible(ops.session, NS, set_name, expected)
    ops.wait_for_set_visible(ops.session, NS, set_name, expected)
    return None


def _wait_index(
    ops: _SeedOps, set_name: str, sindex_filter: Filter,
) -> Awaitable[Any] | None:
    if ops.async_mode:
        return ops.wait_for_index(ops.client, NS, set_name, sindex_filter)
    ops.wait_for_index(ops.client, NS, set_name, sindex_filter)
    return None


def _seed_qsel_steps(ops: _SeedOps) -> Iterator[Awaitable[Any] | None]:
    ds = DataSet.of(NS, SET_NAME)
    for i in range(1, SIZE + 1):
        country = "US" if i % 2 == 0 else "CA"
        yield _upsert(
            ops, ds, key_name(i),
            {BIN_AGE: i, BIN_SCORE: i, BIN_COUNTRY: country},
        )
    yield _wait_set_visible(ops, SET_NAME, SIZE)
    yield _create_idx(
        ops, set_name=SET_NAME, bin_name=BIN_AGE,
        index_name=INDEX_NAME, index_type=IndexType.NUMERIC,
    )
    yield _create_idx(
        ops, set_name=SET_NAME, bin_name=BIN_SCORE,
        index_name=SCORE_INDEX_NAME, index_type=IndexType.NUMERIC,
    )
    yield _wait_index(ops, SET_NAME, Filter.range(BIN_AGE, 1, SIZE))
    yield _wait_index(ops, SET_NAME, Filter.range(BIN_SCORE, 1, SIZE))


def _seed_qscexp_steps(ops: _SeedOps) -> Iterator[Awaitable[Any] | None]:
    ds = DataSet.of(NS, SCOPE_SET_NAME)
    yield _create_idx(
        ops, set_name=SCOPE_SET_NAME, bin_name=SCOPE_AGE_BIN,
        index_name=SCOPE_INT_INDEX, index_type=IndexType.NUMERIC,
    )
    yield _create_idx(
        ops, set_name=SCOPE_SET_NAME, bin_name=SCOPE_BLOB_BIN,
        index_name=SCOPE_BLOB_INDEX, index_type=IndexType.BLOB,
    )
    yield _create_idx(
        ops, set_name=SCOPE_SET_NAME, bin_name=SCOPE_MAP_BIN,
        index_name=SCOPE_MAP_INDEX, index_type=IndexType.STRING,
        collection_type=CollectionIndexType.MAP_KEYS,
    )
    yield _upsert(
        ops, ds, "k1",
        {
            SCOPE_AGE_BIN: 25,
            SCOPE_COUNTRY_BIN: "US",
            SCOPE_BLOB_BIN: SCOPE_BLOB_BYTES,
            SCOPE_MAP_BIN: {SCOPE_MAP_KEY: "v1"},
        },
    )
    yield _upsert(
        ops, ds, "k2",
        {SCOPE_AGE_BIN: 30, SCOPE_COUNTRY_BIN: "CA"},
    )
    yield _wait_set_visible(ops, SCOPE_SET_NAME, 2)
    yield _wait_index(ops, SCOPE_SET_NAME, Filter.equal(SCOPE_AGE_BIN, 25))
    yield _wait_index(
        ops, SCOPE_SET_NAME, Filter.equal(SCOPE_BLOB_BIN, SCOPE_BLOB_BYTES),
    )
    yield _wait_index(
        ops, SCOPE_SET_NAME,
        Filter.contains(SCOPE_MAP_BIN, SCOPE_MAP_KEY, CollectionIndexType.MAP_KEYS),
    )


def _seed_qselhint_steps(ops: _SeedOps) -> Iterator[Awaitable[Any] | None]:
    ds = DataSet.of(NS, HINT_SET_NAME)
    for index_name, bin_name in (
        (HINT_INDEX_NAME, BIN_AGE),
        (HINT_SCORE_INDEX_NAME, BIN_SCORE),
    ):
        yield _create_idx(
            ops, set_name=HINT_SET_NAME, bin_name=bin_name,
            index_name=index_name, index_type=IndexType.NUMERIC,
        )
    yield _upsert(
        ops, ds, hint_key_name("1"),
        {BIN_AGE: 25, BIN_SCORE: 25, BIN_COUNTRY: "US"},
    )
    yield _upsert(
        ops, ds, hint_key_name("2"),
        {BIN_AGE: 30, BIN_SCORE: 30, BIN_COUNTRY: "CA"},
    )
    yield _wait_set_visible(ops, HINT_SET_NAME, 2)
    yield _wait_index(ops, HINT_SET_NAME, Filter.range(BIN_AGE, 25, 30))
    yield _wait_index(ops, HINT_SET_NAME, Filter.range(BIN_SCORE, 25, 30))


def _seed_qp_cdt_steps(ops: _SeedOps) -> Iterator[Awaitable[Any] | None]:
    ds = DataSet.of(NS, CDT_SET_NAME)
    yield _create_idx(
        ops, set_name=CDT_SET_NAME, bin_name=CDT_MAP_BIN,
        index_name=CDT_MAP_INDEX, index_type=IndexType.STRING,
        collection_type=CollectionIndexType.MAP_KEYS,
    )
    yield _create_idx(
        ops, set_name=CDT_SET_NAME, bin_name=CDT_LIST_BIN,
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
        yield _upsert(
            ops, ds, cdt_key_name(i),
            {CDT_MAP_BIN: map_data, CDT_LIST_BIN: list_data},
        )
    yield _wait_set_visible(ops, CDT_SET_NAME, CDT_SIZE)
    yield _wait_index(
        ops, CDT_SET_NAME,
        Filter.contains(CDT_MAP_BIN, CDT_MAP_KEY, CollectionIndexType.MAP_KEYS),
    )
    yield _wait_index(
        ops, CDT_SET_NAME,
        Filter.contains(CDT_LIST_BIN, CDT_LIST_BLOB_BYTES, CollectionIndexType.LIST),
    )


def _run_seed_steps_sync(steps: Iterator[Awaitable[Any] | None]) -> None:
    for step in steps:
        if step is not None:
            raise RuntimeError("sync seed step returned an awaitable")


async def _run_seed_steps_async(steps: Iterator[Awaitable[Any] | None]) -> None:
    for step in steps:
        if step is not None:
            await step


def _seed_all_sync(ops: _SeedOps) -> None:
    _run_seed_steps_sync(_seed_qsel_steps(ops))
    _run_seed_steps_sync(_seed_qscexp_steps(ops))
    _run_seed_steps_sync(_seed_qselhint_steps(ops))
    _run_seed_steps_sync(_seed_qp_cdt_steps(ops))


async def _seed_all_async(ops: _SeedOps) -> None:
    await _run_seed_steps_async(_seed_qsel_steps(ops))
    await _run_seed_steps_async(_seed_qscexp_steps(ops))
    await _run_seed_steps_async(_seed_qselhint_steps(ops))
    await _run_seed_steps_async(_seed_qp_cdt_steps(ops))


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
    await _run_seed_steps_async(_truncate_steps(session, async_mode=True))
    ops = _SeedOps(
        client=client,
        session=session,
        pac=client.underlying_client,
        wait_for_index=wait_for_index,
        wait_for_set_visible=wait_for_set_visible,
        create_index=create_index_quiet_async,
        async_mode=True,
    )
    await _seed_all_async(ops)


def seed_query_selection_sync(
    client: Any,
    session: Any,
    sync_wait_for_index: Callable[..., None],
    sync_wait_for_set_visible: Callable[..., None],
) -> None:
    """Seed all four query-selection sets and wait for SI readiness (sync)."""
    _run_seed_steps_sync(_truncate_steps(session, async_mode=False))
    ops = _SeedOps(
        client=client,
        session=session,
        pac=client.underlying_client,
        wait_for_index=sync_wait_for_index,
        wait_for_set_visible=sync_wait_for_set_visible,
        create_index=create_index_quiet_blocking,
        async_mode=False,
    )
    _seed_all_sync(ops)


async def teardown_query_selection_async(client: Any, session: Any) -> None:
    await _run_seed_steps_async(_truncate_steps(session, async_mode=True))
    await _run_seed_steps_async(_drop_index_steps(client, async_mode=True))


def teardown_query_selection_sync(client: Any, session: Any) -> None:
    _run_seed_steps_sync(_truncate_steps(session, async_mode=False))
    _run_seed_steps_sync(_drop_index_steps(client, async_mode=False))
