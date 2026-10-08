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

"""Shared constants and helpers for query-selection integration tests."""

from __future__ import annotations

import math
import struct
from collections.abc import Callable
from typing import Any, Optional

import pytest
from aerospike_async import QuerySelection, QueryWhereFlags  # noqa: F401 — re-exported for integration tests
from aerospike_sdk import AerospikeError, DataSet, QueryHint, ResultCode
from tests.integration.namespace import general_namespace


NS = general_namespace()
SET_NAME = "qselint"
QSEL_DS = DataSet.of(NS, SET_NAME)
INDEX_NAME = "qsel_age_idx"
SCORE_INDEX_NAME = "qsel_score_idx"
BOGUS_INDEX_NAME = "qsel_nonexistent_idx"
BIN_AGE = "age"
BIN_SCORE = "score"
BIN_COUNTRY = "country"
KEY_PREFIX = "qselkey"
SIZE = 50

# Hint-flags integration fixture (qselhint set)
HINT_SET_NAME = "qselhint"
HINT_DS = DataSet.of(NS, HINT_SET_NAME)
HINT_INDEX_NAME = "qselhint_age_idx"
HINT_SCORE_INDEX_NAME = "qselhint_score_idx"
HINT_BOGUS_INDEX_NAME = "qselhint_missing_idx"
HINT_KEY_PREFIX = "qselhintkey"

# Explain-scope integration fixture (qscexp set)
SCOPE_SET_NAME = "qscexp"
SCOPE_INT_INDEX = "qscexp_age_idx"
SCOPE_BLOB_INDEX = "qscexp_bb_idx"
SCOPE_MAP_INDEX = "qscexp_map_idx"
SCOPE_AGE_BIN = "age"
SCOPE_COUNTRY_BIN = "country"
SCOPE_BLOB_BIN = "bb"
SCOPE_MAP_BIN = "map_bin"
SCOPE_MAP_KEY = "mkey2"
SCOPE_TAG_BIN = "tag"
SCOPE_TAG_INDEX = "qscexp_tag_idx"
SCOPE_TAG_MATCH = "featured"
SCOPE_LOC_BIN = "loc"
SCOPE_LOC_INDEX = "qscexp_loc_idx"
SCOPE_PT_BIN = "pt"
SCOPE_PT_INDEX = "qscexp_pt_idx"
SCOPE_SCORE_LIST_BIN = "scoreList"
SCOPE_SCORE_LIST_INDEX = "qscexp_score_list_idx"
SCOPE_SCORE_LIST_POSITION = 2
SCOPE_SCORE_LIST_MATCH = 42
SCOPE_VENUE_BIN = "venue"
SCOPE_VENUE_INDEX = "qscexp_venue_loc_idx"
SCOPE_VENUE_KEY = "location"
SCOPE_NAME_BIN = "name"
SCOPE_UPPER_INDEX = "qscexp_upper_name_idx"
SCOPE_UPPER_MATCH = "ALICE"
SCOPE_AGE_PLUS_INDEX = "qscexp_age_plus_idx"
SCOPE_AGE_PLUS_MATCH = 26
# STRING and BLOB index bounds are capped at this many bytes; geo bounds are not.
STRING_BOUND_MAX = 2048

# CDT planner integration fixture (qp_cdt set)
CDT_SET_NAME = "qp_cdt"
CDT_KEY_PREFIX = "qpcdt"
CDT_MAP_BIN = "map_bin"
CDT_LIST_BIN = "list_bin"
CDT_LIST_STR_BIN = "list_str_bin"
CDT_INT_LIST_BIN = "int_list_bin"
CDT_INT_MAP_BIN = "int_map_bin"
CDT_NESTED_BIN = "nested_bin"
CDT_NESTED_KEY = "inner"
CDT_MAP_KEY = "mkey2"
CDT_MAP_VALUE_TARGET = "mv_target"
CDT_LIST_STR_TARGET = "ls_target"
CDT_NESTED_TARGET = "nested_target"
CDT_MAP_INDEX = "qp_mapkeys_idx"
CDT_MAP_VALUES_INDEX = "qp_mapvalues_idx"
CDT_LIST_INDEX = "qp_list_idx"
CDT_LIST_STR_INDEX = "qp_list_str_idx"
CDT_INT_LIST_INDEX = "qp_int_list_idx"
CDT_INT_MAP_KEYS_INDEX = "qp_int_mapkeys_idx"
CDT_INT_MAP_VALUES_INDEX = "qp_int_mapvalues_idx"
CDT_NESTED_INDEX = "qp_nested_list_idx"
CDT_LIST_RANGE = (10, 30)
CDT_MAP_KEY_RANGE = (10, 30)
CDT_MAP_VALUE_RANGE = (100, 300)
CDT_STR_MAP_KEY_RANGE = ("mkey10", "mkey20")
CDT_STR_MAP_VALUE_RANGE = ("mv10", "mv20")
CDT_SIZE = 20


def key_name(i: int) -> str:
    return f"{KEY_PREFIX}{i}"


def hint_key_name(suffix: str) -> str:
    return f"{HINT_KEY_PREFIX}{suffix}"


def cdt_key_name(i: int) -> str:
    return f"{CDT_KEY_PREFIX}{i}"


def long_bytes_be(value: int) -> bytes:
    """8-byte big-endian integer."""
    return struct.pack(">q", value)


SCOPE_BLOB_BYTES = long_bytes_be(50001)
CDT_LIST_BLOB_BYTES = long_bytes_be(50003)


def point_geo_json(lng: float, lat: float) -> str:
    return f'{{"type":"Point","coordinates":[{lng:.7f},{lat:.7f}]}}'


def circle_geo_json(lng: float, lat: float, radius_meters: float) -> str:
    return f'{{"type":"AeroCircle","coordinates":[[{lng},{lat}],{radius_meters}]}}'


def circle_polygon_geo_json(lng: float, lat: float, radius_deg: float, vertices: int) -> str:
    """Closed ring approximating a circle; the vertex count sizes the literal."""
    points = []
    for i in range(vertices + 1):
        theta = 2 * math.pi * (i % vertices) / vertices
        points.append(
            f"[{lng + radius_deg * math.cos(theta):.6f},{lat + radius_deg * math.sin(theta):.6f}]"
        )
    return '{"type":"Polygon","coordinates":[[' + ",".join(points) + "]]}"


SCOPE_MATCH_LNG, SCOPE_MATCH_LAT = -122.0986857, 37.4214209
SCOPE_FAR_LNG, SCOPE_FAR_LAT = -121.0, 38.0
SCOPE_MATCH_POINT = point_geo_json(SCOPE_MATCH_LNG, SCOPE_MATCH_LAT)
SCOPE_LARGE_REGION = circle_polygon_geo_json(SCOPE_MATCH_LNG, SCOPE_MATCH_LAT, 0.5, 200)


def blob_hex_literal(blob_bytes: bytes) -> str:
    """Server AEL hex blob literal for equality (``x'...'`` form)."""
    return blob_bytes.hex()


# Server particle type carried in an index range's key-type byte.
GEOJSON_PARTICLE_TYPE = 23


# An explain plan's ``index_range_bytes`` lays out byte 1 as the bin-name length,
# then the bin name, one key-type byte, and a bound prefixed by a 4-byte
# big-endian length.
def index_range_bin_name_len(range_bytes: bytes) -> int:
    return range_bytes[1]


def index_range_bin_name(range_bytes: bytes) -> str:
    return range_bytes[2:2 + index_range_bin_name_len(range_bytes)].decode()


def index_range_ktype(range_bytes: bytes) -> int:
    return range_bytes[2 + index_range_bin_name_len(range_bytes)]


def index_range_bound(range_bytes: bytes) -> bytes:
    offset = 2 + index_range_bin_name_len(range_bytes) + 1
    (length,) = struct.unpack_from(">I", range_bytes, offset)
    return range_bytes[offset + 4:offset + 4 + length]


def explain_where_flags(hint: Optional[QueryHint]) -> Optional[int]:
    """Map :class:`QueryHint` to PAC ``explain_where_flags`` (field ``44``)."""
    if hint is None:
        return None
    flags = QueryWhereFlags.EXPLAIN
    # PAC-level helper: pass through only an explicit disallow. Resolving an
    # unset hint against the Behavior happens in the SDK layer, not here, so
    # unset hints leave the primary-index fallback available.
    if hint.allow_scans_with_where is False:
        flags |= QueryWhereFlags.REQUIRE_INDEX
    if hint.hard_hint:
        flags |= QueryWhereFlags.HARD_HINT
    if flags == QueryWhereFlags.EXPLAIN:
        return None
    return int(flags)


async def explain_plan_async(pac, where: str, *, set_name: str = SET_NAME, hint=None):
    """Run phase-1 explain via PAC ``query_explain``."""
    index_name_hint = hint.index_name if hint is not None else None
    return await pac.query_explain(
        NS,
        where,
        set_name=set_name,
        index_name_hint=index_name_hint,
        explain_where_flags=explain_where_flags(hint),
    )


def explain_plan_blocking(pac, where: str, *, set_name: str = SET_NAME, hint=None):
    index_name_hint = hint.index_name if hint is not None else None
    return pac.query_explain_blocking(
        NS,
        where,
        set_name=set_name,
        index_name_hint=index_name_hint,
        explain_where_flags=explain_where_flags(hint),
    )


async def create_index_quiet_async(
    pac,
    *,
    set_name: str,
    bin_name: Optional[str],
    index_name: str,
    index_type,
    collection_type=None,
    ctx=None,
    expression=None,
) -> None:
    """Create an index and wait for its build, tolerating one that exists.

    The wait lives here so no caller can forget it: an index that is registered
    but still building answers queries with fewer records than it will once the
    build completes. An ``expression`` index has no bin, so ``bin_name`` and
    ``ctx`` are ignored for it.
    """
    try:
        if expression is not None:
            task = await pac.create_index_using_expression(
                NS, set_name, index_name, index_type, expression, collection_type,
            )
        else:
            task = await pac.create_index(
                NS, set_name, bin_name, index_name, index_type, collection_type, ctx,
            )
    except Exception as exc:
        if getattr(exc, "result_code", None) != ResultCode.INDEX_FOUND:
            raise
        return  # already present, so its build finished in an earlier run
    await task.wait_till_complete()


def create_index_quiet_blocking(
    pac,
    *,
    set_name: str,
    bin_name: Optional[str],
    index_name: str,
    index_type,
    collection_type=None,
    ctx=None,
    expression=None,
) -> None:
    """Blocking sibling of :func:`create_index_quiet_async`."""
    try:
        if expression is not None:
            task = pac.create_index_using_expression_blocking(
                NS, set_name, index_name, index_type, expression, collection_type,
            )
        else:
            task = pac.create_index_blocking(
                NS, set_name, bin_name, index_name, index_type, collection_type, ctx,
            )
    except Exception as exc:
        if getattr(exc, "result_code", None) != ResultCode.INDEX_FOUND:
            raise
        return  # already present, so its build finished in an earlier run
    task.wait_till_complete_blocking()


async def drop_index_quiet_async(
    client: Any,
    ns: str,
    set_name: str,
    index_name: str,
) -> None:
    try:
        await client.index(DataSet.of(ns, set_name)).named(index_name).drop()
    except Exception as exc:
        if getattr(exc, "result_code", None) != ResultCode.INDEX_NOT_FOUND:
            raise


def drop_index_quiet_blocking(
    client: Any,
    ns: str,
    set_name: str,
    index_name: str,
) -> None:
    try:
        client.index(DataSet.of(ns, set_name)).named(index_name).drop()
    except Exception as exc:
        if getattr(exc, "result_code", None) != ResultCode.INDEX_NOT_FOUND:
            raise


async def collect_scores_async(stream) -> list[int]:
    scores: list[int] = []
    try:
        async for result in stream:
            rec = result.record_or_raise()
            scores.append(rec.bins[BIN_SCORE])
    finally:
        stream.close()
    return sorted(scores)


def collect_scores_sync(stream) -> list[int]:
    scores: list[int] = []
    try:
        for result in stream:
            rec = result.record_or_raise()
            scores.append(rec.bins[BIN_SCORE])
    finally:
        stream.close()
    return sorted(scores)


async def collect_ages_async(stream) -> list[int]:
    ages: list[int] = []
    try:
        async for result in stream:
            rec = result.record_or_raise()
            ages.append(rec.bins[BIN_AGE])
    finally:
        stream.close()
    return sorted(ages)


def collect_ages_sync(stream) -> list[int]:
    ages: list[int] = []
    try:
        for result in stream:
            rec = result.record_or_raise()
            ages.append(rec.bins[BIN_AGE])
    finally:
        stream.close()
    return sorted(ages)


async def count_matches_async(
    session,
    dataset: DataSet,
    where: str,
    bin_name: str,
    *,
    needs_scan: bool,
    check: Optional[Callable[[Any], bool]] = None,
) -> int:
    """Count the rows ``where`` returns, after pinning whether an index serves it.

    A predicate no index can serve falls back to a primary-index scan, which a
    query disallowing scans refuses. With ``needs_scan`` that refusal is asserted
    first and the rows are then read under the default, which allows the
    fallback; without it the rows are read with scans disallowed, which shows an
    index served them. ``check`` is applied to each returned bin value.
    """
    index_only = QueryHint(allow_scans_with_where=False)

    def query():
        return session.query(dataset).bins([bin_name]).where(where)

    if needs_scan:
        with pytest.raises(AerospikeError) as exc_info:
            await count_records_async(await query().with_hint(index_only).execute())
        assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND
        stream = await query().execute()
    else:
        stream = await query().with_hint(index_only).execute()

    count = 0
    try:
        async for result in stream:
            value = result.record_or_raise().bins[bin_name]
            if check is not None:
                assert check(value), value
            count += 1
    finally:
        stream.close()
    return count


async def count_records_async(stream) -> int:
    count = 0
    try:
        async for result in stream:
            result.record_or_raise()
            count += 1
    finally:
        stream.close()
    return count


def count_records_sync(stream) -> int:
    count = 0
    try:
        for result in stream:
            result.record_or_raise()
            count += 1
    finally:
        stream.close()
    return count
