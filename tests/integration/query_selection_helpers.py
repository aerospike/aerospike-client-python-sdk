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

import struct
from typing import TYPE_CHECKING, Any, Optional

from aerospike_async import QuerySelection

if TYPE_CHECKING:
    from aerospike_async import QuerySelection as QuerySelectionType
    from aerospike_sdk import QueryHint
else:
    QuerySelectionType = Any


class QuerySelectionClientFacade:
    """Test helper: ``Client`` no longer exposes ``query()`` — delegate to ``Session``.

    Fixtures yield this so selection integration tests keep ``qsel_client.query(ns, set)``
    while still exposing ``underlying_client`` for direct PAC explain probes.
    """

    __slots__ = ("_client", "_session")

    def __init__(self, client: Any, session: Any) -> None:
        self._client = client
        self._session = session

    @property
    def underlying_client(self) -> Any:
        return self._client.underlying_client

    def query(self, namespace: str, set_name: str) -> Any:
        return self._session.query(namespace=namespace, set_name=set_name)

    def index(self, *args: Any, **kwargs: Any) -> Any:
        return self._client.index(*args, **kwargs)


NS = "test"
SET_NAME = "qselint"
INDEX_NAME = "qsel_age_idx"
SCORE_INDEX_NAME = "qsel_score_idx"
BOGUS_INDEX_NAME = "qsel_nonexistent_idx"
BIN_AGE = "age"
BIN_SCORE = "score"
BIN_COUNTRY = "country"
KEY_PREFIX = "qselkey"
SIZE = 50

# QuerySelectionHintFlagsTest fixture (Java qselhint set)
HINT_SET_NAME = "qselhint"
HINT_INDEX_NAME = "qselhint_age_idx"
HINT_SCORE_INDEX_NAME = "qselhint_score_idx"
HINT_BOGUS_INDEX_NAME = "qselhint_missing_idx"
HINT_KEY_PREFIX = "qselhintkey"

# QuerySelectionExplainScopeTest fixture (Java qscexp set)
SCOPE_SET_NAME = "qscexp"
SCOPE_INT_INDEX = "qscexp_age_idx"
SCOPE_BLOB_INDEX = "qscexp_bb_idx"
SCOPE_MAP_INDEX = "qscexp_map_idx"
SCOPE_AGE_BIN = "age"
SCOPE_COUNTRY_BIN = "country"
SCOPE_BLOB_BIN = "bb"
SCOPE_MAP_BIN = "map_bin"
SCOPE_MAP_KEY = "mkey2"

# QueryPlannerCollectionCdtTest fixture (Java qp_cdt set)
CDT_SET_NAME = "qp_cdt"
CDT_KEY_PREFIX = "qpcdt"
CDT_MAP_BIN = "map_bin"
CDT_LIST_BIN = "list_bin"
CDT_MAP_KEY = "mkey2"
CDT_MAP_INDEX = "qp_mapkeys_idx"
CDT_LIST_INDEX = "qp_list_idx"
CDT_SIZE = 20


def key_name(i: int) -> str:
    return f"{KEY_PREFIX}{i}"


def hint_key_name(suffix: str) -> str:
    return f"{HINT_KEY_PREFIX}{suffix}"


def cdt_key_name(i: int) -> str:
    return f"{CDT_KEY_PREFIX}{i}"


def long_bytes_be(value: int) -> bytes:
    """8-byte big-endian integer (Java ``Buffer.longToBytes``)."""
    return struct.pack(">q", value)


def blob_hex_literal(blob_bytes: bytes) -> str:
    """Server AEL hex blob literal for equality (Java ``x'...'``)."""
    return blob_bytes.hex()


def explain_where_flags(hint: Optional["QueryHint"]) -> Optional[int]:
    """Map :class:`QueryHint` to PAC ``explain_where_flags`` (field ``44``)."""
    from aerospike_async import QueryWhereFlags

    if hint is None:
        return None
    flags = QueryWhereFlags.EXPLAIN
    if hint.require_index:
        flags |= QueryWhereFlags.REQUIRE_INDEX
    if hint.hard_hint:
        flags |= QueryWhereFlags.HARD_HINT
    if flags == QueryWhereFlags.EXPLAIN:
        return None
    return int(flags)


async def explain_plan_async(pac, where: str, *, set_name: str = SET_NAME, hint=None):
    """Run phase-1 explain (mirrors Java ``IndexProbePlanner.plan``)."""
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
    bin_name: str,
    index_name: str,
    index_type,
    collection_type=None,
) -> None:
    from aerospike_sdk import ResultCode

    try:
        await pac.create_index(
            NS, set_name, bin_name, index_name, index_type, collection_type,
        )
    except Exception as exc:
        if getattr(exc, "result_code", None) != ResultCode.INDEX_FOUND:
            raise


def create_index_quiet_blocking(
    pac,
    *,
    set_name: str,
    bin_name: str,
    index_name: str,
    index_type,
    collection_type=None,
) -> None:
    from aerospike_sdk import ResultCode

    try:
        pac.create_index_blocking(
            NS, set_name, bin_name, index_name, index_type, collection_type,
        )
    except Exception as exc:
        if getattr(exc, "result_code", None) != ResultCode.INDEX_FOUND:
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
