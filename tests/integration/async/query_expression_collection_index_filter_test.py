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

"""Explicit ``filter()`` queries against an expression-based list index."""

import pytest
import pytest_asyncio
from aerospike_native import Filter

from aerospike_sdk import (
    CollectionIndexType,
    DataSet,
    ErrorStrategy,
    Exp,
    IndexNotFoundError,
    ResultCode,
)
from tests.integration.namespace import general_namespace
from tests.pnc_compat import requires_server_compiled_ael

DS = DataSet.of(general_namespace(), "query_exp_coll_filter")
INDEX_NAME = "query_exp_coll_filter_idx"
MISSING_INDEX = "query_exp_coll_filter_missing_idx"
LICENSES_BIN = "licenses"
STATUS_BIN = "status"
MATCH_LICENSE = "7XYZ789"
LICENSES_EXP = Exp.list_bin(LICENSES_BIN)
ROWS = {
    "vehicle-1": ([MATCH_LICENSE, "ABC123"], "active"),
    "vehicle-2": ([MATCH_LICENSE, "XYZ456"], "inactive"),
    "vehicle-3": (["OTHER123"], "active"),
}


async def _drop_index(session):
    try:
        await session.index(DS).named(INDEX_NAME).drop()
    except IndexNotFoundError:
        pass


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def session(aerospike_host, make_cluster_definition):
    async with make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        await _drop_index(session)
        task = await (
            session.index(DS)
            .on_expression(LICENSES_EXP)
            .named(INDEX_NAME)
            .string()
            .collection(CollectionIndexType.LIST)
            .create()
        )
        assert await task.wait_till_complete()
        for key, (licenses, status) in ROWS.items():
            await (
                session.upsert(DS.id(key))
                .bin(LICENSES_BIN).set_to(licenses)
                .bin(STATUS_BIN).set_to(status)
                .execute()
            )
        try:
            yield session
        finally:
            await session.delete([DS.id(key) for key in ROWS]).execute()
            await _drop_index(session)


def _by_index():
    return Filter.contains_by_index(INDEX_NAME, MATCH_LICENSE, CollectionIndexType.LIST)


async def _statuses(stream):
    try:
        return sorted([r.record_or_raise().bins[STATUS_BIN] async for r in stream])
    finally:
        stream.close()


async def test_contains_by_index_returns_rows_from_expression_list_index(session):
    stream = await session.query(DS).filter(_by_index()).execute()
    assert await _statuses(stream) == ["active", "inactive"]


async def test_contains_with_expression_returns_rows_from_expression_list_index(session):
    """The expression form finds the index by its expression rather than its name."""
    flt = Filter.contains(LICENSES_BIN, MATCH_LICENSE, CollectionIndexType.LIST).expression(
        LICENSES_EXP,
    )
    stream = await session.query(DS).filter(flt).execute()
    assert await _statuses(stream) == ["active", "inactive"]


async def test_filter_with_programmatic_residual_returns_active_match(session):
    stream = await (
        session.query(DS)
        .filter(_by_index())
        .where(Exp.eq(Exp.string_bin(STATUS_BIN), Exp.string_val("active")))
        .execute()
    )
    assert await _statuses(stream) == ["active"]


@requires_server_compiled_ael
async def test_filter_with_textual_residual_returns_active_match(session):
    stream = await (
        session.query(DS)
        .filter(_by_index())
        .where("$.status == 'active'")
        .execute()
    )
    assert await _statuses(stream) == ["active"]


async def test_missing_index_failure_surfaces_while_consuming_stream(session):
    """The query starts without complaint; the refusal arrives with the first read."""
    stream = await (
        session.query(DS)
        .filter(Filter.contains_by_index(MISSING_INDEX, MATCH_LICENSE, CollectionIndexType.LIST))
        .execute()
    )
    with pytest.raises(IndexNotFoundError) as exc_info:
        await _statuses(stream)
    assert exc_info.value.result_code == ResultCode.INDEX_NOT_FOUND


async def test_in_stream_error_strategy_preserves_explicit_filter(session):
    stream = await session.query(DS).filter(_by_index()).execute(on_error=ErrorStrategy.IN_STREAM)
    assert await _statuses(stream) == ["active", "inactive"]


async def test_chunked_execution_preserves_explicit_filter(session):
    stream = await session.query(DS).filter(_by_index()).chunk_size(1).execute()
    statuses = []
    try:
        while await stream.has_more_chunks():
            statuses.extend([r.record_or_raise().bins[STATUS_BIN] async for r in stream])
    finally:
        stream.close()
    assert sorted(statuses) == ["active", "inactive"]
