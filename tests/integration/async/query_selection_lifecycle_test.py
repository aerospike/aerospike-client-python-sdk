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

"""Server-side selection across an index's lifetime.

The other selection suites hold the index set still. These create and drop one
underneath a query and assert the rows never change: the planner may switch
between a secondary index and a primary-index scan, but the answer is the same
either way. Only ``allow_scans_with_where=False`` makes the difference visible,
by refusing the fallback.

This module owns its own set and index. It must not touch the shared
query-selection seed, whose indexes every other module in the area depends on.
"""

from __future__ import annotations

import pytest
from aerospike_async import IndexType

from aerospike_sdk import DataSet, QueryHint, ResultCode
from aerospike_sdk.exceptions import AerospikeError

from tests.integration.query_selection_helpers import (
    NS,
    create_index_quiet_async,
    drop_index_quiet_async,
)
from tests.pac_compat import requires_query_selection

LIFECYCLE_SET = "qsel_lifecycle"
AGE_BIN = "age"
SCORE_BIN = "score"
AGE_INDEX = "qsel_lifecycle_age_idx"
SCORE_INDEX = "qsel_lifecycle_score_idx"
KEY_PREFIX = "qsellife"
SIZE = 20
AGE_RANGE_WHERE = f"$.{AGE_BIN} >= 5 and $.{AGE_BIN} <= 12"
EXPECTED_AGES = list(range(5, 13))


@pytest.fixture
async def lifecycle_set(query_selection_cluster):
    """A private set with an age index, restored however the test leaves it."""
    session = query_selection_cluster.session
    client = query_selection_cluster.client
    pac = client.underlying_client
    ds = DataSet.of(NS, LIFECYCLE_SET)

    for i in range(SIZE):
        await (
            session.upsert(ds.id(f"{KEY_PREFIX}{i}"))
            .put({AGE_BIN: i, SCORE_BIN: i * 10})
            .execute()
        )
    await create_index_quiet_async(
        pac,
        set_name=LIFECYCLE_SET,
        bin_name=AGE_BIN,
        index_name=AGE_INDEX,
        index_type=IndexType.NUMERIC,
    )
    yield session, pac, client, ds
    for name in (AGE_INDEX, SCORE_INDEX):
        await drop_index_quiet_async(client, NS, LIFECYCLE_SET, name)
    for i in range(SIZE):
        await session.delete(ds.id(f"{KEY_PREFIX}{i}")).execute()


async def _ages(session, hint=None):
    builder = (
        session.query(namespace=NS, set_name=LIFECYCLE_SET).where(AGE_RANGE_WHERE)
    )
    if hint is not None:
        builder = builder.with_hint(hint)
    stream = await builder.execute()
    ages = []
    try:
        async for result in stream:
            ages.append(result.record_or_raise().bins[AGE_BIN])
    finally:
        stream.close()
    return sorted(ages)


class TestIndexLifecycle:
    """Rows survive the index being dropped and recreated underneath the query."""

    @requires_query_selection
    async def test_rows_are_unchanged_after_the_only_index_is_dropped(
        self, lifecycle_set,
    ):
        """The scan fallback answers the same, but only where it is permitted.

        ``Behavior.DEFAULT`` refuses a primary-index scan behind a ``where()``,
        so dropping the index turns the default query into ``INDEX_NOT_FOUND``
        rather than a silent fallback. Permitting scans restores the original
        rows, which is what shows the answer never depended on the index.
        """
        session, _, client, _ = lifecycle_set
        with_index = await _ages(session)
        assert with_index == EXPECTED_AGES

        await drop_index_quiet_async(client, NS, LIFECYCLE_SET, AGE_INDEX)

        with pytest.raises(AerospikeError) as excinfo:
            await _ages(session)
        assert excinfo.value.result_code == ResultCode.INDEX_NOT_FOUND

        permitted = await _ages(session, QueryHint(allow_scans_with_where=True))
        assert permitted == with_index

    @requires_query_selection
    async def test_rows_are_unchanged_after_an_index_is_created(self, lifecycle_set):
        """Adding an index the predicate cannot use does not change the answer."""
        session, pac, _, _ = lifecycle_set
        before = await _ages(session)
        await create_index_quiet_async(
            pac,
            set_name=LIFECYCLE_SET,
            bin_name=SCORE_BIN,
            index_name=SCORE_INDEX,
            index_type=IndexType.NUMERIC,
        )
        assert await _ages(session) == before == EXPECTED_AGES

    @requires_query_selection
    async def test_chunked_iteration_is_stable_while_an_index_is_created(
        self, lifecycle_set,
    ):
        """A second index appearing mid-iteration does not disturb the set."""
        session, pac, _, _ = lifecycle_set
        created: set[str] = set()
        stream = await (
            session.query(namespace=NS, set_name=LIFECYCLE_SET)
            .where(AGE_RANGE_WHERE)
            .chunk_size(3)
            .execute()
        )
        ages = []
        chunks = 0
        try:
            # Plain iteration yields only the loaded chunk; advancing the
            # cursor is the caller's job.
            while await stream.has_more_chunks():
                chunks += 1
                async for result in stream:
                    ages.append(result.record_or_raise().bins[AGE_BIN])
                if len(ages) >= 3 and SCORE_INDEX not in created:
                    created.add(SCORE_INDEX)
                    await create_index_quiet_async(
                        pac,
                        set_name=LIFECYCLE_SET,
                        bin_name=SCORE_BIN,
                        index_name=SCORE_INDEX,
                        index_type=IndexType.NUMERIC,
                    )
        finally:
            stream.close()
        assert sorted(ages) == EXPECTED_AGES
        assert chunks > 1, "chunk_size did not actually split the result"
