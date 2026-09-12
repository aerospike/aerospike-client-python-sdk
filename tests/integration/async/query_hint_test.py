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

"""Integration tests for QueryHint with index_name and query_duration."""

import pytest_asyncio

from tests.pac_compat import requires_server_compiled_ael
from aerospike_sdk import Filter, QueryDuration

from aerospike_sdk import (
    DataSet,
    QueryHint,
)
from tests.integration.namespace import general_namespace


SET_NAME = "query_hint_test"
INDEX_NAME = "pfc_qhint_age_idx"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(
    aerospike_host, make_cluster_definition, enterprise,
    wait_for_set_visible,
):
    """Setup cluster, data, and a secondary index for hint tests."""
    cluster_def = make_cluster_definition(aerospike_host)
    async with await cluster_def.connect() as c:
        session = c.create_session()
        ds = DataSet.of(general_namespace(), SET_NAME)

        for i in range(10):
            try:
                await session.delete(ds.id(i)).execute()
            except Exception:
                pass

        for i in range(10):
            await (
                session.upsert(ds.id(i))
                .put({"id": i, "age": 20 + i, "name": f"User{i}"})
                .execute()
            )

        # Wait for the 10 writes to be visible to a set scan before creating
        # the SI — otherwise a still-populating index can be flagged "readable"
        # before all records have indexed entries, causing range queries to
        # return short and flaky-fail tests that assert exact counts.
        await wait_for_set_visible(session, general_namespace(), SET_NAME, 10)

        try:
            index_task = await (
                session.index(general_namespace(), SET_NAME)
                .on_bin("age")
                .named(INDEX_NAME)
                .numeric()
                .create()
            )
        except Exception:
            # Already present from an earlier run, so its build is long done and
            # there is no task to wait on.
            index_task = None

        if index_task is not None:
            await index_task.wait_till_complete()

        yield c

        try:
            await session.index(general_namespace(), SET_NAME).named(INDEX_NAME).drop()
        except Exception:
            pass


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def session(cluster):
    return cluster.create_session()


class TestQueryDurationHint:
    """query_duration hint overrides policy.expected_duration."""

    async def test_query_duration_short(self, session):
        stream = await (
            session.query(general_namespace(), SET_NAME)
            .with_hint(QueryHint(query_duration=QueryDuration.SHORT))
            .execute()
        )
        count = 0
        async for result in stream:
            assert result.is_ok
            count += 1
            if count >= 3:
                break
        stream.close()
        assert count > 0

    async def test_query_duration_long(self, session):
        stream = await (
            session.query(general_namespace(), SET_NAME)
            .with_hint(QueryHint(query_duration=QueryDuration.LONG))
            .execute()
        )
        count = 0
        async for result in stream:
            assert result.is_ok
            count += 1
            if count >= 3:
                break
        stream.close()
        assert count > 0


class TestIndexNameHint:
    """index_name hint directs the query to a specific named secondary index."""

    async def test_filter_with_index_name_hint(self, session):
        """Filter.range + index_name hint on a named numeric index."""
        stream = await (
            session.query(general_namespace(), SET_NAME)
            .filter(Filter.range_by_index(INDEX_NAME, 22, 26))
            .execute()
        )
        count = 0
        async for result in stream:
            rec = result.record_or_raise()
            assert 22 <= rec.bins["age"] <= 26
            count += 1
        stream.close()
        assert count == 5

    @requires_server_compiled_ael
    async def test_index_name_via_ael(self, session):
        """AEL where() + index_name hint with auto-discovered index."""
        stream = await (
            session.query(general_namespace(), SET_NAME)
            .where("$.age >= 25")
            .with_hint(QueryHint(index_name=INDEX_NAME))
            .execute()
        )
        count = 0
        async for result in stream:
            rec = result.record_or_raise()
            assert rec.bins["age"] >= 25
            count += 1
        stream.close()
        assert count == 5

    @requires_server_compiled_ael
    async def test_index_name_with_query_duration(self, session):
        """Combine index_name and query_duration in a single hint."""
        stream = await (
            session.query(general_namespace(), SET_NAME)
            .where("$.age == 27")
            .with_hint(QueryHint(
                index_name=INDEX_NAME,
                query_duration=QueryDuration.SHORT,
            ))
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record_or_raise())
        stream.close()
        assert len(records) == 1
        assert records[0].bins["age"] == 27
