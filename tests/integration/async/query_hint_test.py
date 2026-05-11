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

"""Integration tests for QueryHint with index_name, bin_name, and query_duration."""

import pytest
import pytest_asyncio
from aerospike_async import Filter, QueryDuration

from aerospike_sdk import (
    DataSet,
    Client,
    QueryHint,
)


SET_NAME = "query_hint_test"
INDEX_NAME = "pfc_qhint_age_idx"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy, enterprise, wait_for_index):
    """Setup client, data, and a secondary index for hint tests."""
    async with Client(
        seeds=aerospike_host,
        policy=client_policy,
        index_refresh_interval=0.25,
    ) as client:
        session = client.create_session()
        ds = DataSet.of("test", SET_NAME)

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

        try:
            await (
                client.index("test", SET_NAME)
                .on_bin("age")
                .named(INDEX_NAME)
                .numeric()
                .create()
            )
        except Exception:
            pass

        await wait_for_index(client, "test", SET_NAME, Filter.range("age", 20, 29))

        yield client

        try:
            await client.index("test", SET_NAME).named(INDEX_NAME).drop()
        except Exception:
            pass


class TestQueryDurationHint:
    """query_duration hint overrides policy.expected_duration."""

    async def test_query_duration_short(self, client):
        stream = await (
            client.query("test", SET_NAME)
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

    async def test_query_duration_long(self, client):
        stream = await (
            client.query("test", SET_NAME)
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

    async def test_filter_with_index_name_hint(self, client):
        """Filter.range + index_name hint on a named numeric index."""
        stream = await (
            client.query("test", SET_NAME)
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

    async def test_index_name_via_ael(self, client):
        """AEL where() + index_name hint with auto-discovered index."""
        stream = await (
            client.query("test", SET_NAME)
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

    async def test_index_name_with_query_duration(self, client):
        """Combine index_name and query_duration in a single hint."""
        stream = await (
            client.query("test", SET_NAME)
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


class TestBinNameHint:
    """bin_name hint redirects the filter to a different bin."""

    async def test_bin_name_via_ael(self, client):
        """AEL referencing $.age with bin_name hint and auto-discovered index."""
        stream = await (
            client.query("test", SET_NAME)
            .where("$.age == 25")
            .with_hint(QueryHint(bin_name="age"))
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record_or_raise())
        stream.close()
        assert len(records) == 1
        assert records[0].bins["age"] == 25
