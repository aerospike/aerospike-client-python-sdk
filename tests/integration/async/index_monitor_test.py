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

"""Integration tests for automatic secondary index discovery via IndexesMonitor."""

import asyncio
import uuid

import pytest
import pytest_asyncio

from aerospike_sdk import DataSet, Client


SET_NAME = "idx_monitor_integ"
INDEX_NAME = "pfc_auto_idx_age"
NAMESPACE = "test"


async def _wait_for_monitor_cache(
    client, namespace: str, index_name: str, *, present: bool,
    timeout: float = 5.0, interval: float = 0.1,
) -> bool:
    """Poll the monitor cache until *index_name* is (or isn't) discovered.

    Returns ``True`` when the desired state is reached before *timeout*.
    Avoids the fixed-sleep pattern that races on enterprise clusters when
    the refresh interval has been overridden for tests.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        ctx = client._indexes_monitor.get_index_context(namespace)
        names = {idx.name for idx in ctx.indexes} if ctx is not None else set()
        if (index_name in names) == present:
            return True
        await asyncio.sleep(interval)
    return False


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy, enterprise):
    """Client with seed data and a numeric secondary index on 'age'."""
    async with Client(
        seeds=aerospike_host,
        policy=client_policy,
        # refresh interval set super-small to spped up testing:
        index_refresh_interval=.25,
    ) as client:
        session = client.create_session()
        ds = DataSet.of(NAMESPACE, SET_NAME)

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
                client.index(NAMESPACE, SET_NAME)
                .on_bin("age")
                .named(INDEX_NAME)
                .numeric()
                .create()
            )
        except Exception:
            pass

        await asyncio.sleep(0.75 if not enterprise else 0.4)

        yield client

        try:
            await client.index(NAMESPACE, SET_NAME).named(INDEX_NAME).drop()
        except Exception:
            pass


class TestAutoIndexDiscovery:
    """Queries that rely on the monitor auto-discovering secondary indexes."""

    async def test_ael_query_uses_auto_discovered_index(self, client):
        """AEL where() should auto-generate a secondary index filter."""
        stream = await (
            client.query(NAMESPACE, SET_NAME)
            .where("$.age >= 25")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record_or_raise())
        stream.close()
        assert len(records) == 5
        ages = sorted(r.bins["age"] for r in records)
        assert ages == [25, 26, 27, 28, 29]

    async def test_ael_equality_with_auto_index(self, client):
        """AEL equality predicate on an indexed bin."""
        stream = await (
            client.query(NAMESPACE, SET_NAME)
            .where("$.age == 23")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record_or_raise())
        stream.close()
        assert len(records) == 1
        assert records[0].bins["age"] == 23

    async def test_explicit_index_context_overrides_monitor(self, client):
        """An explicit with_index_context() should take precedence."""
        from aerospike_sdk import Index, IndexContext, IndexTypeEnum

        ctx = IndexContext.of(NAMESPACE, [
            Index(
                bin="age",
                index_type=IndexTypeEnum.NUMERIC,
                namespace=NAMESPACE,
                name=INDEX_NAME,
                bin_values_ratio=1,
            ),
        ])
        stream = await (
            client.query(NAMESPACE, SET_NAME)
            .with_index_context(ctx)
            .where("$.age >= 28")
            .execute()
        )
        records = []
        async for result in stream:
            records.append(result.record_or_raise())
        stream.close()
        assert len(records) == 2
        ages = sorted(r.bins["age"] for r in records)
        assert ages == [28, 29]

    async def test_monitor_cache_accessible(self, client):
        """The monitor cache should contain the test namespace."""
        ctx = client._indexes_monitor.get_index_context(NAMESPACE)
        assert ctx is not None
        names = {idx.name for idx in ctx.indexes}
        assert INDEX_NAME in names


class TestIndexLifecycle:
    """Verify the cache updates when indexes are created/dropped."""

    async def test_new_index_discovered(self, client):
        """Creating a new index should appear in the cache after refresh."""
        new_idx = f"pfc_auto_idx_name_{uuid.uuid4().hex[:10]}"
        try:
            await (
                client.index(NAMESPACE, SET_NAME)
                .on_bin("name")
                .named(new_idx)
                .string()
                .create()
            )
            assert await _wait_for_monitor_cache(
                client, NAMESPACE, new_idx, present=True,
            ), f"Monitor never discovered {new_idx!r}"
        finally:
            try:
                await client.index(NAMESPACE, SET_NAME).named(new_idx).drop()
            except Exception:
                pass
            await _wait_for_monitor_cache(
                client, NAMESPACE, new_idx, present=False,
            )

    async def test_dropped_index_evicted(self, client):
        """Dropping an index should remove it from the cache after refresh."""
        temp_idx = f"pfc_auto_idx_temp_{uuid.uuid4().hex[:10]}"
        try:
            await (
                client.index(NAMESPACE, SET_NAME)
                .on_bin("id")
                .named(temp_idx)
                .numeric()
                .create()
            )
            assert await _wait_for_monitor_cache(
                client, NAMESPACE, temp_idx, present=True,
            ), f"Monitor never discovered {temp_idx!r}"

            await client.index(NAMESPACE, SET_NAME).named(temp_idx).drop()

            assert await _wait_for_monitor_cache(
                client, NAMESPACE, temp_idx, present=False,
            ), f"Monitor never evicted {temp_idx!r} after drop"
        finally:
            try:
                await client.index(NAMESPACE, SET_NAME).named(temp_idx).drop()
            except Exception:
                pass
