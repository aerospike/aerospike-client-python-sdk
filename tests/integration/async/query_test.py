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

"""Tests for QueryBuilder SDK API."""

import asyncio
import time

import pytest
import pytest_asyncio
from aerospike_async import Filter, Key, PartitionFilter, QueryPolicy
from aerospike_sdk import DataSet, Exp, Client, val
from aerospike_sdk.aio.operations.query import QueryBuilder


async def _wait_for_set_count(
    client: Client, ns: str, set_name: str, expected: int,
    *, timeout: float = 5.0, interval: float = 0.05,
) -> None:
    """Poll a set scan until it returns ``expected`` records.

    Set scans iterate partitions and can miss writes that were committed
    just before the scan started — there is no read-your-own-writes
    guarantee for scans, only for point reads. A fixed ``asyncio.sleep``
    is therefore flaky under CI load; this helper polls until the scan
    is consistent with the fixture, failing only if the server genuinely
    never catches up within ``timeout``.
    """
    deadline = time.monotonic() + timeout
    last_count = -1
    while time.monotonic() < deadline:
        stream = await client.query(ns, set_name).execute()
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        if count >= expected:
            return
        last_count = count
        await asyncio.sleep(interval)
    raise AssertionError(
        f"set '{ns}.{set_name}' scan never reached {expected} records "
        f"within {timeout}s (last observed: {last_count})"
    )


def _namespace_query(client: Client, namespace: str) -> QueryBuilder:
    return QueryBuilder(
        client=client.underlying_client,
        namespace=namespace,
        set_name=None,
        indexes_monitor=client._indexes_monitor,
    )


async def _collect_query_kinds(query_builder: QueryBuilder) -> set[str]:
    stream = await query_builder.execute()
    kinds = set()
    try:
        async for result in stream:
            rec = result.record_or_raise()
            kinds.add(rec.bins["kind"])
    finally:
        stream.close()
    return kinds


async def _wait_for_query_kinds(
    query_factory, expected: set[str],
    *, timeout: float = 5.0, interval: float = 0.05,
) -> None:
    deadline = time.monotonic() + timeout
    last_kinds: set[str] = set()
    while time.monotonic() < deadline:
        last_kinds = await _collect_query_kinds(query_factory())
        if last_kinds == expected:
            return
        await asyncio.sleep(interval)
    raise AssertionError(
        f"query never returned {expected!r} within {timeout}s "
        f"(last observed: {last_kinds!r})"
    )


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy):
    """Setup SDK client and test data for query tests."""
    async with Client(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        ds = DataSet.of("test", "query_test")

        for i in range(10):
            try:
                await session.delete(ds.id(i)).execute()
            except Exception:
                pass

        for i in range(10):
            await session.upsert(ds.id(i)).put({"id": i, "age": 20 + i, "name": f"User{i}"}).execute()

        # Poll until all 10 writes are visible to a set scan. Fixes the
        # intermittent "count == 0 (expected 5)" failures we saw under CI
        # load when a fixed 100 ms sleep wasn't enough.
        await _wait_for_set_count(client, "test", "query_test", 10)

        yield client

async def test_query_basic(client):
    """Test basic query operation without filters."""
    stream = await client.query("test", "query_test").execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        assert "id" in result.record.bins
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0

async def test_query_with_bins(client):
    """Test query with specific bin selection."""
    stream = await client.query("test", "query_test").bins(["name", "age"]).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        assert "name" in result.record.bins or "age" in result.record.bins
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

async def test_query_with_policy(client):
    """Test query with custom policy."""
    policy = QueryPolicy()
    stream = await client.query("test", "query_test").with_policy(policy).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

async def test_query_with_partition_filter(client):
    """Test query with partition filter."""
    partition_filter = PartitionFilter.all()
    stream = await client.query("test", "query_test").partition(partition_filter).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

async def test_query_builder_chaining(client):
    """Test method chaining on query builder."""
    policy = QueryPolicy()
    partition_filter = PartitionFilter.all()

    stream = await (
        client.query("test", "query_test")
        .bins(["name", "age"])
        .with_policy(policy)
        .partition(partition_filter)
        .execute()
    )
    count = 0
    async for result in stream:
        assert result.is_ok
        assert "name" in result.record.bins or "age" in result.record.bins
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

async def test_query_with_range_filter(client, enterprise, wait_for_index):
    """Test query with range filter (requires index)."""
    try:
        await client.index("test", "query_test").on_bin("age").named("age_idx").numeric().create()
    except Exception:
        pass
    await wait_for_index(client, "test", "query_test", Filter.range("age", 22, 26))

    try:
        stream = await (
            client.query("test", "query_test")
            .filter(Filter.range("age", 22, 26))
            .execute()
        )
        count = 0
        async for result in stream:
            rec = result.record_or_raise()
            assert "age" in rec.bins
            assert 22 <= rec.bins["age"] <= 26
            count += 1
            if count >= 5:
                break

        stream.close()
    finally:
        try:
            await client.index("test", "query_test").named("age_idx").drop()
        except Exception:
            pass

async def test_query_empty_result(client):
    """Test query that returns no results."""
    stream = await client.query("test", "non_existent_set").execute()
    count = 0
    async for result in stream:
        count += 1

    stream.close()
    assert count == 0

async def test_query_iteration(client):
    """Test that query builder can execute and return a RecordStream."""
    query_builder = client.query("test", "query_test")
    assert hasattr(query_builder, "execute")

    stream = await query_builder.execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

async def test_query_with_filter_expression(client):
    """Test query with Exp (FilterExpression) for server-side filtering."""
    filter_exp = Exp.ge(
        Exp.int_bin("age"),
        Exp.int_val(25)
    )

    stream = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        rec = result.record_or_raise()
        assert "age" in rec.bins
        assert rec.bins["age"] >= 25
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0

async def test_query_with_filter_and_filter_expression(client, enterprise, wait_for_index):
    """Test query with both Filter (secondary index) and Exp (FilterExpression)."""
    try:
        await client.index("test", "query_test").on_bin("age").named("age_idx").numeric().create()
    except Exception:
        pass
    await wait_for_index(client, "test", "query_test", Filter.range("age", 20, 30))

    filter_exp = Exp.eq(
        Exp.string_bin("name"),
        Exp.string_val("User5")
    )

    try:
        stream = await (
            client.query("test", "query_test")
            .filter(Filter.range("age", 20, 30))
            .filter_expression(filter_exp)
            .execute()
        )
        count = 0
        async for result in stream:
            rec = result.record_or_raise()
            assert "age" in rec.bins
            assert 20 <= rec.bins["age"] <= 30
            assert rec.bins.get("name") == "User5"
            count += 1
            if count >= 5:
                break

        stream.close()
    finally:
        try:
            await client.index("test", "query_test").named("age_idx").drop()
        except Exception:
            pass

async def test_query_with_filter_expression_and(client):
    """Test query with Exp (FilterExpression) using AND for multiple conditions."""
    filter_exp = Exp.and_([
        Exp.ge(Exp.int_bin("age"), Exp.int_val(25)),
        Exp.le(Exp.int_bin("age"), Exp.int_val(27))
    ])

    stream = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        rec = result.record_or_raise()
        assert "age" in rec.bins
        assert 25 <= rec.bins["age"] <= 27
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0


# ============================================================================
# Metadata-based query tests 
# ============================================================================

async def test_query_with_ael_where(client):
    """Test query with AEL where() clause (expression filter via string AEL)."""
    stream = await (
        client.query("test", "query_test")
        .where("$.age >= 25")
        .execute()
    )
    count = 0
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins["age"] >= 25
        count += 1

    stream.close()
    assert count == 5


async def test_query_ael_and_or(client):
    """Test AEL where() with nested AND/OR conditions."""
    stream = await (
        client.query("test", "query_test")
        .where('$.age >= 22 and $.age <= 26')
        .execute()
    )
    count = 0
    async for result in stream:
        rec = result.record_or_raise()
        assert 22 <= rec.bins["age"] <= 26
        count += 1

    stream.close()
    assert count == 5


async def test_query_ael_not(client):
    """Test AEL where() with NOT condition."""
    stream = await (
        client.query("test", "query_test")
        .where('not ($.age >= 25)')
        .execute()
    )
    count = 0
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins["age"] < 25
        count += 1

    stream.close()
    assert count == 5


async def test_query_digest_modulo(client):
    """Test query with digestModulo metadata expression filter."""
    filter_exp = Exp.eq(Exp.digest_modulo(3), Exp.int_val(1))

    stream = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1

    stream.close()
    assert count >= 1


async def test_query_bin_exists(client):
    """Test query filtering by bin existence."""
    filter_exp = Exp.bin_exists("age")

    stream = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        rec = result.record_or_raise()
        assert "age" in rec.bins
        count += 1

    stream.close()
    assert count == 10


async def test_query_record_size(client):
    """Test query filtering by record size metadata."""
    filter_exp = Exp.ge(Exp.device_size(), Exp.int_val(0))

    stream = await (
        client.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1

    stream.close()
    assert count == 10


async def test_query_ael_set_name_matches_no_set_records(client):
    """Test AEL filtering for records written without a set name."""
    namespace = "test"
    named_set = "query_set_name_no_set"
    probe = "query-set-name-no-set-probe"
    no_set_key = Key(namespace, "", "query-set-name-no-set-empty")
    named_key = Key(namespace, named_set, "query-set-name-no-set-named")
    session = client.create_session()

    try:
        for key in (no_set_key, named_key):
            try:
                await session.delete(key).execute()
            except Exception:
                pass

        await session.upsert(no_set_key).put({"probe": probe, "kind": "no-set"}).execute()
        await session.upsert(named_key).put({"probe": probe, "kind": "named-set"}).execute()

        await _wait_for_query_kinds(
            lambda: _namespace_query(client, namespace).where(f"$.probe == '{probe}'"),
            {"no-set", "named-set"},
        )
        await _wait_for_query_kinds(
            lambda: _namespace_query(client, namespace).where(
                f"$.probe == '{probe}' and $.setName() == ''",
            ),
            {"no-set"},
        )
    finally:
        for key in (no_set_key, named_key):
            try:
                await session.delete(key).execute()
            except Exception:
                pass


async def test_query_exp_set_name_filters_out_no_set_records(client):
    """Test Exp filtering for named-set-only records."""
    namespace = "test"
    named_set = "query_set_name_named_only"
    probe = "query-set-name-named-only-probe"
    no_set_key = Key(namespace, "", "query-set-name-named-only-empty")
    named_key = Key(namespace, named_set, "query-set-name-named-only-named")
    session = client.create_session()

    try:
        for key in (no_set_key, named_key):
            try:
                await session.delete(key).execute()
            except Exception:
                pass

        await session.upsert(no_set_key).put({"probe": probe, "kind": "no-set"}).execute()
        await session.upsert(named_key).put({"probe": probe, "kind": "named-set"}).execute()

        await _wait_for_query_kinds(
            lambda: _namespace_query(client, namespace).where(f"$.probe == '{probe}'"),
            {"no-set", "named-set"},
        )

        named_set_only = Exp.and_([
            Exp.eq(Exp.string_bin("probe"), val(probe)),
            Exp.ne(Exp.set_name(), val("")),
        ])
        await _wait_for_query_kinds(
            lambda: _namespace_query(client, namespace).filter_expression(named_set_only),
            {"named-set"},
        )
    finally:
        for key in (no_set_key, named_key):
            try:
                await session.delete(key).execute()
            except Exception:
                pass


async def test_query_chunked_iteration(client):
    """Server-side chunked iteration via chunk_size + has_more_chunks."""
    stream = await (
        client.query("test", "query_test")
        .chunk_size(3)
        .execute()
    )
    total = 0
    chunks = 0
    while await stream.has_more_chunks():
        chunks += 1
        async for result in stream:
            assert result.is_ok
            total += 1
    stream.close()

    assert total == 10
    assert chunks >= 2


async def test_query_chunked_single_chunk(client):
    """chunk_size larger than dataset returns everything in one chunk."""
    stream = await (
        client.query("test", "query_test")
        .chunk_size(100)
        .execute()
    )
    total = 0
    chunks = 0
    while await stream.has_more_chunks():
        chunks += 1
        async for result in stream:
            total += 1
    stream.close()

    assert total == 10
    assert chunks == 1


async def test_has_more_chunks_on_non_chunked_stream(client):
    """has_more_chunks on a regular stream returns True once then False."""
    stream = await client.query("test", "query_test").execute()
    assert await stream.has_more_chunks() is True
    count = 0
    async for _ in stream:
        count += 1
    assert await stream.has_more_chunks() is False
    stream.close()
    assert count == 10
