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
from aerospike_sdk import Filter, Key
from aerospike_async import PartitionFilter, QueryPolicy
from aerospike_sdk import DataSet, Exp, val
from aerospike_sdk.aio import Cluster
from aerospike_sdk.aio.operations.query import QueryBuilder
from tests.integration.namespace import general_namespace


async def _wait_for_set_count(
    cluster: Cluster, ns: str, set_name: str, expected: int,
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
    session = cluster.create_session()
    while time.monotonic() < deadline:
        stream = await session.query(ns, set_name).execute()
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


def _namespace_query(cluster: Cluster, namespace: str) -> QueryBuilder:
    return QueryBuilder(
        client=cluster._client.underlying_client,
        namespace=namespace,
        set_name=None,
        indexes_monitor=cluster._client._indexes_monitor,
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
        f"query never returned {expected!r} within {timeout}s (last observed: {last_kinds!r})"
    )


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    """Connect a Cluster and seed test data for query tests."""
    async with await make_cluster_definition(aerospike_host).connect() as c:
        session = c.create_session()
        ds = DataSet.of(general_namespace(), "query_test")

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
        await _wait_for_set_count(c, general_namespace(), "query_test", 10)

        yield c


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def session(cluster):
    return cluster.create_session()


async def test_query_basic(session):
    """Test basic query operation without filters."""
    stream = await session.query(general_namespace(), "query_test").execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        assert "id" in result.record.bins
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0

async def test_query_with_bins(session):
    """Test query with specific bin selection."""
    stream = await session.query(general_namespace(), "query_test").bins(["name", "age"]).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        assert "name" in result.record.bins or "age" in result.record.bins
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

async def test_query_with_policy(session):
    """Test query with custom policy."""
    policy = QueryPolicy()
    stream = await session.query(general_namespace(), "query_test").with_policy(policy).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

async def test_query_with_partition_filter(session):
    """Test query with partition filter."""
    partition_filter = PartitionFilter.all()
    stream = await session.query(general_namespace(), "query_test").partition(partition_filter).execute()
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

async def test_query_builder_chaining(session):
    """Test method chaining on query builder."""
    policy = QueryPolicy()
    partition_filter = PartitionFilter.all()

    stream = await (
        session.query(general_namespace(), "query_test")
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

async def test_query_with_range_filter(cluster, session, enterprise, wait_for_index):
    """Test query with range filter (requires index)."""
    try:
        await session.index(general_namespace(), "query_test").on_bin("age").named("age_idx").numeric().create()
    except Exception:
        pass
    await wait_for_index(cluster, general_namespace(), "query_test", Filter.range("age", 22, 26))

    try:
        stream = await (
            session.query(general_namespace(), "query_test")
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
            await session.index(general_namespace(), "query_test").named("age_idx").drop()
        except Exception:
            pass

async def test_query_empty_result(session):
    """Test query that returns no results."""
    stream = await session.query(general_namespace(), "non_existent_set").execute()
    count = 0
    async for result in stream:
        count += 1

    stream.close()
    assert count == 0

async def test_query_iteration(session):
    """Test that query builder can execute and return a RecordStream."""
    query_builder = session.query(general_namespace(), "query_test")
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

async def test_query_with_filter_expression(session):
    """Test query with Exp (FilterExpression) for server-side filtering."""
    filter_exp = Exp.ge(Exp.int_bin("age"), Exp.int_val(25))

    stream = await (
        session.query(general_namespace(), "query_test")
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

async def test_query_with_filter_and_filter_expression(cluster, session, enterprise, wait_for_index):
    """Test query with both Filter (secondary index) and Exp (FilterExpression)."""
    try:
        await session.index(general_namespace(), "query_test").on_bin("age").named("age_idx").numeric().create()
    except Exception:
        pass
    await wait_for_index(cluster, general_namespace(), "query_test", Filter.range("age", 20, 30))

    filter_exp = Exp.eq(Exp.string_bin("name"), Exp.string_val("User5"))

    try:
        stream = await (
            session.query(general_namespace(), "query_test")
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
            await session.index(general_namespace(), "query_test").named("age_idx").drop()
        except Exception:
            pass

async def test_query_with_filter_expression_and(session):
    """Test query with Exp (FilterExpression) using AND for multiple conditions."""
    filter_exp = Exp.and_(
        [Exp.ge(Exp.int_bin("age"), Exp.int_val(25)), Exp.le(Exp.int_bin("age"), Exp.int_val(27))]
    )

    stream = await (
        session.query(general_namespace(), "query_test")
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

async def test_query_with_ael_where(session):
    """Test query with AEL where() clause (expression filter via string AEL)."""
    stream = await (
        session.query(general_namespace(), "query_test")
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


async def test_query_ael_and_or(session):
    """Test AEL where() with nested AND/OR conditions."""
    stream = await (
        session.query(general_namespace(), "query_test")
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


async def test_query_ael_not(session):
    """Test AEL where() with NOT condition."""
    stream = await (
        session.query(general_namespace(), "query_test")
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


async def test_query_digest_modulo(session):
    """Test query with digestModulo metadata expression filter."""
    filter_exp = Exp.eq(Exp.digest_modulo(3), Exp.int_val(1))

    stream = await (
        session.query(general_namespace(), "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1

    stream.close()
    assert count >= 1


async def test_query_bin_exists(session):
    """Test query filtering by bin existence."""
    filter_exp = Exp.bin_exists("age")

    stream = await (
        session.query(general_namespace(), "query_test")
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


async def test_query_record_size(session):
    """Test query filtering by record size metadata."""
    filter_exp = Exp.ge(Exp.device_size(), Exp.int_val(0))

    stream = await (
        session.query(general_namespace(), "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    async for result in stream:
        assert result.is_ok
        count += 1

    stream.close()
    assert count == 10


async def test_query_ael_set_name_matches_no_set_records(cluster):
    """Test AEL filtering for records written without a set name."""
    namespace = general_namespace()
    named_set = "query_set_name_no_set"
    probe = "query-set-name-no-set-probe"
    no_set_key = Key(namespace, "", "query-set-name-no-set-empty")
    named_key = Key(namespace, named_set, "query-set-name-no-set-named")
    session = cluster.create_session()

    try:
        for key in (no_set_key, named_key):
            try:
                await session.delete(key).execute()
            except Exception:
                pass

        await session.upsert(no_set_key).put({"probe": probe, "kind": "no-set"}).execute()
        await session.upsert(named_key).put({"probe": probe, "kind": "named-set"}).execute()

        await _wait_for_query_kinds(
            lambda: _namespace_query(cluster, namespace).where(f"$.probe == '{probe}'"),
            {"no-set", "named-set"},
        )
        await _wait_for_query_kinds(
            lambda: _namespace_query(cluster, namespace).where(
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


async def test_query_exp_set_name_filters_out_no_set_records(cluster):
    """Test Exp filtering for named-set-only records."""
    namespace = general_namespace()
    named_set = "query_set_name_named_only"
    probe = "query-set-name-named-only-probe"
    no_set_key = Key(namespace, "", "query-set-name-named-only-empty")
    named_key = Key(namespace, named_set, "query-set-name-named-only-named")
    session = cluster.create_session()

    try:
        for key in (no_set_key, named_key):
            try:
                await session.delete(key).execute()
            except Exception:
                pass

        await session.upsert(no_set_key).put({"probe": probe, "kind": "no-set"}).execute()
        await session.upsert(named_key).put({"probe": probe, "kind": "named-set"}).execute()

        await _wait_for_query_kinds(
            lambda: _namespace_query(cluster, namespace).where(f"$.probe == '{probe}'"),
            {"no-set", "named-set"},
        )

        named_set_only = Exp.and_([
            Exp.eq(Exp.string_bin("probe"), val(probe)),
            Exp.ne(Exp.set_name(), val("")),
        ])
        await _wait_for_query_kinds(
            lambda: _namespace_query(cluster, namespace).filter_expression(named_set_only),
            {"named-set"},
        )
    finally:
        for key in (no_set_key, named_key):
            try:
                await session.delete(key).execute()
            except Exception:
                pass


async def test_query_chunked_iteration(session):
    """Server-side chunked iteration via chunk_size + has_more_chunks."""
    stream = await (
        session.query(general_namespace(), "query_test")
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


async def test_query_chunked_single_chunk(session):
    """chunk_size larger than dataset returns everything in one chunk."""
    stream = await (
        session.query(general_namespace(), "query_test")
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


async def test_has_more_chunks_on_non_chunked_stream(session):
    """has_more_chunks on a regular stream returns True once then False."""
    stream = await session.query(general_namespace(), "query_test").execute()
    assert await stream.has_more_chunks() is True
    count = 0
    async for _ in stream:
        count += 1
    assert await stream.has_more_chunks() is False
    stream.close()
    assert count == 10


class TestStreamAcrossBuilders:
    """`stream()` (lazy) is exposed consistently across the
    query-path builders and yields the same rows as buffered `execute()`,
    which stays the default. Streaming yields in completion order, so
    every comparison is by :attr:`RecordResult.index`."""

    async def test_batch_read_stream_matches_execute(self, cluster):
        """QueryBuilder.stream on a multi-key read yields the same
        rows (by index) as buffered execute()."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        keys = ds.ids(0, 1, 2)

        buffered = await (await session.query(keys).execute()).collect()
        lazy = await (await session.query(keys).stream()).collect()

        assert {r.index for r in lazy} == {r.index for r in buffered} == {0, 1, 2}
        assert all(r.is_ok for r in lazy)
        assert {r.index: r.record.bins["id"] for r in lazy} == \
            {r.index: r.record.bins["id"] for r in buffered}

    async def test_mixed_write_chain_stream(self, cluster):
        """A query→write→delete chain (terminates on WriteSegmentBuilder)
        exposes stream and yields one row per op."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "estream_qmix")
        keys = [ds.id(i) for i in range(4)]
        try:
            for i, k in enumerate(keys):
                await session.upsert(k).put({"v": i}).execute()

            stream = await (
                session.query(ds.ids(0, 1))
                .upsert(keys[2]).bin("status").set_to("active")
                .delete(keys[3])
                .stream()
            )
            results = await stream.collect()
            assert {r.index for r in results} == {0, 1, 2, 3}
            assert all(r.is_ok for r in results)

            rec2 = await (await session.query(keys[2]).execute()).first_or_raise()
            assert rec2.record.bins["status"] == "active"
            gone = await (await session.query(keys[3]).execute()).collect()
            assert gone == []
        finally:
            for k in keys:
                try:
                    await session.delete(k).execute()
                except Exception:
                    pass

    async def test_single_key_write_segment_stream(self, cluster):
        """A single-key write segment exposes stream (one record)."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "estream_qsingle")
        k = ds.id(0)
        try:
            stream = await session.upsert(k).put({"v": 1}).stream()
            results = await stream.collect()
            assert len(results) == 1
            assert results[0].is_ok
        finally:
            try:
                await session.delete(k).execute()
            except Exception:
                pass

    async def test_dataset_query_stream_delegates_to_scan(self, session):
        """stream on a keyless dataset query streams the scan
        lazily (delegates to execute())."""
        stream = await session.query(general_namespace(), "query_test").stream()
        count = 0
        async for r in stream:
            assert r.is_ok
            count += 1
        stream.close()
        assert count == 10


class TestPopVsFirst:
    """`pop()` takes one row and keeps the stream open; `first()` takes one row
    and closes it. Both come in an ``_or_raise`` variant."""

    async def test_pop_keeps_stream_open(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        stream = await session.query(ds.ids(0, 1, 2)).stream()
        head = await stream.pop()
        assert head is not None
        rest = await stream.collect()
        assert {head.index} | {r.index for r in rest} == {0, 1, 2}

    async def test_first_closes_stream(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        stream = await session.query(ds.ids(0, 1, 2)).stream()
        head = await stream.first()
        assert head is not None
        # first() closed the stream: nothing remains.
        assert await stream.collect() == []

    async def test_pop_or_raise_and_first_or_raise(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")

        open_stream = await session.query(ds.ids(0, 1)).stream()
        head = await open_stream.pop_or_raise()
        assert head.is_ok
        assert len(await open_stream.collect()) == 1   # still open

        closed_stream = await session.query(ds.ids(0, 1)).stream()
        rec = await closed_stream.first_or_raise()
        assert rec.is_ok
        assert await closed_stream.collect() == []     # closed


class TestStreamClose:
    """Closing a lazy stream releases the producer and stops iteration.

    Covers the same ground as a Closeable/try-with-resources stream — early
    abandon, idempotent close, close-on-exception via ``async with`` — plus
    cases a bare Closeable contract typically leaves untested: re-iterating a
    closed stream, and proving the cluster is still usable afterward.
    """

    async def test_close_mid_stream_stops_iteration(self, cluster):
        """After close(), no further rows are delivered even if the batch had
        more buffered/in-flight."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        keys = ds.ids(*range(10))

        stream = await session.query(keys).stream()
        seen = 0
        async for _ in stream:
            seen += 1
            if seen == 1:
                stream.close()
                break

        remaining = 0
        async for _ in stream:
            remaining += 1
        assert remaining == 0

    async def test_close_is_idempotent(self, cluster):
        """Repeated close() calls are safe and keep the stream drained."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        stream = await session.query(ds.ids(0, 1, 2)).stream()
        stream.close()
        stream.close()
        stream.close()
        assert await stream.collect() == []

    async def test_reiterate_after_close_yields_nothing(self, cluster):
        """Re-entering ``async for`` on a closed stream terminates immediately
        (a scenario Closeable contracts commonly leave unspecified)."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        stream = await session.query(ds.ids(0, 1, 2, 3)).stream()
        stream.close()
        first_pass = [r async for r in stream]
        second_pass = [r async for r in stream]
        assert first_pass == [] and second_pass == []

    async def test_client_usable_after_early_close(self, cluster):
        """Abandoning a stream early must not wedge the cluster — a subsequent
        operation on the same session still succeeds."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")

        stream = await session.query(ds.ids(*range(10))).stream()
        async for _ in stream:
            stream.close()
            break

        # The cluster keeps working after the partial-then-closed stream.
        rec = await (await session.query(ds.id(0)).execute()).first_or_raise()
        assert rec.record.bins["id"] == 0

    async def test_async_with_closes_on_normal_exit(self, cluster):
        """``async with`` drains and releases the stream on normal exit."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        keys = ds.ids(0, 1, 2)

        seen = []
        async with (await session.query(keys).stream()) as stream:
            async for r in stream:
                seen.append(r.index)
        assert set(seen) == {0, 1, 2}
        # Post-context, the stream is closed: no further rows.
        assert await stream.collect() == []

    async def test_async_with_closes_on_early_break(self, cluster):
        """Breaking out of ``async with`` still closes the stream."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        keys = ds.ids(*range(10))

        async with (await session.query(keys).stream()) as stream:
            async for _ in stream:
                break
        assert await stream.collect() == []
        # Client still usable.
        rec = await (await session.query(ds.id(1)).execute()).first_or_raise()
        assert rec.record.bins["id"] == 1

    async def test_async_with_closes_on_exception(self, cluster):
        """An exception inside ``async with`` closes the stream and propagates."""
        session = cluster.create_session()
        ds = DataSet.of(general_namespace(), "query_test")
        keys = ds.ids(*range(10))

        stream_ref = {}
        with pytest.raises(RuntimeError, match="boom"):
            async with (await session.query(keys).stream()) as stream:
                stream_ref["s"] = stream
                async for _ in stream:
                    raise RuntimeError("boom")
        # The stream was closed by __aexit__ despite the exception.
        assert await stream_ref["s"].collect() == []
        # And the cluster survives it.
        rec = await (await session.query(ds.id(2)).execute()).first_or_raise()
        assert rec.record.bins["id"] == 2
