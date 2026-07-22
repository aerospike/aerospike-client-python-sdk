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

"""Tests for SyncClient Query operations."""

import time

import pytest
from aerospike_sdk import DataSet, Exp


@pytest.fixture
def cluster(aerospike_host, make_cluster_definition, enterprise):
    """Setup sync SDK cluster and test data for query tests."""
    with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        for i in range(10):
            session.delete(ds.id(i)).execute()

        for i in range(10):
            session.upsert(ds.id(i)).put({"id": i, "age": 20 + i, "name": f"User{i}"}).execute()

        time.sleep(0.25 if not enterprise else 0.01)
        yield cluster


@pytest.fixture
def session(cluster):
    return cluster.create_session()

def test_query_basic(session):
    """Test basic query operation without filters."""
    stream = session.query("test", "query_test").execute()
    count = 0
    for result in stream:
        record = result.record
        assert record is not None
        assert "id" in record.bins
        count += 1
        if count >= 5:  # Limit to first 5 for speed
            break

def test_query_with_dataset(session):
    """Test query using DataSet."""
    users = DataSet.of("test", "query_test")
    stream = session.query(dataset=users).execute()
    count = 0
    for result in stream:
        record = result.record
        assert record is not None
        assert "id" in record.bins
        count += 1
        if count >= 5:
            break

def test_query_with_single_key(cluster):
    """Test query using a single Key."""
    users = DataSet.of("test", "query_test")
    key = users.id(5)

def test_query_with_multiple_keys(cluster):
    """Test query using multiple Keys."""
    users = DataSet.of("test", "query_test")
    keys = users.ids(6, 7)

def test_query_with_bins(session):
    """Test query with specific bin selection."""
    stream = session.query("test", "query_test").bins(["name", "age"]).execute()
    count = 0
    for result in stream:
        record = result.record
        assert record is not None
        # Verify that at least one of the requested bins is present
        assert "name" in record.bins or "age" in record.bins
        count += 1
        if count >= 3:
            break

    stream.close()
    assert count > 0

def test_query_with_filter_expression(session):
    """Test query with Exp (FilterExpression) for server-side filtering."""
    # Create a filter expression for age >= 25
    filter_exp = Exp.ge(
        Exp.int_bin("age"),
        Exp.int_val(25)
    )

    stream = (
        session.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    for result in stream:
        record = result.record
        assert record is not None
        assert "age" in record.bins
        assert record.bins["age"] >= 25
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0


class TestSyncExecuteStreamAcrossBuilders:
    """Sync `execute_stream()` mirrors the async contract across the
    query-path builders. Also guards the mixed-chain fix: the sync
    ``query(reads).write(...)`` transition must finalize the pending read
    spec before overwriting op_type/keys — otherwise the reads are dropped
    (the bug this covers), on both buffered ``execute()`` and lazy
    ``execute_stream()``."""

    def test_batch_read_stream_matches_execute(self, cluster):
        """Multi-key read: execute_stream yields the same rows (by index)
        as buffered execute()."""
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        keys = ds.ids(0, 1, 2)

        lazy = session.query(keys).execute_stream().collect()
        eager = session.query(keys).execute().collect()
        assert {r.index for r in lazy} == {r.index for r in eager} == {0, 1, 2}
        assert all(r.is_ok for r in lazy)

    def test_mixed_write_chain_stream_and_buffered(self, cluster):
        """A query→write→delete chain yields one row per op on both the
        buffered and streaming paths, and the chained reads are NOT dropped —
        they come back carrying their record data. This is the regression
        guard for the sync ``_start_write_verb`` finalize-first fix: before
        it, the read spec (indices 0, 1) was silently overwritten by the
        upsert."""
        session = cluster.create_session()
        ds = DataSet.of("test", "sestream_qmix")
        keys = [ds.id(i) for i in range(4)]
        try:
            for i, k in enumerate(keys):
                session.upsert(k).put({"v": i}).execute()

            for terminal in ("execute", "execute_stream"):
                # Re-seed the delete target each round.
                session.upsert(keys[3]).put({"v": 3}).execute()
                chain = (
                    session.query(ds.ids(0, 1))
                        .upsert(keys[2]).bin("status").set_to("active")
                        .delete(keys[3])
                )
                results = getattr(chain, terminal)().collect()
                assert {r.index for r in results} == {0, 1, 2, 3}, terminal
                assert all(r.is_ok for r in results), terminal
                # The two chained reads survive with their seeded bin data.
                by_idx = {r.index: r for r in results}
                assert by_idx[0].record.bins["v"] == 0, terminal
                assert by_idx[1].record.bins["v"] == 1, terminal
        finally:
            for k in keys:
                try:
                    session.delete(k).execute()
                except Exception:
                    pass

    def test_single_key_write_segment_stream(self, cluster):
        """A single-key write segment exposes execute_stream (one record)."""
        session = cluster.create_session()
        ds = DataSet.of("test", "sestream_qsingle")
        k = ds.id(0)
        try:
            results = session.upsert(k).put({"v": 1}).execute_stream().collect()
            assert len(results) == 1
            assert results[0].is_ok
        finally:
            try:
                session.delete(k).execute()
            except Exception:
                pass


class TestSyncPopVsFirst:
    """`pop()` keeps the stream open; `first()` closes it — plus ``_or_raise``."""

    def test_pop_keeps_stream_open(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        stream = session.query(ds.ids(0, 1, 2)).execute_stream()
        head = stream.pop()
        assert head is not None
        rest = stream.collect()
        assert {head.index} | {r.index for r in rest} == {0, 1, 2}

    def test_first_closes_stream(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        stream = session.query(ds.ids(0, 1, 2)).execute_stream()
        head = stream.first()
        assert head is not None
        assert stream.collect() == []

    def test_pop_or_raise_and_first_or_raise(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")

        open_stream = session.query(ds.ids(0, 1)).execute_stream()
        assert open_stream.pop_or_raise().is_ok
        assert len(open_stream.collect()) == 1

        closed_stream = session.query(ds.ids(0, 1)).execute_stream()
        assert closed_stream.first_or_raise().is_ok
        assert closed_stream.collect() == []


class TestSyncExecuteStreamClose:
    """Sync sibling of the async close/context-manager suite: early abandon,
    idempotent close, close-on-exception via ``with``, plus re-iterate and
    client-usable-after-close."""

    def test_close_mid_stream_stops_iteration(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        keys = ds.ids(*range(10))

        stream = session.query(keys).execute_stream()
        seen = 0
        for _ in stream:
            seen += 1
            if seen == 1:
                stream.close()
                break

        remaining = sum(1 for _ in stream)
        assert remaining == 0

    def test_close_is_idempotent(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        stream = session.query(ds.ids(0, 1, 2)).execute_stream()
        stream.close()
        stream.close()
        stream.close()
        assert stream.collect() == []

    def test_reiterate_after_close_yields_nothing(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        stream = session.query(ds.ids(0, 1, 2, 3)).execute_stream()
        stream.close()
        assert list(stream) == []
        assert list(stream) == []

    def test_client_usable_after_early_close(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        stream = session.query(ds.ids(*range(10))).execute_stream()
        for _ in stream:
            stream.close()
            break
        rec = session.query(ds.id(0)).execute().first_or_raise()
        assert rec.record.bins["id"] == 0

    def test_with_closes_on_normal_exit(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        seen = []
        with session.query(ds.ids(0, 1, 2)).execute_stream() as stream:
            for r in stream:
                seen.append(r.index)
        assert set(seen) == {0, 1, 2}
        assert stream.collect() == []

    def test_with_closes_on_early_break(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        with session.query(ds.ids(*range(10))).execute_stream() as stream:
            for _ in stream:
                break
        assert stream.collect() == []
        rec = session.query(ds.id(1)).execute().first_or_raise()
        assert rec.record.bins["id"] == 1

    def test_with_closes_on_exception(self, cluster):
        session = cluster.create_session()
        ds = DataSet.of("test", "query_test")
        stream_ref = {}
        with pytest.raises(RuntimeError, match="boom"):
            with session.query(ds.ids(*range(10))).execute_stream() as stream:
                stream_ref["s"] = stream
                for _ in stream:
                    raise RuntimeError("boom")
        assert stream_ref["s"].collect() == []
        rec = session.query(ds.id(2)).execute().first_or_raise()
        assert rec.record.bins["id"] == 2


def test_query_with_filter_expression_and(session):
    """Test query with Exp (FilterExpression) using AND for multiple conditions."""
    # Create filter expression: age >= 25 AND age <= 27
    filter_exp = Exp.and_([
        Exp.ge(Exp.int_bin("age"), Exp.int_val(25)),
        Exp.le(Exp.int_bin("age"), Exp.int_val(27))
    ])

    stream = (
        session.query("test", "query_test")
        .filter_expression(filter_exp)
        .execute()
    )
    count = 0
    for result in stream:
        record = result.record
        assert record is not None
        assert "age" in record.bins
        assert 25 <= record.bins["age"] <= 27
        count += 1
        if count >= 5:
            break

    stream.close()
    assert count > 0
