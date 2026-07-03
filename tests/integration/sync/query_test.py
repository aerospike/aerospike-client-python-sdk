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
from aerospike_sdk import DataSet, Exp, SyncClient


@pytest.fixture
def client(aerospike_host, client_policy, enterprise):
    """Setup sync SDK client and test data for query tests."""
    with SyncClient(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        ds = DataSet.of("test", "query_test")
        for i in range(10):
            session.delete(ds.id(i)).execute()

        for i in range(10):
            session.upsert(ds.id(i)).put({"id": i, "age": 20 + i, "name": f"User{i}"}).execute()

        time.sleep(0.25 if not enterprise else 0.01)
        yield client

def test_query_basic(client):
    """Test basic query operation without filters."""
    stream = client.query("test", "query_test").execute()
    count = 0
    for result in stream:
        record = result.record
        assert record is not None
        assert "id" in record.bins
        count += 1
        if count >= 5:  # Limit to first 5 for speed
            break

def test_query_with_dataset(client):
    """Test query using DataSet."""
    users = DataSet.of("test", "query_test")
    stream = client.query(dataset=users).execute()
    count = 0
    for result in stream:
        record = result.record
        assert record is not None
        assert "id" in record.bins
        count += 1
        if count >= 5:
            break

def test_query_with_single_key(client):
    """Test query using a single Key."""
    users = DataSet.of("test", "query_test")
    key = users.id(5)

def test_query_with_multiple_keys(client):
    """Test query using multiple Keys."""
    users = DataSet.of("test", "query_test")
    keys = users.ids(6, 7)

def test_query_with_bins(client):
    """Test query with specific bin selection."""
    stream = client.query("test", "query_test").bins(["name", "age"]).execute()
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

def test_query_with_filter_expression(client):
    """Test query with Exp (FilterExpression) for server-side filtering."""
    # Create a filter expression for age >= 25
    filter_exp = Exp.ge(
        Exp.int_bin("age"),
        Exp.int_val(25)
    )

    stream = (
        client.query("test", "query_test")
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

    def test_batch_read_stream_matches_execute(self, client):
        """Multi-key read: execute_stream yields the same rows (by index)
        as buffered execute()."""
        session = client.create_session()
        ds = DataSet.of("test", "query_test")
        keys = ds.ids(0, 1, 2)

        lazy = session.query(keys).execute_stream().collect()
        eager = session.query(keys).execute().collect()
        assert {r.index for r in lazy} == {r.index for r in eager} == {0, 1, 2}
        assert all(r.is_ok for r in lazy)

    def test_mixed_write_chain_stream_and_buffered(self, client):
        """A query→write→delete chain yields one row per op on both the
        buffered and streaming paths, and the chained reads are NOT dropped —
        they come back carrying their record data. This is the regression
        guard for the sync ``_start_write_verb`` finalize-first fix: before
        it, the read spec (indices 0, 1) was silently overwritten by the
        upsert."""
        session = client.create_session()
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

    def test_single_key_write_segment_stream(self, client):
        """A single-key write segment exposes execute_stream (one record)."""
        session = client.create_session()
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


def test_query_with_filter_expression_and(client):
    """Test query with Exp (FilterExpression) using AND for multiple conditions."""
    # Create filter expression: age >= 25 AND age <= 27
    filter_exp = Exp.and_([
        Exp.ge(Exp.int_bin("age"), Exp.int_val(25)),
        Exp.le(Exp.int_bin("age"), Exp.int_val(27))
    ])

    stream = (
        client.query("test", "query_test")
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
