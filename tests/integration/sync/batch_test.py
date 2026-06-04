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

"""Synchronous :meth:`SyncSession.batch` integration tests (mirrors async batch paths)."""

import time

import pytest

from aerospike_sdk import DataSet, SyncClient


@pytest.fixture
def client(aerospike_host, client_policy, enterprise):
    with SyncClient(seeds=aerospike_host, policy=client_policy) as c:
        yield c


@pytest.fixture
def users():
    return DataSet.of("test", "sync_batch_test")


class TestSyncBatchOperations:

    def test_batch_insert_multiple_keys(self, client: SyncClient, users: DataSet):
        session = client.create_session()
        key1 = users.id("sb_user_1")
        key2 = users.id("sb_user_2")

        for k in (key1, key2):
            try:
                session.delete(k).execute()
            except Exception:
                pass

        stream = (
            session.batch()
            .insert(key1).bin("name").set_to("Ada")
            .insert(key2).bin("name").set_to("Bob")
            .execute()
        )
        results = stream.collect()
        assert len(results) == 2

        r1 = session.query(key1).execute().first_or_raise()
        assert r1.record.bins["name"] == "Ada"
        r2 = session.query(key2).execute().first_or_raise()
        assert r2.record.bins["name"] == "Bob"

        session.delete(key1).execute()
        session.delete(key2).execute()

    def test_batch_mixed_update_delete_insert(self, client: SyncClient, users: DataSet):
        session = client.create_session()
        key1 = users.id("sb_mix_1")
        key2 = users.id("sb_mix_2")
        key3 = users.id("sb_mix_3")

        session.upsert(key1).put({"counter": 10}).execute()
        session.upsert(key2).put({"name": "gone"}).execute()
        try:
            session.delete(key3).execute()
        except Exception:
            pass

        stream = (
            session.batch()
            .update(key1).bin("counter").add(5)
            .delete(key2)
            .insert(key3).bin("status").set_to("new")
            .execute()
        )
        assert len(stream.collect()) == 3

        assert session.query(key1).execute().first_or_raise().record.bins["counter"] == 15
        ex = session.exists(key2).respond_all_keys().execute().first()
        assert ex is not None and ex.as_bool() is False
        assert session.query(key3).execute().first_or_raise().record.bins["status"] == "new"

        session.delete(key1).execute()
        session.delete(key3).execute()

    def test_batch_empty_raises(self, client: SyncClient):
        session = client.create_session()
        with pytest.raises(ValueError, match="No operations to execute"):
            session.batch().execute()


class TestSyncBatchExpressionOps:

    def test_batch_upsert_from(self, client: SyncClient, users: DataSet, enterprise):
        session = client.create_session()
        keys = [users.id(f"sbx_{i}") for i in range(2)]

        for i, key in enumerate(keys):
            session.upsert(key).put({"A": (i + 1) * 10}).execute()

        stream = (
            session.batch()
            .upsert(keys[0]).bin("C").upsert_from("$.A + 1")
            .upsert(keys[1]).bin("C").upsert_from("$.A + 1")
            .execute()
        )
        assert len(stream.collect()) == 2
        time.sleep(0.25 if not enterprise else 0.01)

        for i, key in enumerate(keys):
            rec = session.query(key).bin("C").get().execute().first_or_raise()
            assert rec.record.bins["C"] == (i + 1) * 10 + 1

        for key in keys:
            session.delete(key).execute()


class TestSyncBatchExecuteStream:
    """Sync lazy `execute_stream()` — same contract as the async sibling."""

    @pytest.fixture
    def track_key(self, client):
        """Factory: register a Key for auto-cleanup at fixture teardown.

        Replaces manual ``try/except session.delete(k).execute()`` loops at
        the end of every test. Pass each Key through this factory once and
        the fixture handles the drop in teardown order.
        """
        session = client.create_session()
        created: list = []

        def track(key):
            created.append(key)
            return key

        yield track

        for k in created:
            try:
                session.delete(k).execute()
            except Exception:
                pass

    def test_execute_stream_mixed_ops_yields_all(
        self, client: SyncClient, users: DataSet, track_key,
    ):
        """Mixed writes + AEL read + delete dispatch correctly via
        ``batch_stream_blocking``; results yielded one-by-one with idx
        preserved on each :class:`RecordResult`.

        Verifies:
        - All 4 ops yield a RecordResult (set-equality on input indices).
        - The streamed expression-read result carries the computed value
          (`select_from "$.A + $.B"` → sum bin).
        - Post-batch persisted state matches op semantics: the WRITE
          actually flipped its bin; the two READS did NOT persist a
          `sum` bin (select_from is a read, not a write); the DELETE
          removed its record.
        """
        session = client.create_session()
        keys = [track_key(users.id(f"sb_estream_mix_{i}")) for i in range(4)]
        for i, k in enumerate(keys):
            session.upsert(k).put({"A": i, "B": i * 2}).execute()

        stream = (
            session.batch()
                .upsert(keys[0]).bin("A").set_to(99)
                .update(keys[1]).bin("sum").select_from("$.A + $.B")
                .update(keys[2]).bin("sum").select_from("$.A + $.B")
                .delete(keys[3])
                .execute_stream()
        )
        results = list(stream)
        assert len(results) == 4
        assert {r.index for r in results} == {0, 1, 2, 3}

        by_idx = {r.index: r for r in results}
        for r in results:
            assert r.is_ok

        # In-stream value checks: select_from carries the computed `sum` bin.
        # keys[1]: A=1, B=2 → 1+2=3
        # keys[2]: A=2, B=4 → 2+4=6
        assert by_idx[1].record.bins["sum"] == 3
        assert by_idx[2].record.bins["sum"] == 6

        # Persisted state checks:
        # (write) keys[0]: bin A flipped from 0 → 99; B unchanged.
        rec0 = session.query(keys[0]).execute().first_or_raise()
        assert rec0.record.bins["A"] == 99
        assert rec0.record.bins["B"] == 0

        # (read) keys[1] / keys[2]: `select_from` is a read — original bins
        # untouched, `sum` NOT persisted.
        rec1 = session.query(keys[1]).execute().first_or_raise()
        assert rec1.record.bins == {"A": 1, "B": 2}
        rec2 = session.query(keys[2]).execute().first_or_raise()
        assert rec2.record.bins == {"A": 2, "B": 4}

        # (delete) keys[3]: gone.
        empty = list(session.query(keys[3]).execute())
        assert empty == []

    def test_execute_stream_read_only_ops_dispatch_as_reads(
        self, client: SyncClient, users: DataSet, track_key,
    ):
        """Read-only op lists (AEL `select_from` under UPDATE) must land as
        BatchReadOp on the wire — verifies the has_any_write_op inspection
        in the sync dispatch helper. Also verifies the persisted record
        was NOT mutated (if select_from regressed and landed as a write,
        the `sum` bin would persist)."""
        session = client.create_session()
        keys = [track_key(users.id(f"sb_estream_ro_{i}")) for i in range(2)]
        for i, k in enumerate(keys):
            session.upsert(k).put({"A": 5 + i, "B": 3}).execute()

        stream = (
            session.batch()
                .update(keys[0]).bin("sum").select_from("$.A + $.B")
                .update(keys[1]).bin("sum").select_from("$.A + $.B")
                .execute_stream()
        )
        results = list(stream)
        assert len(results) == 2
        results.sort(key=lambda r: r.index)
        assert results[0].record.bins["sum"] == 8  # 5 + 3
        assert results[1].record.bins["sum"] == 9  # 6 + 3

        # Persisted state: `sum` should NOT be on disk — select_from is read.
        rec0 = session.query(keys[0]).execute().first_or_raise()
        assert rec0.record.bins == {"A": 5, "B": 3}
        rec1 = session.query(keys[1]).execute().first_or_raise()
        assert rec1.record.bins == {"A": 6, "B": 3}
