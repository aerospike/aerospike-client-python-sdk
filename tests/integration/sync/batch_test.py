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

"""Synchronous multi-key write-chain batch integration tests (mirrors async batch paths)."""

import time

import pytest

from aerospike_sdk import DataSet, ErrorDetailVerbosity
from aerospike_sdk.exceptions import ResultCode
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Scope, Settings
from aerospike_sdk.sync import Cluster

from tests.pac_compat import requires_server_compiled_ael
from tests.integration.namespace import general_namespace


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition, enterprise):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


@pytest.fixture
def users():
    return DataSet.of(general_namespace(), "sync_batch_test")


class TestSyncBatchOperations:

    def test_batch_insert_multiple_keys(self, cluster: Cluster, users: DataSet):
        session = cluster.create_session()
        key1 = users.id("sb_user_1")
        key2 = users.id("sb_user_2")

        for k in (key1, key2):
            try:
                session.delete(k).execute()
            except Exception:
                pass

        stream = (
            session.insert(key1).bin("name").set_to("Ada")
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

    def test_batch_mixed_update_delete_insert(self, cluster: Cluster, users: DataSet):
        session = cluster.create_session()
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
            session.update(key1).bin("counter").add(5)
            .delete(key2)
            .insert(key3).bin("status").set_to("new")
            .execute()
        )
        assert len(stream.collect()) == 3

        assert session.query(key1).execute().first_or_raise().record.bins["counter"] == 15
        ex = session.exists(key2).include_missing_keys().execute().first()
        assert ex is not None and ex.as_bool() is False
        assert session.query(key3).execute().first_or_raise().record.bins["status"] == "new"

        session.delete(key1).execute()
        session.delete(key3).execute()

class TestSyncBatchExpressionOps:

    @requires_server_compiled_ael
    def test_batch_upsert_from(self, cluster: Cluster, users: DataSet, enterprise):
        session = cluster.create_session()
        keys = [users.id(f"sbx_{i}") for i in range(2)]

        for i, key in enumerate(keys):
            session.upsert(key).put({"A": (i + 1) * 10}).execute()

        stream = (
            session.upsert(keys[0]).bin("C").upsert_from("$.A + 1")
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


class TestSyncBatchStream:
    """Sync lazy `stream()` — same contract as the async sibling."""

    @pytest.fixture
    def track_key(self, cluster):
        """Factory: register a Key for auto-cleanup at fixture teardown.

        Replaces manual ``try/except session.delete(k).execute()`` loops at
        the end of every test. Pass each Key through this factory once and
        the fixture handles the drop in teardown order.
        """
        session = cluster.create_session()
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

    @requires_server_compiled_ael
    def test_stream_mixed_ops_yields_all(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        """Mixed writes + AEL read + delete dispatch correctly via
        ``batch_stream_blocking``; results yielded one-by-one with idx
        preserved on each :class:`RecordResult`.

        Verifies:
        - All 4 ops yield a RecordResult (set-equality on input indices).
        - The streamed expression-read result carries the computed value
          (`select_from` bin+bin sum → sum bin).
        - Post-batch persisted state matches op semantics: the WRITE
          actually flipped its bin; the two READS did NOT persist a
          `sum` bin (select_from is a read, not a write); the DELETE
          removed its record.
        """
        session = cluster.create_session()
        keys = [track_key(users.id(f"sb_estream_mix_{i}")) for i in range(4)]
        for i, k in enumerate(keys):
            session.upsert(k).put({"A": i, "B": i * 2}).execute()

        stream = (
            session.upsert(keys[0]).bin("A").set_to(99)
            .query(keys[1]).bin("sum").select_from("$.A:INT + $.B:INT")
            .query(keys[2]).bin("sum").select_from("$.A:INT + $.B:INT")
            .delete(keys[3])
            .stream()
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

    @requires_server_compiled_ael
    def test_stream_read_only_ops_dispatch_as_reads(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        """Read-only op lists (AEL `select_from` under the read verb) land
        as BatchReadOp on the wire, even in a lazy write-batch stream.
        Also verifies the persisted record was NOT mutated (if select_from
        landed as a write, the `sum` bin would persist)."""
        session = cluster.create_session()
        keys = [track_key(users.id(f"sb_estream_ro_{i}")) for i in range(2)]
        for i, k in enumerate(keys):
            session.upsert(k).put({"A": 5 + i, "B": 3}).execute()

        stream = (
            session.query(keys[0]).bin("sum").select_from("$.A:INT + $.B:INT")
            .query(keys[1]).bin("sum").select_from("$.A:INT + $.B:INT")
            .stream()
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


class TestSyncBatchStreamClose:
    """Sync sibling of the async write-batch close/context-manager suite:
    early abandon, idempotent close, close-on-exception via ``with``,
    re-iterate, and client-usable-after-close."""

    @pytest.fixture
    def track_key(self, cluster):
        session = cluster.create_session()
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

    def _seed(self, session, users, track_key, n):
        keys = [track_key(users.id(f"sb_estream_close_{i}")) for i in range(n)]
        for i, k in enumerate(keys):
            session.upsert(k).put({"v": i}).execute()
        return keys

    def _write_batch(self, session, keys):
        b = session.upsert(keys[0]).put({"v": 0})
        for i, k in enumerate(keys[1:], start=1):
            b = b.upsert(k).put({"v": i})
        return b

    def test_close_mid_stream_stops_iteration(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = self._seed(session, users, track_key, 10)
        stream = self._write_batch(session, keys).stream()
        seen = 0
        for _ in stream:
            seen += 1
            if seen == 1:
                stream.close()
                break
        assert sum(1 for _ in stream) == 0

    def test_close_is_idempotent(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = self._seed(session, users, track_key, 3)
        stream = self._write_batch(session, keys).stream()
        stream.close()
        stream.close()
        stream.close()
        assert stream.collect() == []

    def test_reiterate_after_close_yields_nothing(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = self._seed(session, users, track_key, 4)
        stream = self._write_batch(session, keys).stream()
        stream.close()
        assert list(stream) == []
        assert list(stream) == []

    def test_client_usable_after_early_close(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = self._seed(session, users, track_key, 10)
        stream = self._write_batch(session, keys).stream()
        for _ in stream:
            stream.close()
            break
        rec = session.query(keys[0]).execute().first_or_raise()
        assert rec.record.bins["v"] == 0

    def test_with_closes_on_normal_exit(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = self._seed(session, users, track_key, 3)
        seen = 0
        with self._write_batch(session, keys).stream() as stream:
            for _ in stream:
                seen += 1
        assert seen == 3
        assert stream.collect() == []

    def test_with_closes_on_early_break(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = self._seed(session, users, track_key, 10)
        with self._write_batch(session, keys).stream() as stream:
            for _ in stream:
                break
        assert stream.collect() == []

    def test_with_closes_on_exception(
        self, cluster: Cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = self._seed(session, users, track_key, 10)
        stream_ref = {}
        with pytest.raises(RuntimeError, match="boom"):
            with self._write_batch(session, keys).stream() as stream:
                stream_ref["s"] = stream
                for _ in stream:
                    raise RuntimeError("boom")
        assert stream_ref["s"].collect() == []
        rec = session.query(keys[1]).execute().first_or_raise()
        assert rec.record.bins["v"] == 1


class TestSyncBatchErrorDetail:
    """Per-record error detail travels on RecordResult.sub_code through the
    sync batch stream (independent of the async implementation)."""

    def test_batch_row_carries_sub_code(self, cluster: Cluster, users: DataSet):
        builds = cluster.create_session().info().build()
        versions = [tuple(int(p) for p in b.split("-")[0].split(".")) for b in builds]
        if not versions or min(versions) < (8, 1, 3):
            pytest.skip("cluster does not supply extended error detail (server < 8.1.3)")

        behavior = Behavior(
            "sync-batch-error-detail",
            {Scope.ALL: Settings(error_detail_verbosity=ErrorDetailVerbosity.MESSAGE)},
        )
        session = cluster.create_session(behavior=behavior)
        k_bad = users.id("sb_subcode_bad")
        k_good = users.id("sb_subcode_good")
        for k in (k_bad, k_good):
            session.upsert(k).put({"nums": [1, 2, 3]}).execute()

        rs = (
            session
            .query(k_bad).bin("nums").on_list_index(99).get_values()
            .query(k_good).bins(["nums"])
            .execute()
        )
        results = rs.collect()

        assert len(results) == 2
        assert results[0].result_code == ResultCode.OP_NOT_APPLICABLE
        # Subcode 1 = CDT index out of bounds, scoped to OP_NOT_APPLICABLE.
        assert results[0].sub_code == 1
        assert results[1].is_ok
        assert results[1].sub_code is None
        # MESSAGE verbosity: the failed row also carries the server's
        # explanation; the sync stream is an independent implementation, so
        # assert the full surface here too.
        assert results[0].server_message and "out of bounds" in results[0].server_message
        assert results[0].exp_trace is None
        assert results[1].server_message is None


class TestBatchGeneration:
    """Generation policy on batch delete + write (``BatchDelete/WritePolicy``, sync).

    Sync mirror of the async coverage; the single-key contract lives in the
    generation suite. Sync is an independent implementation, so the batch
    sub-policy path is exercised here too.
    """

    def test_batch_delete_matching_generation_deletes_all(self, cluster, users: DataSet):
        session = cluster.create_session()
        k1, k2 = users.id("del_gen_ok_1"), users.id("del_gen_ok_2")
        session.upsert(k1).put({"n": 1}).execute()
        session.upsert(k2).put({"n": 2}).execute()
        gen1 = session.query(k1).execute().first_or_raise().record.generation
        gen2 = session.query(k2).execute().first_or_raise().record.generation
        assert gen1 == gen2

        stream = session.delete(k1, k2).ensure_generation_is(gen1).execute()
        assert all(rr.is_ok for rr in stream)
        for k in (k1, k2):
            assert list(session.query(k).execute()) == []

    def test_batch_delete_wrong_generation_reports_error(self, cluster, users: DataSet):
        session = cluster.create_session()
        k1, k2 = users.id("del_gen_bad_1"), users.id("del_gen_bad_2")
        session.upsert(k1).put({"n": 1}).execute()
        session.upsert(k2).put({"n": 2}).execute()

        stream = (
            session.delete(k1, k2).ensure_generation_is(9999).include_missing_keys().execute()
        )
        results = {rr.key.value: rr for rr in stream}
        assert results["del_gen_bad_1"].result_code == ResultCode.GENERATION_ERROR
        assert results["del_gen_bad_2"].result_code == ResultCode.GENERATION_ERROR
        for k in (k1, k2):
            assert len(list(session.query(k).execute())) == 1

    def test_batch_write_wrong_generation_reports_error(self, cluster, users: DataSet):
        session = cluster.create_session()
        k1, k2 = users.id("wr_gen_bad_1"), users.id("wr_gen_bad_2")
        session.upsert(k1).put({"n": 1}).execute()
        session.upsert(k2).put({"n": 2}).execute()

        stream = (
            session.update(k1).put({"n": 10}).ensure_generation_is(9999)
            .update(k2).put({"n": 20}).ensure_generation_is(9999)
            .execute()
        )
        results = {rr.key.value: rr for rr in stream}
        assert results["wr_gen_bad_1"].result_code == ResultCode.GENERATION_ERROR
        assert results["wr_gen_bad_2"].result_code == ResultCode.GENERATION_ERROR
        assert session.query(k1).execute().first_or_raise().record.bins.get("n") == 1

    def test_batch_write_matching_generation_writes(self, cluster, users: DataSet):
        session = cluster.create_session()
        k1, k2 = users.id("wr_gen_ok_1"), users.id("wr_gen_ok_2")
        session.upsert(k1).put({"n": 1}).execute()
        session.upsert(k2).put({"n": 2}).execute()
        gen = session.query(k1).execute().first_or_raise().record.generation

        stream = (
            session.update(k1).put({"n": 10}).ensure_generation_is(gen)
            .update(k2).put({"n": 20}).ensure_generation_is(gen)
            .execute()
        )
        assert all(rr.is_ok for rr in stream)
        assert session.query(k1).execute().first_or_raise().record.bins.get("n") == 10


class TestSameKeyChainOrdering:
    """A key spanning chain segments must observe the earlier segments' writes.

    Batch sub-transactions against one key are unordered server-side, so a
    chain that writes a key and then reads it back cannot fold into a single
    batch — the read would race its own write and miss it, and the resulting
    not-found row is dropped from the stream, leaving only a short result.
    """

    def test_read_segment_sees_write_from_earlier_segment(
        self, cluster, users: DataSet,
    ):
        session = cluster.create_session()
        k = users.id("chain_same_key_rw")
        session.delete(k).execute()

        stream = (
            session.upsert(k).put({"seed": "new"})
            .query(k).bins(["seed"])
            .execute()
        )
        rows = stream.collect()

        # One row per segment, in order, with nothing dropped.
        assert len(rows) == 2
        assert [r.result_code for r in rows] == [ResultCode.OK, ResultCode.OK]
        # The read observed the write issued earlier in the same chain.
        assert rows[1].record.bins["seed"] == "new"

        # Persisted state, read back through a separate chain.
        after = session.query(k).bins(["seed"]).execute().first_or_raise()
        assert after.record.bins["seed"] == "new"
