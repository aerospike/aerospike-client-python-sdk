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

"""Sync integration tests for complex batch (mixed read + write chains).

Mirror of ``tests/integration/async/complex_batch_test.py``. The sync
builders are an independent implementation against PAC's ``*_blocking``
surface (not an async façade), so mixed-chain finalization, per-spec
settings, and exists/touch verbs traverse their own code path and need
their own coverage — a gap that previously let the ``query(reads).write()``
dropped-read defect go unnoticed.
"""

import pytest
from aerospike_async.exceptions import ResultCode

from aerospike_sdk import DataSet, SyncClient


@pytest.fixture
def client(aerospike_host, client_policy, enterprise):
    with SyncClient(seeds=aerospike_host, policy=client_policy) as c:
        yield c


@pytest.fixture
def ds():
    return DataSet.of("test", "sync_complex_batch")


@pytest.fixture
def session(client):
    return client.create_session()


def _cleanup(session, *keys):
    for k in keys:
        try:
            session.delete(k).execute()
        except Exception:
            pass


class TestMixedReadWrite:
    """Chained read + write operations in a single execute()."""

    def test_query_then_upsert(self, session, ds):
        k1 = ds.id("cb_rw_1")
        k2 = ds.id("cb_rw_2")
        _cleanup(session, k1, k2)

        session.upsert(key=k1).set_bins({"name": "Alice", "age": 21}).execute()

        results = (
            session
                .query(k1)
                .upsert(k2).bin("status").set_to("active")
                .execute()
        ).collect()
        # Input order preserved: read (k1) first, write (k2) second — and the
        # read is NOT dropped (the finalize-first fix).
        assert len(results) == 2
        assert results[0].record.bins["name"] == "Alice"

        r2 = session.query(k2).execute().first_or_raise().record
        assert r2.bins["status"] == "active"

        _cleanup(session, k1, k2)

    def test_upsert_then_query_bins(self, session, ds):
        k1 = ds.id("cb_rw_3")
        k2 = ds.id("cb_rw_4")
        _cleanup(session, k1, k2)

        session.upsert(k2).set_bins({"x": 10, "y": 20}).execute()

        results = (
            session
                .upsert(k1).bin("label").set_to("new")
                .query(k2).bins(["x"])
                .execute()
        ).collect()
        assert len(results) == 2
        assert results[0].result_code == ResultCode.OK

        read_result = results[1].record
        assert read_result.bins.get("x") == 10
        assert "y" not in read_result.bins

        _cleanup(session, k1, k2)

    def test_query_expression_then_write(self, session, ds):
        k1 = ds.id("cb_rw_5")
        k2 = ds.id("cb_rw_6")
        _cleanup(session, k1, k2)

        session.upsert(k1).set_bins({"score": 50}).execute()

        results = (
            session
                .query(k1).bin("doubled").select_from("$.score * 2")
                .upsert(k2).bin("tag").set_to("written")
                .execute()
        ).collect()
        assert results[0].record.bins["doubled"] == 100

        r2 = session.query(k2).execute().first_or_raise().record
        assert r2.bins["tag"] == "written"

        _cleanup(session, k1, k2)


class TestMixedOpTypes:
    """Chain different write op types in a single execute()."""

    def test_upsert_insert_replace(self, session, ds):
        k_upsert = ds.id("cb_op_1")
        k_insert = ds.id("cb_op_2")
        k_replace = ds.id("cb_op_3")
        _cleanup(session, k_upsert, k_insert, k_replace)

        session.upsert(k_replace).set_bins({"original": True}).execute()

        results = (
            session
                .query(k_upsert)
                .upsert(k_upsert).bin("type").set_to("upsert")
                .insert(k_insert).bin("type").set_to("insert")
                .replace_if_exists(k_replace).bin("type").set_to("replaced")
                .execute()
        ).collect()
        ok_count = sum(1 for r in results if r.result_code == ResultCode.OK)
        assert ok_count >= 3

        assert session.query(k_upsert).execute().first_or_raise().record.bins["type"] == "upsert"
        assert session.query(k_insert).execute().first_or_raise().record.bins["type"] == "insert"
        r3 = session.query(k_replace).execute().first_or_raise().record
        assert r3.bins["type"] == "replaced"
        assert "original" not in r3.bins

        _cleanup(session, k_upsert, k_insert, k_replace)

    def test_insert_existing_key_fails(self, session, ds):
        k = ds.id("cb_op_4")
        _cleanup(session, k)

        session.upsert(k).set_bins({"x": 1}).execute()

        results = (
            session
                .query(k)
                .insert(k).bin("x").set_to(999)
                .execute()
        ).collect()
        assert results[1].result_code != ResultCode.OK

        assert session.query(k).execute().first_or_raise().record.bins["x"] == 1

        _cleanup(session, k)


class TestWriteWithExpressions:
    """Expression-based writes in a chained context."""

    def test_upsert_from_expression(self, session, ds):
        k = ds.id("cb_exp_1")
        _cleanup(session, k)

        session.upsert(k).set_bins({"value": 6}).execute()

        (
            session
                .query(k)
                .upsert(k).bin("computed").upsert_from("$.value + 1000")
                .execute()
        ).collect()

        assert session.query(k).execute().first_or_raise().record.bins["computed"] == 1006

        _cleanup(session, k)

    def test_expression_write_and_scalar_write(self, session, ds):
        k = ds.id("cb_exp_2")
        _cleanup(session, k)

        session.upsert(k).set_bins({"base": 10}).execute()

        (
            session
                .query(k)
                .upsert(k)
                    .bin("derived").upsert_from("$.base * 3")
                    .bin("label").set_to("combo")
                .execute()
        ).collect()

        rec = session.query(k).execute().first_or_raise().record
        assert rec.bins["derived"] == 30
        assert rec.bins["label"] == "combo"

        _cleanup(session, k)


class TestDeleteInChain:
    """Delete operations mixed into a chained execute()."""

    def test_write_then_delete(self, session, ds):
        k1 = ds.id("cb_del_1")
        k2 = ds.id("cb_del_2")
        _cleanup(session, k1, k2)

        session.upsert(k2).set_bins({"temp": "remove_me"}).execute()

        results = (
            session
                .query(k1)
                .upsert(k1).bin("score").set_to(100)
                .delete(k2)
                .execute()
        ).collect()
        assert len(results) >= 2

        assert session.query(k1).execute().first_or_raise().record.bins["score"] == 100
        assert session.query(k2).execute().collect() == []

        _cleanup(session, k1)

    def test_read_write_delete(self, session, ds):
        k1 = ds.id("cb_del_3")
        k2 = ds.id("cb_del_4")
        k3 = ds.id("cb_del_5")
        _cleanup(session, k1, k2, k3)

        session.upsert(k1).set_bins({"name": "Alice"}).execute()
        session.upsert(k3).set_bins({"tmp": True}).execute()

        results = (
            session
                .query(k1)
                .upsert(k2).bin("created").set_to(True)
                .delete(k3)
                .execute()
        ).collect()
        assert len(results) == 3
        assert results[0].record.bins["name"] == "Alice"

        assert session.query(k2).execute().first_or_raise().record.bins["created"] is True
        assert session.query(k3).execute().collect() == []

        _cleanup(session, k1, k2)


class TestPerSpecSettings:
    """Per-spec write settings: TTL, generation."""

    def test_expire_record_after_seconds(self, session, ds):
        k = ds.id("cb_ttl_1")
        _cleanup(session, k)

        (
            session
                .query(k)
                .upsert(k)
                    .bin("data").set_to("expiring")
                    .expire_record_after_seconds(86400)
                .execute()
        ).collect()

        rec = session.query(k).execute().first_or_raise().record
        assert rec.bins["data"] == "expiring"
        assert rec.ttl is not None and rec.ttl > 0

        _cleanup(session, k)

    def test_generation_check(self, session, ds):
        k = ds.id("cb_gen_1")
        _cleanup(session, k)

        session.upsert(k).set_bins({"v": 1}).execute()
        gen = session.query(k).execute().first_or_raise().record.generation

        results = (
            session
                .query(k)
                .update(k)
                    .bin("v").set_to(2)
                    .ensure_generation_is(gen)
                .execute()
        ).collect()
        # results[0] = read (OK), results[1] = write (OK)
        assert len(results) == 2
        assert results[1].result_code == ResultCode.OK

        assert session.query(k).execute().first_or_raise().record.bins["v"] == 2

        _cleanup(session, k)

    def test_generation_mismatch_fails(self, session, ds):
        k = ds.id("cb_gen_2")
        _cleanup(session, k)

        session.upsert(key=k).set_bins({"v": 1}).execute()

        results = (
            session
                .query(k)
                .update(k)
                    .bin("v").set_to(2)
                    .ensure_generation_is(999)
                .execute()
        ).collect()
        # results[0] = read (OK), results[1] = write (generation error)
        assert len(results) == 2
        assert results[0].result_code == ResultCode.OK
        assert results[1].result_code == ResultCode.GENERATION_ERROR

        assert session.query(k).execute().first_or_raise().record.bins["v"] == 1

        _cleanup(session, k)


class TestChainLevelDefaults:
    """Chain-level default_expire_record_after_seconds."""

    def test_default_ttl(self, session, ds):
        k1 = ds.id("cb_dttl_1")
        k2 = ds.id("cb_dttl_2")
        _cleanup(session, k1, k2)

        (
            session
                .query(k1)
                .default_expire_record_after_seconds(3600)
                .upsert(k1).bin("a").set_to(1)
                .upsert(k2).bin("b").set_to(2)
                .execute()
        ).collect()

        r1 = session.query(k1).execute().first_or_raise().record
        assert r1.bins["a"] == 1
        assert r1.ttl is not None and r1.ttl > 0

        r2 = session.query(k2).execute().first_or_raise().record
        assert r2.bins["b"] == 2
        assert r2.ttl is not None and r2.ttl > 0

        _cleanup(session, k1, k2)

    def test_per_spec_ttl_overrides_default(self, session, ds):
        k1 = ds.id("cb_dttl_3")
        k2 = ds.id("cb_dttl_4")
        _cleanup(session, k1, k2)

        (
            session
                .query(k1)
                .default_expire_record_after_seconds(3600)
                .upsert(k1)
                    .bin("a").set_to(1)
                    .expire_record_after_seconds(86400)
                .upsert(k2).bin("b").set_to(2)
                .execute()
        ).collect()

        r1 = session.query(k1).execute().first_or_raise().record
        r2 = session.query(k2).execute().first_or_raise().record
        assert r1.ttl > r2.ttl

        _cleanup(session, k1, k2)


class TestBatchTouch:
    """Touch operations in chained batches."""

    def test_touch_with_read(self, session, ds):
        """Touch one key while reading another in a single batch."""
        k1 = ds.id("cb_touch_1")
        k2 = ds.id("cb_touch_2")
        _cleanup(session, k1, k2)
        try:
            session.upsert(k1).set_bins({"a": 1}).execute()
            session.upsert(k2).set_bins({"a": 2}).execute()

            results = (
                session
                    .query(k1).bins(["a"])
                    .touch(k2)
                    .execute()
            ).collect()
            assert len(results) == 2
            read_r = [r for r in results if r.key == k1][0]
            assert read_r.record.bins["a"] == 1
            touch_r = [r for r in results if r.key == k2][0]
            assert touch_r.result_code == ResultCode.OK
        finally:
            _cleanup(session, k1, k2)

    def test_touch_with_upsert(self, session, ds):
        """Touch one key while upserting another."""
        k1 = ds.id("cb_touch_u1")
        k2 = ds.id("cb_touch_u2")
        _cleanup(session, k1, k2)
        try:
            session.upsert(k1).set_bins({"a": 1}).execute()

            results = (
                session
                    .upsert(k2).bin("a").set_to(99)
                    .touch(k1)
                    .execute()
            ).collect()
            assert len(results) == 2

            assert session.query(k2).execute().first_or_raise().record.bins["a"] == 99
        finally:
            _cleanup(session, k1, k2)

    def test_touch_not_found(self, session, ds):
        """Touch on a non-existent key surfaces KEY_NOT_FOUND_ERROR."""
        k_exists = ds.id("cb_touch_nf1")
        k_missing = ds.id("cb_touch_nf2")
        _cleanup(session, k_exists, k_missing)
        try:
            session.upsert(k_exists).set_bins({"a": 1}).execute()

            results = (
                session
                    .query(k_exists).bins(["a"])
                    .touch(k_missing).respond_all_keys()
                    .execute()
            ).collect()
            assert len(results) == 2
            read_r = [r for r in results if r.key == k_exists][0]
            assert read_r.record.bins["a"] == 1
            touch_r = [r for r in results if r.key == k_missing][0]
            assert touch_r.result_code == ResultCode.KEY_NOT_FOUND_ERROR
        finally:
            _cleanup(session, k_exists)


class TestChainedExists:
    """Exists as a chainable verb in mixed-batch chains."""

    def test_exists_with_read(self, session, ds):
        """Check existence of one key while reading another."""
        k1 = ds.id("cb_ex_1")
        k2 = ds.id("cb_ex_2")
        _cleanup(session, k1, k2)
        try:
            session.upsert(k1).set_bins({"a": 1}).execute()
            session.upsert(k2).set_bins({"a": 2}).execute()

            results = (
                session
                    .query(k1).bins(["a"])
                    .exists(k2).respond_all_keys()
                    .execute()
            ).collect()
            assert len(results) == 2
            read_r = [r for r in results if r.key == k1][0]
            assert read_r.record.bins["a"] == 1
            exists_r = [r for r in results if r.key == k2][0]
            assert exists_r.result_code == ResultCode.OK
        finally:
            _cleanup(session, k1, k2)

    def test_exists_not_found_in_chain(self, session, ds):
        """Exists on a missing key surfaces KEY_NOT_FOUND_ERROR."""
        k_exists = ds.id("cb_ex_nf1")
        k_missing = ds.id("cb_ex_nf2")
        _cleanup(session, k_exists, k_missing)
        try:
            session.upsert(k_exists).set_bins({"a": 10}).execute()

            results = (
                session
                    .query(k_exists).bins(["a"])
                    .exists(k_missing).respond_all_keys()
                    .execute()
            ).collect()
            assert len(results) == 2
            read_r = [r for r in results if r.key == k_exists][0]
            assert read_r.record.bins["a"] == 10
            exists_r = [r for r in results if r.key == k_missing][0]
            assert exists_r.result_code == ResultCode.KEY_NOT_FOUND_ERROR
        finally:
            _cleanup(session, k_exists)

    def test_exists_mixed_found_and_missing(self, session, ds):
        """Exists + touch + exists(missing) in a single chain."""
        k1 = ds.id("cb_ex_mix1")
        k2 = ds.id("cb_ex_mix2")
        k3 = ds.id("cb_ex_mix3")
        _cleanup(session, k1, k2, k3)
        try:
            session.upsert(k1).set_bins({"a": 1}).execute()
            session.upsert(k2).set_bins({"a": 2}).execute()

            results = (
                session
                    .query(k1).bins(["a"])
                    .touch(k2)
                    .exists(k3).respond_all_keys()
                    .execute()
            ).collect()
            assert len(results) == 3
            read_r = [r for r in results if r.key == k1][0]
            assert read_r.record.bins["a"] == 1
            touch_r = [r for r in results if r.key == k2][0]
            assert touch_r.result_code == ResultCode.OK
            ex3 = [r for r in results if r.key == k3][0]
            assert ex3.result_code == ResultCode.KEY_NOT_FOUND_ERROR
        finally:
            _cleanup(session, k1, k2)
