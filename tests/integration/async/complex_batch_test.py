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

"""Integration tests for complex batch (mixed read + write chains)."""

import asyncio

import pytest
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Settings

from tests.pac_compat import xfail_if_server_compiled_ael_wire_active


@pytest.fixture
def ds():
    return DataSet.of("test", "complex_batch")


@pytest.fixture
async def session(client):
    return client.create_session()


async def _cleanup(session, *keys):
    for k in keys:
        try:
            await session.delete(k).execute()
        except Exception:
            pass


class TestMixedReadWrite:
    """Chained read + write operations in a single execute()."""

    async def test_query_then_upsert(self, session, ds):
        k1 = ds.id("cb_rw_1")
        k2 = ds.id("cb_rw_2")
        await _cleanup(session, k1, k2)

        await session.upsert(key=k1).set_bins({"name": "Alice", "age": 21}).execute()

        rs = await (
            session
                .query(k1)
                .upsert(k2).bin("status").set_to("active")
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2

        r1 = results[0].record
        assert r1.bins["name"] == "Alice"

        r2_result = await (await session.query(k2).execute()).first_or_raise()
        r2 = r2_result.record
        assert r2.bins["status"] == "active"

        await _cleanup(session, k1, k2)

    async def test_upsert_then_query_bins(self, session, ds):
        k1 = ds.id("cb_rw_3")
        k2 = ds.id("cb_rw_4")
        await _cleanup(session, k1, k2)

        await session.upsert(k2).set_bins({"x": 10, "y": 20}).execute()

        rs = await (
            session
                .upsert(k1).bin("label").set_to("new")
                .query(k2).bins(["x"])
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2

        upsert_result = results[0]
        assert upsert_result.result_code == ResultCode.OK

        read_result = results[1].record
        assert read_result.bins.get("x") == 10
        assert "y" not in read_result.bins

        await _cleanup(session, k1, k2)

    async def test_query_expression_then_write(self, session, ds):
        k1 = ds.id("cb_rw_5")
        k2 = ds.id("cb_rw_6")
        await _cleanup(session, k1, k2)

        await session.upsert(k1).set_bins({"score": 50}).execute()

        rs = await (
            session
                .query(k1).bin("doubled").select_from("$.score * 2")
                .upsert(k2).bin("tag").set_to("written")
                .execute()
        )
        results = await rs.collect()

        read_result = results[0].record
        assert read_result.bins["doubled"] == 100

        r2_result = await (await session.query(k2).execute()).first_or_raise()
        r2 = r2_result.record
        assert r2.bins["tag"] == "written"

        await _cleanup(session, k1, k2)


class TestMixedOpTypes:
    """Chain different write op types in a single execute()."""

    async def test_upsert_insert_replace(self, session, ds):
        k_upsert = ds.id("cb_op_1")
        k_insert = ds.id("cb_op_2")
        k_replace = ds.id("cb_op_3")
        await _cleanup(session, k_upsert, k_insert, k_replace)

        await session.upsert(k_replace).set_bins({"original": True}).execute()

        rs = await (
            session
                .query(k_upsert)
                .upsert(k_upsert).bin("type").set_to("upsert")
                .insert(k_insert).bin("type").set_to("insert")
                .replace_if_exists(k_replace).bin("type").set_to("replaced")
                .execute()
        )
        results = await rs.collect()
        ok_count = sum(1 for r in results if r.result_code == ResultCode.OK)
        assert ok_count >= 3

        r1_result = await (await session.query(k_upsert).execute()).first_or_raise()
        r1 = r1_result.record
        assert r1.bins["type"] == "upsert"

        r2_result = await (await session.query(k_insert).execute()).first_or_raise()
        r2 = r2_result.record
        assert r2.bins["type"] == "insert"

        r3_result = await (await session.query(k_replace).execute()).first_or_raise()
        r3 = r3_result.record
        assert r3.bins["type"] == "replaced"
        assert "original" not in r3.bins

        await _cleanup(session, k_upsert, k_insert, k_replace)

    async def test_insert_existing_key_fails(self, session, ds):
        k = ds.id("cb_op_4")
        await _cleanup(session, k)

        await session.upsert(k).set_bins({"x": 1}).execute()

        rs = await (
            session
                .query(k)
                .insert(k).bin("x").set_to(999)
                .execute()
        )
        results = await rs.collect()

        write_result = results[1]
        assert write_result.result_code != ResultCode.OK

        rec_result = await (await session.query(k).execute()).first_or_raise()
        rec = rec_result.record
        assert rec.bins["x"] == 1

        await _cleanup(session, k)


class TestWriteWithExpressions:
    """Expression-based writes in a chained context."""

    async def test_upsert_from_expression(self, session, ds):
        xfail_if_server_compiled_ael_wire_active(session.client)
        k = ds.id("cb_exp_1")
        await _cleanup(session, k)

        await session.upsert(k).set_bins({"value": 6}).execute()

        rs = await (
            session
                .query(k)
                .upsert(k).bin("computed").upsert_from("$.value + 1000")
                .execute()
        )
        await rs.collect()

        rec_result = await (await session.query(k).execute()).first_or_raise()
        rec = rec_result.record
        assert "computed" in rec.bins, (
            "expected upsert_from to create bin 'computed'; "
            f"bins={rec.bins!r}"
        )
        assert rec.bins["computed"] == 1006

        await _cleanup(session, k)

    async def test_expression_write_and_scalar_write(self, session, ds):
        k = ds.id("cb_exp_2")
        await _cleanup(session, k)

        await session.upsert(k).set_bins({"base": 10}).execute()

        rs = await (
            session
                .query(k)
                .upsert(k)
                    .bin("derived").upsert_from("$.base * 3")
                    .bin("label").set_to("combo")
                .execute()
        )
        await rs.collect()

        rec_result = await (await session.query(k).execute()).first_or_raise()
        rec = rec_result.record
        assert rec.bins["derived"] == 30
        assert rec.bins["label"] == "combo"

        await _cleanup(session, k)


class TestDeleteInChain:
    """Delete operations mixed into a chained execute()."""

    async def test_write_then_delete(self, session, ds):
        k1 = ds.id("cb_del_1")
        k2 = ds.id("cb_del_2")
        await _cleanup(session, k1, k2)

        await session.upsert(k2).set_bins({"temp": "remove_me"}).execute()

        rs = await (
            session
                .query(k1)
                .upsert(k1).bin("score").set_to(100)
                .delete(k2)
                .execute()
        )
        results = await rs.collect()
        assert len(results) >= 2

        r1_result = await (await session.query(k1).execute()).first_or_raise()
        r1 = r1_result.record
        assert r1.bins["score"] == 100

        exists_stream = await session.exists(k2).execute()
        exists_first = await exists_stream.first()
        assert not exists_first.as_bool() if exists_first else True

        await _cleanup(session, k1)

    async def test_read_write_delete(self, session, ds):
        k1 = ds.id("cb_del_3")
        k2 = ds.id("cb_del_4")
        k3 = ds.id("cb_del_5")
        await _cleanup(session, k1, k2, k3)

        await session.upsert(k1).set_bins({"name": "Alice"}).execute()
        await session.upsert(k3).set_bins({"tmp": True}).execute()

        rs = await (
            session
                .query(k1)
                .upsert(k2).bin("created").set_to(True)
                .delete(k3)
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 3

        assert results[0].record.bins["name"] == "Alice"

        r2_result = await (await session.query(k2).execute()).first_or_raise()
        r2 = r2_result.record
        assert r2.bins["created"] is True

        exists_stream = await session.exists(k3).execute()
        exists_first = await exists_stream.first()
        assert not exists_first.as_bool() if exists_first else True

        await _cleanup(session, k1, k2)


class TestPerSpecSettings:
    """Per-spec write settings: where, TTL, generation."""

    async def test_expire_record_after_seconds(self, session, ds):
        k = ds.id("cb_ttl_1")
        await _cleanup(session, k)

        rs = await (
            session
                .query(k)
                .upsert(k)
                    .bin("data").set_to("expiring")
                    .expire_record_after_seconds(86400)
                .execute()
        )
        await rs.collect()

        rec_result = await (await session.query(k).execute()).first_or_raise()
        rec = rec_result.record
        assert rec.bins["data"] == "expiring"
        assert rec.ttl is not None and rec.ttl > 0

        await _cleanup(session, k)

    async def test_generation_check(self, session, ds):
        k = ds.id("cb_gen_1")
        await _cleanup(session, k)

        await session.upsert(k).set_bins({"v": 1}).execute()

        rec_result = await (await session.query(k).execute()).first_or_raise()
        rec = rec_result.record
        gen = rec.generation

        rs = await (
            session
                .query(k)
                .update(k)
                    .bin("v").set_to(2)
                    .ensure_generation_is(gen)
                .execute()
        )
        results = await rs.collect()
        # results[0] = read (OK), results[1] = write (OK)
        assert len(results) == 2
        assert results[1].result_code == ResultCode.OK

        rec2_result = await (await session.query(k).execute()).first_or_raise()
        rec2 = rec2_result.record
        assert rec2.bins["v"] == 2

        await _cleanup(session, k)

    async def test_generation_mismatch_fails(self, session, ds):
        k = ds.id("cb_gen_2")
        await _cleanup(session, k)

        await session.upsert(key=k).set_bins({"v": 1}).execute()

        rs = await (
            session
                .query(k)
                .update(k)
                    .bin("v").set_to(2)
                    .ensure_generation_is(999)
                .execute()
        )
        results = await rs.collect()
        # results[0] = read (OK), results[1] = write (generation error)
        assert len(results) == 2
        assert results[0].result_code == ResultCode.OK
        assert results[1].result_code == ResultCode.GENERATION_ERROR

        rec_result = await (await session.query(k).execute()).first_or_raise()
        rec = rec_result.record
        assert rec.bins["v"] == 1

        await _cleanup(session, k)


class TestChainLevelDefaults:
    """Chain-level default_where and default_expire_record_after_seconds."""

    async def test_default_ttl(self, session, ds):
        k1 = ds.id("cb_dttl_1")
        k2 = ds.id("cb_dttl_2")
        await _cleanup(session, k1, k2)

        rs = await (
            session
                .query(k1)
                .default_expire_record_after_seconds(3600)
                .upsert(k1).bin("a").set_to(1)
                .upsert(k2).bin("b").set_to(2)
                .execute()
        )
        await rs.collect()

        r1_result = await (await session.query(k1).execute()).first_or_raise()
        r1 = r1_result.record
        assert r1.bins["a"] == 1
        assert r1.ttl is not None and r1.ttl > 0

        r2_result = await (await session.query(k2).execute()).first_or_raise()
        r2 = r2_result.record
        assert r2.bins["b"] == 2
        assert r2.ttl is not None and r2.ttl > 0

        await _cleanup(session, k1, k2)

    async def test_per_spec_ttl_overrides_default(self, session, ds):
        k1 = ds.id("cb_dttl_3")
        k2 = ds.id("cb_dttl_4")
        await _cleanup(session, k1, k2)

        rs = await (
            session
                .query(k1)
                .default_expire_record_after_seconds(3600)
                .upsert(k1)
                    .bin("a").set_to(1)
                    .expire_record_after_seconds(86400)
                .upsert(k2).bin("b").set_to(2)
                .execute()
        )
        await rs.collect()

        r1_result = await (await session.query(k1).execute()).first_or_raise()
        r1 = r1_result.record
        r2_result = await (await session.query(k2).execute()).first_or_raise()
        r2 = r2_result.record

        assert r1.ttl > r2.ttl

        await _cleanup(session, k1, k2)


BIN_NAME = "bbin"
BIN_NAME2 = "bbin2"
BIN_NAME3 = "bbin3"
VALUE_PREFIX = "batchvalue"
KEY_PREFIX = "batchkey"
SIZE = 10


@pytest.fixture
async def seed_data(session, ds):
    """Seed 10 records mirroring the shared batch test dataset.

    Keys 1-5, 7-10: bbin = "batchvalue{i}" (string)
    Key 6:           bbin = 6              (int)
    Keys 10000-10009: bbin = key_value     (int, for delete tests)
    """
    keys = [ds.id(f"{KEY_PREFIX}{i}") for i in range(1, SIZE + 1)]
    del_keys = [ds.id(i) for i in range(10000, 10010)]

    for i, k in enumerate(keys, start=1):
        val = i if i == 6 else f"{VALUE_PREFIX}{i}"
        await session.upsert(k).set_bins({BIN_NAME: val}).execute()

    for i, k in enumerate(del_keys, start=10000):
        await session.upsert(k).set_bins({BIN_NAME: i}).execute()

    yield {"keys": keys, "del_keys": del_keys}

    for k in keys + del_keys:
        try:
            await session.delete(k).execute()
        except Exception:
            pass


class TestBatchExists:
    """Batch exists with respondAllKeys — verify all keys report as existing."""

    async def test_batch_exists(self, session, ds, seed_data):
        keys = seed_data["keys"]

        rs = await session.exists(keys).respond_all_keys().execute()
        results = await rs.collect()

        assert len(results) == SIZE
        for i, result in enumerate(results):
            assert result.as_bool() is True, f"exists[{i}] is False"


class TestBatchReads:
    """Batch read with specific bins — verify string and int values."""

    async def test_batch_reads(self, session, ds, seed_data):
        keys = seed_data["keys"]

        rs = await (
            session
                .query(keys).bins([BIN_NAME])
                .execute()
        )
        results = await rs.collect()

        assert len(results) == SIZE
        for i, result in enumerate(results):
            rec = result.record_or_raise()
            val = rec.bins[BIN_NAME]
            expected = i + 1 if i == 5 else f"{VALUE_PREFIX}{i + 1}"
            assert val == expected, f"key {i + 1}: expected {expected!r}, got {val!r}"


class TestBatchReadHeaders:
    """Batch read with withNoBins — verify generation metadata."""

    async def test_batch_read_headers(self, session, ds, seed_data):
        keys = seed_data["keys"]

        rs = await (
            session
                .query(keys).with_no_bins()
                .execute()
        )
        results = await rs.collect()

        assert len(results) == SIZE
        for i, result in enumerate(results):
            rec = result.record_or_raise()
            assert rec.generation != 0, f"key {i + 1}: generation is 0"


class TestBatchReadComplex:
    """Chained per-key read configs: specific bins, no bins, expression, missing bin, missing key."""

    async def test_batch_read_complex(self, session, ds, seed_data):
        keys = seed_data["keys"]
        k1, k2, k3, k4 = keys[0], keys[1], keys[2], keys[3]
        k6, k7 = keys[5], keys[6]
        k_missing = ds.id("keynotfound")

        rs = await (
            session
                .query(k1).bins([BIN_NAME])
                .query(k2)
                .query(k3).with_no_bins()
                .query(k4)
                .query(k6).bin(BIN_NAME).select_from(f"$.{BIN_NAME} * 8")
                .query(k7).bins(["binnotfound"])
                .query(k_missing).bins([BIN_NAME])
                .execute()
        )
        results = await rs.collect()

        assert results[0].record.bins[BIN_NAME] == f"{VALUE_PREFIX}1"
        assert len(results[0].record.bins) == 1

        assert results[1].record.bins[BIN_NAME] == f"{VALUE_PREFIX}2"

        assert results[2].record.bins == {} or not results[2].record.bins

        assert results[3].record.bins[BIN_NAME] == f"{VALUE_PREFIX}4"

        # Expression: $.bbin * 8 on key 6 (bbin=6) → 48
        assert results[4].record.bins[BIN_NAME] == 48

        assert results[5].record.bins.get(BIN_NAME) is None

        # Missing key omitted → 6 results
        assert len(results) == 6

    async def test_batch_read_complex_respond_all_keys(self, session, ds, seed_data):
        """Missing key appears when respond_all_keys is set."""
        k1 = seed_data["keys"][0]
        k_missing = ds.id("keynotfound")

        rs = await (
            session
                .query(k1).bins([BIN_NAME])
                .query(k_missing).bins([BIN_NAME])
                .respond_all_keys()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2
        assert results[0].result_code == ResultCode.OK
        assert results[1].result_code == ResultCode.KEY_NOT_FOUND_ERROR


class TestBatchReadGuards:
    """Guard / edge-case tests for batch reads."""

    async def test_batch_reads_empty_bin_names(self, session, ds, seed_data):
        """Passing an empty bin-name list raises ValueError."""
        keys = seed_data["keys"]
        with pytest.raises(ValueError, match="must not be empty"):
            await session.query(keys).bins([]).execute()


class TestBatchDeleteLifecycle:
    """Full lifecycle: exists → delete → verify gone."""

    async def test_batch_delete(self, session, ds, seed_data):
        del_keys = seed_data["del_keys"]

        # Verify all keys exist.
        rs = await session.exists(del_keys).respond_all_keys().execute()
        exists = [r.as_bool() for r in await rs.collect()]
        assert len(exists) == len(del_keys)
        for status in exists:
            assert status is True

        # Delete all keys.
        rs = await session.delete(del_keys).respond_all_keys().execute()
        deletes = [r.as_bool() for r in await rs.collect()]
        assert len(deletes) == len(del_keys)
        for status in deletes:
            assert status is True

        # Verify all keys are gone.
        rs = await session.exists(del_keys).respond_all_keys().execute()
        exists_after = [r.as_bool() for r in await rs.collect()]
        assert len(exists_after) == len(del_keys)
        for status in exists_after:
            assert status is False


class TestBatchDeleteEdgeCases:
    """Edge-case tests for batch deletes."""

    async def test_batch_delete_single_not_found(self, session, ds):
        """Deleting keys that don't exist yields an empty stream."""
        first_key = 98929923
        keys = [ds.id(first_key + i) for i in range(10)]
        await _cleanup(session, *keys)

        rs = await session.delete(keys).execute()
        results = await rs.collect()
        assert len(results) == 0


class TestBatchWriteComplex:
    """Chained writes starting from session.upsert() (BinBuilder → QueryBuilder transition)."""

    @pytest.mark.xfail(
        reason="Rust core rejects entire batch when any key targets an unknown "
               "namespace; pending core fix for per-key INVALID_NAMESPACE",
        raises=Exception,
        strict=True,
    )
    async def test_batch_write_complex(self, session, ds, seed_data):
        keys = seed_data["keys"]
        k1, k6 = keys[0], keys[5]
        k_del = seed_data["del_keys"][2]
        invalid_ds = DataSet.of("invalid", ds.set_name)
        k_invalid = invalid_ds.id(f"{KEY_PREFIX}1")

        rs = await (
            session
                .upsert(key=k1).bin(BIN_NAME2).set_to(100)
                .upsert(k_invalid).bin(BIN_NAME2).set_to(100)
                .upsert(k6).bin(BIN_NAME3).upsert_from(f"$.{BIN_NAME} + 1000")
                .delete(k_del)
                .execute()
        )
        results = await rs.collect()

        assert results[0].result_code == ResultCode.OK
        assert results[1].result_code == ResultCode.INVALID_NAMESPACE
        assert results[2].result_code == ResultCode.OK

        # Verify by reading back
        rs2 = await (
            session
                .query(k1).bins([BIN_NAME2])
                .query(k6).bins([BIN_NAME3])
                .query(k_del)
                .respond_all_keys()
                .execute()
        )
        verify = await rs2.collect()

        assert verify[0].record.bins[BIN_NAME2] == 100
        assert verify[1].record.bins[BIN_NAME3] == 1006
        assert verify[2].result_code == ResultCode.KEY_NOT_FOUND_ERROR

    @pytest.mark.xfail(
        reason="Rust core rejects entire batch when any key targets an unknown "
               "namespace; pending core fix for per-key INVALID_NAMESPACE",
        raises=Exception,
        strict=True,
    )
    async def test_batch_write_invalid_namespace(self, session, ds, seed_data):
        """A chain targeting an invalid namespace embeds the error in the
        stream by default (batch disposition is IN_STREAM)."""
        keys = seed_data["keys"]
        k1 = keys[0]
        invalid_ds = DataSet.of("invalid", ds.set_name)
        k_invalid = invalid_ds.id(f"{KEY_PREFIX}1")

        rs = await (
            session
                .upsert(key=k1).bin(BIN_NAME2).set_to(100)
                .upsert(k_invalid).bin(BIN_NAME2).set_to(100)
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2
        errors = [r for r in results if not r.is_ok]
        assert len(errors) >= 1

    async def test_batch_write_delete_nonexistent(self, session, ds, seed_data):
        """Delete of a nonexistent key in a write chain."""
        k_good = seed_data["keys"][0]
        k_gone = ds.id("definitely_gone")

        rs = await (
            session
                .upsert(key=k_good).bin(BIN_NAME2).set_to(200)
                .delete(k_gone)
                .execute()
        )
        results = await rs.collect()

        assert results[0].result_code == ResultCode.OK
        rec_result = await (await session.query(k_good).execute()).first_or_raise()
        rec = rec_result.record
        assert rec.bins[BIN_NAME2] == 200


class TestMultiKeyBatchWrite:
    """Write operations targeting multiple keys in a single spec."""

    async def test_upsert_to_multiple_keys(self, session, ds):
        k1 = ds.id("cb_mk_1")
        k2 = ds.id("cb_mk_2")
        k3 = ds.id("cb_mk_3")
        await _cleanup(session, k1, k2, k3)

        rs = await (
            session
                .query(k1)
                .upsert([k1, k2, k3]).bin("status").set_to("batch_written")
                .execute()
        )
        results = await rs.collect()

        for k in (k1, k2, k3):
            rec_result = await (await session.query(k).execute()).first_or_raise()
            rec = rec_result.record
            assert rec.bins["status"] == "batch_written"

        await _cleanup(session, k1, k2, k3)

    async def test_delete_multiple_keys(self, session, ds):
        k1 = ds.id("cb_mk_4")
        k2 = ds.id("cb_mk_5")
        await _cleanup(session, k1, k2)

        await session.upsert(k1).set_bins({"x": 1}).execute()
        await session.upsert(k2).set_bins({"x": 2}).execute()

        rs = await (
            session
                .query(k1)
                .delete(k1, k2)
                .execute()
        )
        await rs.collect()

        ex1 = await session.exists(k1).execute()
        ex2 = await session.exists(k2).execute()
        first1 = await ex1.first()
        first2 = await ex2.first()
        assert not (first1.as_bool() if first1 else False)
        assert not (first2.as_bool() if first2 else False)


class TestBatchReadTTL:
    """Verify that read_touch_ttl_percent controls TTL reset on batch reads."""

    TTL_SECS = 5
    SLEEP_SECS = 3

    async def test_batch_read_ttl(self, client, session, ds):
        k1 = ds.id(88888)
        k2 = ds.id(88889)
        keys = [k1, k2]
        await _cleanup(session, *keys)

        try:
            # Seed both keys with a short TTL.
            await (
                session
                    .query(k1)
                    .upsert(keys)
                        .bin("a").set_to(1)
                        .expire_record_after_seconds(self.TTL_SECS)
                    .execute()
            )

            await asyncio.sleep(self.SLEEP_SECS)

            # Read key1 with TTL-reset enabled (80 %).
            reset_behavior = Behavior.DEFAULT.derive_with_changes(
                "read_touch_80",
                reads=Settings(read_touch_ttl_percent=80),
            )
            session_reset = client.create_session(reset_behavior)

            rs = await (
                session_reset
                    .query(k1).bins(["a"])
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 1
            assert results[0].record.bins["a"] == 1

            # Read key2 with TTL-reset disabled (-1).
            no_reset_behavior = Behavior.DEFAULT.derive_with_changes(
                "read_touch_off",
                reads=Settings(read_touch_ttl_percent=-1),
            )
            session_no_reset = client.create_session(no_reset_behavior)

            rs = await (
                session_no_reset
                    .query(k2).bins(["a"])
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 1
            assert results[0].record.bins["a"] == 1

            await asyncio.sleep(self.SLEEP_SECS)

            # key1 should still be alive (TTL was reset); key2 expired.
            rs = await (
                session_no_reset
                    .query(keys).bins(["a"])
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 1
            assert results[0].record.bins["a"] == 1

            await asyncio.sleep(self.SLEEP_SECS)

            # Both keys should now be expired.
            rs = await (
                session_no_reset
                    .query(keys).bins(["a"])
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 0
        finally:
            await _cleanup(session, *keys)


class TestBatchTouch:
    """Touch operations in chained batches."""

    async def test_touch_with_read(self, session, ds):
        """Touch one key while reading another in a single batch."""
        k1 = ds.id("cb_touch_1")
        k2 = ds.id("cb_touch_2")
        await _cleanup(session, k1, k2)
        try:
            await session.upsert(k1).set_bins({"a": 1}).execute()
            await session.upsert(k2).set_bins({"a": 2}).execute()

            rs = await (
                session
                    .query(k1).bins(["a"])
                    .touch(k2)
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 2
            read_r = [r for r in results if r.key == k1][0]
            assert read_r.record.bins["a"] == 1
            touch_r = [r for r in results if r.key == k2][0]
            assert touch_r.result_code == ResultCode.OK
        finally:
            await _cleanup(session, k1, k2)

    async def test_touch_with_upsert(self, session, ds):
        """Touch one key while upserting another."""
        k1 = ds.id("cb_touch_u1")
        k2 = ds.id("cb_touch_u2")
        await _cleanup(session, k1, k2)
        try:
            await session.upsert(k1).set_bins({"a": 1}).execute()

            rs = await (
                session
                    .upsert(k2).bin("a").set_to(99)
                    .touch(k1)
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 2

            verify_result = await (await session.query(k2).execute()).first_or_raise()
            verify = verify_result.record
            assert verify.bins["a"] == 99
        finally:
            await _cleanup(session, k1, k2)

    async def test_touch_not_found(self, session, ds):
        """Touch on a non-existent key surfaces KEY_NOT_FOUND_ERROR."""
        k_exists = ds.id("cb_touch_nf1")
        k_missing = ds.id("cb_touch_nf2")
        await _cleanup(session, k_exists, k_missing)
        try:
            await session.upsert(k_exists).set_bins({"a": 1}).execute()

            rs = await (
                session
                    .query(k_exists).bins(["a"])
                    .touch(k_missing).respond_all_keys()
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 2
            read_r = [r for r in results if r.key == k_exists][0]
            assert read_r.record.bins["a"] == 1
            touch_r = [r for r in results if r.key == k_missing][0]
            assert touch_r.result_code == ResultCode.KEY_NOT_FOUND_ERROR
        finally:
            await _cleanup(session, k_exists)


class TestChainedExists:
    """Exists as a chainable verb in mixed-batch chains."""

    async def test_exists_with_read(self, session, ds):
        """Check existence of one key while reading another."""
        k1 = ds.id("cb_ex_1")
        k2 = ds.id("cb_ex_2")
        await _cleanup(session, k1, k2)
        try:
            await session.upsert(k1).set_bins({"a": 1}).execute()
            await session.upsert(k2).set_bins({"a": 2}).execute()

            rs = await (
                session
                    .query(k1).bins(["a"])
                    .exists(k2).respond_all_keys()
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 2
            read_r = [r for r in results if r.key == k1][0]
            assert read_r.record.bins["a"] == 1
            exists_r = [r for r in results if r.key == k2][0]
            assert exists_r.result_code == ResultCode.OK
        finally:
            await _cleanup(session, k1, k2)

    async def test_exists_not_found_in_chain(self, session, ds):
        """Exists on a missing key surfaces KEY_NOT_FOUND_ERROR."""
        k_exists = ds.id("cb_ex_nf1")
        k_missing = ds.id("cb_ex_nf2")
        await _cleanup(session, k_exists, k_missing)
        try:
            await session.upsert(k_exists).set_bins({"a": 10}).execute()

            rs = await (
                session
                    .query(k_exists).bins(["a"])
                    .exists(k_missing).respond_all_keys()
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 2
            read_r = [r for r in results if r.key == k_exists][0]
            assert read_r.record.bins["a"] == 10
            exists_r = [r for r in results if r.key == k_missing][0]
            assert exists_r.result_code == ResultCode.KEY_NOT_FOUND_ERROR
        finally:
            await _cleanup(session, k_exists)

    async def test_exists_mixed_found_and_missing(self, session, ds):
        """Exists + touch + exists(missing) in a single chain."""
        k1 = ds.id("cb_ex_mix1")
        k2 = ds.id("cb_ex_mix2")
        k3 = ds.id("cb_ex_mix3")
        await _cleanup(session, k1, k2, k3)
        try:
            await session.upsert(k1).set_bins({"a": 1}).execute()
            await session.upsert(k2).set_bins({"a": 2}).execute()

            rs = await (
                session
                    .query(k1).bins(["a"])
                    .touch(k2)
                    .exists(k3).respond_all_keys()
                    .execute()
            )
            results = await rs.collect()
            assert len(results) == 3
            read_r = [r for r in results if r.key == k1][0]
            assert read_r.record.bins["a"] == 1
            touch_r = [r for r in results if r.key == k2][0]
            assert touch_r.result_code == ResultCode.OK
            ex3 = [r for r in results if r.key == k3][0]
            assert ex3.result_code == ResultCode.KEY_NOT_FOUND_ERROR
        finally:
            await _cleanup(session, k1, k2)
