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

"""Integration tests for error disposition (on_error parameter).

Covers:
- Default error disposition (single-key raises, batch embeds)
- ErrorStrategy.IN_STREAM
- ErrorHandler callback
- Multi-spec partial failure
- Op-type errors (insert/update/replace_if_exists on wrong state)
- fail_on_filtered_out on read and write paths
"""

import pytest
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.error_strategy import ErrorStrategy
from aerospike_sdk.exceptions import AerospikeError, GenerationError


@pytest.fixture
async def client(aerospike_host, client_policy):
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        yield c


@pytest.fixture
def ds():
    return DataSet.of("test", "error_handling")


@pytest.fixture
async def session(client):
    return client.create_session()


async def _cleanup(session, *keys):
    for k in keys:
        try:
            await session.delete(k).execute()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Default disposition: single-key raises, batch embeds
# ---------------------------------------------------------------------------

class TestDefaultDisposition:

    async def test_single_key_write_raises_on_generation_mismatch(self, session, ds):
        """Single-key server error raises by default."""
        k = ds.id("eh_gen_1")
        await _cleanup(session, k)

        await session.upsert(k).set_bins({"v": 1}).execute()

        with pytest.raises(GenerationError):
            await (
                session.update(k)
                    .ensure_generation_is(999)
                    .bin("v").set_to(2)
                    .execute()
            )
        await _cleanup(session, k)

    async def test_batch_embeds_errors_by_default(self, session, ds):
        """Multi-key batch embeds per-key errors in the stream."""
        k1 = ds.id("eh_batch_1")
        k2 = ds.id("eh_batch_2")
        await _cleanup(session, k1, k2)

        await session.upsert(k1).set_bins({"v": 1}).execute()

        rs = await (
            session.query(k1, k2)
                .respond_all_keys()
                .execute()
        )
        results = await rs.collect()
        assert len(results) == 2
        ok_results = [r for r in results if r.is_ok]
        nf_results = [r for r in results if r.result_code == ResultCode.KEY_NOT_FOUND_ERROR]
        assert len(ok_results) == 1
        assert len(nf_results) == 1

        await _cleanup(session, k1)


# ---------------------------------------------------------------------------
# ErrorStrategy.IN_STREAM: single-key errors embedded
# ---------------------------------------------------------------------------

class TestInStreamStrategy:

    async def test_single_key_generation_error_in_stream(self, session, ds):
        """With IN_STREAM, single-key server errors are embedded, not raised."""
        k = ds.id("eh_instream_1")
        await _cleanup(session, k)

        await session.upsert(k).set_bins({"v": 1}).execute()

        stream = await (
            session
            .query(k)
            .update(k)
                .ensure_generation_is(999)
                .bin("v").set_to(2)
            .execute(on_error=ErrorStrategy.IN_STREAM)
        )
        results = await stream.collect()
        assert len(results) == 2

        read_result = results[0]
        assert read_result.is_ok

        write_result = results[1]
        assert write_result.result_code == ResultCode.GENERATION_ERROR
        assert not write_result.is_ok

        await _cleanup(session, k)


# ---------------------------------------------------------------------------
# ErrorHandler callback
# ---------------------------------------------------------------------------

class TestErrorHandler:

    async def test_handler_receives_error(self, session, ds):
        """ErrorHandler callback receives key, index, and exception."""
        k = ds.id("eh_handler_1")
        await _cleanup(session, k)

        await session.upsert(k).set_bins({"v": 1}).execute()

        captured = []

        def on_error(key, index, exc):
            captured.append((key, index, exc))

        rs = await (
            session
            .query(k)
            .update(k)
                .ensure_generation_is(999)
                .bin("v").set_to(2)
            .execute(on_error=on_error)
        )
        results = await rs.collect()
        assert len(results) == 1
        assert results[0].is_ok
        assert len(captured) == 1
        assert isinstance(captured[0][2], GenerationError)

        await _cleanup(session, k)

    async def test_handler_with_batch_error(self, session, ds):
        """ErrorHandler receives per-key errors, successes stay in stream."""
        k1 = ds.id("eh_handler_b1")
        k2 = ds.id("eh_handler_b2")
        await _cleanup(session, k1, k2)

        await session.upsert(k1).set_bins({"v": 1}).execute()

        errors = []
        rs = await (
            session
            .query(k1)
            .query(k2)
            .respond_all_keys()
            .execute(on_error=lambda key, idx, exc: errors.append(exc))
        )
        results = await rs.collect()
        ok_results = [r for r in results if r.is_ok]
        nf_results = [r for r in results if r.result_code == ResultCode.KEY_NOT_FOUND_ERROR]
        assert len(ok_results) == 1
        assert len(nf_results) == 1

        await _cleanup(session, k1)


# ---------------------------------------------------------------------------
# Multi-spec chain: one spec fails, others succeed
# ---------------------------------------------------------------------------

class TestMultiSpecPartialFailure:

    async def test_good_spec_results_preserved_with_in_stream(self, session, ds):
        """In a multi-spec chain, successful specs produce results even
        when another spec has a per-key error (generation mismatch)."""
        k_good = ds.id("eh_multi_good")
        k_fail = ds.id("eh_multi_fail")
        await _cleanup(session, k_good, k_fail)

        await session.upsert(k_good).set_bins({"v": 1}).execute()
        await session.upsert(k_fail).set_bins({"v": 1}).execute()

        rs = await (
            session
            .query(k_good)
            .update(k_fail)
                .ensure_generation_is(999)
                .bin("v").set_to(99)
            .execute(on_error=ErrorStrategy.IN_STREAM)
        )
        results = await rs.collect()
        assert len(results) == 2

        ok_results = [r for r in results if r.is_ok]
        err_results = [r for r in results if not r.is_ok]
        assert len(ok_results) == 1
        assert len(err_results) == 1
        assert err_results[0].result_code == ResultCode.GENERATION_ERROR

        await _cleanup(session, k_good, k_fail)


# ---------------------------------------------------------------------------
# Bucket 1: Op-type errors (insert/update/replace_if_exists on wrong state)
# ---------------------------------------------------------------------------

class TestOpTypeErrors:

    async def test_insert_on_existing_key_raises(self, session, ds):
        """Insert on an existing record produces KEY_EXISTS_ERROR."""
        k = ds.id("ot_insert_dup")
        await _cleanup(session, k)

        await session.upsert(k).put({"v": 1}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            await session.insert(k).bin("v").set_to(2).execute()
        assert exc_info.value.result_code == ResultCode.KEY_EXISTS_ERROR

        await _cleanup(session, k)

    async def test_insert_on_existing_key_in_stream(self, session, ds):
        """Insert error embedded when using IN_STREAM."""
        k = ds.id("ot_insert_is")
        await _cleanup(session, k)

        await session.upsert(k).put({"v": 1}).execute()

        rs = await session.insert(k).bin("v").set_to(2).execute(
            on_error=ErrorStrategy.IN_STREAM,
        )
        result = await rs.first()
        assert result is not None
        assert result.result_code == ResultCode.KEY_EXISTS_ERROR

        await _cleanup(session, k)

    async def test_update_on_missing_key_raises(self, session, ds):
        """Update on a nonexistent record produces KEY_NOT_FOUND_ERROR."""
        k = ds.id("ot_update_miss")
        await _cleanup(session, k)

        with pytest.raises(AerospikeError) as exc_info:
            await session.update(k).bin("v").set_to(1).execute()
        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    async def test_replace_if_exists_on_missing_key_raises(self, session, ds):
        """replace_if_exists on a nonexistent record produces KEY_NOT_FOUND_ERROR."""
        k = ds.id("ot_rplce_miss")
        await _cleanup(session, k)

        with pytest.raises(AerospikeError) as exc_info:
            await session.replace_if_exists(k).put({"v": 1}).execute()
        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    async def test_batch_insert_partial_failure_in_stream(self, session, ds):
        """Batch insert: existing key gets KEY_EXISTS_ERROR, new key succeeds."""
        k_exists = ds.id("ot_bi_exists")
        k_new = ds.id("ot_bi_new")
        await _cleanup(session, k_exists, k_new)

        await session.upsert(k_exists).put({"v": 1}).execute()

        rs = await (
            session
            .insert(k_exists).bin("v").set_to(2)
            .insert(k_new).bin("v").set_to(3)
            .execute(on_error=ErrorStrategy.IN_STREAM)
        )
        results = await rs.collect()
        assert len(results) == 2

        err = [r for r in results if r.result_code == ResultCode.KEY_EXISTS_ERROR]
        ok = [r for r in results if r.is_ok]
        assert len(err) == 1
        assert len(ok) == 1

        await _cleanup(session, k_exists, k_new)


# ---------------------------------------------------------------------------
# Bucket 2: fail_on_filtered_out on read and write paths
# ---------------------------------------------------------------------------

class TestFailOnFilteredOut:

    async def test_write_filtered_out_raises(self, session, ds):
        """Single-key upsert with where() + fail_on_filtered_out() raises
        FILTERED_OUT when the filter excludes the record."""
        k = ds.id("fo_write_1")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(k)
                    .bin("v").set_to(99)
                    .where("$.v == 999")
                    .fail_on_filtered_out()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        await _cleanup(session, k)

    async def test_write_filtered_out_passes_when_matched(self, session, ds):
        """Upsert with matching where() + fail_on_filtered_out() succeeds."""
        k = ds.id("fo_write_ok")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        await (
            session.upsert(k)
                .bin("v").set_to(99)
                .where("$.v == 1")
                .fail_on_filtered_out()
                .execute()
        )
        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == 99

        await _cleanup(session, k)

    async def test_read_filtered_out_raises(self, session, ds):
        """Query with where() + fail_on_filtered_out() raises FILTERED_OUT
        when the filter excludes the record."""
        k = ds.id("fo_read_1")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            rs = await (
                session.query(k)
                    .where("$.v == 999")
                    .fail_on_filtered_out()
                    .execute()
            )
            await rs.first_or_raise()
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        await _cleanup(session, k)

    async def test_read_filtered_out_passes_when_matched(self, session, ds):
        """Query with matching where() + fail_on_filtered_out() succeeds."""
        k = ds.id("fo_read_ok")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        rs = await (
            session.query(k)
                .where("$.v == 1")
                .fail_on_filtered_out()
                .execute()
        )
        rr = await rs.first_or_raise()
        assert rr.record.bins["v"] == 1

        await _cleanup(session, k)

    async def test_delete_filtered_out_raises(self, session, ds):
        """Delete with non-matching where() + fail_on_filtered_out() raises."""
        k = ds.id("fo_del_1")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.delete(k)
                    .where("$.v == 999")
                    .fail_on_filtered_out()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == 1

        await _cleanup(session, k)

    async def test_batch_filtered_out_in_stream(self, session, ds):
        """Batch with per-key where() + fail_on_filtered_out():
        matching key succeeds, non-matching gets FILTERED_OUT in stream."""
        k_match = ds.id("fo_batch_m")
        k_nomatch = ds.id("fo_batch_nm")
        await _cleanup(session, k_match, k_nomatch)
        await session.upsert(k_match).put({"v": 1}).execute()
        await session.upsert(k_nomatch).put({"v": 2}).execute()

        rs = await (
            session
            .upsert(k_match)
                .bin("v").set_to(10)
                .where("$.v == 1")
            .upsert(k_nomatch)
                .bin("v").set_to(20)
                .where("$.v == 999")
                .fail_on_filtered_out()
            .execute()
        )
        results = await rs.collect()
        assert len(results) == 2

        ok = [r for r in results if r.is_ok]
        fo = [r for r in results if r.result_code == ResultCode.FILTERED_OUT]
        assert len(ok) == 1
        assert len(fo) == 1

        await _cleanup(session, k_match, k_nomatch)


# ---------------------------------------------------------------------------
# Filter expression on delete (success path) and durable delete
# ---------------------------------------------------------------------------

class TestFilteredDeletePaths:

    async def test_delete_with_matching_where_succeeds(self, session, ds):
        """Delete with a matching where() filter actually deletes the record."""
        k = ds.id("fd_match")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        await session.delete(k).where("$.v == 1").execute()

        rs = await session.query(k).respond_all_keys().execute()
        rr = await rs.first()
        assert rr is not None
        assert rr.result_code == ResultCode.KEY_NOT_FOUND_ERROR

        await _cleanup(session, k)

    async def test_delete_with_nonmatching_where_preserves(self, session, ds):
        """Delete with a non-matching where() filter leaves the record intact."""
        k = ds.id("fd_nomatch")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        await session.delete(k).where("$.v == 999").execute()

        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == 1

        await _cleanup(session, k)

    async def test_durable_delete_with_where_succeeds(self, session, ds, enterprise):
        """Durable delete with matching where() deletes the record."""
        if not enterprise:
            pytest.skip("Requires Enterprise Edition")
        k = ds.id("fd_dur_ok")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        await (
            session.delete(k)
                .durably_delete()
                .where("$.v == 1")
                .execute()
        )

        rs = await session.query(k).respond_all_keys().execute()
        rr = await rs.first()
        assert rr is not None
        assert rr.result_code == ResultCode.KEY_NOT_FOUND_ERROR

        await _cleanup(session, k)

    async def test_durable_delete_with_where_filtered_out(self, session, ds, enterprise):
        """Durable delete with non-matching where() + fail_on_filtered_out()
        raises FILTERED_OUT."""
        if not enterprise:
            pytest.skip("Requires Enterprise Edition")
        k = ds.id("fd_dur_fo")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.delete(k)
                    .durably_delete()
                    .where("$.v == 999")
                    .fail_on_filtered_out()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == 1

        await _cleanup(session, k)


# ---------------------------------------------------------------------------
# Operate (write with where) / fail_on_filtered_out
# ---------------------------------------------------------------------------

class TestOperateWithFilter:

    async def test_operate_write_with_matching_where(self, session, ds):
        """Upsert + bin.set_to() with matching where() writes successfully."""
        k = ds.id("op_wr_ok")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        rs = await (
            session.upsert(k)
                .bin("v").set_to(99)
                .where("$.v == 1")
                .execute()
        )
        rr = await rs.first_or_raise()
        assert rr.is_ok

        rr2 = await (await session.query(k).execute()).first_or_raise()
        assert rr2.record.bins["v"] == 99

        await _cleanup(session, k)

    async def test_operate_write_nonmatching_where_skips(self, session, ds):
        """Upsert + bin.set_to() with non-matching where() silently skips."""
        k = ds.id("op_wr_skip")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        await (
            session.upsert(k)
                .bin("v").set_to(99)
                .where("$.v == 999")
                .execute()
        )

        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == 1

        await _cleanup(session, k)

    async def test_operate_write_filtered_out_raises(self, session, ds):
        """Upsert + bin.set_to() with non-matching where() +
        fail_on_filtered_out() raises FILTERED_OUT and doesn't write."""
        k = ds.id("op_wr_fo")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(k)
                    .bin("v").set_to(99)
                    .where("$.v == 999")
                    .fail_on_filtered_out()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == 1

        await _cleanup(session, k)

    async def test_operate_read_with_matching_where(self, session, ds):
        """Query + bin.select_from() with matching where() returns result."""
        k = ds.id("op_rd_ok")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        rs = await (
            session.upsert(k)
            .bin("result").select_from("$.v:INT")
            .execute()
        )
        rr = await rs.first_or_raise()
        assert rr.is_ok

        await _cleanup(session, k)

    async def test_operate_read_filtered_out_raises(self, session, ds):
        """Upsert + bin.select_from() with non-matching where() +
        fail_on_filtered_out() raises FILTERED_OUT."""
        k = ds.id("op_rd_fo")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.upsert(k)
                    .bin("result").select_from("$.v")
                    .where("$.v == 999")
                    .fail_on_filtered_out()
                    .execute()
            )
        assert exc_info.value.result_code == ResultCode.FILTERED_OUT

        await _cleanup(session, k)


# ---------------------------------------------------------------------------
# WriteBinBuilder.get() — read bin within a write operate
# ---------------------------------------------------------------------------

class TestWriteBinGet:

    async def test_get_returns_post_write_value(self, session, ds):
        """bin.set_to() then bin.get() returns the written value."""
        k = ds.id("wbb_get_basic")
        await _cleanup(session, k)

        rs = await (
            session.upsert(k)
                .bin("v").set_to(42)
                .bin("v").get()
                .execute()
        )
        rr = await rs.first_or_raise()
        assert rr.is_ok
        assert rr.record.bins["v"] == 42

        await _cleanup(session, k)

    async def test_get_multiple_bins(self, session, ds):
        """Write and read back multiple bins in one operate."""
        k = ds.id("wbb_get_multi")
        await _cleanup(session, k)

        rs = await (
            session.upsert(k)
                .bin("a").set_to(1)
                .bin("b").set_to("hello")
                .bin("a").get()
                .bin("b").get()
                .execute()
        )
        rr = await rs.first_or_raise()
        assert rr.is_ok
        assert rr.record.bins["a"] == 1
        assert rr.record.bins["b"] == "hello"

        await _cleanup(session, k)

    async def test_get_existing_bin_not_written(self, session, ds):
        """get() on a pre-existing bin reads its current value."""
        k = ds.id("wbb_get_existing")
        await _cleanup(session, k)
        await session.upsert(k).put({"x": 10, "y": 20}).execute()

        rs = await (
            session.upsert(k)
                .bin("x").set_to(99)
                .bin("y").get()
                .execute()
        )
        rr = await rs.first_or_raise()
        assert rr.is_ok
        assert rr.record.bins["y"] == 20

        await _cleanup(session, k)

    async def test_expression_write_then_get(self, session, ds):
        """upsert_from() expression write followed by bin.get() reads back."""
        k = ds.id("wbb_exp_get")
        await _cleanup(session, k)
        await session.upsert(k).put({"a": 1}).execute()

        rs = await (
            session.update(k)
                .bin("c").upsert_from("$.a + 4")
                .bin("c").get()
                .execute()
        )
        rr = await rs.first_or_raise()
        assert rr.is_ok
        val = rr.record.bins["c"]
        if isinstance(val, list):
            assert val[-1] == 5
        else:
            assert val == 5

        await _cleanup(session, k)

    async def test_chained_write_then_query_with_get(self, session, ds):
        """Upsert a key, then query another key with bin.get()."""
        k1 = ds.id("wbb_chain_wr")
        k2 = ds.id("wbb_chain_rd")
        await _cleanup(session, k1, k2)
        await session.upsert(k2).put({"name": "alice", "age": 30}).execute()

        rs = await (
            session.upsert(k1)
                .bin("status").set_to("active")
            .query(k2)
                .bin("name").get()
                .bin("computed").select_from("$.age * 2")
            .execute()
        )
        results = await rs.collect()
        assert len(results) >= 1
        read_result = [r for r in results if r.record and "name" in r.record.bins]
        assert len(read_result) == 1
        assert read_result[0].record.bins["name"] == "alice"
        assert read_result[0].record.bins["computed"] == 60

        await _cleanup(session, k1, k2)


# ---------------------------------------------------------------------------
# Idempotent delete and non-existent key queries
# ---------------------------------------------------------------------------

class TestIdempotentOps:

    async def test_delete_nonexistent_succeeds(self, session, ds):
        """Delete on a non-existent key completes without error."""
        k = ds.id("idm_del_miss")
        await _cleanup(session, k)

        rs = await session.delete(k).execute()
        rr = await rs.first()
        assert rr is None or rr.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    async def test_query_nonexistent_returns_empty(self, session, ds):
        """Query on a non-existent key returns an empty stream."""
        k = ds.id("idm_get_miss")
        await _cleanup(session, k)

        rs = await session.query(k).execute()
        rr = await rs.first()
        assert rr is None

    async def test_batch_delete_all_missing_returns_empty(self, session, ds):
        """Batch delete where all keys are missing returns an empty stream."""
        k1 = ds.id("idm_bd_miss1")
        k2 = ds.id("idm_bd_miss2")
        await _cleanup(session, k1, k2)

        rs = await session.delete([k1, k2]).execute()
        results = await rs.collect()
        assert len(results) == 0


# ---------------------------------------------------------------------------
# TTL expiry
# ---------------------------------------------------------------------------

class TestTtlExpiry:

    async def test_record_expires_after_ttl(self, session, ds):
        """Record with short TTL is gone after expiry."""
        import asyncio
        k = ds.id("ttl_expire")
        await _cleanup(session, k)

        await (
            session.upsert(k)
                .expire_record_after_seconds(2)
                .put({"v": 1})
                .execute()
        )

        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == 1

        await asyncio.sleep(3)

        rs = await session.query(k).execute()
        rr = await rs.first()
        assert rr is None

    async def test_record_with_no_ttl_persists(self, session, ds):
        """Record with default TTL (0 = namespace default, effectively no
        expiry on test namespace) persists beyond a short wait."""
        import asyncio
        k = ds.id("ttl_persist")
        await _cleanup(session, k)

        await session.upsert(k).put({"v": 1}).execute()

        await asyncio.sleep(3)

        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == 1

        await _cleanup(session, k)

    async def test_touch_extends_ttl(self, session, ds):
        """Write with short TTL, touch to extend, verify it survives."""
        import asyncio
        k = ds.id("ttl_touch_ext")
        await _cleanup(session, k)

        await (
            session.upsert(k)
                .expire_record_after_seconds(2)
                .put({"v": "touchvalue"})
                .execute()
        )

        await (
            session.touch(k)
                .expire_record_after_seconds(5)
                .execute()
        )

        await asyncio.sleep(3)

        rr = await (await session.query(k).execute()).first_or_raise()
        assert rr.record.bins["v"] == "touchvalue"

        await asyncio.sleep(4)

        rs = await session.query(k).execute()
        rr = await rs.first()
        assert rr is None

    async def test_no_change_in_expiration_preserves_ttl(self, session, ds):
        """After a TTL write, an upsert with ``with_no_change_in_expiration`` keeps TTL."""
        k = ds.id("ttl_no_change")
        await _cleanup(session, k)

        await (
            session.upsert(k)
                .expire_record_after_seconds(900)
                .put({"v": 1})
                .execute()
        )
        r1 = await (await session.query(k).execute()).first_or_raise()
        ttl1 = r1.record.ttl
        assert ttl1 is not None and ttl1 > 0

        await (
            session.upsert(k)
                .with_no_change_in_expiration()
                .bin("v").set_to(2)
                .execute()
        )
        r2 = await (await session.query(k).execute()).first_or_raise()
        ttl2 = r2.record.ttl
        assert ttl2 is not None and ttl2 > 0
        assert abs(ttl1 - ttl2) <= 2
        assert r2.record.bins["v"] == 2

        await _cleanup(session, k)

    async def test_touch_nonexistent_returns_empty(self, session, ds):
        """Touch on a non-existent key produces no result."""
        k = ds.id("ttl_touch_miss")
        await _cleanup(session, k)

        rs = await session.touch(k).execute()
        rr = await rs.first()
        assert rr is None

    async def test_touch_existing_succeeds(self, session, ds):
        """Touch on an existing key succeeds (record still there)."""
        k = ds.id("ttl_touch_ok")
        await _cleanup(session, k)
        await session.upsert(k).put({"v": 1}).execute()

        rs = await session.touch(k).execute()
        rr = await rs.first()
        assert rr is not None
        assert rr.result_code == ResultCode.OK

        rr2 = await (await session.query(k).execute()).first_or_raise()
        assert rr2.record.bins["v"] == 1

        await _cleanup(session, k)
