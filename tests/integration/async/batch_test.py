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

"""Tests for batch operations with multi-key chaining.

Tests both:
1. Heterogeneous batch operations (different ops on different keys) - multi-segment verb chains
2. Homogeneous batch operations (same op on multiple keys) - session.exists/delete/query with multiple keys
3. RecordResult/RecordStream integration (result codes, or_raise, failures, first)
"""

import base64

import pytest
import pytest_asyncio
from aerospike_async import FilterExpression as Exp

from aerospike_sdk.dataset import DataSet
from aerospike_sdk import ErrorDetailVerbosity
from aerospike_sdk.exceptions import AerospikeError, ResultCode
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Scope, Settings

from tests.pac_compat import requires_server_compiled_ael
from tests.integration.namespace import general_namespace


@pytest.fixture
def users():
    """DataSet fixture for batch tests."""
    return DataSet.of(general_namespace(), "batch_test")


class TestBatchOperations:
    """Test multi-key write chains executed as a single batch."""

    async def test_batch_insert_multiple_keys(self, cluster, users: DataSet):
        """Test inserting multiple records in a single batch."""
        session = cluster.create_session()
        
        key1 = users.id("batch_user_1")
        key2 = users.id("batch_user_2")
        key3 = users.id("batch_user_3")
        
        # Clean up first
        try:
            await session.delete(key1).execute()
            await session.delete(key2).execute()
            await session.delete(key3).execute()
        except Exception:
            pass
        
        # Insert multiple records with chained operations
        stream = await (
            session.insert(key1)
            .bin("name").set_to("Alice")
            .bin("age").set_to(25)
            .insert(key2)
            .bin("name").set_to("Bob")
            .bin("age").set_to(30)
            .insert(key3).put({"name": "Charlie", "age": 35})
            .execute()
        )
        results = await stream.collect()

        assert len(results) == 3
        
        # Verify records were created
        rs1 = await session.query(key1).execute()
        record1 = await rs1.first_or_raise()
        assert record1 is not None
        assert record1.record.bins["name"] == "Alice"
        assert record1.record.bins["age"] == 25

        rs2 = await session.query(key2).execute()
        record2 = await rs2.first_or_raise()
        assert record2 is not None
        assert record2.record.bins["name"] == "Bob"
        assert record2.record.bins["age"] == 30

        rs3 = await session.query(key3).execute()
        record3 = await rs3.first_or_raise()
        assert record3 is not None
        assert record3.record.bins["name"] == "Charlie"
        assert record3.record.bins["age"] == 35

        # Cleanup
        await session.delete(key1).execute()
        await session.delete(key2).execute()
        await session.delete(key3).execute()

    async def test_batch_mixed_operations(self, cluster, users: DataSet):
        """Test batch with mixed insert, update, and delete operations."""
        session = cluster.create_session()
        
        key1 = users.id("batch_mixed_1")
        key2 = users.id("batch_mixed_2")
        key3 = users.id("batch_mixed_3")
        
        # Setup: create initial records
        await session.upsert(key1).put({"counter": 10}).execute()
        await session.upsert(key2).put({"name": "ToDelete"}).execute()

        # Clean key3 if exists
        try:
            await session.delete(key3).execute()
        except Exception:
            pass
        
        # Execute mixed batch operations
        stream = await (
            session.update(key1).bin("counter").add(5)
            .delete(key2)
            .insert(key3).bin("status").set_to("new")
            .execute()
        )
        results = await stream.collect()

        assert len(results) == 3
        
        # Verify update worked
        rs1 = await session.query(key1).execute()
        record1 = await rs1.first_or_raise()
        assert record1 is not None
        assert record1.record.bins["counter"] == 15

        # Verify delete worked
        exists_stream = await session.exists(key2).include_missing_keys().execute()
        result = await exists_stream.first()
        assert result is not None and result.as_bool() is False

        # Verify insert worked
        rs3 = await session.query(key3).execute()
        record3 = await rs3.first_or_raise()
        assert record3 is not None
        assert record3.record.bins["status"] == "new"

        # Cleanup
        await session.delete(key1).execute()
        await session.delete(key3).execute()

    async def test_batch_upsert_operations(self, cluster, users: DataSet):
        """Test batch upsert operations."""
        session = cluster.create_session()
        
        key1 = users.id("batch_upsert_1")
        key2 = users.id("batch_upsert_2")
        
        # Clean up first
        try:
            await session.delete(key1).delete()
            await session.delete(key2).delete()
        except Exception:
            pass
        
        # First batch: create records
        await (
            session.upsert(key1).bin("value").set_to("initial1")
            .upsert(key2).bin("value").set_to("initial2")
            .execute()
        )
        
        # Verify initial values
        rs1 = await session.query(key1).execute()
        record1 = await rs1.first_or_raise()
        assert record1.record.bins["value"] == "initial1"

        # Second batch: update existing records (upsert)
        await (
            session.upsert(key1).bin("value").set_to("updated1")
            .upsert(key2).bin("value").set_to("updated2")
            .execute()
        )
        
        # Verify updated values
        rs1 = await session.query(key1).execute()
        record1 = await rs1.first_or_raise()
        assert record1.record.bins["value"] == "updated1"

        rs2 = await session.query(key2).execute()
        record2 = await rs2.first_or_raise()
        assert record2.record.bins["value"] == "updated2"

        # Cleanup
        await session.delete(key1).execute()
        await session.delete(key2).execute()

    async def test_batch_delete_multiple_keys(self, cluster, users: DataSet):
        """Test deleting multiple records in a single batch."""
        session = cluster.create_session()
        
        key1 = users.id("batch_del_1")
        key2 = users.id("batch_del_2")
        key3 = users.id("batch_del_3")
        
        # Setup: create records
        await session.upsert(key1).put({"data": "1"}).execute()
        await session.upsert(key2).put({"data": "2"}).execute()
        await session.upsert(key3).put({"data": "3"}).execute()

        # Delete all in one batch
        stream = await (
            session.delete(key1)
            .delete(key2)
            .delete(key3)
            .execute()
        )
        results = await stream.collect()

        assert len(results) == 3
        
        # Verify all deleted
        for k in (key1, key2, key3):
            exists_stream = await session.exists(k).include_missing_keys().execute()
            result = await exists_stream.first()
            assert result is not None and result.as_bool() is False

    async def test_batch_bin_string_operations(self, cluster, users: DataSet):
        """Test batch with string bin operations (append/prepend)."""
        session = cluster.create_session()
        
        key1 = users.id("batch_str_1")
        key2 = users.id("batch_str_2")
        
        # Setup
        await session.upsert(key1).put({"message": "Hello"}).execute()
        await session.upsert(key2).put({"message": "World"}).execute()

        # Append and prepend in batch
        await (
            session.update(key1).bin("message").append(" World")
            .update(key2).bin("message").prepend("Hello ")
            .execute()
        )
        
        # Verify
        rs1 = await session.query(key1).execute()
        record1 = await rs1.first_or_raise()
        assert record1.record.bins["message"] == "Hello World"

        rs2 = await session.query(key2).execute()
        record2 = await rs2.first_or_raise()
        assert record2.record.bins["message"] == "Hello World"

        # Cleanup
        await session.delete(key1).execute()
        await session.delete(key2).execute()


class TestHomogeneousBatchOperations:
    """
    Test homogeneous batch operations (same operation on multiple keys).
    
    Tests for homogeneous batch operations:
    - batchExists
    - batchReads (via query)
    - batchReadHeaders (via query with no bins)
    - batchDelete
    """

    @pytest.fixture
    async def setup_batch_data(self, cluster, users: DataSet):
        """Setup test data for batch operations."""
        session = cluster.create_session()
        size = 10
        key_prefix = "batchkey"
        value_prefix = "batchvalue"
        
        # Create test records
        for i in range(1, size + 1):
            key = users.id(f"{key_prefix}{i}")
            list_data = [j * i for j in range(i)]
            
            if i != 6:
                await session.upsert(key).put({
                    "bbin": f"{value_prefix}{i}",
                    "lbin": list_data,
                }).execute()
            else:
                # Record 6 has integer value instead of string
                await session.upsert(key).put({
                    "bbin": i,
                    "lbin": list_data,
                }).execute()
        
        yield {
            "session": session,
            "size": size,
            "key_prefix": key_prefix,
            "value_prefix": value_prefix,
            "users": users,
        }
        
        # Cleanup
        for i in range(1, size + 1):
            key = users.id(f"{key_prefix}{i}")
            try:
                await session.delete(key).execute()
            except Exception:
                pass

    async def test_batch_exists_homogeneous(self, cluster, users: DataSet, setup_batch_data):
        """
        Test batch exists operation on multiple keys.
        Test batch exists operation.
        """
        data = setup_batch_data
        session = data["session"]
        size = data["size"]
        key_prefix = data["key_prefix"]
        
        # Create list of keys
        keys = users.ids(*[f"{key_prefix}{i}" for i in range(1, size + 1)])

        # Check existence of all keys
        stream = await session.exists(*keys).include_missing_keys().execute()
        results = await stream.collect()

        assert len(results) == size
        for i, result in enumerate(results):
            assert result.as_bool() is True, f"exists[{i}] is False"

    async def test_batch_reads_homogeneous(self, cluster, users: DataSet, setup_batch_data):
        """
        Test batch read operation on multiple keys via query.
        Test batch reads operation.
        """
        data = setup_batch_data
        session = data["session"]
        size = data["size"]
        key_prefix = data["key_prefix"]
        value_prefix = data["value_prefix"]
        
        # Create list of keys
        keys = users.ids(*[f"{key_prefix}{i}" for i in range(1, size + 1)])

        # Read all keys with specific bin
        stream = await session.query(*keys).bins(["bbin"]).execute()

        results = await stream.collect()

        assert len(results) == size

        for i, rr in enumerate(results):
            rec = rr.record_or_raise()
            if i != 5:  # Record 6 (index 5) has integer value
                val = rec.bins.get("bbin")
                assert val == f"{value_prefix}{i + 1}", f"record[{i}] has wrong value"
            else:
                val = rec.bins.get("bbin")
                assert val == i + 1, f"record[{i}] has wrong integer value"

    async def test_batch_read_headers_homogeneous(self, cluster, users: DataSet, setup_batch_data):
        """
        Test batch read headers (metadata only) via query.
        Test batch read headers operation.
        """
        data = setup_batch_data
        session = data["session"]
        size = data["size"]
        key_prefix = data["key_prefix"]
        
        # Create list of keys
        keys = users.ids(*[f"{key_prefix}{i}" for i in range(1, size + 1)])

        # Read headers only (no bins)
        stream = await session.query(*keys).with_no_bins().execute()

        results = await stream.collect()

        assert len(results) == size

        for i, rr in enumerate(results):
            rec = rr.record_or_raise()
            assert rec.generation != 0, f"record[{i}] generation is 0"

    async def test_batch_delete_homogeneous(self, cluster, users: DataSet):
        """
        Test batch delete operation on multiple keys.
        Test batch delete operation.
        """
        session = cluster.create_session()
        
        # Create test records
        first_key = 10000
        num_keys = 10
        keys = users.ids(*[first_key + i for i in range(num_keys)])
        
        for i, key in enumerate(keys):
            await session.upsert(key).put({"bbin": first_key + i}).execute()

        # Ensure keys exist
        exists_stream = await session.exists(*keys).include_missing_keys().execute()
        exists_results = await exists_stream.collect()
        assert len(exists_results) == num_keys
        for result in exists_results:
            assert result.as_bool() is True

        # Delete all keys using homogeneous batch delete
        delete_stream = await session.delete(*keys).include_missing_keys().execute()
        delete_results = await delete_stream.collect()
        assert len(delete_results) == num_keys

        # Ensure keys no longer exist
        exists_after_stream = await session.exists(*keys).include_missing_keys().execute()
        exists_after = await exists_after_stream.collect()
        assert len(exists_after) == num_keys
        for result in exists_after:
            assert result.as_bool() is False

    async def test_batch_exists_with_varargs(self, cluster, users: DataSet):
        """Test batch exists using varargs style."""
        session = cluster.create_session()
        
        key1 = users.id("vararg_exist_1")
        key2 = users.id("vararg_exist_2")
        key3 = users.id("vararg_exist_3")
        
        # Create some records
        await session.upsert(key1).put({"data": "1"}).execute()
        await session.upsert(key2).put({"data": "2"}).execute()
        # key3 intentionally not created

        # Check exists using varargs (include_missing_keys to include non-existent key3)
        stream = await session.exists(key1, key2, key3).include_missing_keys().execute()
        results = await stream.collect()

        assert len(results) == 3
        assert results[0].as_bool() is True   # key1 exists
        assert results[1].as_bool() is True   # key2 exists
        assert results[2].as_bool() is False  # key3 does not exist
        
        # Cleanup
        await session.delete(key1).execute()
        await session.delete(key2).execute()

    async def test_batch_delete_with_varargs(self, cluster, users: DataSet):
        """Test batch delete using varargs style."""
        session = cluster.create_session()
        
        key1 = users.id("vararg_del_1")
        key2 = users.id("vararg_del_2")
        key3 = users.id("vararg_del_3")
        
        # Create records
        await session.upsert(key1).put({"data": "1"}).execute()
        await session.upsert(key2).put({"data": "2"}).execute()
        await session.upsert(key3).put({"data": "3"}).execute()

        # Delete using varargs
        stream = await session.delete(key1, key2, key3).execute()
        results = await stream.collect()

        assert len(results) == 3

        # Verify all deleted
        exists_stream = await session.exists(key1, key2, key3).execute()
        exists_results = await exists_stream.collect()
        for result in exists_results:
            assert result.as_bool() is False


class TestRecordResultIntegration:
    """Verify RecordResult / RecordStream behavior against a live server."""

    async def test_exists_mixed_result_codes(self, cluster, users: DataSet):
        """Exists with mixed present/absent keys yields per-key result codes."""
        session = cluster.create_session()
        key_exists = users.id("rr_exists_yes")
        key_missing = users.id("rr_exists_no")

        await session.upsert(key_exists).put({"v": 1}).execute()
        try:
            await session.delete(key_missing).delete()
        except Exception:
            pass

        stream = await (
            session.exists(key_exists, key_missing)
            .include_missing_keys()
            .execute()
        )
        results = await stream.collect()

        assert len(results) == 2
        assert results[0].is_ok
        assert results[0].result_code == ResultCode.OK
        assert not results[1].is_ok
        assert results[1].result_code == ResultCode.KEY_NOT_FOUND_ERROR

        await session.delete(key_exists).execute()

    async def test_or_raise_on_not_found_result(self, cluster, users: DataSet):
        """or_raise() raises a PFC exception for a KEY_NOT_FOUND result."""
        session = cluster.create_session()
        key_exists = users.id("rr_or_raise_ok")
        key_missing = users.id("rr_or_raise_fail")

        await session.upsert(key_exists).put({"v": 1}).execute()
        try:
            await session.delete(key_missing).execute()
        except Exception:
            pass

        stream = await (
            session.exists(key_exists, key_missing)
            .include_missing_keys()
            .execute()
        )
        results = await stream.collect()

        # OK result returns self
        assert results[0].or_raise() is results[0]

        # Not-found result raises
        with pytest.raises(AerospikeError) as exc_info:
            results[1].or_raise()
        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR

        await session.delete(key_exists).execute()

    async def test_failures_filters_stream(self, cluster, users: DataSet):
        """failures() returns only non-OK results from a mixed stream."""
        session = cluster.create_session()
        key1 = users.id("rr_fail_filt_1")
        key2 = users.id("rr_fail_filt_2")
        key3 = users.id("rr_fail_filt_3")

        await session.upsert(key1).put({"v": 1}).execute()
        await session.upsert(key2).put({"v": 2}).execute()
        try:
            await session.delete(key3).execute()
        except Exception:
            pass

        stream = await (
            session.exists(key1, key2, key3)
            .include_missing_keys()
            .execute()
        )
        fails = await stream.failures()

        assert len(fails) == 1
        assert fails[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR

        await session.delete(key1).execute()
        await session.delete(key2).execute()

    async def test_first_on_query_stream(self, cluster, users: DataSet):
        """first() returns the first RecordResult from a single-key query."""
        session = cluster.create_session()
        key = users.id("rr_first")

        await session.upsert(key).put({"v": 42}).execute()

        stream = await session.query(key).execute()
        result = await stream.first()

        assert result is not None
        assert result.is_ok
        assert result.record_or_raise().bins["v"] == 42

        await session.delete(key).execute()

    async def test_first_or_raise_on_batch_query_with_missing_key(self, cluster, users: DataSet):
        """first_or_raise() raises when the first batch-query result is not OK."""
        session = cluster.create_session()
        key_missing = users.id("rr_first_or_raise_miss")

        try:
            await session.delete(key_missing).execute()
        except Exception:
            pass

        # Single-element batch is optimised to a point query; errors are
        # wrapped (not thrown) so include_missing_keys is needed to surface
        # KEY_NOT_FOUND in the stream.
        keys = users.ids("rr_first_or_raise_miss")
        stream = await session.query(keys).include_missing_keys().execute()

        with pytest.raises(AerospikeError):
            await stream.first_or_raise()

    async def test_batch_delete_returns_results_for_all_keys(self, cluster, users: DataSet):
        """Batch delete returns a RecordResult per key."""
        session = cluster.create_session()
        keys = users.ids(*[f"rr_del_{i}" for i in range(3)])

        for key in keys:
            await session.upsert(key).put({"v": 1}).execute()

        stream = await session.delete(*keys).execute()
        results = await stream.collect()

        assert len(results) == 3
        for r in results:
            assert r.is_ok


class TestBatchExpressionOps:
    """Test batch operations with expression reads and writes."""

    @requires_server_compiled_ael
    async def test_batch_upsert_from(self, cluster, users: DataSet):
        """upsert_from across multiple batch keys."""
        session = cluster.create_session()
        keys = [users.id(f"bexp_{i}") for i in range(3)]

        for i, key in enumerate(keys):
            await session.upsert(key).put({"A": (i + 1) * 10}).execute()

        stream = await (
            session.upsert(keys[0]).bin("C").upsert_from("$.A + 1")
            .upsert(keys[1]).bin("C").upsert_from("$.A + 1")
            .upsert(keys[2]).bin("C").upsert_from("$.A + 1")
            .execute()
        )
        results = await stream.collect()
        assert len(results) == 3
        for r in results:
            assert r.is_ok

        for i, key in enumerate(keys):
            rs = await session.query(key).bin("C").get().execute()
            rec = await rs.first_or_raise()
            assert rec.record.bins["C"] == (i + 1) * 10 + 1

    @requires_server_compiled_ael
    async def test_batch_select_from(self, cluster, users: DataSet):
        """select_from (expression read) in batch context."""
        session = cluster.create_session()
        keys = [users.id(f"bexp_sel_{i}") for i in range(2)]

        await session.upsert(keys[0]).put({"A": 5, "B": 3}).execute()
        await session.upsert(keys[1]).put({"A": 10, "B": 7}).execute()

        stream = await (
            session.query(keys[0]).bin("sum").select_from("$.A:INT + $.B:INT")
            .query(keys[1]).bin("sum").select_from("$.A:INT + $.B:INT")
            .execute()
        )
        results = await stream.collect()
        assert len(results) == 2
        assert results[0].record.bins["sum"] == 8
        assert results[1].record.bins["sum"] == 17

    @requires_server_compiled_ael
    async def test_batch_mixed_set_to_and_expression(
        self, cluster, users: DataSet,
    ):
        """set_to + upsert_from on same key in batch."""
        session = cluster.create_session()
        key = users.id("bexp_mixed")

        await session.upsert(key).put({"A": 10}).execute()

        stream = await (
            session.upsert(key)
            .bin("tag").set_to("done")
            .bin("doubled").upsert_from("$.A * 2")
            .execute()
        )
        results = await stream.collect()
        assert len(results) == 1
        assert results[0].is_ok

        rs = await session.query(key).bin("tag").get().bin("doubled").get().execute()
        rec = await rs.first_or_raise()
        assert rec.record.bins["tag"] == "done"
        assert rec.record.bins["doubled"] == 20


class TestBatchStream:
    """Lazy `stream()` — completion-order yields, no
    writes-complete-on-return guarantee. Mixed ops in one call."""

    @pytest_asyncio.fixture
    async def track_key(self, cluster):
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
                await session.delete(k).execute()
            except Exception:
                pass

    @requires_server_compiled_ael
    async def test_stream_mixed_ops_yields_all(
        self, cluster, users: DataSet, track_key,
    ):
        """Mixed writes + AEL read + delete in one streaming batch.

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
        keys = [track_key(users.id(f"estream_mix_{i}")) for i in range(4)]
        for i, k in enumerate(keys):
            await session.upsert(k).put({"A": i, "B": i * 2}).execute()

        stream = await (
            session.upsert(keys[0]).bin("A").set_to(99)
            .query(keys[1]).bin("sum").select_from("$.A:INT + $.B:INT")
            .query(keys[2]).bin("sum").select_from("$.A:INT + $.B:INT")
            .delete(keys[3])
            .stream()
        )
        results = await stream.collect()
        assert len(results) == 4
        assert {r.index for r in results} == {0, 1, 2, 3}

        by_idx = {r.index: r for r in results}
        for r in results:
            assert r.is_ok

        # In-stream value checks: the two select_from ops carry the
        # computed `sum` bin on their BatchRecord even though the persistent
        # record won't have it (verified below).
        # keys[1]: A=1, B=2 → 1+2=3
        # keys[2]: A=2, B=4 → 2+4=6
        assert by_idx[1].record.bins["sum"] == 3
        assert by_idx[2].record.bins["sum"] == 6

        # Persisted state checks:
        # (write) keys[0]: bin A flipped from 0 → 99; B unchanged.
        rec0 = await (await session.query(keys[0]).execute()).first_or_raise()
        assert rec0.record.bins["A"] == 99
        assert rec0.record.bins["B"] == 0

        # (read) keys[1] / keys[2]: `select_from` is a read — the original
        # bins are untouched, and `sum` is NOT persisted.
        rec1 = await (await session.query(keys[1]).execute()).first_or_raise()
        assert rec1.record.bins == {"A": 1, "B": 2}
        rec2 = await (await session.query(keys[2]).execute()).first_or_raise()
        assert rec2.record.bins == {"A": 2, "B": 4}

        # (delete) keys[3]: gone.
        empty = await (await session.query(keys[3]).execute()).collect()
        assert empty == []

    @requires_server_compiled_ael
    async def test_stream_read_only_ops_dispatch_as_reads(
        self, cluster, users: DataSet, track_key,
    ):
        """AEL select_from under the read verb dispatches as BatchReadOp on
        the wire so the server accepts it, even in a lazy write-batch
        stream. Also verifies the persisted record was NOT mutated
        (select_from is a read; if it landed as a write, the `sum` bin
        would persist)."""
        session = cluster.create_session()
        keys = [track_key(users.id(f"estream_ro_{i}")) for i in range(2)]
        for i, k in enumerate(keys):
            await session.upsert(k).put({"A": 5 + i, "B": 3}).execute()

        stream = await (
            session.query(keys[0]).bin("sum").select_from("$.A:INT + $.B:INT")
            .query(keys[1]).bin("sum").select_from("$.A:INT + $.B:INT")
            .stream()
        )
        results = await stream.collect()
        assert len(results) == 2
        results.sort(key=lambda r: r.index)
        assert results[0].record.bins["sum"] == 8  # 5 + 3
        assert results[1].record.bins["sum"] == 9  # 6 + 3

        # Persisted state: `sum` should NOT be on disk — select_from is read.
        rec0 = await (await session.query(keys[0]).execute()).first_or_raise()
        assert rec0.record.bins == {"A": 5, "B": 3}
        rec1 = await (await session.query(keys[1]).execute()).first_or_raise()
        assert rec1.record.bins == {"A": 6, "B": 3}


class TestBatchStreamClose:
    """Closing a lazy WRITE-batch stream (multi-key write chain) releases
    the producer and stops iteration. Early abandon, idempotent close,
    close-on-exception via ``async with``, re-iterate, and
    client-usable-after-close — asserted against a cluster. Complements the
    read-batch stream close coverage in ``query_test.py``."""

    @pytest_asyncio.fixture
    async def track_key(self, cluster):
        session = cluster.create_session()
        created: list = []

        def track(key):
            created.append(key)
            return key

        yield track

        for k in created:
            try:
                await session.delete(k).execute()
            except Exception:
                pass

    async def _seed(self, session, users, track_key, n):
        keys = [track_key(users.id(f"estream_close_{i}")) for i in range(n)]
        for i, k in enumerate(keys):
            await session.upsert(k).put({"v": i}).execute()
        return keys

    def _write_batch(self, session, keys):
        """Build a multi-key write chain (one upsert segment per key)."""
        b = session.upsert(keys[0]).put({"v": 0})
        for i, k in enumerate(keys[1:], start=1):
            b = b.upsert(k).put({"v": i})
        return b

    async def test_close_mid_stream_stops_iteration(
        self, cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = await self._seed(session, users, track_key, 10)

        stream = await self._write_batch(session, keys).stream()
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

    async def test_close_is_idempotent(
        self, cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = await self._seed(session, users, track_key, 3)
        stream = await self._write_batch(session, keys).stream()
        stream.close()
        stream.close()
        stream.close()
        assert await stream.collect() == []

    async def test_reiterate_after_close_yields_nothing(
        self, cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = await self._seed(session, users, track_key, 4)
        stream = await self._write_batch(session, keys).stream()
        stream.close()
        assert [r async for r in stream] == []
        assert [r async for r in stream] == []

    async def test_client_usable_after_early_close(
        self, cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = await self._seed(session, users, track_key, 10)
        stream = await self._write_batch(session, keys).stream()
        async for _ in stream:
            stream.close()
            break
        rec = await (await session.query(keys[0]).execute()).first_or_raise()
        assert rec.record.bins["v"] == 0

    async def test_async_with_closes_on_normal_exit(
        self, cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = await self._seed(session, users, track_key, 3)
        seen = 0
        async with (await self._write_batch(session, keys).stream()) as stream:
            async for _ in stream:
                seen += 1
        assert seen == 3
        assert await stream.collect() == []

    async def test_async_with_closes_on_early_break(
        self, cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = await self._seed(session, users, track_key, 10)
        async with (await self._write_batch(session, keys).stream()) as stream:
            async for _ in stream:
                break
        assert await stream.collect() == []

    async def test_async_with_closes_on_exception(
        self, cluster, users: DataSet, track_key,
    ):
        session = cluster.create_session()
        keys = await self._seed(session, users, track_key, 10)
        stream_ref = {}
        with pytest.raises(RuntimeError, match="boom"):
            async with (await self._write_batch(session, keys).stream()) as stream:
                stream_ref["s"] = stream
                async for _ in stream:
                    raise RuntimeError("boom")
        assert await stream_ref["s"].collect() == []
        rec = await (await session.query(keys[1]).execute()).first_or_raise()
        assert rec.record.bins["v"] == 1


class TestBatchVerbExistenceEnforcement:
    """Verbs (``insert``/``update``/``replace``/``replace_if_exists``) carry a
    per-key :class:`BatchWritePolicy` with the right
    :class:`RecordExistsAction`, so the server enforces the verb's
    existence constraint on the wire. ``upsert`` is the default (no
    enforcement) and exists as the always-succeeds variant."""

    @pytest_asyncio.fixture
    async def track_key(self, cluster):
        session = cluster.create_session()
        created: list = []
        def track(key):
            created.append(key)
            return key
        yield track
        for k in created:
            try:
                await session.delete(k).execute()
            except Exception:
                pass

    async def test_update_nonexistent_returns_key_not_found(
        self, cluster, users: DataSet, track_key,
    ):
        """``update`` against a non-existent key must surface KEY_NOT_FOUND
        — not silently upsert. A control upsert keeps the chain multi-key
        so it dispatches as a batch."""
        session = cluster.create_session()
        control = track_key(users.id("verb_update_control"))
        missing = track_key(users.id("verb_update_missing"))
        stream = await (
            session.upsert(control).bin("v").set_to(0)
            .update(missing).bin("v").set_to(1)
            .include_missing_keys()
            .execute()
        )
        results = await stream.collect()
        assert len(results) == 2
        missing_r = [r for r in results if r.key == missing][0]
        assert not missing_r.is_ok
        assert missing_r.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    async def test_insert_existing_returns_key_exists(
        self, cluster, users: DataSet, track_key,
    ):
        """``insert`` against an existing key must surface KEY_EXISTS
        — not silently upsert."""
        session = cluster.create_session()
        control = track_key(users.id("verb_insert_control"))
        existing = track_key(users.id("verb_insert_existing"))
        await session.upsert(existing).put({"v": 1}).execute()

        stream = await (
            session.upsert(control).bin("v").set_to(0)
            .insert(existing).bin("v").set_to(2)
            .execute()
        )
        results = await stream.collect()
        assert len(results) == 2
        existing_r = [r for r in results if r.key == existing][0]
        assert not existing_r.is_ok
        assert existing_r.result_code == ResultCode.KEY_EXISTS_ERROR

    async def test_replace_if_exists_nonexistent_returns_key_not_found(
        self, cluster, users: DataSet, track_key,
    ):
        """``replace_if_exists`` against a non-existent key surfaces KEY_NOT_FOUND."""
        session = cluster.create_session()
        control = track_key(users.id("verb_replace_if_exists_control"))
        missing = track_key(users.id("verb_replace_if_exists_missing"))
        stream = await (
            session.upsert(control).bin("v").set_to(0)
            .replace_if_exists(missing).bin("v").set_to(1)
            .include_missing_keys()
            .execute()
        )
        results = await stream.collect()
        assert len(results) == 2
        missing_r = [r for r in results if r.key == missing][0]
        assert not missing_r.is_ok
        assert missing_r.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    async def test_upsert_against_nonexistent_succeeds(
        self, cluster, users: DataSet, track_key,
    ):
        """``upsert`` is the no-enforcement verb — succeeds on either side."""
        session = cluster.create_session()
        keys = [
            track_key(users.id("verb_upsert_missing_1")),
            track_key(users.id("verb_upsert_missing_2")),
        ]
        stream = await (session.upsert(keys).bin("v").set_to(1).execute())
        results = await stream.collect()
        assert len(results) == 2
        for r in results:
            assert r.is_ok

    async def test_stream_enforces_verbs(
        self, cluster, users: DataSet, track_key,
    ):
        """Streaming path enforces verb existence semantics the same as
        the buffered path."""
        session = cluster.create_session()
        missing = [
            track_key(users.id("verb_stream_missing_1")),
            track_key(users.id("verb_stream_missing_2")),
        ]
        stream = await (
            session.update(missing).bin("v").set_to(1).stream()
        )
        results = await stream.collect()
        assert len(results) == 2
        for r in results:
            assert not r.is_ok
            assert r.result_code == ResultCode.KEY_NOT_FOUND_ERROR


class TestBatchGeneration:
    """Generation policy on batch delete + write (``BatchDelete/WritePolicy``).

    The single-key generation contract lives in ``generation_test.py``; these
    exercise the batch sub-policy path — an expected generation becomes a
    per-row CAS check carried on the batch write/delete policy.
    """

    async def test_batch_delete_matching_generation_deletes_all(self, cluster, users: DataSet):
        session = cluster.create_session()
        k1, k2 = users.id("del_gen_ok_1"), users.id("del_gen_ok_2")
        await session.upsert(k1).put({"n": 1}).execute()
        await session.upsert(k2).put({"n": 2}).execute()
        gen1 = (await (await session.query(k1).execute()).first_or_raise()).record.generation
        gen2 = (await (await session.query(k2).execute()).first_or_raise()).record.generation
        assert gen1 == gen2  # freshly seeded once

        stream = await session.delete(k1, k2).ensure_generation_is(gen1).execute()
        assert all(rr.is_ok for rr in [rr async for rr in stream])
        for k in (k1, k2):
            assert [rr async for rr in await session.query(k).execute()] == []

    async def test_batch_delete_wrong_generation_reports_error(self, cluster, users: DataSet):
        session = cluster.create_session()
        k1, k2 = users.id("del_gen_bad_1"), users.id("del_gen_bad_2")
        await session.upsert(k1).put({"n": 1}).execute()
        await session.upsert(k2).put({"n": 2}).execute()

        stream = await (
            session.delete(k1, k2).ensure_generation_is(9999).include_missing_keys().execute()
        )
        results = {rr.key.value: rr async for rr in stream}
        assert results["del_gen_bad_1"].result_code == ResultCode.GENERATION_ERROR
        assert results["del_gen_bad_2"].result_code == ResultCode.GENERATION_ERROR
        for k in (k1, k2):
            assert len([rr async for rr in await session.query(k).execute()]) == 1

    async def test_batch_write_wrong_generation_reports_error(self, cluster, users: DataSet):
        session = cluster.create_session()
        k1, k2 = users.id("wr_gen_bad_1"), users.id("wr_gen_bad_2")
        await session.upsert(k1).put({"n": 1}).execute()
        await session.upsert(k2).put({"n": 2}).execute()

        stream = await (
            session.update(k1).put({"n": 10}).ensure_generation_is(9999)
            .update(k2).put({"n": 20}).ensure_generation_is(9999)
            .execute()
        )
        results = {rr.key.value: rr async for rr in stream}
        assert results["wr_gen_bad_1"].result_code == ResultCode.GENERATION_ERROR
        assert results["wr_gen_bad_2"].result_code == ResultCode.GENERATION_ERROR
        r1 = await (await session.query(k1).execute()).first_or_raise()
        assert r1.record.bins.get("n") == 1

    async def test_batch_write_matching_generation_writes(self, cluster, users: DataSet):
        session = cluster.create_session()
        k1, k2 = users.id("wr_gen_ok_1"), users.id("wr_gen_ok_2")
        await session.upsert(k1).put({"n": 1}).execute()
        await session.upsert(k2).put({"n": 2}).execute()
        gen = (await (await session.query(k1).execute()).first_or_raise()).record.generation

        stream = await (
            session.update(k1).put({"n": 10}).ensure_generation_is(gen)
            .update(k2).put({"n": 20}).ensure_generation_is(gen)
            .execute()
        )
        assert all(rr.is_ok for rr in [rr async for rr in stream])
        r1 = await (await session.query(k1).execute()).first_or_raise()
        assert r1.record.bins.get("n") == 10


class TestSameKeyChainOrdering:
    """A key spanning chain segments must observe the earlier segments' writes.

    Batch sub-transactions against one key are unordered server-side, so a
    chain that writes a key and then reads it back cannot fold into a single
    batch — the read would race its own write and miss it, and the resulting
    not-found row is dropped from the stream, leaving only a short result.
    """

    async def test_read_segment_sees_write_from_earlier_segment(
        self, cluster, users: DataSet,
    ):
        session = cluster.create_session()
        k = users.id("chain_same_key_rw")
        await session.delete(k).execute()

        stream = await (
            session.upsert(k).put({"seed": "new"})
            .query(k).bins(["seed"])
            .execute()
        )
        rows = await stream.collect()

        # One row per segment, in order, with nothing dropped.
        assert len(rows) == 2
        assert [r.result_code for r in rows] == [ResultCode.OK, ResultCode.OK]
        # The read observed the write issued earlier in the same chain.
        assert rows[1].record.bins["seed"] == "new"

        # Persisted state, read back through a separate chain.
        after = await (
            await session.query(k).bins(["seed"]).execute()
        ).first_or_raise()
        assert after.record.bins["seed"] == "new"


class TestBatchWriteMissingKeyRows:
    """A batch write reports a missing key with no opt-in required.

    The verb names a key it expects to exist, so dropping the row would report
    success by omission: the caller sees a shorter stream and no error. Read
    rows stay opt-in, where a missing key is an ordinary outcome.
    """

    async def test_update_missing_key_reports_row_without_opt_in(
        self, cluster, users: DataSet,
    ):
        session = cluster.create_session()
        present, missing = users.id("wmk_present"), users.id("wmk_missing")
        await session.upsert(present).put({"v": 1}).execute()
        await session.delete(missing).execute()

        stream = await (
            session.update(present).bin("v").set_to(2)
            .update(missing).bin("v").set_to(2)
            .execute()
        )
        rows = {r.key.value: r for r in await stream.collect()}

        # Default disposition, no include_missing_keys: both rows are present.
        assert len(rows) == 2
        assert rows["wmk_present"].result_code == ResultCode.OK
        assert rows["wmk_missing"].result_code == ResultCode.KEY_NOT_FOUND_ERROR

        # The applied half persisted; the missing key was not created.
        after = await (await session.query(present).bins(["v"]).execute()).first_or_raise()
        assert after.record.bins["v"] == 2
        assert [r async for r in await session.query(missing).execute()] == []


class TestBatchFilterExpression:
    """Filter expressions carried on a multi-key (batch) operation.

    The single-key filter tests exercise a different dispatch path; nothing
    covered a filter that fans out across a batch. A filter travels with each
    row here rather than once for the whole batch, so every row carries its own
    copy and is judged independently.

    Reporting differs by verb, which is the part easiest to regress: a
    filtered-out *read* is dropped from the stream entirely, while a
    filtered-out *write* or *delete* comes back as a ``FILTERED_OUT`` row.
    """

    @staticmethod
    async def _seed(session, users: DataSet, prefix: str, values: dict):
        keys = {name: users.id(f"{prefix}_{name}") for name in values}
        for name, key in keys.items():
            await session.upsert(key).put({"v": values[name]}).execute()
        return keys

    @requires_server_compiled_ael
    async def test_batch_read_returns_only_matching_rows(self, cluster, users: DataSet):
        session = cluster.create_session()
        keys = await self._seed(session, users, "bfr", {"lo": 1, "hi": 9})

        stream = await session.query(list(keys.values())).where("$.v >= 5").execute()
        rows = await stream.collect()

        # A filtered-out read is dropped rather than reported.
        assert [r.key.value for r in rows] == [keys["hi"].value]
        assert rows[0].record.bins["v"] == 9

    @requires_server_compiled_ael
    async def test_batch_write_applies_only_to_matching_rows(self, cluster, users: DataSet):
        session = cluster.create_session()
        keys = await self._seed(session, users, "bfw", {"lo": 1, "hi": 9})

        stream = await (
            session.upsert(list(keys.values()))
            .bin("tagged").set_to(True)
            .where("$.v >= 5")
            .execute()
        )
        by_key = {r.key.value: r for r in await stream.collect()}

        assert by_key[keys["hi"].value].is_ok
        assert by_key[keys["lo"].value].result_code == ResultCode.FILTERED_OUT

        # The write reached the match and only the match.
        for name, expected in (("hi", True), ("lo", None)):
            rs = await session.query(keys[name]).execute()
            rec = await rs.first_or_raise()
            assert rec.record.bins.get("tagged") is expected

    @requires_server_compiled_ael
    async def test_batch_delete_removes_only_matching_rows(self, cluster, users: DataSet):
        session = cluster.create_session()
        keys = await self._seed(session, users, "bfd", {"lo": 1, "hi": 9})

        stream = await session.delete(list(keys.values())).where("$.v >= 5").execute()
        by_key = {r.key.value: r for r in await stream.collect()}

        assert by_key[keys["hi"].value].is_ok
        assert by_key[keys["lo"].value].result_code == ResultCode.FILTERED_OUT

        stream = await session.exists(list(keys.values())).include_missing_keys().execute()
        present = {r.key.value: r.as_bool() for r in await stream.collect()}
        assert present[keys["hi"].value] is False
        assert present[keys["lo"].value] is True

    @requires_server_compiled_ael
    async def test_batch_survives_a_long_filter_expression(self, cluster, users: DataSet):
        """A long filter must survive being repeated across every row.

        Each row carries its own copy of the expression, so a long filter
        multiplies the request size by the row count -- the shape where a
        length or offset mistake in encoding would show up first.
        """
        session = cluster.create_session()
        keys = await self._seed(session, users, "bflong", {"lo": 1, "hi": 9})

        # Semantically "$.v >= 5", padded with redundant terms to lengthen the
        # encoded expression without changing which records it selects.
        padding = " and ".join(f"$.v != {n}" for n in range(100, 140))
        long_filter = f"$.v >= 5 and {padding}"

        stream = await session.query(list(keys.values())).where(long_filter).execute()
        rows = await stream.collect()

        assert [r.key.value for r in rows] == [keys["hi"].value]
        assert rows[0].record.bins["v"] == 9


def _batch_rows_by_key(results):
    """Index batch rows by ``(namespace, user key)``.

    Folded batch responses are not ordered by segment order on every server
    build; match the invalid-filter row by key instead of list position.
    """
    return {(r.key.namespace, r.key.value): r for r in results}


def _batch_row(rows, key):
    ident = (key.namespace, key.value)
    assert ident in rows, (
        f"no row for {key.value!r}; returned rows: "
        + ", ".join(f"{k[1]!r}={v.result_code}" for k, v in rows.items())
    )
    return rows[ident]


def _invalid_filter_expression() -> Exp:
    return Exp.from_base64(base64.b64encode(bytes([0xFF, 0xFE, 0xFD])).decode())


def _assert_batch_invalid_filter_error(res, *, supports_detail: bool) -> None:
    assert not res.is_ok, (
        f"expected batch filter build failure for {res.key.value!r}, got {res.result_code}"
    )
    assert res.result_code == ResultCode.PARAMETER_ERROR
    assert res.sub_code in (None, 0)
    if supports_detail:
        assert res.server_message is not None
        assert res.exp_trace is not None


class TestBatchInvalidFilterError:
    """Batch row errors for invalid filter expressions (field 45 extended detail)."""

    @staticmethod
    def _verbose_session(cluster):
        behavior = Behavior(
            "batch-invalid-filter",
            {Scope.ALL: Settings(error_detail_verbosity=ErrorDetailVerbosity.EXPRESSION_TRACE)},
        )
        return cluster.create_session(behavior=behavior)

    @requires_server_compiled_ael
    async def test_batch_read_mixed_expressions_invalid_row_returns_parameter_error(
        self, cluster, users: DataSet, supports_error_detail,
    ):
        session = self._verbose_session(cluster)
        k_ok = users.id("bif_ok")
        k_bad = users.id("bif_bad")
        await session.upsert(k_ok).put({"v": 1}).execute()
        await session.upsert(k_bad).put({"v": 2}).execute()

        stream = await (
            session.query(k_ok).where(Exp.bin_exists("v"))
            .query(k_bad).where(_invalid_filter_expression())
            .include_missing_keys()
            .execute()
        )
        results = await stream.collect()

        assert len(results) == 2
        rows = _batch_rows_by_key(results)
        assert _batch_row(rows, k_ok).is_ok
        _assert_batch_invalid_filter_error(
            _batch_row(rows, k_bad), supports_detail=supports_error_detail,
        )

    @requires_server_compiled_ael
    async def test_batch_read_with_invalid_expression_returns_parameter_error(
        self, cluster, users: DataSet, supports_error_detail,
    ):
        session = self._verbose_session(cluster)
        keys = [users.id(f"bif_{i}") for i in (1, 2)]
        for key in keys:
            await session.upsert(key).put({"v": 1}).execute()

        stream = await (
            session.query(keys)
            .where(_invalid_filter_expression())
            .include_missing_keys()
            .execute()
        )
        results = await stream.collect()

        assert len(results) == 2
        rows = _batch_rows_by_key(results)
        for key in keys:
            _assert_batch_invalid_filter_error(
                _batch_row(rows, key), supports_detail=supports_error_detail,
            )
