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
1. Heterogeneous batch operations (different ops on different keys) - session.batch()
2. Homogeneous batch operations (same op on multiple keys) - session.exists/delete/query with multiple keys
3. RecordResult/RecordStream integration (result codes, or_raise, failures, first)
"""

import pytest
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import AerospikeError


@pytest.fixture
def users():
    """DataSet fixture for batch tests."""
    return DataSet.of("test", "batch_test")


class TestBatchOperations:
    """Test batch operation builder with multi-key chaining."""

    async def test_batch_insert_multiple_keys(self, client: Client, users: DataSet):
        """Test inserting multiple records in a single batch."""
        session = client.create_session()
        
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
            session.batch()
                .insert(key1)
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

    async def test_batch_mixed_operations(self, client: Client, users: DataSet):
        """Test batch with mixed insert, update, and delete operations."""
        session = client.create_session()
        
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
            session.batch()
                .update(key1).bin("counter").add(5)
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
        exists_stream = await session.exists(key2).respond_all_keys().execute()
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

    async def test_batch_upsert_operations(self, client: Client, users: DataSet):
        """Test batch upsert operations."""
        session = client.create_session()
        
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
            session.batch()
                .upsert(key1).bin("value").set_to("initial1")
                .upsert(key2).bin("value").set_to("initial2")
                .execute()
        )
        
        # Verify initial values
        rs1 = await session.query(key1).execute()
        record1 = await rs1.first_or_raise()
        assert record1.record.bins["value"] == "initial1"

        # Second batch: update existing records (upsert)
        await (
            session.batch()
                .upsert(key1).bin("value").set_to("updated1")
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

    async def test_batch_delete_multiple_keys(self, client: Client, users: DataSet):
        """Test deleting multiple records in a single batch."""
        session = client.create_session()
        
        key1 = users.id("batch_del_1")
        key2 = users.id("batch_del_2")
        key3 = users.id("batch_del_3")
        
        # Setup: create records
        await session.upsert(key1).put({"data": "1"}).execute()
        await session.upsert(key2).put({"data": "2"}).execute()
        await session.upsert(key3).put({"data": "3"}).execute()

        # Delete all in one batch
        stream = await (
            session.batch()
                .delete(key1)
                .delete(key2)
                .delete(key3)
                .execute()
        )
        results = await stream.collect()

        assert len(results) == 3
        
        # Verify all deleted
        for k in (key1, key2, key3):
            exists_stream = await session.exists(k).respond_all_keys().execute()
            result = await exists_stream.first()
            assert result is not None and result.as_bool() is False

    async def test_batch_empty_raises_error(self, client: Client):
        """Test that executing an empty batch raises an error."""
        session = client.create_session()
        
        with pytest.raises(ValueError, match="No operations to execute"):
            await session.batch().execute()

    async def test_batch_bin_string_operations(self, client: Client, users: DataSet):
        """Test batch with string bin operations (append/prepend)."""
        session = client.create_session()
        
        key1 = users.id("batch_str_1")
        key2 = users.id("batch_str_2")
        
        # Setup
        await session.upsert(key1).put({"message": "Hello"}).execute()
        await session.upsert(key2).put({"message": "World"}).execute()

        # Append and prepend in batch
        await (
            session.batch()
                .update(key1).bin("message").append(" World")
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
    async def setup_batch_data(self, client: Client, users: DataSet):
        """Setup test data for batch operations."""
        session = client.create_session()
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

    async def test_batch_exists_homogeneous(
        self, client: Client, users: DataSet, setup_batch_data
    ):
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
        stream = await session.exists(*keys).respond_all_keys().execute()
        results = await stream.collect()

        assert len(results) == size
        for i, result in enumerate(results):
            assert result.as_bool() is True, f"exists[{i}] is False"

    async def test_batch_reads_homogeneous(
        self, client: Client, users: DataSet, setup_batch_data
    ):
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

    async def test_batch_read_headers_homogeneous(
        self, client: Client, users: DataSet, setup_batch_data
    ):
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

    async def test_batch_delete_homogeneous(
        self, client: Client, users: DataSet
    ):
        """
        Test batch delete operation on multiple keys.
        Test batch delete operation.
        """
        session = client.create_session()
        
        # Create test records
        first_key = 10000
        num_keys = 10
        keys = users.ids(*[first_key + i for i in range(num_keys)])
        
        for i, key in enumerate(keys):
            await session.upsert(key).put({"bbin": first_key + i}).execute()

        # Ensure keys exist
        exists_stream = await session.exists(*keys).respond_all_keys().execute()
        exists_results = await exists_stream.collect()
        assert len(exists_results) == num_keys
        for result in exists_results:
            assert result.as_bool() is True

        # Delete all keys using homogeneous batch delete
        delete_stream = await session.delete(*keys).respond_all_keys().execute()
        delete_results = await delete_stream.collect()
        assert len(delete_results) == num_keys

        # Ensure keys no longer exist
        exists_after_stream = await session.exists(*keys).respond_all_keys().execute()
        exists_after = await exists_after_stream.collect()
        assert len(exists_after) == num_keys
        for result in exists_after:
            assert result.as_bool() is False

    async def test_batch_exists_with_varargs(
        self, client: Client, users: DataSet
    ):
        """Test batch exists using varargs style."""
        session = client.create_session()
        
        key1 = users.id("vararg_exist_1")
        key2 = users.id("vararg_exist_2")
        key3 = users.id("vararg_exist_3")
        
        # Create some records
        await session.upsert(key1).put({"data": "1"}).execute()
        await session.upsert(key2).put({"data": "2"}).execute()
        # key3 intentionally not created

        # Check exists using varargs (respond_all_keys to include non-existent key3)
        stream = await session.exists(key1, key2, key3).respond_all_keys().execute()
        results = await stream.collect()

        assert len(results) == 3
        assert results[0].as_bool() is True   # key1 exists
        assert results[1].as_bool() is True   # key2 exists
        assert results[2].as_bool() is False  # key3 does not exist
        
        # Cleanup
        await session.delete(key1).execute()
        await session.delete(key2).execute()

    async def test_batch_delete_with_varargs(
        self, client: Client, users: DataSet
    ):
        """Test batch delete using varargs style."""
        session = client.create_session()
        
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

    async def test_exists_mixed_result_codes(
        self, client: Client, users: DataSet
    ):
        """Exists with mixed present/absent keys yields per-key result codes."""
        session = client.create_session()
        key_exists = users.id("rr_exists_yes")
        key_missing = users.id("rr_exists_no")

        await session.upsert(key_exists).put({"v": 1}).execute()
        try:
            await session.delete(key_missing).delete()
        except Exception:
            pass

        stream = await (
            session.exists(key_exists, key_missing)
                .respond_all_keys()
                .execute()
        )
        results = await stream.collect()

        assert len(results) == 2
        assert results[0].is_ok
        assert results[0].result_code == ResultCode.OK
        assert not results[1].is_ok
        assert results[1].result_code == ResultCode.KEY_NOT_FOUND_ERROR

        await session.delete(key_exists).execute()

    async def test_or_raise_on_not_found_result(
        self, client: Client, users: DataSet
    ):
        """or_raise() raises a PFC exception for a KEY_NOT_FOUND result."""
        session = client.create_session()
        key_exists = users.id("rr_or_raise_ok")
        key_missing = users.id("rr_or_raise_fail")

        await session.upsert(key_exists).put({"v": 1}).execute()
        try:
            await session.delete(key_missing).execute()
        except Exception:
            pass

        stream = await (
            session.exists(key_exists, key_missing)
                .respond_all_keys()
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

    async def test_failures_filters_stream(
        self, client: Client, users: DataSet
    ):
        """failures() returns only non-OK results from a mixed stream."""
        session = client.create_session()
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
                .respond_all_keys()
                .execute()
        )
        fails = await stream.failures()

        assert len(fails) == 1
        assert fails[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR

        await session.delete(key1).execute()
        await session.delete(key2).execute()

    async def test_first_on_query_stream(
        self, client: Client, users: DataSet
    ):
        """first() returns the first RecordResult from a single-key query."""
        session = client.create_session()
        key = users.id("rr_first")

        await session.upsert(key).put({"v": 42}).execute()

        stream = await session.query(key).execute()
        result = await stream.first()

        assert result is not None
        assert result.is_ok
        assert result.record_or_raise().bins["v"] == 42

        await session.delete(key).execute()

    async def test_first_or_raise_on_batch_query_with_missing_key(
        self, client: Client, users: DataSet
    ):
        """first_or_raise() raises when the first batch-query result is not OK."""
        session = client.create_session()
        key_missing = users.id("rr_first_or_raise_miss")

        try:
            await session.delete(key_missing).execute()
        except Exception:
            pass

        # Single-element batch is optimised to a point query; errors are
        # wrapped (not thrown) so respond_all_keys is needed to surface
        # KEY_NOT_FOUND in the stream.
        keys = users.ids("rr_first_or_raise_miss")
        stream = await session.query(keys).respond_all_keys().execute()

        with pytest.raises(AerospikeError):
            await stream.first_or_raise()

    async def test_batch_delete_returns_results_for_all_keys(
        self, client: Client, users: DataSet
    ):
        """Batch delete returns a RecordResult per key."""
        session = client.create_session()
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

    async def test_batch_upsert_from(self, client: Client, users: DataSet):
        """upsert_from across multiple batch keys."""
        session = client.create_session()
        keys = [users.id(f"bexp_{i}") for i in range(3)]

        for i, key in enumerate(keys):
            await session.upsert(key).put({"A": (i + 1) * 10}).execute()

        stream = await (
            session.batch()
                .upsert(keys[0]).bin("C").upsert_from("$.A + 1")
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

    async def test_batch_select_from(self, client: Client, users: DataSet):
        """select_from (expression read) in batch context."""
        session = client.create_session()
        keys = [users.id(f"bexp_sel_{i}") for i in range(2)]

        await session.upsert(keys[0]).put({"A": 5, "B": 3}).execute()
        await session.upsert(keys[1]).put({"A": 10, "B": 7}).execute()

        stream = await (
            session.batch()
                .update(keys[0]).bin("sum").select_from("$.A + $.B")
                .update(keys[1]).bin("sum").select_from("$.A + $.B")
                .execute()
        )
        results = await stream.collect()
        assert len(results) == 2
        assert results[0].record.bins["sum"] == 8
        assert results[1].record.bins["sum"] == 17

    async def test_batch_mixed_set_to_and_expression(
        self, client: Client, users: DataSet,
    ):
        """set_to + upsert_from on same key in batch."""
        session = client.create_session()
        key = users.id("bexp_mixed")

        await session.upsert(key).put({"A": 10}).execute()

        stream = await (
            session.batch()
                .upsert(key)
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
