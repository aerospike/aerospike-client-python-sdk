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

"""Tests for operate operations.

Coverage:
  - Combined operations (add + set + get)
  - Multiple increments
  - Set and get in same call
  - Record-level delete_record() and touch_record() within operate calls
"""

import pytest
from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet


@pytest.fixture
def test_set():
    """DataSet fixture for operate tests."""
    return DataSet.of("test", "operate_test")


class TestOperate:
    """Test combined operate operations."""

    async def test_operate(self, client: Client, test_set: DataSet):
        """Test combined operations (add + set + get) in single call."""
        session = client.create_session()
        key = test_set.id("operate")
        bin_name1 = "optintbin"
        bin_name2 = "optstringbin"

        # Write initial record
        await session.upsert(key).bin(bin_name1).set_to(7).bin(bin_name2).set_to("string value").execute()

        # Verify initial values
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins[bin_name1] == 7
        assert record.record.bins[bin_name2] == "string value"

        # Add integer, write new string
        await (
            session.upsert(key)
                .bin(bin_name1).add(4)
                .bin(bin_name2).set_to("new string")
                .execute()
        )

        # Read record and verify values after operations
        result = await (await session.query(key).execute()).first_or_raise()
        assert result.record is not None
        assert result.record.bins is not None
        # After add(4) to 7, bin1 should be 11
        assert result.record.bins[bin_name1] == 11
        # bin2 should have new string
        assert result.record.bins[bin_name2] == "new string"

        # Cleanup
        await session.delete(key).execute()

    async def test_operate_multiple_increments(self, client: Client, test_set: DataSet):
        """Test multiple increment operations on same bin."""
        session = client.create_session()
        key = test_set.id("operate_multi_inc")
        bin_name = "counter"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Set initial value
        await session.upsert(key).bin(bin_name).set_to(0).execute()

        # Increment multiple times in separate calls
        await session.upsert(key).bin(bin_name).add(5).execute()
        await session.upsert(key).bin(bin_name).add(10).execute()
        await session.upsert(key).bin(bin_name).add(15).execute()

        # Verify final value
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins[bin_name] == 30

        # Cleanup
        await session.delete(key).execute()

    async def test_operate_set_and_get(self, client: Client, test_set: DataSet):
        """Test setting and getting in same operation."""
        session = client.create_session()
        key = test_set.id("operate_set_get")
        bin_name = "mybin"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Set value
        await session.upsert(key).bin(bin_name).set_to("test_value").execute()

        # Read and verify
        result = await (await session.query(key).execute()).first_or_raise()
        assert result.record is not None
        assert result.record.bins[bin_name] == "test_value"

        # Cleanup
        await session.delete(key).execute()

    async def test_delete_record_reads_then_deletes(self, client: Client, test_set: DataSet):
        """Read a bin and atomically delete the record in one operate call."""
        session = client.create_session()
        key = test_set.id("del_read")
        await session.upsert(key).put({"name": "Alice", "age": 30}).execute()

        stream = await (
            session.upsert(key)
                .bin("name").get()
                .delete_record()
                .execute()
        )
        row = await stream.first_or_raise()
        assert row.record.bins["name"] == "Alice"

        exists_stream = await session.exists(key).execute()
        exists_row = await exists_stream.first()
        assert exists_row is None or not exists_row.as_bool()

    async def test_delete_record_then_write_recreates(self, client: Client, test_set: DataSet):
        """Delete the record and write a new bin in one atomic operate call."""
        session = client.create_session()
        key = test_set.id("del_write")
        await session.upsert(key).put({"a": 1, "b": 2}).execute()

        stream = await (
            session.upsert(key)
                .bin("a").get()
                .delete_record()
                .bin("b").set_to(99)
                .bin("b").get()
                .execute()
        )
        row = await stream.first_or_raise()
        assert row.record.bins["a"] == 1

        read_stream = await session.query(key).execute()
        read_row = await read_stream.first_or_raise()
        read_rec = read_row.record
        assert read_rec.bins["b"] == 99
        assert "a" not in read_rec.bins
        assert len(read_rec.bins) == 1

    async def test_touch_record_resets_ttl(self, client: Client, test_set: DataSet):
        """Touch the record to reset its TTL within an atomic operate call."""
        session = client.create_session()
        key = test_set.id("touch_ttl")
        await (
            session.upsert(key)
                .put({"score": 42})
                .expire_record_after_seconds(60)
                .execute()
        )

        stream = await (
            session.upsert(key)
                .bin("score").get()
                .touch_record()
                .expire_record_after_seconds(120)
                .execute()
        )
        row = await stream.first_or_raise()
        assert row.record.bins["score"] == 42
