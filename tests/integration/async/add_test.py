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

"""Tests for numeric add (increment) operations."""

import pytest
from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet


@pytest.fixture
def test_set():
    """DataSet fixture for add tests."""
    return DataSet.of("test", "add_test")


class TestAdd:
    """Test numeric add (increment) operations."""

    async def test_add(self, client: Client, test_set: DataSet):
        """Test adding integers to a bin."""
        session = client.create_session()
        key = test_set.id("addkey")
        bin_name = "addbin"

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).add(10).execute()
        await session.upsert(key).bin(bin_name).add(5).execute()

        result = await session.query(key).execute()
        first = await result.first_or_raise()
        assert first.is_ok
        assert first.record_or_raise().bins[bin_name] == 15

        await session.upsert(key).bin(bin_name).add(30).execute()
        result = await session.query(key).execute()
        first = await result.first_or_raise()
        assert first.record_or_raise().bins[bin_name] == 45

        await session.delete(key).execute()

    async def test_add_negative(self, client: Client, test_set: DataSet):
        """Test adding negative values (decrement)."""
        session = client.create_session()
        key = test_set.id("add_negative")
        bin_name = "counter"

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).add(100).execute()
        await session.upsert(key).bin(bin_name).add(-30).execute()

        result = await session.query(key).execute()
        first = await result.first_or_raise()
        assert first.is_ok
        assert first.record_or_raise().bins[bin_name] == 70

        await session.delete(key).execute()

    async def test_increment_by_alias(self, client: Client, test_set: DataSet):
        """Test that increment_by is an alias for add."""
        session = client.create_session()
        key = test_set.id("increment_alias")
        bin_name = "counter"

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).increment_by(10).execute()
        await session.upsert(key).bin(bin_name).increment_by(5).execute()

        result = await session.query(key).execute()
        first = await result.first_or_raise()
        assert first.is_ok
        assert first.record_or_raise().bins[bin_name] == 15

        await session.delete(key).execute()

    async def test_add_batch(self, client: Client, test_set: DataSet):
        """Test adding to multiple keys via batch operations."""
        session = client.create_session()
        bin_name = "addbin"
        keys = [test_set.id(i) for i in range(10, 20)]

        await session.delete(keys).execute()

        await session.upsert(keys).add(bin_name, 10).execute()
        await session.upsert(keys).add(bin_name, 5).execute()

        rs = await session.query(keys).bins([bin_name]).execute()
        records = await rs.collect()
        assert len(records) == 10
        for rr in records:
            assert rr.record_or_raise().bins[bin_name] == 15

        # Combined add + get in a single operate (direct segment style)
        rs = await (
            session.upsert(keys)
                .add(bin_name, 30)
                .get(bin_name)
                .execute()
        )
        records = await rs.collect()
        assert len(records) == 10
        for rr in records:
            # Batch returns [write_result, read_result] for same-bin add+get
            result = rr.record_or_raise().bins[bin_name]
            val = result[1] if isinstance(result, list) else result
            assert val == 45

        await session.delete(keys).execute()
