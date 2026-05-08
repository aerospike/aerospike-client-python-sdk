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

"""Tests for generation (optimistic locking) operations."""

import pytest
from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.exceptions import GenerationError


@pytest.fixture
def test_set():
    """DataSet fixture for generation tests."""
    return DataSet.of("test", "generation_test")


class TestGeneration:
    """Test generation-based optimistic locking."""

    async def test_generation_basic(self, client: Client, test_set: DataSet):
        """Test that generation increments with each update."""
        session = client.create_session()
        key = test_set.id("generation_basic")
        bin_name = "genbin"

        # Delete record if it already exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # First write - generation should be 1
        await session.upsert(key).bin(bin_name).set_to("genvalue1").execute()

        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.generation == 1

        # Second write - generation should be 2
        await session.upsert(key).bin(bin_name).set_to("genvalue2").execute()

        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.generation == 2
        assert record.record.bins[bin_name] == "genvalue2"

        # Cleanup
        await session.delete(key).execute()

    async def test_generation_check_success(self, client: Client, test_set: DataSet):
        """Test successful update with correct generation."""
        session = client.create_session()
        key = test_set.id("generation_check_success")
        bin_name = "genbin"

        # Delete and create fresh record
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).set_to("genvalue1").execute()
        await session.upsert(key).bin(bin_name).set_to("genvalue2").execute()

        # Get current generation
        record = await (await session.query(key).execute()).first_or_raise()
        current_gen = record.record.generation

        # Update with correct generation - should succeed
        await (
            session.upsert(key)
                .ensure_generation_is(current_gen)
                .bin(bin_name).set_to("genvalue3")
                .execute()
        )

        # Verify update succeeded
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record.bins[bin_name] == "genvalue3"

        # Cleanup
        await session.delete(key).execute()

    async def test_generation_check_failure(self, client: Client, test_set: DataSet):
        """Test that update fails with incorrect generation."""
        session = client.create_session()
        key = test_set.id("generation_check_failure")
        bin_name = "genbin"

        # Delete and create fresh record
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        await session.upsert(key).bin(bin_name).set_to("genvalue1").execute()

        # Try to update with wrong generation - should fail
        with pytest.raises(GenerationError):
            await (
                session.upsert(key)
                    .ensure_generation_is(9999)
                    .bin(bin_name).set_to("genvalue_should_fail")
                    .execute()
            )

        # Verify original value unchanged
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record.bins[bin_name] == "genvalue1"

        # Cleanup
        await session.delete(key).execute()

    async def test_generation_concurrent_update(self, client: Client, test_set: DataSet):
        """Test optimistic locking pattern for concurrent updates."""
        session = client.create_session()
        key = test_set.id("generation_concurrent")
        bin_name = "counter"

        # Delete and create fresh record
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Initialize counter
        await session.upsert(key).bin(bin_name).set_to(0).execute()

        # Simulate read-modify-write pattern
        record = await (await session.query(key).execute()).first_or_raise()
        current_value = record.record.bins[bin_name]
        current_gen = record.record.generation

        # Update with generation check
        new_value = current_value + 10
        await (
            session.upsert(key)
                .ensure_generation_is(current_gen)
                .bin(bin_name).set_to(new_value)
                .execute()
        )

        # Verify
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record.bins[bin_name] == 10
        assert record.record.generation == current_gen + 1

        # Cleanup
        await session.delete(key).execute()
