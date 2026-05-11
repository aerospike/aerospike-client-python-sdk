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

"""Tests for delete bin operations."""

import pytest
from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet


@pytest.fixture
def test_set():
    """DataSet fixture for delete bin tests."""
    return DataSet.of("test", "delete_bin_test")


class TestDeleteBin:
    """Test deleting individual bins from records."""

    async def test_delete_bin(self, client: Client, test_set: DataSet):
        """Test deleting a single bin from a record."""
        session = client.create_session()
        key = test_set.id("deleteBin")
        bin_name1 = "bin1"
        bin_name2 = "bin2"

        # Create record with two bins
        await (
            session.upsert(key)
                .bin(bin_name1).set_to("value1")
                .bin(bin_name2).set_to("value2")
                .execute()
        )

        # Verify both bins exist
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins[bin_name1] == "value1"
        assert record.record.bins[bin_name2] == "value2"

        # Remove bin1
        await session.upsert(key).bin(bin_name1).remove().execute()

        # Verify bin1 is gone but bin2 remains
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert bin_name1 not in record.record.bins or record.record.bins.get(bin_name1) is None
        assert record.record.bins[bin_name2] == "value2"

        # Cleanup
        await session.delete(key).execute()

    async def test_delete_multiple_bins(self, client: Client, test_set: DataSet):
        """Test deleting multiple bins from a record."""
        session = client.create_session()
        key = test_set.id("deleteMultipleBins")

        # Create record with three bins
        await (
            session.upsert(key)
                .bin("bin1").set_to("value1")
                .bin("bin2").set_to("value2")
                .bin("bin3").set_to("value3")
                .execute()
        )

        # Remove bin1 and bin2
        await (
            session.upsert(key)
                .bin("bin1").remove()
                .bin("bin2").remove()
                .execute()
        )

        # Verify only bin3 remains
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert "bin1" not in record.record.bins or record.record.bins.get("bin1") is None
        assert "bin2" not in record.record.bins or record.record.bins.get("bin2") is None
        assert record.record.bins["bin3"] == "value3"

        # Cleanup
        await session.delete(key).execute()

    async def test_delete_bin_nonexistent(self, client: Client, test_set: DataSet):
        """Test removing a bin that doesn't exist (should not error)."""
        session = client.create_session()
        key = test_set.id("deleteNonexistentBin")

        # Create record with one bin
        await session.upsert(key).bin("bin1").set_to("value1").execute()

        # Try to remove a bin that doesn't exist (should not error)
        await session.upsert(key).bin("nonexistent").remove().execute()

        # Verify original bin still exists
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert record.record.bins["bin1"] == "value1"

        # Cleanup
        await session.delete(key).execute()

    async def test_delete_and_set_bin(self, client: Client, test_set: DataSet):
        """Test deleting one bin while setting another in same operation."""
        session = client.create_session()
        key = test_set.id("deleteAndSetBin")

        # Create record with two bins
        await (
            session.upsert(key)
                .bin("bin1").set_to("value1")
                .bin("bin2").set_to("value2")
                .execute()
        )

        # Remove bin1 and update bin2 in same operation
        await (
            session.upsert(key)
                .bin("bin1").remove()
                .bin("bin2").set_to("new_value2")
                .execute()
        )

        # Verify
        record = await (await session.query(key).execute()).first_or_raise()
        assert record.record is not None
        assert "bin1" not in record.record.bins or record.record.bins.get("bin1") is None
        assert record.record.bins["bin2"] == "new_value2"

        # Cleanup
        await session.delete(key).execute()
