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

"""Tests for list and map data types."""

import pytest
from aerospike_sdk.aio.client import Client
from aerospike_sdk.dataset import DataSet


@pytest.fixture
def test_set():
    """DataSet fixture for list/map tests."""
    return DataSet.of("test", "listmap_test")


class TestListMap:
    """Test storing and retrieving list and map data types."""

    async def test_list_strings(self, client: Client, test_set: DataSet):
        """Test storing and retrieving a list of strings."""
        session = client.create_session()
        key = test_set.id("listStrings")
        bin_name = "listbin1"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Create list
        list_data = ["string1", "string2", "string3"]

        # Store list
        await session.upsert(key).bin(bin_name).set_to(list_data).execute()

        # Retrieve and verify
        result = await (await session.query(key).execute()).first_or_raise()
        record = result.record
        assert record is not None
        received_list = record.bins[bin_name]
        
        assert len(received_list) == 3
        assert received_list[0] == "string1"
        assert received_list[1] == "string2"
        assert received_list[2] == "string3"

        # Cleanup
        await session.delete(key).execute()

    async def test_list_complex(self, client: Client, test_set: DataSet):
        """Test storing and retrieving a list with mixed types."""
        session = client.create_session()
        key = test_set.id("listComplex")
        bin_name = "listbin2"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Create complex list
        blob = bytes([3, 52, 125])
        list_data = ["string1", 2, blob]

        # Store list
        await session.upsert(key).bin(bin_name).set_to(list_data).execute()

        # Retrieve and verify
        result = await (await session.query(key).execute()).first_or_raise()
        record = result.record
        assert record is not None
        received_list = record.bins[bin_name]

        assert len(received_list) >= 3
        assert received_list[0] == "string1"
        # Server converts numbers to int/long
        assert received_list[1] == 2
        assert received_list[2] == blob

        # Cleanup
        await session.delete(key).execute()

    async def test_map_strings(self, client: Client, test_set: DataSet):
        """Test storing and retrieving a map of strings."""
        session = client.create_session()
        key = test_set.id("mapStrings")
        bin_name = "mapbin1"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Create map
        map_data = {
            "key1": "string1",
            "key2": "loooooooooooooooooooooooooongerstring2",
            "key3": "string3"
        }

        # Store map
        await session.upsert(key).bin(bin_name).set_to(map_data).execute()

        # Retrieve and verify
        result = await (await session.query(key).execute()).first_or_raise()
        record = result.record
        assert record is not None
        received_map = record.bins[bin_name]

        assert len(received_map) == 3
        assert received_map["key1"] == "string1"
        assert received_map["key2"] == "loooooooooooooooooooooooooongerstring2"
        assert received_map["key3"] == "string3"

        # Cleanup
        await session.delete(key).execute()

    async def test_map_complex(self, client: Client, test_set: DataSet):
        """Test storing and retrieving a map with mixed types."""
        session = client.create_session()
        key = test_set.id("mapComplex")
        bin_name = "mapbin2"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Create complex map
        blob = bytes([3, 52, 125])
        inner_list = [100034, 12384955, 3, 512]

        map_data = {
            "key1": "string1",
            "key2": 2,
            "key3": blob,
            "key4": inner_list,
            "key5": True,
            "key6": False
        }

        # Store map
        await session.upsert(key).bin(bin_name).set_to(map_data).execute()

        # Retrieve and verify
        result = await (await session.query(key).execute()).first_or_raise()
        record = result.record
        assert record is not None
        received_map = record.bins[bin_name]

        assert len(received_map) == 6
        assert received_map["key1"] == "string1"
        assert received_map["key2"] == 2
        assert received_map["key3"] == blob

        received_inner = received_map["key4"]
        assert len(received_inner) == 4
        assert received_inner[0] == 100034
        assert received_inner[1] == 12384955
        assert received_inner[2] == 3
        assert received_inner[3] == 512

        assert received_map["key5"] is True
        assert received_map["key6"] is False

        # Cleanup
        await session.delete(key).execute()

    async def test_list_sorted(self, client: Client, test_set: DataSet):
        """Store a pre-sorted list and verify order is preserved on retrieval."""
        session = client.create_session()
        key = test_set.id("listSorted")
        bin_name = "sortedlistbin"

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        items = ["e", "d", "c", "b", "a"]
        items.sort()

        await session.upsert(key).bin(bin_name).set_to(items).execute()

        result = await (await session.query(key).execute()).first_or_raise()
        received = result.record.bins[bin_name]

        assert len(received) == 5
        assert received == ["a", "b", "c", "d", "e"]

        await session.delete(key).execute()

    async def test_map_with_integer_keys(self, client: Client, test_set: DataSet):
        """Store a map with integer keys and mixed value types."""
        session = client.create_session()
        key = test_set.id("mapIntKeys")
        bin_name = "intkeymapbin"

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        map_data = {1: "one", 2: "two", 3: "three"}
        await session.upsert(key).bin(bin_name).set_to(map_data).execute()

        result = await (await session.query(key).execute()).first_or_raise()
        received = result.record.bins[bin_name]

        assert len(received) == 3
        assert received[1] == "one"
        assert received[2] == "two"
        assert received[3] == "three"

        await session.delete(key).execute()

    async def test_multiple_bin_list_and_map(self, client: Client, test_set: DataSet):
        """Store list in one bin and map in another, verify independent retrieval."""
        session = client.create_session()
        key = test_set.id("multiBinListMap")

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        list_data = [10, 20, 30]
        map_data = {"x": 1, "y": 2}

        await (
            session.upsert(key)
                .bin("listbin").set_to(list_data)
                .bin("mapbin").set_to(map_data)
                .execute()
        )

        result = await (await session.query(key).execute()).first_or_raise()
        bins = result.record.bins
        assert bins["listbin"] == [10, 20, 30]
        assert bins["mapbin"]["x"] == 1
        assert bins["mapbin"]["y"] == 2

        await session.delete(key).execute()

    async def test_empty_list_and_map(self, client: Client, test_set: DataSet):
        """Store and retrieve empty list and empty map."""
        session = client.create_session()
        key = test_set.id("emptyListMap")

        try:
            await session.delete(key).execute()
        except Exception:
            pass

        await (
            session.upsert(key)
                .bin("emptylist").set_to([])
                .bin("emptymap").set_to({})
                .execute()
        )

        result = await (await session.query(key).execute()).first_or_raise()
        bins = result.record.bins
        assert bins["emptylist"] == []
        assert bins["emptymap"] == {}

        await session.delete(key).execute()

    async def test_list_map_combined(self, client: Client, test_set: DataSet):
        """Test storing and retrieving nested lists and maps."""
        session = client.create_session()
        key = test_set.id("listMapCombined")
        bin_name = "listmapbin"

        # Delete if exists
        try:
            await session.delete(key).execute()
        except Exception:
            pass

        # Create nested structure
        blob = bytes([3, 52, 125])
        inner_list = ["string2", 5]
        inner_map = {
            "a": 1,
            2: "b",
            3: blob,
            "list": inner_list
        }

        list_data = ["string1", 8, inner_list, inner_map]

        # Store
        await session.upsert(key).bin(bin_name).set_to(list_data).execute()

        # Retrieve and verify
        result = await (await session.query(key).execute()).first_or_raise()
        record = result.record
        assert record is not None
        received = record.bins[bin_name]

        assert len(received) == 4
        assert received[0] == "string1"
        assert received[1] == 8

        received_inner = received[2]
        assert len(received_inner) == 2
        assert received_inner[0] == "string2"
        assert received_inner[1] == 5

        received_map = received[3]
        assert len(received_map) == 4
        assert received_map["a"] == 1
        assert received_map[2] == "b"
        assert received_map[3] == blob

        received_inner2 = received_map["list"]
        assert len(received_inner2) == 2
        assert received_inner2[0] == "string2"
        assert received_inner2[1] == 5

        # Cleanup
        await session.delete(key).execute()
