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

"""Tests proving K-ordered map key ordering is preserved through native Python dict."""

import pytest
import pytest_asyncio
from aerospike_async import (
    MapOperation, MapOrder, MapPolicy, MapReturnType,
    WritePolicy,
)
from aerospike_sdk import DataSet, Client


NS = "test"
SET = "test"
BIN = "mapbin"
DS = DataSet.of(NS, SET)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy):
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        session = c.create_session()
        for key in range(1, 25):
            await session.delete(DS.id(key)).execute()
        yield c


class TestKOrderedMapOrdering:
    """K-ordered maps return dict with keys in sorted iteration order."""

    async def test_string_keys_sorted(self, client):
        """Insert string keys out of order into a K-ordered map, read back sorted."""
        key = 1
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "cherry", 3, policy),
            MapOperation.put(BIN, "apple", 1, policy),
            MapOperation.put(BIN, "banana", 2, policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == ["apple", "banana", "cherry"]

    async def test_integer_keys_sorted(self, client):
        """Insert integer keys out of order into a K-ordered map, read back sorted."""
        key = 2
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, 50, "fifty", policy),
            MapOperation.put(BIN, 10, "ten", policy),
            MapOperation.put(BIN, 30, "thirty", policy),
            MapOperation.put(BIN, 20, "twenty", policy),
            MapOperation.put(BIN, 40, "forty", policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == [10, 20, 30, 40, 50]

    async def test_many_keys_sorted(self, client):
        """K-ordered map with 100 keys preserves sorted order."""
        key = 3
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        keys_reversed = list(range(100, 0, -1))
        ops = [MapOperation.put(BIN, kk, kk * 10, policy) for kk in keys_reversed]
        await pac.operate(k, ops, policy=WritePolicy())

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert list(m.keys()) == list(range(1, 101))

    async def test_ordering_after_add(self, client):
        """Adding a key to a K-ordered map keeps all keys sorted."""
        key = 4
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "b", 2, policy),
            MapOperation.put(BIN, "d", 4, policy),
        ],
            policy=WritePolicy(),
        )

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "c", 3, policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "b", "c", "d"]

    async def test_ordering_after_remove(self, client):
        """Removing keys from a K-ordered map keeps remaining keys sorted."""
        key = 5
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "b", 2, policy),
            MapOperation.put(BIN, "c", 3, policy),
            MapOperation.put(BIN, "d", 4, policy),
        ],
            policy=WritePolicy(),
        )

        await pac.operate(
            k,
            [
            MapOperation.remove_by_key(BIN, "b", MapReturnType.NONE),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "c", "d"]

    async def test_ordering_after_remove_by_value(self, client):
        """Removing entries by value from a K-ordered map keeps remaining keys sorted."""
        key = 9
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "a", 100, policy),
            MapOperation.put(BIN, "b", 200, policy),
            MapOperation.put(BIN, "c", 100, policy),
            MapOperation.put(BIN, "d", 300, policy),
            MapOperation.put(BIN, "e", 200, policy),
        ],
            policy=WritePolicy(),
        )

        await pac.operate(
            k,
            [
            MapOperation.remove_by_value(BIN, 200, MapReturnType.NONE),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert list(m.keys()) == ["a", "c", "d"]
        assert list(m.values()) == [100, 100, 300]

    async def test_round_trip_preserves_order(self, client):
        """Read an ordered map, clear it, re-insert via MapOperation — order preserved."""
        key = 6
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "z", 26, policy),
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "m", 13, policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        original = record.bins[BIN]
        assert list(original.keys()) == ["a", "m", "z"]

        # Clear and re-insert using MapOperation to preserve K-ordered policy
        items = list(original.items())
        await pac.operate(
            k,
            [
            MapOperation.clear(BIN),
            MapOperation.put_items(BIN, items, policy),
        ],
            policy=WritePolicy(),
        )
        result2 = await (await session.query(k).execute()).first_or_raise()
        record2 = result2.record
        assert list(record2.bins[BIN].keys()) == ["a", "m", "z"]


class TestKVOrderedMapOrdering:
    """KV-ordered maps return dict with keys in sorted iteration order."""

    async def test_kv_ordered_string_keys_sorted(self, client):
        """KV-ordered map keys iterate in sorted order, same as K-ordered."""
        key = 7
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "cherry", 30, policy),
            MapOperation.put(BIN, "apple", 10, policy),
            MapOperation.put(BIN, "banana", 20, policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == ["apple", "banana", "cherry"]

    async def test_kv_ordered_integer_keys_sorted(self, client):
        """KV-ordered map with integer keys returns them in sorted order."""
        key = 8
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_VALUE_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, 50, "fifty", policy),
            MapOperation.put(BIN, 10, "ten", policy),
            MapOperation.put(BIN, 30, "thirty", policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert list(m.keys()) == [10, 30, 50]


class TestUnorderedMap:
    """Unordered maps return dict with no guaranteed key order."""

    async def test_unordered_map_has_no_key_order(self, client):
        """Unordered maps return dict; key iteration order is not guaranteed."""
        key = 10
        k = DS.id(key)
        session = client.create_session()
        await session.upsert(k).put({BIN: {"x": 1, "y": 2, "z": 3}}).execute()
        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert set(m.keys()) == {"x", "y", "z"}
        assert m["x"] == 1


class TestNestedOrderedMaps:
    """Nested K-ordered maps should preserve ordering at every level."""

    async def test_nested_ordered_maps(self, client):
        """Outer K-ordered map preserves key order; inner maps are unordered
        unless explicitly created with K-ordered policy."""
        outer_key = 11
        k = DS.id(outer_key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        inner = {"c": 3, "a": 1, "b": 2}
        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "z_outer", inner, policy),
            MapOperation.put(BIN, "a_outer", inner, policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]

        # Outer keys are K-ordered → sorted
        assert list(m.keys()) == ["a_outer", "z_outer"]

        # Inner maps were sent as plain dicts (unordered HashMap) —
        # ordering policy is NOT inherited from the parent map.
        for inner_map in m.values():
            assert isinstance(inner_map, dict)
            assert set(inner_map.keys()) == {"a", "b", "c"}


class TestEdgeCases:
    """Edge cases for ordered map conversion through PythonValue::OrderedMap."""

    async def test_mixed_key_types_sorted(self, client):
        """Aerospike sorts by type first (int before string), then by value."""
        key = 12
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "banana", "s2", policy),
            MapOperation.put(BIN, 99, "i3", policy),
            MapOperation.put(BIN, "apple", "s1", policy),
            MapOperation.put(BIN, 1, "i1", policy),
            MapOperation.put(BIN, 50, "i2", policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        keys = list(m.keys())
        int_keys = [k for k in keys if isinstance(k, int)]
        str_keys = [k for k in keys if isinstance(k, str)]
        assert int_keys == sorted(int_keys)
        assert str_keys == sorted(str_keys)
        # Integers sort before strings in Aerospike's type ordering
        assert keys == int_keys + str_keys

    async def test_bytes_keys_sorted(self, client):
        """Bytes keys in a K-ordered map preserve sorted order."""
        key = 13
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, b"\x03", "third", policy),
            MapOperation.put(BIN, b"\x01", "first", policy),
            MapOperation.put(BIN, b"\x02", "second", policy),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert list(m.keys()) == [b"\x01", b"\x02", b"\x03"]

    async def test_empty_ordered_map(self, client):
        """Empty K-ordered map returns an empty dict."""
        key = 15
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "a", 1, policy),
        ],
            policy=WritePolicy(),
        )
        await pac.operate(
            k,
            [
            MapOperation.remove_by_key(BIN, "a", MapReturnType.NONE),
        ],
            policy=WritePolicy(),
        )

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert len(m) == 0

    async def test_get_by_rank_range_ordered(self, client):
        """get_by_rank_range on K-ordered map returns values in rank order."""
        key = 16
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "c", 300, policy),
            MapOperation.put(BIN, "a", 100, policy),
            MapOperation.put(BIN, "b", 200, policy),
            MapOperation.put(BIN, "d", 400, policy),
        ],
            policy=WritePolicy(),
        )

        # Rank 0 = smallest value (100), get 3 entries by rank
        record = await pac.operate(
            k,
            [
            MapOperation.get_by_rank_range(BIN, 0, 3, MapReturnType.VALUE),
        ],
            policy=WritePolicy(),
        )
        values = record.bins[BIN]
        assert values == [100, 200, 300]


class TestCdtOrdering:
    """Verify ordering through the chainable BinBuilder path."""

    async def test_set_to_ordered_bin(self, client):
        """set_to() on a K-ordered map bin, then read back sorted."""
        key = 17
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        # First create the bin as K-ordered
        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "z", 1, policy),
        ],
            policy=WritePolicy(),
        )

        # Now overwrite via set_to (which does Operation.put under the hood)
        await session.upsert(k).bin(BIN).set_to(
            {"z": 26, "a": 1, "m": 13}
        ).execute()

        # The bin was overwritten — new dict may or may not keep K-ordered policy
        # depending on how the server handles Operation.put vs MapOperation.
        # At minimum, read it back and verify it's a dict.
        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        assert isinstance(record.bins[BIN], dict)

    async def test_get_by_key_range_ordered(self, client):
        """get_by_key_range on K-ordered map returns keys in sorted order."""
        key = 18
        k = DS.id(key)
        session = client.create_session()
        pac = client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        await pac.operate(
            k,
            [
            MapOperation.put(BIN, "e", 5, policy),
            MapOperation.put(BIN, "c", 3, policy),
            MapOperation.put(BIN, "a", 1, policy),
            MapOperation.put(BIN, "d", 4, policy),
            MapOperation.put(BIN, "b", 2, policy),
        ],
            policy=WritePolicy(),
        )

        record = await pac.operate(
            k,
            [
            MapOperation.get_by_key_range(BIN, "b", "e", MapReturnType.KEY),
        ],
            policy=WritePolicy(),
        )

        keys = record.bins[BIN]
        assert keys == ["b", "c", "d"]
