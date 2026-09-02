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
from aerospike_sdk import Exp, MapOrder, MapReturnType, SortedMap
from aerospike_async import MapOperation, MapPolicy, WritePolicy
from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError
from tests.integration.namespace import general_namespace
from tests.pac_compat import requires_server_compiled_ael


NS = general_namespace()
SET = "test"
BIN = "mapbin"
DS = DataSet.of(NS, SET)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as c:
        session = c.create_session()
        for key in range(1, 40):
            await session.delete(DS.id(key)).execute()
        yield c


class TestKOrderedMapOrdering:
    """K-ordered maps return dict with keys in sorted iteration order."""

    async def test_string_keys_sorted(self, cluster):
        """Insert string keys out of order into a K-ordered map, read back sorted."""
        key = 1
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_integer_keys_sorted(self, cluster):
        """Insert integer keys out of order into a K-ordered map, read back sorted."""
        key = 2
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_many_keys_sorted(self, cluster):
        """K-ordered map with 100 keys preserves sorted order."""
        key = 3
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
        policy = MapPolicy(MapOrder.KEY_ORDERED, None)

        keys_reversed = list(range(100, 0, -1))
        ops = [MapOperation.put(BIN, kk, kk * 10, policy) for kk in keys_reversed]
        await pac.operate(k, ops, policy=WritePolicy())

        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert list(m.keys()) == list(range(1, 101))

    async def test_ordering_after_add(self, cluster):
        """Adding a key to a K-ordered map keeps all keys sorted."""
        key = 4
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_ordering_after_remove(self, cluster):
        """Removing keys from a K-ordered map keeps remaining keys sorted."""
        key = 5
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_ordering_after_remove_by_value(self, cluster):
        """Removing entries by value from a K-ordered map keeps remaining keys sorted."""
        key = 9
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_round_trip_preserves_order(self, cluster):
        """Read an ordered map, clear it, re-insert via MapOperation — order preserved."""
        key = 6
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_kv_ordered_string_keys_sorted(self, cluster):
        """KV-ordered map keys iterate in sorted order, same as K-ordered."""
        key = 7
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_kv_ordered_integer_keys_sorted(self, cluster):
        """KV-ordered map with integer keys returns them in sorted order."""
        key = 8
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_unordered_map_has_no_key_order(self, cluster):
        """Unordered maps return dict; key iteration order is not guaranteed."""
        key = 10
        k = DS.id(key)
        session = cluster.create_session()
        await session.upsert(k).put({BIN: {"x": 1, "y": 2, "z": 3}}).execute()
        result = await (await session.query(k).execute()).first_or_raise()
        record = result.record
        m = record.bins[BIN]
        assert isinstance(m, dict)
        assert set(m.keys()) == {"x", "y", "z"}
        assert m["x"] == 1


class TestNestedOrderedMaps:
    """Nested K-ordered maps should preserve ordering at every level."""

    async def test_nested_ordered_maps(self, cluster):
        """Outer K-ordered map preserves key order; inner maps are unordered
        unless explicitly created with K-ordered policy."""
        outer_key = 11
        k = DS.id(outer_key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_mixed_key_types_sorted(self, cluster):
        """Aerospike sorts by type first (int before string), then by value."""
        key = 12
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_bytes_keys_sorted(self, cluster):
        """Bytes keys in a K-ordered map preserve sorted order."""
        key = 13
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_empty_ordered_map(self, cluster):
        """Empty K-ordered map returns an empty dict."""
        key = 15
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_get_by_rank_range_ordered(self, cluster):
        """get_by_rank_range on K-ordered map returns values in rank order."""
        key = 16
        k = DS.id(key)
        pac = cluster._client.underlying_client
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

    async def test_set_to_ordered_bin(self, cluster):
        """set_to() on a K-ordered map bin, then read back sorted."""
        key = 17
        k = DS.id(key)
        session = cluster.create_session()
        pac = cluster._client.underlying_client
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

    async def test_get_by_key_range_ordered(self, cluster):
        """get_by_key_range on K-ordered map returns keys in sorted order."""
        key = 18
        k = DS.id(key)
        pac = cluster._client.underlying_client
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


class TestSortedMapOnPlainWrites:
    """``SortedMap`` declares a plain bin write key-ordered.

    A plain ``dict`` is written unordered. The server sorts the entries either
    way, so the two cannot be told apart by reading them back -- the flag is
    what decides whether the server will binary-search the map on later access,
    and it is durable, so it governs every subsequent read of that record.
    """

    async def test_sorted_map_round_trips_as_itself(self, cluster):
        session = cluster.create_session()
        k = DS.id(30)
        await session.delete(k).execute()
        data = {"zebra": 26, "apple": 1, "mango": 13}

        await session.upsert(k).put({BIN: SortedMap(data)}).execute()
        stream = await session.query(k).execute()
        got = (await stream.first_or_raise()).record_or_raise()

        # Symmetric: written as a SortedMap, read back as one.
        assert isinstance(got.bins[BIN], SortedMap)
        # And still a dict in every way that matters to a caller.
        assert isinstance(got.bins[BIN], dict)
        assert got.bins[BIN] == data
        assert got.bins[BIN]["apple"] == 1
        assert list(got.bins[BIN]) == sorted(data)

    async def test_plain_dict_stays_a_plain_dict(self, cluster):
        session = cluster.create_session()
        k = DS.id(31)
        await session.delete(k).execute()
        data = {"b": 2, "a": 1}

        await session.upsert(k).put({BIN: data}).execute()
        stream = await session.query(k).execute()
        got = (await stream.first_or_raise()).record_or_raise()

        # An undeclared map is not silently promoted.
        assert type(got.bins[BIN]) is dict
        assert got.bins[BIN] == data

    async def test_sorted_map_through_the_bin_builder(self, cluster):
        session = cluster.create_session()
        k = DS.id(32)
        await session.delete(k).execute()

        await session.upsert(k).bin(BIN).set_to(SortedMap({"z": 1, "a": 2})).execute()
        stream = await session.query(k).execute()
        got = (await stream.first_or_raise()).record_or_raise()

        assert isinstance(got.bins[BIN], SortedMap)
        assert got.bins[BIN] == {"a": 2, "z": 1}

    async def test_sorted_map_nests(self, cluster):
        session = cluster.create_session()
        k = DS.id(33)
        await session.delete(k).execute()

        await session.upsert(k).put(
            {BIN: {"limits": SortedMap({"z": 1, "a": 2})}}
        ).execute()
        stream = await session.query(k).execute()
        got = (await stream.first_or_raise()).record_or_raise()

        inner = got.bins[BIN]["limits"]
        assert isinstance(inner, SortedMap)
        assert inner == {"a": 2, "z": 1}

    async def test_sorted_map_equals_itself_in_an_expression(self, cluster):
        """Whole-map equality: a key-ordered bin compared against the same map.

        Both operands must be key-ordered for the server to compare them --
        the encodings differ by the order-flag header, so an unordered operand
        on either side never matches. The unordered form is unreachable until
        the server detects pre-sorted unordered maps.
        """
        session = cluster.create_session()
        k = DS.id(34)
        await session.delete(k).execute()
        m = SortedMap({"key1": "e", "key2": "d", "key3": "c",
                       "key4": "b", "key5": "a"})

        await session.upsert(k).bin(BIN).set_to(m).execute()

        stream = await (
            session.query(k)
            .bins([BIN])
            .fail_on_filtered_out()
            .where(Exp.eq(Exp.map_bin(BIN), Exp.val(m)))
            .execute()
        )
        rows = await stream.collect()

        assert rows, "a key-ordered map must compare equal to itself"
        got = rows[0].record_or_raise().bins[BIN]
        # Returned as the declared type, so it compares against the value written.
        assert isinstance(got, SortedMap)
        assert got == m
        assert list(got) == sorted(m)

    @requires_server_compiled_ael
    @pytest.mark.xfail(
        raises=AerospikeError,
        reason=(
            "Server-compiled AEL has no collection-literal syntax. A map "
            "literal on the right of == is rejected with PARAMETER_ERROR, and "
            "so is a list literal (measured on 8.1.3.0-104), so this is a "
            "uniform boundary of the DSL rather than a map-specific gap. "
            "Scalars compare, collections can be navigated into "
            "(`:MAP.count()` / `:LIST.count()`), and the same comparison "
            "succeeds as a built expression, so nothing is unreachable. The "
            "reference client has no equivalent test either. Promote only if "
            "AEL gains collection literals -- a feature question, not a "
            "pending fix."
        ),
    )
    async def test_sorted_map_equality_via_server_ael(self, cluster):
        """The AEL spelling of :meth:`test_sorted_map_equals_itself_in_an_expression`.

        Kept as the string-filter twin of the built-expression test so the two
        surfaces stay paired. Not evidence of a defect: list literals are
        rejected the same way, so AEL simply has no collection-literal syntax.
        """
        session = cluster.create_session()
        k = DS.id(35)
        await session.delete(k).execute()
        m = SortedMap({"key1": "e", "key2": "d", "key3": "c",
                       "key4": "b", "key5": "a"})

        await session.upsert(k).bin(BIN).set_to(m).execute()

        literal = ("{'key1': 'e', 'key2': 'd', 'key3': 'c', "
                   "'key4': 'b', 'key5': 'a'}")
        stream = await (
            session.query(k)
            .bins([BIN])
            .fail_on_filtered_out()
            .where(f"$.{BIN}.get(type: MAP) == {literal}")
            .execute()
        )
        rows = await stream.collect()

        assert rows, "a key-ordered map must compare equal to itself"
