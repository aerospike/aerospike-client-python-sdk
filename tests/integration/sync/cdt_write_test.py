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

"""Integration tests for sync CDT write operations: list_add, append, pop, trim, remove, exists."""

import pytest
from aerospike_async import ListOrderType, ListSortFlags, MapOrder
from aerospike_sdk.exceptions import AerospikeError, ResultCode

from aerospike_sdk import DataSet


NS = "test"
SET = "cdt_write_sync"
DS = DataSet.of(NS, SET)


def _flatten_cdt_values(obj):
    """Flatten nested lists from multi-op CDT read results."""
    if isinstance(obj, list):
        acc = []
        for x in obj:
            acc.extend(_flatten_cdt_values(x))
        return acc
    return [obj]


_MAP_GET_RELATIVE_EXPECTED = [
    [5, 9], [9], [4, 5, 9], [9], [0, 4, 5, 9], [5], [9], [4], [9], [0],
    [17], [10, 15, 17], [17], [10],
]

_LIST_GET_RELATIVE_EXPECTED = [
    6,
    [5, 9, 11, 15],
    [9, 11, 15],
    [4, 5, 9, 11, 15],
    [4, 5, 9, 11, 15],
    [11, 15],
    [0, 4, 5, 9, 11, 15],
    [5, 9],
    [9],
    [4, 5],
    [4],
    [11, 15],
    [],
]

def _assert_map_get_relative_batch(raw_bin):
    assert isinstance(raw_bin, list)
    assert len(raw_bin) == len(_MAP_GET_RELATIVE_EXPECTED)
    for got, exp in zip(raw_bin, _MAP_GET_RELATIVE_EXPECTED):
        assert [int(x) for x in got] == exp


def _assert_list_get_relative_batch(raw_bin):
    assert isinstance(raw_bin, list)
    assert len(raw_bin) == len(_LIST_GET_RELATIVE_EXPECTED)
    first, rest = raw_bin[0], raw_bin[1:]
    assert int(first) == int(_LIST_GET_RELATIVE_EXPECTED[0])
    for got, exp in zip(rest, _LIST_GET_RELATIVE_EXPECTED[1:]):
        assert [int(x) for x in got] == exp


@pytest.fixture
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        session = c.create_session()
        for key_id in range(1, 130):
            session.delete(DS.id(key_id)).execute()
        yield c


# ===================================================================
# list_add (ordered list)
# ===================================================================

class TestListAdd:
    """Ordered list add operations via list_add()."""

    def test_list_add_maintains_sorted_order(self, cluster):
        """Insert values out of order; verify they are stored sorted."""
        session = cluster.create_session()
        k = DS.id(1)
        session.upsert(k).put({"name": "placeholder"}).execute()

        for v in [30, 10, 50, 20, 40]:
            session.update(k).bin("scores").list_add(v).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30, 40, 50]

    def test_list_add_to_empty_bin(self, cluster):
        """Create an ordered list from scratch on a non-existent bin."""
        session = cluster.create_session()
        k = DS.id(2)
        session.upsert(k).put({"name": "placeholder"}).execute()

        session.update(k).bin("tags").list_add("alpha").execute()
        session.update(k).bin("tags").list_add("beta").execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["tags"] == ["alpha", "beta"]


# ===================================================================
# list_append (unordered list)
# ===================================================================

class TestListAppend:
    """Unordered list append operations via list_append()."""

    def test_list_append_adds_to_end(self, cluster):
        """Append a value to an existing list; verify it lands at the end."""
        session = cluster.create_session()
        k = DS.id(3)
        session.upsert(k).put({"items": ["a", "b"]}).execute()

        session.update(k).bin("items").list_append("c").execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["items"] == ["a", "b", "c"]

    def test_list_append_multiple(self, cluster):
        """Append multiple values sequentially; verify order is preserved."""
        session = cluster.create_session()
        k = DS.id(4)
        session.upsert(k).put({"nums": [1]}).execute()

        session.update(k).bin("nums").list_append(2).execute()
        session.update(k).bin("nums").list_append(3).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [1, 2, 3]


# ===================================================================
# remove (map key)
# ===================================================================

class TestMapKeyRemove:
    """Remove map entries by key or key range."""

    def test_remove_map_key(self, cluster):
        """Remove a single map entry by key."""
        session = cluster.create_session()
        k = DS.id(5)
        session.upsert(k).put({"ratings": {"a": 1, "b": 2, "c": 3}}).execute()

        session.update(k).bin("ratings").on_map_key("b").remove().execute()

        result = session.query(k).execute().first_or_raise()
        ratings = result.record.bins["ratings"]
        assert "b" not in ratings
        assert ratings == {"a": 1, "c": 3}

    def test_remove_map_key_range(self, cluster):
        """Remove map entries within a key range [b, d)."""
        session = cluster.create_session()
        k = DS.id(6)
        session.upsert(k).put({"data": {"a": 1, "b": 2, "c": 3, "d": 4}}).execute()

        session.update(k).bin("data").on_map_key_range("b", "d").remove().execute()

        result = session.query(k).execute().first_or_raise()
        data = result.record.bins["data"]
        assert data == {"a": 1, "d": 4}


# ===================================================================
# remove (list value)
# ===================================================================

class TestListValueRemove:
    """Remove list elements by value or index."""

    def test_remove_list_value(self, cluster):
        """Remove a list element matching a specific value."""
        session = cluster.create_session()
        k = DS.id(7)
        session.upsert(k).put({"scores": [10, 20, 30, 40]}).execute()

        session.update(k).bin("scores").on_list_value(20).remove().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["scores"] == [10, 30, 40]

    def test_remove_list_by_index(self, cluster):
        """Remove a list element at a specific index."""
        session = cluster.create_session()
        k = DS.id(8)
        session.upsert(k).put({"items": ["x", "y", "z"]}).execute()

        session.update(k).bin("items").on_list_index(1).remove().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["items"] == ["x", "z"]


# ===================================================================
# exists (map key)
# ===================================================================

class TestMapKeyExists:
    """Check map key existence via on_map_key().exists()."""

    def test_map_key_exists_true(self, cluster):
        """Existing key returns True."""
        session = cluster.create_session()
        k = DS.id(9)
        session.upsert(k).put({"info": {"name": "Alice", "age": 30}}).execute()

        result = session.query(k).bin("info").on_map_key("name").exists().execute().first_or_raise()
        assert result.record.bins["info"] is True

    def test_map_key_exists_false(self, cluster):
        """Non-existent key returns False."""
        session = cluster.create_session()
        k = DS.id(10)
        session.upsert(k).put({"info": {"name": "Bob"}}).execute()

        result = session.query(k).bin("info").on_map_key("missing").exists().execute().first_or_raise()
        assert result.record.bins["info"] is False


# ===================================================================
# exists (list value)
# ===================================================================

class TestListValueExists:
    """Check list value existence via on_list_value().exists()."""

    def test_list_value_exists_true(self, cluster):
        """Present value returns True."""
        session = cluster.create_session()
        k = DS.id(11)
        session.upsert(k).put({"tags": ["red", "green", "blue"]}).execute()

        result = session.query(k).bin("tags").on_list_value("green").exists().execute().first_or_raise()
        assert result.record.bins["tags"] is True

    def test_list_value_exists_false(self, cluster):
        """Absent value returns False."""
        session = cluster.create_session()
        k = DS.id(12)
        session.upsert(k).put({"tags": ["red", "green"]}).execute()

        result = session.query(k).bin("tags").on_list_value("purple").exists().execute().first_or_raise()
        assert result.record.bins["tags"] is False


# ===================================================================
# Combined operations
# ===================================================================

class TestCdtWriteCombined:
    """Multi-step CDT write scenarios combining different operations."""

    def test_list_add_then_remove(self, cluster):
        """Add several values to an ordered list, then remove one."""
        session = cluster.create_session()
        k = DS.id(13)
        session.upsert(k).put({"name": "placeholder"}).execute()

        for v in [10, 20, 30]:
            session.update(k).bin("nums").list_add(v).execute()
        session.update(k).bin("nums").list_add(25).execute()
        session.update(k).bin("nums").on_list_value(10).remove().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [20, 25, 30]

    def test_multi_bin_cdt_ops(self, cluster):
        """Multiple CDT operations on different bins in one request."""
        session = cluster.create_session()
        k = DS.id(14)
        session.upsert(k).put({
            "scores": [10, 20, 30],
            "meta": {"x": 1, "y": 2},
        }).execute()

        (
            session.update(k)
            .bin("scores").list_append(40)
            .bin("meta").on_map_key("x").remove()
            .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30, 40]
        assert result.record.bins["meta"] == {"y": 2}


# ===================================================================
# Nested navigation: set_to / add
# ===================================================================

class TestNestedSetTo:
    """Nested CDT navigation with set_to() write terminal."""

    def test_set_to_on_map_key(self, cluster):
        """Set a value at a single-level map key."""
        session = cluster.create_session()
        k = DS.id(15)
        session.upsert(k).put({"ratings": {"a": 1, "b": 2}}).execute()

        session.update(k).bin("ratings").on_map_key("a").set_to(10).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["ratings"] == {"a": 10, "b": 2}

    def test_set_to_creates_new_key(self, cluster):
        """set_to() on a non-existent key creates it."""
        session = cluster.create_session()
        k = DS.id(16)
        session.upsert(k).put({"m": {"a": 1}}).execute()

        session.update(k).bin("m").on_map_key("b").set_to(2).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["m"] == {"a": 1, "b": 2}

    def test_nested_set_to_two_deep(self, cluster):
        """Set a value at a 2-deep nested map path."""
        session = cluster.create_session()
        k = DS.id(17)
        session.upsert(k).put({
            "rooms": {"room1": {"rate": 100, "avail": True}},
        }).execute()

        (
            session.update(k)
            .bin("rooms").on_map_key("room1").on_map_key("rate").set_to(150)
            .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["rooms"]["room1"]["rate"] == 150
        assert result.record.bins["rooms"]["room1"]["avail"] is True


class TestNestedAdd:
    """Nested CDT navigation with add() (increment) terminal."""

    def test_add_on_map_key(self, cluster):
        """Increment a value at a single-level map key."""
        session = cluster.create_session()
        k = DS.id(18)
        session.upsert(k).put({"counters": {"views": 10}}).execute()

        session.update(k).bin("counters").on_map_key("views").add(5).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["counters"]["views"] == 15

    def test_nested_add_two_deep(self, cluster):
        """Increment at a 2-deep nested map path."""
        session = cluster.create_session()
        k = DS.id(19)
        session.upsert(k).put({
            "rooms": {"room1": {"rates": {"base": 100}}},
        }).execute()

        (
            session.update(k)
            .bin("rooms").on_map_key("room1").on_map_key("rates").on_map_key("base").add(10)
            .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["rooms"]["room1"]["rates"]["base"] == 110


class TestNestedCombined:
    """Combined nested write operations."""

    def test_nested_set_to_and_flat_read(self, cluster):
        """Combine nested set_to with flat read in one execute."""
        session = cluster.create_session()
        k = DS.id(20)
        session.upsert(k).put({
            "meta": {"status": "draft"},
            "scores": [10, 20],
        }).execute()

        (
            session.update(k)
            .bin("meta").on_map_key("status").set_to("published")
            .bin("scores").list_append(30)
            .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["meta"]["status"] == "published"
        assert result.record.bins["scores"] == [10, 20, 30]

    def test_multiple_nested_writes_same_map(self, cluster):
        """Multiple set_to operations on different keys in the same map."""
        session = cluster.create_session()
        k = DS.id(21)
        session.upsert(k).put({"info": {"a": 1, "b": 2, "c": 3}}).execute()

        (
            session.update(k)
            .bin("info").on_map_key("a").set_to(10)
            .bin("info").on_map_key("c").set_to(30)
            .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["info"] == {"a": 10, "b": 2, "c": 30}


# ===================================================================
# Product ratings workflow (end-to-end CDT map operations)
# ===================================================================

class TestProductRatingsWorkflow:
    """End-to-end CDT map workflow: write, read by key/rank/range, update, remove."""

    def test_bulk_set_to_multiple_keys(self, cluster):
        """Write multiple map entries in one upsert."""
        session = cluster.create_session()
        k = DS.id(22)
        session.upsert(k).put({"name": "product"}).execute()

        (
            session.upsert(k)
            .bin("ratings").on_map_key("alice").set_to(5)
            .bin("ratings").on_map_key("bob").set_to(4)
            .bin("ratings").on_map_key("carol").set_to(4)
            .bin("ratings").on_map_key("dave").set_to(3)
            .bin("ratings").on_map_key("eve").set_to(5)
            .bin("ratings").on_map_key("frank").set_to(2)
            .bin("ratings").on_map_key("grace").set_to(4)
            .execute()
        )

        result = session.query(k).execute().first_or_raise()
        ratings = result.record.bins["ratings"]
        assert len(ratings) == 7
        assert ratings["alice"] == 5
        assert ratings["frank"] == 2

    def test_read_by_map_key(self, cluster):
        """Read a specific map entry by key."""
        session = cluster.create_session()
        k = DS.id(23)
        session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 4, "carol": 3},
        }).execute()

        result = (
            session.query(k).bin("ratings").on_map_key("alice").get_values()
            .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == 5

    def test_read_highest_by_rank(self, cluster):
        """Read the highest-ranked entry (rank -1)."""
        session = cluster.create_session()
        k = DS.id(24)
        session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 3, "carol": 4},
        }).execute()

        result = (
            session.query(k)
            .bin("ratings").on_map_rank(-1).get_keys_and_values()
            .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == {"alice": 5}

    def test_count_by_value_range(self, cluster):
        """Count entries within a value range [4, 6)."""
        session = cluster.create_session()
        k = DS.id(25)
        session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 4, "carol": 3, "dave": 5, "eve": 2},
        }).execute()

        result = (
            session.query(k)
            .bin("ratings").on_map_value_range(4, 6).count()
            .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == 3

    def test_update_then_remove_then_verify(self, cluster):
        """Update a rating, remove another, then verify the map state."""
        session = cluster.create_session()
        k = DS.id(26)
        session.upsert(k).put({
            "ratings": {"alice": 5, "bob": 3, "carol": 4, "dave": 2},
        }).execute()

        session.upsert(k).bin("ratings").on_map_key("bob").set_to(5).execute()
        session.upsert(k).bin("ratings").on_map_key("dave").remove().execute()

        result = session.query(k).execute().first_or_raise()
        ratings = result.record.bins["ratings"]
        assert ratings == {"alice": 5, "bob": 5, "carol": 4}

    def test_count_after_update_and_remove(self, cluster):
        """Verify value-range count changes after updates and removals."""
        session = cluster.create_session()
        k = DS.id(27)
        session.upsert(k).put({
            "ratings": {"a": 5, "b": 4, "c": 3, "d": 5, "e": 2, "f": 4},
        }).execute()

        result = (
            session.query(k).bin("ratings").on_map_value_range(4, 6).count()
            .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == 4

        session.upsert(k).bin("ratings").on_map_key("c").set_to(5).execute()
        session.upsert(k).bin("ratings").on_map_key("e").remove().execute()

        result = (
            session.query(k).bin("ratings").on_map_value_range(4, 6).count()
            .execute().first_or_raise()
        )
        assert result.record.bins["ratings"] == 5


# ===================================================================
# CDT reads within write operations
# ===================================================================

class TestCdtReadsInWriteContext:
    """CDT read operations (get_values, count) issued as part of an upsert/update."""

    def test_cdt_read_in_upsert(self, cluster):
        """Read a CDT value as part of an upsert; the result is returned."""
        session = cluster.create_session()
        k = DS.id(28)
        session.upsert(k).put({"info": {"name": "Alice", "age": 30}}).execute()

        rs = (
            session.upsert(k)
            .bin("info").on_map_key("name").get_values()
            .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["info"] == "Alice"

    def test_cdt_count_in_upsert(self, cluster):
        """Count CDT elements in an upsert context."""
        session = cluster.create_session()
        k = DS.id(29)
        session.upsert(k).put({"tags": {"a": 1, "b": 2, "c": 3}}).execute()

        rs = (
            session.upsert(k)
            .bin("tags").on_map_key_range("a", "c").count()
            .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["tags"] == 2

    def test_mixed_cdt_read_and_write_in_upsert(self, cluster):
        """Mix CDT reads and writes on different bins in a single upsert."""
        session = cluster.create_session()
        k = DS.id(30)
        session.upsert(k).put({
            "rooms": {"r1": {"rate": 100}, "r2": {"rate": 200}},
            "meta": {"ver": 1},
        }).execute()

        rs = (
            session.upsert(k)
            .bin("rooms").on_map_key("r1").get_values()
            .bin("meta").on_map_key("ver").set_to(2)
            .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["rooms"] == {"rate": 100}

        verify = session.query(k).execute().first_or_raise()
        assert verify.record.bins["meta"]["ver"] == 2

    def test_cdt_read_and_flat_write_in_upsert(self, cluster):
        """CDT read + flat bin write in the same upsert."""
        session = cluster.create_session()
        k = DS.id(31)
        session.upsert(k).put({"scores": {"math": 90, "sci": 85}}).execute()

        rs = (
            session.upsert(k)
            .bin("scores").on_map_key("math").get_values()
            .bin("status").set_to("reviewed")
            .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["scores"] == 90

        verify = session.query(k).execute().first_or_raise()
        assert verify.record.bins["status"] == "reviewed"

    def test_inverted_count_in_write_context(self, cluster):
        """Inverted count (count_all_others) in an upsert context."""
        session = cluster.create_session()
        k = DS.id(32)
        session.upsert(k).put({
            "data": {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
        }).execute()

        rs = (
            session.upsert(k)
            .bin("data").on_map_key_range("b", "d").count_all_others()
            .execute()
        )
        result = rs.first_or_raise()
        assert result.record.bins["data"] == 3


# ===================================================================
# Collection-level map operations
# ===================================================================

class TestMapCollectionOps:
    """Top-level and nested map collection CDT operations."""

    def test_map_clear_top_level(self, cluster):
        session = cluster.create_session()
        k = DS.id(33)
        session.upsert(k).put({"m": {"a": 1, "b": 2}}).execute()

        session.update(k).bin("m").map_clear().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["m"] == {}

    def test_map_clear_nested(self, cluster):
        session = cluster.create_session()
        k = DS.id(34)
        session.upsert(k).put({"outer": {"inner": {"x": 1, "y": 2}}}).execute()

        session.update(k).bin("outer").on_map_key("inner").map_clear().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["outer"]["inner"] == {}

    def test_map_upsert_items(self, cluster):
        session = cluster.create_session()
        k = DS.id(35)
        session.upsert(k).put({"m": {"a": 1}}).execute()

        session.update(k).bin("m").map_upsert_items({"b": 2, "c": 3}).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["m"] == {"a": 1, "b": 2, "c": 3}

    def test_map_insert_items_new_keys_only(self, cluster):
        session = cluster.create_session()
        k = DS.id(36)
        session.upsert(k).put({"m": {"a": 1}}).execute()

        session.update(k).bin("m").map_insert_items({"b": 2}).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["m"] == {"a": 1, "b": 2}

    def test_map_update_items_existing_keys_only(self, cluster):
        session = cluster.create_session()
        k = DS.id(37)
        session.upsert(k).put({"m": {"a": 1, "b": 2}}).execute()

        session.update(k).bin("m").map_update_items({"a": 10}).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["m"] == {"a": 10, "b": 2}

    def test_map_insert_items_rejects_existing_key(self, cluster):
        """CREATE_ONLY refuses to overwrite an existing map key."""
        session = cluster.create_session()
        k = DS.id(49)
        session.upsert(k).put({"m": {"a": 1}}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            session.update(k).bin("m").map_insert_items({"a": 2}).execute()
        assert exc_info.value.result_code == ResultCode.ELEMENT_EXISTS

    def test_map_update_items_rejects_new_key(self, cluster):
        """UPDATE_ONLY refuses to create a map entry for a missing key."""
        session = cluster.create_session()
        k = DS.id(50)
        session.upsert(k).put({"m": {"a": 1}}).execute()

        with pytest.raises(AerospikeError) as exc_info:
            session.update(k).bin("m").map_update_items({"b": 2}).execute()
        assert exc_info.value.result_code == ResultCode.ELEMENT_NOT_FOUND

    def test_map_create_then_upsert_key_ordered(self, cluster):
        session = cluster.create_session()
        k = DS.id(38)
        session.upsert(k).put({"p": 1}).execute()

        session.update(k).bin("ord").map_create(MapOrder.KEY_ORDERED).execute()
        session.update(k).bin("ord").map_upsert_items({"c": 3, "a": 1, "b": 2}).execute()

        result = session.query(k).execute().first_or_raise()
        assert list(result.record.bins["ord"].keys()) == ["a", "b", "c"]

    def test_map_set_policy_key_ordered(self, cluster):
        session = cluster.create_session()
        k = DS.id(39)
        session.upsert(k).put({"m": {"z": 1, "a": 2}}).execute()

        session.update(k).bin("m").map_set_policy(MapOrder.KEY_ORDERED).execute()

        result = session.query(k).execute().first_or_raise()
        assert list(result.record.bins["m"].keys()) == ["a", "z"]

    def test_map_size_in_update_context(self, cluster):
        session = cluster.create_session()
        k = DS.id(40)
        session.upsert(k).put({"m": {"x": 1, "y": 2, "z": 3}}).execute()

        rs = session.update(k).bin("m").map_size().execute()
        result = rs.first_or_raise()
        assert result.record.bins["m"] == 3


# ===================================================================
# Collection-level list operations
# ===================================================================

class TestListCollectionOps:
    """List clear, sort, bulk append, create, and order."""

    def test_list_clear(self, cluster):
        session = cluster.create_session()
        k = DS.id(41)
        session.upsert(k).put({"lst": [1, 2, 3]}).execute()

        session.update(k).bin("lst").list_clear().execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["lst"] == []

    def test_list_sort_descending(self, cluster):
        session = cluster.create_session()
        k = DS.id(42)
        session.upsert(k).put({"lst": [1, 5, 3, 2]}).execute()

        session.update(k).bin("lst").list_sort(ListSortFlags.DESCENDING).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["lst"] == [5, 3, 2, 1]

    def test_list_append_items(self, cluster):
        session = cluster.create_session()
        k = DS.id(43)
        session.upsert(k).put({"lst": [1, 2]}).execute()

        session.update(k).bin("lst").list_append_items([3, 4]).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["lst"] == [1, 2, 3, 4]

    def test_list_add_items_ordered(self, cluster):
        session = cluster.create_session()
        k = DS.id(44)
        session.upsert(k).put({"name": "x", "scores": []}).execute()
        session.update(k).bin("scores").list_set_order(
            ListOrderType.ORDERED,
        ).execute()
        session.update(k).bin("scores").list_add_items([30, 10, 20]).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["scores"] == [10, 20, 30]

    def test_empty_list_then_append_items(self, cluster):
        session = cluster.create_session()
        k = DS.id(45)
        session.upsert(k).put({"t": 1, "items": []}).execute()
        session.update(k).bin("items").list_append_items(["a", "b"]).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["items"] == ["a", "b"]

    def test_list_set_order(self, cluster):
        session = cluster.create_session()
        k = DS.id(46)
        session.upsert(k).put({"lst": [3, 1, 2]}).execute()

        session.update(k).bin("lst").list_set_order(ListOrderType.ORDERED).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["lst"] == [1, 2, 3]


# ===================================================================
# Nested collection-level after on_map_key
# ===================================================================

class TestNestedMapCollectionOps:
    """Collection-level ops with accumulated CDT context."""

    def test_nested_map_upsert_items(self, cluster):
        session = cluster.create_session()
        k = DS.id(47)
        session.upsert(k).put({"root": {"inner": {"a": 1}}}).execute()

        (
            session.update(k)
            .bin("root").on_map_key("inner").map_upsert_items({"b": 2})
            .execute()
        )

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["root"]["inner"] == {"a": 1, "b": 2}

    def test_nested_map_size_read_path(self, cluster):
        session = cluster.create_session()
        k = DS.id(48)
        session.upsert(k).put({"data": {"sub": {"u": 1, "v": 2}}}).execute()

        result = (
            session.query(k).bin("data").on_map_key("sub").map_size().execute()
            .first_or_raise()
        )
        assert result.record.bins["data"] == 2


# ===================================================================
# Relative range navigation (key/value anchors)
# ===================================================================

class TestRelativeRangeNavigation:
    """Map/list CDT ranges relative to an anchor key or value."""

    def _seed_key_ordered_map(self, session, k):
        session.upsert(k).put({"p": 1}).execute()
        session.update(k).bin("mapbin").map_create(MapOrder.KEY_ORDERED).execute()
        session.update(k).bin("mapbin").map_upsert_items(
            {0: 17, 4: 2, 5: 15, 9: 10},
        ).execute()

    def _seed_ordered_list(self, session, k):
        session.upsert(k).put({"lst": []}).execute()
        session.update(k).bin("lst").list_set_order(ListOrderType.ORDERED).execute()
        session.update(k).bin("lst").list_add_items(
            [0, 4, 5, 9, 11, 15],
        ).execute()

    def test_map_get_by_key_relative_index_range(self, cluster):
        session = cluster.create_session()
        k = DS.id(52)
        self._seed_key_ordered_map(session, k)

        rs = (
            session.update(k)
            .bin("mapbin")
            .on_map_key_relative_index_range(5, 0, None)
            .get_keys()
            .execute()
        )
        flat = _flatten_cdt_values(rs.first_or_raise().record.bins["mapbin"])
        assert 5 in flat
        assert 9 in flat

    def test_map_get_by_value_relative_rank_range(self, cluster):
        session = cluster.create_session()
        k = DS.id(53)
        self._seed_key_ordered_map(session, k)

        rs = (
            session.update(k)
            .bin("mapbin")
            .on_map_value_relative_rank_range(11, 1, None)
            .get_values()
            .execute()
        )
        flat = _flatten_cdt_values(rs.first_or_raise().record.bins["mapbin"])
        assert [int(x) for x in flat] == [17]

    def test_map_remove_by_key_relative_index_range(self, cluster):
        session = cluster.create_session()
        k = DS.id(54)
        self._seed_key_ordered_map(session, k)

        (
            session.update(k)
            .bin("mapbin")
            .on_map_key_relative_index_range(5, 0, None)
            .remove()
            .execute()
        )

        rec = session.query(k).execute().first_or_raise()
        assert rec.record.bins["mapbin"] == {0: 17, 4: 2}

    def test_map_remove_by_value_relative_rank_range(self, cluster):
        session = cluster.create_session()
        k = DS.id(55)
        self._seed_key_ordered_map(session, k)

        (
            session.update(k)
            .bin("mapbin")
            .on_map_value_relative_rank_range(11, 1, None)
            .remove()
            .execute()
        )

        rec = session.query(k).execute().first_or_raise()
        assert rec.record.bins["mapbin"] == {4: 2, 5: 15, 9: 10}

    def test_list_get_by_value_relative_rank_range(self, cluster):
        session = cluster.create_session()
        k = DS.id(56)
        self._seed_ordered_list(session, k)

        rs = (
            session.update(k)
            .bin("lst")
            .on_list_value_relative_rank_range(5, 0, None)
            .get_values()
            .execute()
        )
        flat = _flatten_cdt_values(rs.first_or_raise().record.bins["lst"])
        assert [int(x) for x in flat] == [5, 9, 11, 15]

    def test_list_remove_by_value_relative_rank_range(self, cluster):
        session = cluster.create_session()
        k = DS.id(57)
        self._seed_ordered_list(session, k)

        (
            session.update(k)
            .bin("lst")
            .on_list_value_relative_rank_range(5, 0, None)
            .remove()
            .execute()
        )

        rec = session.query(k).execute().first_or_raise()
        assert rec.record.bins["lst"] == [0, 4]

    def test_relative_range_unbounded_count_none(self, cluster):
        session = cluster.create_session()
        k = DS.id(58)
        self._seed_key_ordered_map(session, k)
        self._seed_ordered_list(session, k)

        rs_map = (
            session.update(k)
            .bin("mapbin")
            .on_map_key_relative_index_range(5, 0, None)
            .get_keys()
            .execute()
        )
        assert _flatten_cdt_values(rs_map.first_or_raise().record.bins["mapbin"])

        rs_list = (
            session.update(k)
            .bin("lst")
            .on_list_value_relative_rank_range(5, 0, None)
            .get_values()
            .execute()
        )
        assert _flatten_cdt_values(rs_list.first_or_raise().record.bins["lst"])

    def test_relative_range_with_count(self, cluster):
        session = cluster.create_session()
        k = DS.id(59)
        self._seed_ordered_list(session, k)

        rs = (
            session.update(k)
            .bin("lst")
            .on_list_value_relative_rank_range(5, 0, 2)
            .get_values()
            .execute()
        )
        flat = _flatten_cdt_values(rs.first_or_raise().record.bins["lst"])
        assert 5 in flat
        assert 9 in flat

    def test_relative_range_inverted_get_all_other_values(self, cluster):
        session = cluster.create_session()
        k = DS.id(60)
        self._seed_ordered_list(session, k)

        rs = (
            session.update(k)
            .bin("lst")
            .on_list_value_relative_rank_range(5, 0, 2)
            .get_all_other_values()
            .execute()
        )
        flat = _flatten_cdt_values(rs.first_or_raise().record.bins["lst"])
        assert 0 in flat or 4 in flat or 11 in flat or 15 in flat

    def test_query_read_path_key_relative_index_range(self, cluster):
        session = cluster.create_session()
        k = DS.id(61)
        self._seed_key_ordered_map(session, k)

        rs = (
            session.query(k)
            .bin("mapbin")
            .on_map_key_relative_index_range(5, 0, None)
            .get_keys()
            .execute()
        )
        flat = _flatten_cdt_values(rs.first_or_raise().record.bins["mapbin"])
        assert 5 in flat
        assert 9 in flat


# ===================================================================
# Relative-range multi-op batches: exact per-op result ordering
# ===================================================================

class TestRelativeRangeBatchOrdering:
    """Batched CDT reads: per-op return lists in operate order.

    Multi-op **remove** batches do not surface the same per-op list in the
    PAC record (map bin may be absent; list may expose only the first op).
    Remove coverage uses a trailing ``.get()`` and asserts final bin state.
    """

    def test_operate_map_get_relative_batch(self, cluster):
        session = cluster.create_session()
        k = DS.id(70)
        session.delete(k).execute()
        session.upsert(k).put({"p": 1}).execute()
        session.update(k).bin("mapbin").map_create(MapOrder.KEY_ORDERED).execute()
        session.update(k).bin("mapbin").map_upsert_items(
            {0: 17, 4: 2, 5: 15, 9: 10},
        ).execute()

        b = session.update(k)
        b = b.bin("mapbin").on_map_key_relative_index_range(5, 0, None).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(5, 1, None).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(5, -1, None).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(3, 2, None).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(3, -2, None).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(5, 0, 1).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(5, 1, 2).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(5, -1, 1).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(3, 2, 1).get_keys()
        b = b.bin("mapbin").on_map_key_relative_index_range(3, -2, 2).get_keys()
        b = b.bin("mapbin").on_map_value_relative_rank_range(11, 1, None).get_values()
        b = b.bin("mapbin").on_map_value_relative_rank_range(11, -1, None).get_values()
        b = b.bin("mapbin").on_map_value_relative_rank_range(11, 1, 1).get_values()
        b = b.bin("mapbin").on_map_value_relative_rank_range(11, -1, 1).get_values()
        rs = b.execute()
        raw = rs.first_or_raise().record.bins["mapbin"]
        _assert_map_get_relative_batch(raw)

    def test_operate_list_get_relative_batch(self, cluster):
        session = cluster.create_session()
        k = DS.id(71)
        session.delete(k).execute()
        session.upsert(k).put({"x": 1}).execute()
        b = session.update(k)
        b = b.bin("lst").list_add_items([0, 4, 5, 9, 11, 15])
        b = b.bin("lst").on_list_value_relative_rank_range(5, 0, None).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(5, 1, None).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(5, -1, None).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(3, 0, None).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(3, 3, None).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(3, -3, None).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(5, 0, 2).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(5, 1, 1).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(5, -1, 2).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(3, 0, 1).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(3, 3, 7).get_values()
        b = b.bin("lst").on_list_value_relative_rank_range(3, -3, 2).get_values()
        rs = b.execute()
        raw = rs.first_or_raise().record.bins["lst"]
        _assert_list_get_relative_batch(raw)

    def test_operate_map_remove_relative_final_state(self, cluster):
        session = cluster.create_session()
        k = DS.id(72)
        session.delete(k).execute()
        session.upsert(k).put({"p": 1}).execute()
        session.update(k).bin("mapbin").map_create(MapOrder.KEY_ORDERED).execute()
        session.update(k).bin("mapbin").map_upsert_items(
            {0: 17, 4: 2, 5: 15, 9: 10},
        ).execute()

        rs = (
            session.update(k)
            .bin("mapbin").on_map_key_relative_index_range(5, 0, None).remove()
            .bin("mapbin").on_map_key_relative_index_range(5, 1, None).remove()
            .bin("mapbin").on_map_key_relative_index_range(5, -1, 1).remove()
            .bin("mapbin").get()
            .execute()
        )
        m = rs.first_or_raise().record.bins["mapbin"]
        assert m == {0: 17}

        session.delete(k).execute()
        session.upsert(k).put({"p": 1}).execute()
        session.update(k).bin("mapbin").map_create(MapOrder.KEY_ORDERED).execute()
        session.update(k).bin("mapbin").map_upsert_items(
            {0: 17, 4: 2, 5: 15, 9: 10},
        ).execute()

        rs2 = (
            session.update(k)
            .bin("mapbin").on_map_value_relative_rank_range(11, 1, None).remove()
            .bin("mapbin").on_map_value_relative_rank_range(11, -1, 1).remove()
            .bin("mapbin").get()
            .execute()
        )
        m2 = rs2.first_or_raise().record.bins["mapbin"]
        assert m2 == {4: 2, 5: 15}

    def test_operate_list_remove_relative_final_state(self, cluster):
        session = cluster.create_session()
        k = DS.id(73)
        session.delete(k).execute()
        session.upsert(k).put({"x": 1}).execute()
        session.update(k).bin("lst").list_add_items([0, 4, 5, 9, 11, 15]).execute()
        rs = (
            session.update(k)
            .bin("lst").on_list_value_relative_rank_range(5, 0, None).remove()
            .bin("lst").on_list_value_relative_rank_range(5, 1, None).remove()
            .bin("lst").on_list_value_relative_rank_range(5, -1, None).remove()
            .bin("lst").on_list_value_relative_rank_range(3, -3, 1).remove()
            .bin("lst").on_list_value_relative_rank_range(3, -3, 2).remove()
            .bin("lst").on_list_value_relative_rank_range(3, -3, 3).remove()
            .bin("lst").get()
            .execute()
        )
        lst = rs.first_or_raise().record.bins["lst"]
        assert lst == []


# ===================================================================
# List pop, trim, and remove_range
# ===================================================================

class TestListPopAndTrim:
    """Index-based list removal: pop, pop_range, trim, remove_range."""

    def test_list_pop_returns_element(self, cluster):
        """Pop index 0; returned value and remaining list match expectations."""
        session = cluster.create_session()
        k = DS.id(100)
        session.upsert(k).put({"items": ["a", "b", "c"]}).execute()

        rs = session.update(k).bin("items").list_pop(0).execute()
        out = rs.first_or_raise()
        assert out.record.bins["items"] == "a"

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["items"] == ["b", "c"]

    def test_list_pop_range_returns_elements(self, cluster):
        """Pop two elements starting at index 1; verify slice and remainder."""
        session = cluster.create_session()
        k = DS.id(101)
        session.upsert(k).put({"nums": [10, 20, 30, 40]}).execute()

        rs = session.update(k).bin("nums").list_pop_range(1, 2).execute()
        out = rs.first_or_raise()
        assert out.record.bins["nums"] == [20, 30]

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [10, 40]

    def test_list_pop_range_to_end(self, cluster):
        """Pop from index through the end when count is None."""
        session = cluster.create_session()
        k = DS.id(102)
        session.upsert(k).put({"nums": [10, 20, 30, 40]}).execute()

        rs = session.update(k).bin("nums").list_pop_range(2, None).execute()
        out = rs.first_or_raise()
        assert list(out.record.bins["nums"]) == [30, 40]

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [10, 20]

    def test_list_trim_keeps_middle(self, cluster):
        """trim(index, count) keeps only that slice of the list."""
        session = cluster.create_session()
        k = DS.id(103)
        session.upsert(k).put({"letters": ["a", "b", "c", "d"]}).execute()

        session.update(k).bin("letters").list_trim(1, 2).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["letters"] == ["b", "c"]

    def test_list_remove_range(self, cluster):
        """remove_range removes a fixed count from an index."""
        session = cluster.create_session()
        k = DS.id(104)
        session.upsert(k).put({"nums": [1, 2, 3, 4]}).execute()

        session.update(k).bin("nums").list_remove_range(0, 2).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [3, 4]

    def test_list_remove_range_to_end(self, cluster):
        """remove_range with count None removes through the end."""
        session = cluster.create_session()
        k = DS.id(105)
        session.upsert(k).put({"nums": [1, 2, 3, 4]}).execute()

        session.update(k).bin("nums").list_remove_range(2, None).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [1, 2]

    def test_nested_list_pop(self, cluster):
        """list_pop after on_map_key on a nested list."""
        session = cluster.create_session()
        k = DS.id(106)
        session.upsert(k).put({"wrap": {"items": [1, 2, 3]}}).execute()

        rs = session.update(k).bin("wrap").on_map_key("items").list_pop(0).execute()
        out = rs.first_or_raise()
        assert int(out.record.bins["wrap"]) == 1

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["wrap"]["items"] == [2, 3]


class TestListInsertItems:
    """Bulk list insert via list_insert_items."""

    def test_list_insert_items(self, cluster):
        """Insert multiple values at an index."""
        session = cluster.create_session()
        k = DS.id(107)
        session.upsert(k).put({"nums": [1, 2, 3]}).execute()

        session.update(k).bin("nums").list_insert_items(1, [10, 20]).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["nums"] == [1, 10, 20, 2, 3]

    def test_nested_list_insert_items(self, cluster):
        """list_insert_items on a list under a map key."""
        session = cluster.create_session()
        k = DS.id(108)
        session.upsert(k).put({"outer": {"data": [1, 2]}}).execute()

        session.update(k).bin("outer").on_map_key("data").list_insert_items(0, [7, 8]).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["outer"]["data"] == [7, 8, 1, 2]


class TestListSortDropDuplicates:
    """List sort flags beyond descending order."""

    def test_list_sort_drop_duplicates(self, cluster):
        """Sort with DROP_DUPLICATES collapses equal elements."""
        session = cluster.create_session()
        k = DS.id(109)
        session.upsert(k).put({"lst": [3, 1, 2, 1, 3]}).execute()

        session.update(k).bin("lst").list_sort(ListSortFlags.DROP_DUPLICATES).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["lst"] == [1, 2, 3]


class TestNoneElementsInList:
    """Round-trip for null list elements."""

    def test_none_elements_round_trip(self, cluster):
        """None in a list is stored and read back as null."""
        session = cluster.create_session()
        k = DS.id(110)
        session.upsert(k).put({"tags": ["x"]}).execute()

        session.update(k).bin("tags").list_append_items([None, "y"]).execute()

        result = session.query(k).execute().first_or_raise()
        assert result.record.bins["tags"][1] is None
        assert result.record.bins["tags"] == ["x", None, "y"]


# ===================================================================
# Batch CDT operations (multi-key)
# ===================================================================

class TestBatchCdtWrite:
    """Multi-key batch writes combined with CDT operations."""

    def test_batch_uniform_map_key_set_to(self, cluster):
        session = cluster.create_session()
        k1, k2, k3 = DS.id(80), DS.id(81), DS.id(82)
        for k in (k1, k2, k3):
            session.upsert(k).put({"info": {"a": 0, "b": 0}}).execute()

        (
            session.update(k1, k2, k3)
            .bin("info").on_map_key("a").set_to(99)
            .execute()
        )

        for k in (k1, k2, k3):
            rec = session.query(k).execute().first_or_raise()
            assert rec.record.bins["info"]["a"] == 99
            assert rec.record.bins["info"]["b"] == 0

    def test_batch_per_key_cdt_ops(self, cluster):
        session = cluster.create_session()
        k1, k2 = DS.id(83), DS.id(84)
        session.upsert(k1).put({"m": {"x": 1}}).execute()
        session.upsert(k2).put({"m": {"y": 2}}).execute()

        (
            session
            .update(k1).bin("m").on_map_key("x").set_to(10)
            .update(k2).bin("m").on_map_key("y").set_to(20)
            .execute()
        )

        r1 = session.query(k1).execute().first_or_raise()
        assert r1.record.bins["m"]["x"] == 10
        r2 = session.query(k2).execute().first_or_raise()
        assert r2.record.bins["m"]["y"] == 20

    def test_batch_mixed_cdt_and_scalar(self, cluster):
        session = cluster.create_session()
        k1, k2 = DS.id(85), DS.id(86)
        session.upsert(k1).put({"name": "old", "m": {"k": 0}}).execute()
        session.upsert(k2).put({"name": "old"}).execute()

        (
            session
            .update(k1).bin("name").set_to("new").bin("m").on_map_key("k").set_to(5)
            .update(k2).bin("name").set_to("new")
            .execute()
        )

        r1 = session.query(k1).execute().first_or_raise()
        assert r1.record.bins["name"] == "new"
        assert r1.record.bins["m"]["k"] == 5
        r2 = session.query(k2).execute().first_or_raise()
        assert r2.record.bins["name"] == "new"

    def test_batch_cdt_collection_clear(self, cluster):
        session = cluster.create_session()
        k1, k2 = DS.id(87), DS.id(88)
        session.upsert(k1).put({"m": {"a": 1, "b": 2}}).execute()
        session.upsert(k2).put({"m": {"c": 3, "d": 4}}).execute()

        (
            session.update(k1, k2)
            .bin("m").map_clear()
            .execute()
        )

        for k in (k1, k2):
            rec = session.query(k).execute().first_or_raise()
            assert rec.record.bins["m"] == {}

    def test_batch_cdt_remove(self, cluster):
        session = cluster.create_session()
        k1, k2 = DS.id(89), DS.id(90)
        session.upsert(k1).put({"m": {"keep": 1, "drop": 2}}).execute()
        session.upsert(k2).put({"m": {"keep": 3, "drop": 4}}).execute()

        (
            session.update(k1, k2)
            .bin("m").on_map_key("drop").remove()
            .execute()
        )

        for k in (k1, k2):
            rec = session.query(k).execute().first_or_raise()
            assert "drop" not in rec.record.bins["m"]
            assert "keep" in rec.record.bins["m"]

    def test_batch_cdt_list_add_items(self, cluster):
        session = cluster.create_session()
        k1, k2 = DS.id(91), DS.id(92)
        session.upsert(k1).put({"tags": []}).execute()
        session.upsert(k2).put({"tags": []}).execute()

        (
            session.update(k1, k2)
            .bin("tags").list_append_items(["a", "b"])
            .execute()
        )

        for k in (k1, k2):
            rec = session.query(k).execute().first_or_raise()
            assert rec.record.bins["tags"] == ["a", "b"]
