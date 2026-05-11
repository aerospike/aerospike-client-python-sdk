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

"""Integration tests for nested CDT navigation, ranges, create-if-missing, and value chaining."""

import pytest
import pytest_asyncio

from aerospike_async import ListOrderType, MapOrder

from aerospike_sdk import Client, DataSet, ListReturnType, MapReturnType


NS = "test"
SET = "cdt_navigation"
DS = DataSet.of(NS, SET)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy):
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        session = c.create_session()
        for i in range(1, 40):
            await session.delete(DS.id(i)).execute()
        yield c


def _key(n: int):
    return DS.id(n)


def _cdt_remove_payload(bins: dict, bin_name: str):
    """Normalize CDT remove return data for a segment with only that remove op."""
    v = bins.get(bin_name)
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


# ===================================================================
# Nested navigation (2+ levels)
# ===================================================================


class TestNestedMapNavigationDeep:

    async def test_three_level_map_read(self, client):
        session = client.create_session()
        k = _key(1)
        await session.upsert(k).put({
            "doc": {"mid": {"leaf": 7}},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("doc").on_map_key("mid").on_map_key("leaf").get_values()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["doc"] == 7

    async def test_three_level_map_write_set_to(self, client):
        session = client.create_session()
        k = _key(2)
        await session.upsert(k).put({
            "doc": {"mid": {"leaf": 1}},
        }).execute()

        await (
            session.update(k)
                .bin("doc").on_map_key("mid").on_map_key("leaf").set_to(99)
                .execute()
        )

        rs = await (
            await session.query(k)
                .bin("doc").on_map_key("mid").on_map_key("leaf").get_values()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["doc"] == 99

    async def test_map_then_list_then_map_read(self, client):
        session = client.create_session()
        k = _key(3)
        await session.upsert(k).put({
            "root": {"items": [{"id": 1, "v": 10}, {"id": 2, "v": 20}]},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("root").on_map_key("items").on_list_index(1).on_map_key("v").get_values()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["root"] == 20


# ===================================================================
# Range navigation on nested builders
# ===================================================================


class TestNestedRangeNavigation:

    async def test_nested_map_key_range_count(self, client):
        session = client.create_session()
        k = _key(4)
        await session.upsert(k).put({
            "outer": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("outer").on_map_key_range("b", "d").count()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["outer"] == 2

    async def test_nested_map_value_range_get_values(self, client):
        session = client.create_session()
        k = _key(5)
        await session.upsert(k).put({
            "scores": {"alice": 80, "bob": 90, "carol": 95},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("scores").on_map_value_range(85, 100).get_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["scores"]
        assert isinstance(vals, list)
        assert sorted(int(x) for x in vals) == [90, 95]

    async def test_nested_list_index_range_count(self, client):
        session = client.create_session()
        k = _key(6)
        await session.upsert(k).put({
            "wrap": {"nums": [10, 20, 30, 40, 50]},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("wrap").on_map_key("nums").on_list_index_range(1, 3).count()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["wrap"] == 3


# ===================================================================
# Create-if-missing (map key / list index)
# ===================================================================


class TestCreateIfMissing:

    async def test_on_map_key_create_type_adds_key(self, client):
        session = client.create_session()
        k = _key(7)
        await session.upsert(k).put({"shell": {"existing": 1}}).execute()

        await (
            session.update(k)
                .bin("shell")
                .on_map_key("brand_new", create_type=MapOrder.UNORDERED)
                .set_to(42)
                .execute()
        )

        rs = await (
            await session.query(k)
                .bin("shell").on_map_key("brand_new").get_values()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["shell"] == 42

    async def test_nested_on_map_key_create_type(self, client):
        session = client.create_session()
        k = _key(8)
        await session.upsert(k).put({
            "root": {"inner": {"x": 0}},
        }).execute()

        await (
            session.update(k)
                .bin("root").on_map_key("inner")
                .on_map_key("y", create_type=MapOrder.KEY_ORDERED)
                .set_to(100)
                .execute()
        )

        rs = await (
            await session.query(k)
                .bin("root").on_map_key("inner").on_map_key("y").get_values()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["root"] == 100

    async def test_on_list_index_pad_nested_list(self, client):
        session = client.create_session()
        k = _key(9)
        await session.upsert(k).put({
            "m": {"lst": [1, 2]},
        }).execute()

        await (
            session.update(k)
                .bin("m").on_map_key("lst")
                .on_list_index(5, order=ListOrderType.UNORDERED, pad=True)
                .list_set(0, 9)
                .execute()
        )

        rs = await (
            await session.query(k).bin("m").on_map_key("lst").list_get_range(0, None).execute()
        ).first_or_raise()
        raw = rs.record.bins["m"]
        assert isinstance(raw, list)
        assert raw[0] == 1
        assert raw[1] == 2
        # list_set index is relative to the navigated slot; a new padded cell is a list.
        assert raw[5] == [9]


# ===================================================================
# Value-selector chaining
# ===================================================================


class TestValueSelectorChaining:

    async def test_nested_list_value_count(self, client):
        session = client.create_session()
        k = _key(10)
        await session.upsert(k).put({
            "m": {"nums": [1, 5, 5, 5, 2]},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("m").on_map_key("nums").on_list_value(5).count()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["m"] == 3

    async def test_nested_list_value_remove(self, client):
        session = client.create_session()
        k = _key(11)
        await session.upsert(k).put({
            "m": {"nums": [1, 9, 9, 3]},
        }).execute()

        await (
            session.update(k)
                .bin("m").on_map_key("nums").on_list_value(9).remove()
                .execute()
        )

        rs = await (
            await session.query(k).bin("m").on_map_key("nums").list_get_range(0, None).execute()
        ).first_or_raise()
        lst = rs.record.bins["m"]
        assert [int(x) for x in lst] == [1, 3]

    async def test_bin_list_value_then_inverted_read(self, client):
        session = client.create_session()
        k = _key(12)
        await session.upsert(k).put({"tags": [10, 20, 10, 30]}).execute()

        rs = await (
            await session.query(k)
                .bin("tags").on_list_value(10).get_all_other_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["tags"]
        assert isinstance(vals, list)
        assert 10 not in [int(x) for x in vals]
        assert 20 in [int(x) for x in vals]
        assert 30 in [int(x) for x in vals]


# ===================================================================
# Optional: SpecialValue open range (requires PAC with SpecialValue)
# ===================================================================


class TestSpecialValueOpenRange:

    @pytest.fixture(autouse=True)
    def _require_special_value(self):
        try:
            from aerospike_async import SpecialValue  # noqa: F401
        except ImportError:
            pytest.skip("aerospike_async.SpecialValue not available")

    async def test_nested_map_key_range_to_infinity(self, client):
        from aerospike_async import SpecialValue

        session = client.create_session()
        k = _key(13)
        await session.upsert(k).put({
            "m": {"a": 1, "m": 2, "z": 3},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("m").on_map_key_range("m", SpecialValue.INFINITY).count()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["m"] == 2

    async def test_wildcard_in_map_value_list(self, client):
        """WILDCARD matches all values in a value-list selector."""
        from aerospike_async import SpecialValue

        session = client.create_session()
        k = _key(14)
        await session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("m").on_map_value_list([SpecialValue.WILDCARD]).get_keys()
                .execute()
        ).first_or_raise()
        keys = rs.record.bins["m"]
        assert sorted(keys) == ["a", "b", "c"]

    async def test_list_value_range_to_infinity(self, client):
        """SpecialValue.INFINITY as upper bound on list value range."""
        from aerospike_async import SpecialValue

        session = client.create_session()
        k = _key(15)
        await session.upsert(k).put({"nums": [10, 20, 30, 40, 50]}).execute()

        rs = await (
            await session.query(k)
                .bin("nums").on_list_value_range(25, SpecialValue.INFINITY).get_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["nums"]
        assert sorted(int(x) for x in vals) == [30, 40, 50]


# ===================================================================
# on_map_value() single selector
# ===================================================================


class TestMapValueSelector:

    async def test_on_map_value_count(self, client):
        """Count map entries matching a specific value."""
        session = client.create_session()
        k = _key(16)
        await session.upsert(k).put({
            "grades": {"alice": "A", "bob": "B", "carol": "A", "dave": "C"},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("grades").on_map_value("A").count()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["grades"] == 2

    async def test_on_map_value_get_keys(self, client):
        """Get keys for entries matching a specific value."""
        session = client.create_session()
        k = _key(17)
        await session.upsert(k).put({
            "colors": {"apple": "red", "sky": "blue", "fire": "red"},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("colors").on_map_value("red").get_keys()
                .execute()
        ).first_or_raise()
        keys = rs.record.bins["colors"]
        assert sorted(keys) == ["apple", "fire"]

    async def test_on_map_value_remove(self, client):
        """Remove map entries by value."""
        session = client.create_session()
        k = _key(18)
        await session.upsert(k).put({
            "scores": {"a": 10, "b": 20, "c": 10},
        }).execute()

        await (
            session.update(k)
                .bin("scores").on_map_value(10).remove()
                .execute()
        )

        rs = await (await session.query(k).execute()).first_or_raise()
        assert rs.record.bins["scores"] == {"b": 20}


# ===================================================================
# on_list_value_range
# ===================================================================


class TestListValueRange:

    async def test_list_value_range_get_values(self, client):
        """Select list elements by value range."""
        session = client.create_session()
        k = _key(19)
        await session.upsert(k).put({"nums": [5, 15, 25, 35, 45]}).execute()

        rs = await (
            await session.query(k)
                .bin("nums").on_list_value_range(10, 30).get_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["nums"]
        assert sorted(int(x) for x in vals) == [15, 25]

    async def test_list_value_range_remove(self, client):
        """Remove list elements in a value range."""
        session = client.create_session()
        k = _key(20)
        await session.upsert(k).put({"nums": [5, 15, 25, 35, 45]}).execute()

        await (
            session.update(k)
                .bin("nums").on_list_value_range(10, 30).remove()
                .execute()
        )

        rs = await (await session.query(k).execute()).first_or_raise()
        remaining = [int(x) for x in rs.record.bins["nums"]]
        assert sorted(remaining) == [5, 35, 45]


# ===================================================================
# on_map_key_list / on_map_value_list / on_list_value_list
# ===================================================================


class TestListSelectors:

    async def test_on_map_key_list_get_values(self, client):
        """Select map entries by a list of keys."""
        session = client.create_session()
        k = _key(21)
        await session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("m").on_map_key_list(["a", "c"]).get_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["m"]
        assert sorted(int(x) for x in vals) == [1, 3]

    async def test_on_map_value_list_get_keys(self, client):
        """Select map entries by a list of values."""
        session = client.create_session()
        k = _key(22)
        await session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 1, "d": 3},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("m").on_map_value_list([1, 3]).get_keys()
                .execute()
        ).first_or_raise()
        keys = rs.record.bins["m"]
        assert sorted(keys) == ["a", "c", "d"]

    async def test_on_list_value_list_get_values(self, client):
        """Select list elements matching any value in a list."""
        session = client.create_session()
        k = _key(23)
        await session.upsert(k).put({"nums": [1, 2, 3, 4, 5, 3]}).execute()

        rs = await (
            await session.query(k)
                .bin("nums").on_list_value_list([2, 4]).get_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["nums"]
        assert sorted(int(x) for x in vals) == [2, 4]

    async def test_on_map_key_list_remove(self, client):
        """Remove map entries by a list of keys."""
        session = client.create_session()
        k = _key(24)
        await session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3},
        }).execute()

        await (
            session.update(k)
                .bin("m").on_map_key_list(["a", "c"]).remove()
                .execute()
        )

        rs = await (await session.query(k).execute()).first_or_raise()
        assert rs.record.bins["m"] == {"b": 2}


# ===================================================================
# remove_all_others()
# ===================================================================


class TestRemoveAllOthers:

    async def test_remove_all_others_map_key_range(self, client):
        """Keep only map entries in a key range, remove the rest."""
        session = client.create_session()
        k = _key(25)
        await session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        await (
            session.update(k)
                .bin("m").on_map_key_range("b", "d").remove_all_others()
                .execute()
        )

        rs = await (await session.query(k).execute()).first_or_raise()
        assert rs.record.bins["m"] == {"b": 2, "c": 3}

    async def test_remove_all_others_list_value(self, client):
        """Keep only list elements matching a value, remove others."""
        session = client.create_session()
        k = _key(26)
        await session.upsert(k).put({"nums": [1, 5, 3, 5, 2]}).execute()

        await (
            session.update(k)
                .bin("nums").on_list_value(5).remove_all_others()
                .execute()
        )

        rs = await (await session.query(k).execute()).first_or_raise()
        assert [int(x) for x in rs.record.bins["nums"]] == [5, 5]


# ===================================================================
# Inverted reads after 2+ navigation steps
# ===================================================================


class TestDeepInvertedReads:

    async def test_nested_map_value_range_get_all_other_values(self, client):
        """Inverted read after navigating into a nested map."""
        session = client.create_session()
        k = _key(27)
        await session.upsert(k).put({
            "root": {"scores": {"alice": 80, "bob": 90, "carol": 95}},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("root").on_map_key("scores")
                .on_map_value_range(85, 100).get_all_other_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["root"]
        assert isinstance(vals, list)
        assert sorted(int(x) for x in vals) == [80]

    async def test_nested_list_value_get_all_other_values(self, client):
        """Inverted read on a list nested inside a map."""
        session = client.create_session()
        k = _key(28)
        await session.upsert(k).put({
            "wrap": {"tags": [10, 20, 30, 40]},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("wrap").on_map_key("tags")
                .on_list_value(20).get_all_other_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["wrap"]
        assert sorted(int(x) for x in vals) == [10, 30, 40]


# ===================================================================
# Deep path + range (2 hops then range)
# ===================================================================


class TestDeepPathThenRange:

    async def test_two_hops_then_key_range_count(self, client):
        """Navigate 2 map levels deep, then key-range count."""
        session = client.create_session()
        k = _key(29)
        await session.upsert(k).put({
            "l1": {"l2": {"a": 1, "b": 2, "c": 3, "d": 4}},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("l1").on_map_key("l2")
                .on_map_key_range("b", "d").count()
                .execute()
        ).first_or_raise()
        assert rs.record.bins["l1"] == 2

    async def test_two_hops_then_value_range_get_values(self, client):
        """Navigate map then list, then value-range select."""
        session = client.create_session()
        k = _key(30)
        await session.upsert(k).put({
            "data": {"items": [5, 15, 25, 35]},
        }).execute()

        rs = await (
            await session.query(k)
                .bin("data").on_map_key("items")
                .on_list_value_range(10, 30).get_values()
                .execute()
        ).first_or_raise()
        vals = rs.record.bins["data"]
        assert sorted(int(x) for x in vals) == [15, 25]

    async def test_two_hops_then_range_remove(self, client):
        """Navigate 2 levels deep, remove by key range."""
        session = client.create_session()
        k = _key(31)
        await session.upsert(k).put({
            "l1": {"l2": {"a": 1, "b": 2, "c": 3, "d": 4}},
        }).execute()

        await (
            session.update(k)
                .bin("l1").on_map_key("l2")
                .on_map_key_range("a", "c").remove()
                .execute()
        )

        rs = await (await session.query(k).execute()).first_or_raise()
        assert rs.record.bins["l1"] == {"l2": {"c": 3, "d": 4}}


# ===================================================================
# remove() / remove_all_others() return_type
# ===================================================================


class TestRemoveReturnType:

    async def test_remove_map_by_value_range_returns_values(self, client):
        session = client.create_session()
        k = _key(32)
        await session.upsert(k).put({
            "scores": {"alice": 80, "bob": 90, "carol": 95},
        }).execute()

        stream = await (
            session.update(k)
                .bin("scores")
                .on_map_value_range(85, 100)
                .remove(return_type=MapReturnType.VALUE)
                .execute()
        )
        rs = await stream.first_or_raise()
        raw = _cdt_remove_payload(rs.record_or_raise().bins, "scores")
        assert sorted(int(x) for x in raw) == [90, 95]

        final = await (await session.query(k).execute()).first_or_raise()
        assert final.record_or_raise().bins["scores"] == {"alice": 80}

    async def test_remove_map_by_key_range_returns_count(self, client):
        session = client.create_session()
        k = _key(33)
        await session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        stream = await (
            session.update(k)
                .bin("m")
                .on_map_key_range("b", "d")
                .remove(return_type=MapReturnType.COUNT)
                .execute()
        )
        rs = await stream.first_or_raise()
        cnt = _cdt_remove_payload(rs.record_or_raise().bins, "m")
        assert len(cnt) == 1
        assert int(cnt[0]) == 2

        final = await (await session.query(k).execute()).first_or_raise()
        assert final.record_or_raise().bins["m"] == {"a": 1, "d": 4}

    async def test_remove_map_by_key_range_returns_keys(self, client):
        session = client.create_session()
        k = _key(34)
        await session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        stream = await (
            session.update(k)
                .bin("m")
                .on_map_key_range("b", "d")
                .remove(return_type=MapReturnType.KEY)
                .execute()
        )
        rs = await stream.first_or_raise()
        keys = _cdt_remove_payload(rs.record_or_raise().bins, "m")
        assert sorted(keys) == ["b", "c"]

        final = await (await session.query(k).execute()).first_or_raise()
        assert final.record_or_raise().bins["m"] == {"a": 1, "d": 4}

    async def test_remove_all_others_returns_values(self, client):
        session = client.create_session()
        k = _key(35)
        await session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        stream = await (
            session.update(k)
                .bin("m")
                .on_map_key_range("b", "d")
                .remove_all_others(return_type=MapReturnType.VALUE)
                .execute()
        )
        rs = await stream.first_or_raise()
        raw = _cdt_remove_payload(rs.record_or_raise().bins, "m")
        assert sorted(int(x) for x in raw) == [1, 4]

        final = await (await session.query(k).execute()).first_or_raise()
        assert final.record_or_raise().bins["m"] == {"b": 2, "c": 3}

    async def test_remove_list_by_value_returns_count(self, client):
        session = client.create_session()
        k = _key(36)
        await session.upsert(k).put({
            "m": {"nums": [1, 9, 9, 3]},
        }).execute()

        stream = await (
            session.update(k)
                .bin("m")
                .on_map_key("nums")
                .on_list_value(9)
                .remove(return_type=ListReturnType.COUNT)
                .execute()
        )
        rs = await stream.first_or_raise()
        cnt = _cdt_remove_payload(rs.record_or_raise().bins, "m")
        assert len(cnt) == 1
        assert int(cnt[0]) == 2

        lst_rs = await (
            await session.query(k)
                .bin("m")
                .on_map_key("nums")
                .list_get_range(0, None)
                .execute()
        ).first_or_raise()
        lst = lst_rs.record_or_raise().bins["m"]
        assert [int(x) for x in lst] == [1, 3]
