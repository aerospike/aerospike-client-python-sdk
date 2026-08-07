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

"""Sync integration tests for nested CDT navigation, ranges, create-if-missing, and value chaining."""

import pytest

from aerospike_sdk import ListOrderType, MapOrder

from aerospike_sdk import DataSet, MapReturnType
from tests.integration.namespace import general_namespace


NS = general_namespace()
SET = "cdt_navigation_sync"
DS = DataSet.of(NS, SET)


@pytest.fixture(scope="module")
def shared_cluster(aerospike_host, make_cluster_definition):
    """Module-scoped connection: the auth handshake (~1s/node on the SC leg) is
    paid once per file. Per-test data freshness stays in the seeding fixtures,
    which re-seed on every test against this shared cluster."""
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


@pytest.fixture
def cluster(shared_cluster):
    c = shared_cluster
    session = c.create_session()
    for i in range(1, 40):
        session.delete(DS.id(i)).execute()
    yield c


def _key(n: int):
    return DS.id(n)


def _cdt_remove_payload(bins: dict, bin_name: str):
    v = bins.get(bin_name)
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


class TestNestedMapNavigationDeepSync:

    def test_three_level_map_read(self, cluster):
        session = cluster.create_session()
        k = _key(1)
        session.upsert(k).put({"doc": {"mid": {"leaf": 7}}}).execute()

        rs = (
            session.query(k)
            .bin("doc").on_map_key("mid").on_map_key("leaf").get_values()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["doc"] == 7

    def test_three_level_map_write_set_to(self, cluster):
        session = cluster.create_session()
        k = _key(2)
        session.upsert(k).put({"doc": {"mid": {"leaf": 1}}}).execute()

        session.update(k).bin("doc").on_map_key("mid").on_map_key("leaf").set_to(99).execute()

        rs = (
            session.query(k)
            .bin("doc").on_map_key("mid").on_map_key("leaf").get_values()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["doc"] == 99

    def test_map_then_list_then_map_read(self, cluster):
        session = cluster.create_session()
        k = _key(3)
        session.upsert(k).put({
            "root": {"items": [{"id": 1, "v": 10}, {"id": 2, "v": 20}]},
        }).execute()

        rs = (
            session.query(k)
            .bin("root").on_map_key("items").on_list_index(1).on_map_key("v").get_values()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["root"] == 20


class TestNestedRangeNavigationSync:

    def test_nested_map_key_range_count(self, cluster):
        session = cluster.create_session()
        k = _key(4)
        session.upsert(k).put({
            "outer": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        rs = (
            session.query(k)
            .bin("outer").on_map_key_range("b", "d").count()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["outer"] == 2

    def test_nested_map_value_range_get_values(self, cluster):
        session = cluster.create_session()
        k = _key(5)
        session.upsert(k).put({
            "scores": {"alice": 80, "bob": 90, "carol": 95},
        }).execute()

        rs = (
            session.query(k)
            .bin("scores").on_map_value_range(85, 100).get_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["scores"]
        assert isinstance(vals, list)
        assert sorted(int(x) for x in vals) == [90, 95]

    def test_nested_list_index_range_count(self, cluster):
        session = cluster.create_session()
        k = _key(6)
        session.upsert(k).put({
            "wrap": {"nums": [10, 20, 30, 40, 50]},
        }).execute()

        rs = (
            session.query(k)
            .bin("wrap").on_map_key("nums").on_list_index_range(1, 3).count()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["wrap"] == 3


class TestCreateIfMissingSync:

    def test_on_map_key_create_type_adds_key(self, cluster):
        session = cluster.create_session()
        k = _key(7)
        session.upsert(k).put({"shell": {"existing": 1}}).execute()

        session.update(k).bin("shell").on_map_key(
            "brand_new", create_type=MapOrder.UNORDERED,
        ).set_to(42).execute()

        rs = (
            session.query(k)
            .bin("shell").on_map_key("brand_new").get_values()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["shell"] == 42

    def test_nested_on_map_key_create_type(self, cluster):
        session = cluster.create_session()
        k = _key(8)
        session.upsert(k).put({"root": {"inner": {"x": 0}}}).execute()

        session.update(k).bin("root").on_map_key("inner").on_map_key(
            "y", create_type=MapOrder.KEY_ORDERED,
        ).set_to(100).execute()

        rs = (
            session.query(k)
            .bin("root").on_map_key("inner").on_map_key("y").get_values()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["root"] == 100

    def test_on_list_index_pad_nested_list(self, cluster):
        session = cluster.create_session()
        k = _key(9)
        session.upsert(k).put({"m": {"lst": [1, 2]}}).execute()

        session.update(k).bin("m").on_map_key("lst").on_list_index(
            5, order=ListOrderType.UNORDERED, pad=True,
        ).list_set(0, 9).execute()

        rs = (
            session.query(k).bin("m").on_map_key("lst").list_get_range(0, None).execute().first_or_raise()
        )
        raw = rs.record.bins["m"]
        assert isinstance(raw, list)
        assert raw[0] == 1
        assert raw[1] == 2
        # list_set index is relative to the navigated slot; a new padded cell is a list.
        assert raw[5] == [9]


class TestValueSelectorChainingSync:

    def test_nested_list_value_count(self, cluster):
        session = cluster.create_session()
        k = _key(10)
        session.upsert(k).put({"m": {"nums": [1, 5, 5, 5, 2]}}).execute()

        rs = (
            session.query(k)
            .bin("m").on_map_key("nums").on_list_value(5).count()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["m"] == 3

    def test_nested_list_value_remove(self, cluster):
        session = cluster.create_session()
        k = _key(11)
        session.upsert(k).put({"m": {"nums": [1, 9, 9, 3]}}).execute()

        session.update(k).bin("m").on_map_key("nums").on_list_value(9).remove().execute()

        rs = (
            session.query(k).bin("m").on_map_key("nums").list_get_range(0, None).execute().first_or_raise()
        )
        lst = rs.record.bins["m"]
        assert [int(x) for x in lst] == [1, 3]

    def test_bin_list_value_then_inverted_read(self, cluster):
        session = cluster.create_session()
        k = _key(12)
        session.upsert(k).put({"tags": [10, 20, 10, 30]}).execute()

        rs = (
            session.query(k)
            .bin("tags").on_list_value(10).get_all_other_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["tags"]
        assert isinstance(vals, list)
        assert 10 not in [int(x) for x in vals]
        assert 20 in [int(x) for x in vals]
        assert 30 in [int(x) for x in vals]


class TestSpecialValueOpenRangeSync:

    def test_nested_map_key_range_to_infinity(self, cluster):
        from aerospike_sdk import SpecialValue

        session = cluster.create_session()
        k = _key(13)
        session.upsert(k).put({"m": {"a": 1, "m": 2, "z": 3}}).execute()

        rs = (
            session.query(k)
            .bin("m").on_map_key_range("m", SpecialValue.INFINITY).count()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["m"] == 2

    def test_wildcard_in_map_value_list(self, cluster):
        """WILDCARD matches all values in a value-list selector."""
        from aerospike_sdk import SpecialValue

        session = cluster.create_session()
        k = _key(14)
        session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3},
        }).execute()

        rs = (
            session.query(k)
            .bin("m").on_map_value_list([SpecialValue.WILDCARD]).get_keys()
            .execute()
            .first_or_raise()
        )
        keys = rs.record.bins["m"]
        assert sorted(keys) == ["a", "b", "c"]

    def test_list_value_range_to_infinity(self, cluster):
        """SpecialValue.INFINITY as upper bound on list value range."""
        from aerospike_sdk import SpecialValue

        session = cluster.create_session()
        k = _key(15)
        session.upsert(k).put({"nums": [10, 20, 30, 40, 50]}).execute()

        rs = (
            session.query(k)
            .bin("nums").on_list_value_range(25, SpecialValue.INFINITY).get_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["nums"]
        assert sorted(int(x) for x in vals) == [30, 40, 50]


class TestMapValueSelectorSync:

    def test_on_map_value_count(self, cluster):
        """Count map entries matching a specific value."""
        session = cluster.create_session()
        k = _key(16)
        session.upsert(k).put({
            "grades": {"alice": "A", "bob": "B", "carol": "A", "dave": "C"},
        }).execute()

        rs = (
            session.query(k)
            .bin("grades").on_map_value("A").count()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["grades"] == 2

    def test_on_map_value_get_keys(self, cluster):
        """Get keys for entries matching a specific value."""
        session = cluster.create_session()
        k = _key(17)
        session.upsert(k).put({
            "colors": {"apple": "red", "sky": "blue", "fire": "red"},
        }).execute()

        rs = (
            session.query(k)
            .bin("colors").on_map_value("red").get_keys()
            .execute()
            .first_or_raise()
        )
        keys = rs.record.bins["colors"]
        assert sorted(keys) == ["apple", "fire"]

    def test_on_map_value_remove(self, cluster):
        """Remove map entries by value."""
        session = cluster.create_session()
        k = _key(18)
        session.upsert(k).put({
            "scores": {"a": 10, "b": 20, "c": 10},
        }).execute()

        session.update(k).bin("scores").on_map_value(10).remove().execute()

        rs = session.query(k).execute().first_or_raise()
        assert rs.record.bins["scores"] == {"b": 20}


class TestListValueRangeSync:

    def test_list_value_range_get_values(self, cluster):
        """Select list elements by value range."""
        session = cluster.create_session()
        k = _key(19)
        session.upsert(k).put({"nums": [5, 15, 25, 35, 45]}).execute()

        rs = (
            session.query(k)
            .bin("nums").on_list_value_range(10, 30).get_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["nums"]
        assert sorted(int(x) for x in vals) == [15, 25]

    def test_list_value_range_remove(self, cluster):
        """Remove list elements in a value range."""
        session = cluster.create_session()
        k = _key(20)
        session.upsert(k).put({"nums": [5, 15, 25, 35, 45]}).execute()

        session.update(k).bin("nums").on_list_value_range(10, 30).remove().execute()

        rs = session.query(k).execute().first_or_raise()
        remaining = [int(x) for x in rs.record.bins["nums"]]
        assert sorted(remaining) == [5, 35, 45]


class TestListSelectorsSync:

    def test_on_map_key_list_get_values(self, cluster):
        """Select map entries by a list of keys."""
        session = cluster.create_session()
        k = _key(21)
        session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        rs = (
            session.query(k)
            .bin("m").on_map_key_list(["a", "c"]).get_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["m"]
        assert sorted(int(x) for x in vals) == [1, 3]

    def test_on_map_value_list_get_keys(self, cluster):
        """Select map entries by a list of values."""
        session = cluster.create_session()
        k = _key(22)
        session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 1, "d": 3},
        }).execute()

        rs = (
            session.query(k)
            .bin("m").on_map_value_list([1, 3]).get_keys()
            .execute()
            .first_or_raise()
        )
        keys = rs.record.bins["m"]
        assert sorted(keys) == ["a", "c", "d"]

    def test_on_list_value_list_get_values(self, cluster):
        """Select list elements matching any value in a list."""
        session = cluster.create_session()
        k = _key(23)
        session.upsert(k).put({"nums": [1, 2, 3, 4, 5, 3]}).execute()

        rs = (
            session.query(k)
            .bin("nums").on_list_value_list([2, 4]).get_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["nums"]
        assert sorted(int(x) for x in vals) == [2, 4]

    def test_on_map_key_list_remove(self, cluster):
        """Remove map entries by a list of keys."""
        session = cluster.create_session()
        k = _key(24)
        session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3},
        }).execute()

        session.update(k).bin("m").on_map_key_list(["a", "c"]).remove().execute()

        rs = session.query(k).execute().first_or_raise()
        assert rs.record.bins["m"] == {"b": 2}


class TestRemoveAllOthersSync:

    def test_remove_all_others_map_key_range(self, cluster):
        """Keep only map entries in a key range, remove the rest."""
        session = cluster.create_session()
        k = _key(25)
        session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        session.update(k).bin("m").on_map_key_range("b", "d").remove_all_others().execute()

        rs = session.query(k).execute().first_or_raise()
        assert rs.record.bins["m"] == {"b": 2, "c": 3}

    def test_remove_all_others_list_value(self, cluster):
        """Keep only list elements matching a value, remove others."""
        session = cluster.create_session()
        k = _key(26)
        session.upsert(k).put({"nums": [1, 5, 3, 5, 2]}).execute()

        session.update(k).bin("nums").on_list_value(5).remove_all_others().execute()

        rs = session.query(k).execute().first_or_raise()
        assert [int(x) for x in rs.record.bins["nums"]] == [5, 5]


class TestDeepInvertedReadsSync:

    def test_nested_map_value_range_get_all_other_values(self, cluster):
        """Inverted read after navigating into a nested map."""
        session = cluster.create_session()
        k = _key(27)
        session.upsert(k).put({
            "root": {"scores": {"alice": 80, "bob": 90, "carol": 95}},
        }).execute()

        rs = (
            session.query(k)
            .bin("root").on_map_key("scores")
            .on_map_value_range(85, 100).get_all_other_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["root"]
        assert isinstance(vals, list)
        assert sorted(int(x) for x in vals) == [80]

    def test_nested_list_value_get_all_other_values(self, cluster):
        """Inverted read on a list nested inside a map."""
        session = cluster.create_session()
        k = _key(28)
        session.upsert(k).put({
            "wrap": {"tags": [10, 20, 30, 40]},
        }).execute()

        rs = (
            session.query(k)
            .bin("wrap").on_map_key("tags")
            .on_list_value(20).get_all_other_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["wrap"]
        assert sorted(int(x) for x in vals) == [10, 30, 40]


class TestDeepPathThenRangeSync:

    def test_two_hops_then_key_range_count(self, cluster):
        """Navigate 2 map levels deep, then key-range count."""
        session = cluster.create_session()
        k = _key(29)
        session.upsert(k).put({
            "l1": {"l2": {"a": 1, "b": 2, "c": 3, "d": 4}},
        }).execute()

        rs = (
            session.query(k)
            .bin("l1").on_map_key("l2")
            .on_map_key_range("b", "d").count()
            .execute()
            .first_or_raise()
        )
        assert rs.record.bins["l1"] == 2

    def test_two_hops_then_value_range_get_values(self, cluster):
        """Navigate map then list, then value-range select."""
        session = cluster.create_session()
        k = _key(30)
        session.upsert(k).put({
            "data": {"items": [5, 15, 25, 35]},
        }).execute()

        rs = (
            session.query(k)
            .bin("data").on_map_key("items")
            .on_list_value_range(10, 30).get_values()
            .execute()
            .first_or_raise()
        )
        vals = rs.record.bins["data"]
        assert sorted(int(x) for x in vals) == [15, 25]

    def test_two_hops_then_range_remove(self, cluster):
        """Navigate 2 levels deep, remove by key range."""
        session = cluster.create_session()
        k = _key(31)
        session.upsert(k).put({
            "l1": {"l2": {"a": 1, "b": 2, "c": 3, "d": 4}},
        }).execute()

        session.update(k).bin("l1").on_map_key("l2").on_map_key_range("a", "c").remove().execute()

        rs = session.query(k).execute().first_or_raise()
        assert rs.record.bins["l1"] == {"l2": {"c": 3, "d": 4}}


class TestRemoveReturnTypeSync:

    def test_remove_map_by_value_range_returns_values(self, cluster):
        session = cluster.create_session()
        k = _key(32)
        session.upsert(k).put({
            "scores": {"alice": 80, "bob": 90, "carol": 95},
        }).execute()

        stream = session.update(k).bin("scores").on_map_value_range(
            85, 100,
        ).remove(return_type=MapReturnType.VALUE).execute()
        rs = stream.first_or_raise()
        raw = _cdt_remove_payload(rs.record_or_raise().bins, "scores")
        assert sorted(int(x) for x in raw) == [90, 95]

        final = session.query(k).execute().first_or_raise()
        assert final.record_or_raise().bins["scores"] == {"alice": 80}

    def test_remove_map_by_key_range_returns_count(self, cluster):
        session = cluster.create_session()
        k = _key(33)
        session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        stream = session.update(k).bin("m").on_map_key_range(
            "b", "d",
        ).remove(return_type=MapReturnType.COUNT).execute()
        rs = stream.first_or_raise()
        cnt = _cdt_remove_payload(rs.record_or_raise().bins, "m")
        assert len(cnt) == 1
        assert int(cnt[0]) == 2

        final = session.query(k).execute().first_or_raise()
        assert final.record_or_raise().bins["m"] == {"a": 1, "d": 4}

    def test_remove_all_others_returns_values(self, cluster):
        session = cluster.create_session()
        k = _key(35)
        session.upsert(k).put({
            "m": {"a": 1, "b": 2, "c": 3, "d": 4},
        }).execute()

        stream = session.update(k).bin("m").on_map_key_range(
            "b", "d",
        ).remove_all_others(return_type=MapReturnType.VALUE).execute()
        rs = stream.first_or_raise()
        raw = _cdt_remove_payload(rs.record_or_raise().bins, "m")
        assert sorted(int(x) for x in raw) == [1, 4]

        final = session.query(k).execute().first_or_raise()
        assert final.record_or_raise().bins["m"] == {"b": 2, "c": 3}
