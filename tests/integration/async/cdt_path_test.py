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

"""The fluent CDT *path* surface: ``on_each_child`` and its terminals.

The other CDT navigation picks one element. A path walks a whole level -- every
child, or those matching a predicate -- and applies one server operation across
the selection. These tests drive the fluent form; the equivalent low-level
``CdtOperation.*`` factories are exercised by
``examples/cdt_path_expression_example.py``.
"""

from __future__ import annotations

from aerospike_async import GeoJSON

from aerospike_sdk import DataSet, Exp, ExpType, LoopVarPart, MapReturnType
from tests.integration.namespace import general_namespace

SET_NAME = "cdt_path_test"


def _key(n: int):
    return DataSet.of(general_namespace(), SET_NAME).id(n)


def _value_over(threshold: int):
    """A predicate over the current element's loop variable."""
    return Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(threshold))


async def _bins(session, key):
    return (await session.get(key)).bins


class TestPathModify:
    """``modify_by`` rewrites every element the path selects."""

    async def test_modify_each_child_of_a_list(self, cluster):
        session = cluster.create_session()
        k = _key(1)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [1, 2, 3]}).execute()

        add_10 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10)])
        await session.update(k).bin("nums").on_each_child().modify_by(add_10).execute()

        assert (await _bins(session, k))["nums"] == [11, 12, 13]

    async def test_modify_only_the_children_matching_a_predicate(self, cluster):
        session = cluster.create_session()
        k = _key(2)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [1, 7, 3, 9]}).execute()

        double = Exp.num_mul([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(2)])
        await (
            session.update(k)
            .bin("nums")
            .on_each_child_where(_value_over(5))
            .modify_by(double)
            .execute()
        )

        assert (await _bins(session, k))["nums"] == [1, 14, 3, 18]


class TestPathRemove:
    """``remove_matches`` drops every element the path selects."""

    async def test_remove_children_matching_a_predicate(self, cluster):
        session = cluster.create_session()
        k = _key(3)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [1, 7, 3, 9]}).execute()

        await (
            session.update(k)
            .bin("nums")
            .on_each_child_where(_value_over(5))
            .remove_matches()
            .execute()
        )

        assert (await _bins(session, k))["nums"] == [1, 3]


class TestPathCollect:
    """The ``collect_*`` terminals read across a selection."""

    async def test_collect_values_of_every_child(self, cluster):
        session = cluster.create_session()
        k = _key(4)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [4, 5, 6]}).execute()

        result = await (
            await session.query(k)
            .bin("nums")
            .on_each_child()
            .collect_values()
            .execute()
        ).first_or_raise()
        assert result.record.bins["nums"] == [4, 5, 6]

    async def test_collect_values_across_a_nested_path(self, cluster):
        """Two levels: every list under the map, then its values over 5."""
        session = cluster.create_session()
        k = _key(5)
        await session.delete(k).execute()
        await session.upsert(k).put({"m": {"a": [1, 9], "b": [2, 8]}}).execute()

        result = await (
            await session.query(k)
            .bin("m")
            .on_each_child()
            .on_each_child_where(_value_over(5))
            .collect_values()
            .execute()
        ).first_or_raise()
        assert sorted(result.record.bins["m"]) == [8, 9]

    async def test_collect_map_keys_of_every_child(self, cluster):
        session = cluster.create_session()
        k = _key(6)
        await session.delete(k).execute()
        await session.upsert(k).put({"m": {"a": 1, "b": 2}}).execute()

        result = await (
            await session.query(k)
            .bin("m")
            .on_each_child_where(_value_over(1))
            .collect_map_keys()
            .execute()
        ).first_or_raise()
        assert result.record.bins["m"] == ["b"]


class TestPathLeavesOtherDataAlone:
    """A path touches only what it selects."""

    async def test_a_sibling_bin_is_untouched(self, cluster):
        session = cluster.create_session()
        k = _key(7)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [1, 2], "other": "keep"}).execute()

        add_1 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(1)])
        await session.update(k).bin("nums").on_each_child().modify_by(add_1).execute()

        bins = await _bins(session, k)
        assert bins["nums"] == [2, 3]
        assert bins["other"] == "keep"


class TestMixedPaths:
    """A path may interleave plain navigation with each-child steps."""

    CATALOG = {
        "book": [
            {"title": "Sayings of the Century", "price": 8.95},
            {"title": "Sword of Honour", "price": 12.99},
            {"title": "Moby Dick", "price": 8.99},
            {"title": "The Lord of the Rings", "price": 22.99},
        ]
    }

    @staticmethod
    def _price_at_most(limit: float):
        price = Exp.map_get_by_key(
            MapReturnType.VALUE, ExpType.FLOAT,
            Exp.val("price"), Exp.map_loop_var(LoopVarPart.VALUE), [],
        )
        return Exp.le(price, Exp.val(limit))

    async def test_map_key_then_each_child_then_map_key(self, cluster):
        """map_key -> each_child_where -> map_key, the shape real paths take."""
        session = cluster.create_session()
        k = _key(8)
        await session.delete(k).execute()
        await session.upsert(k).bin("catalog").set_to(self.CATALOG).execute()

        result = await (
            await session.query(k)
            .bin("catalog")
            .on_map_key("book")
            .on_each_child_where(self._price_at_most(10.0))
            .on_map_key("title")
            .collect_values()
            .execute()
        ).first_or_raise()
        assert set(result.record.bins["catalog"]) == {
            "Sayings of the Century", "Moby Dick",
        }

    async def test_each_child_reached_from_element_navigation(self, cluster):
        """The element-wise builders hand off into a path via on_each_child."""
        session = cluster.create_session()
        k = _key(9)
        await session.delete(k).execute()
        await session.upsert(k).put({"m": {"a": [1, 9]}}).execute()

        add_1 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(1)])
        await (
            session.update(k)
            .bin("m").on_map_key("a").on_each_child().modify_by(add_1)
            .execute()
        )
        assert (await _bins(session, k))["m"] == {"a": [2, 10]}


# ---------------------------------------------------------------------------
# Filter combinations and edge cases, ported from the reference CDT operate
# suite. Several of its cases assert only that the call did not fail; these
# assert the values instead, since a path that silently selects nothing would
# pass the weaker check.
# ---------------------------------------------------------------------------
PRODUCTS = {
    "items": [
        {"name": "Widget", "price": 10.0, "in_stock": True},
        {"name": "Gadget", "price": 25.0, "in_stock": False},
        {"name": "Gizmo", "price": 5.0, "in_stock": True},
        {"name": "Doohickey", "price": 50.0, "in_stock": False},
    ]
}


def _field(name: str, exp_type, part=LoopVarPart.VALUE):
    """Pull one field out of the current map loop variable."""
    return Exp.map_get_by_key(
        MapReturnType.VALUE, exp_type,
        Exp.val(name), Exp.map_loop_var(part), [],
    )


async def _collect(session, key, bin_name, path_fn):
    stream = await path_fn(session.query(key).bin(bin_name)).execute()
    result = await stream.first_or_raise()
    return result.record.bins[bin_name]


class TestFilterCombinations:
    """Predicates composed with and / or / not over element fields."""

    async def test_and_filter_selects_the_intersection(self, cluster):
        session = cluster.create_session()
        k = _key(10)
        await session.delete(k).execute()
        await session.upsert(k).bin("cat").set_to(PRODUCTS).execute()

        cheap_and_stocked = Exp.and_([
            Exp.lt(_field("price", ExpType.FLOAT), Exp.val(20.0)),
            Exp.eq(_field("in_stock", ExpType.BOOL), Exp.val(True)),
        ])
        names = await _collect(
            session, k, "cat",
            lambda b: b.on_map_key("items")
            .on_each_child_where(cheap_and_stocked)
            .on_map_key("name")
            .collect_values(),
        )
        assert sorted(names) == ["Gizmo", "Widget"]

    async def test_or_filter_selects_the_union(self, cluster):
        session = cluster.create_session()
        k = _key(11)
        await session.delete(k).execute()
        await session.upsert(k).bin("cat").set_to(PRODUCTS).execute()

        very_cheap_or_very_dear = Exp.or_([
            Exp.lt(_field("price", ExpType.FLOAT), Exp.val(6.0)),
            Exp.gt(_field("price", ExpType.FLOAT), Exp.val(40.0)),
        ])
        names = await _collect(
            session, k, "cat",
            lambda b: b.on_map_key("items")
            .on_each_child_where(very_cheap_or_very_dear)
            .on_map_key("name")
            .collect_values(),
        )
        assert sorted(names) == ["Doohickey", "Gizmo"]

    async def test_nested_and_inside_or(self, cluster):
        """(in stock AND under 20) OR over 40."""
        session = cluster.create_session()
        k = _key(12)
        await session.delete(k).execute()
        await session.upsert(k).bin("cat").set_to(PRODUCTS).execute()

        predicate = Exp.or_([
            Exp.and_([
                Exp.eq(_field("in_stock", ExpType.BOOL), Exp.val(True)),
                Exp.lt(_field("price", ExpType.FLOAT), Exp.val(20.0)),
            ]),
            Exp.gt(_field("price", ExpType.FLOAT), Exp.val(40.0)),
        ])
        names = await _collect(
            session, k, "cat",
            lambda b: b.on_map_key("items")
            .on_each_child_where(predicate)
            .on_map_key("name")
            .collect_values(),
        )
        assert sorted(names) == ["Doohickey", "Gizmo", "Widget"]

    async def test_boolean_field_filters_directly(self, cluster):
        session = cluster.create_session()
        k = _key(13)
        await session.delete(k).execute()
        await session.upsert(k).bin("cat").set_to(PRODUCTS).execute()

        names = await _collect(
            session, k, "cat",
            lambda b: b.on_map_key("items")
            .on_each_child_where(
                Exp.eq(_field("in_stock", ExpType.BOOL), Exp.val(False))
            )
            .on_map_key("name")
            .collect_values(),
        )
        assert sorted(names) == ["Doohickey", "Gadget"]


class TestPathEdgeCases:
    """Selections that match nothing, and collections that hold nothing."""

    async def test_a_filter_matching_nothing_yields_an_empty_selection(
        self, cluster,
    ):
        session = cluster.create_session()
        k = _key(14)
        await session.delete(k).execute()
        await session.upsert(k).bin("cat").set_to(PRODUCTS).execute()

        names = await _collect(
            session, k, "cat",
            lambda b: b.on_map_key("items")
            .on_each_child_where(
                Exp.gt(_field("price", ExpType.FLOAT), Exp.val(1000.0))
            )
            .on_map_key("name")
            .collect_values(no_fail=True),
        )
        assert names == []

    async def test_an_empty_list_selects_nothing(self, cluster):
        session = cluster.create_session()
        k = _key(15)
        await session.delete(k).execute()
        await session.upsert(k).put({"d": {"empty": [], "items": [1, 2, 3]}}).execute()

        assert await _collect(
            session, k, "d",
            lambda b: b.on_map_key("empty").on_each_child().collect_values(
                no_fail=True,
            ),
        ) == []
        # the populated sibling is unaffected
        assert await _collect(
            session, k, "d",
            lambda b: b.on_map_key("items").on_each_child().collect_values(),
        ) == [1, 2, 3]

    async def test_an_empty_map_selects_nothing(self, cluster):
        session = cluster.create_session()
        k = _key(16)
        await session.delete(k).execute()
        await session.upsert(k).put({"d": {"empty": {}, "full": {"a": 1}}}).execute()

        assert await _collect(
            session, k, "d",
            lambda b: b.on_map_key("empty").on_each_child().collect_values(
                no_fail=True,
            ),
        ) == []
        assert await _collect(
            session, k, "d",
            lambda b: b.on_map_key("full").on_each_child().collect_values(),
        ) == [1]

    async def test_removing_every_element_leaves_an_empty_collection(self, cluster):
        session = cluster.create_session()
        k = _key(17)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [1, 2, 3]}).execute()

        await session.update(k).bin("nums").on_each_child().remove_matches().execute()
        assert (await _bins(session, k))["nums"] == []

    async def test_a_single_element_collection_still_works(self, cluster):
        session = cluster.create_session()
        k = _key(18)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [7]}).execute()

        add_1 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(1)])
        await session.update(k).bin("nums").on_each_child().modify_by(add_1).execute()
        assert (await _bins(session, k))["nums"] == [8]


class TestMatchingTree:
    """``collect_matching_tree`` keeps the nesting the selection was found in."""

    async def test_tree_preserves_structure(self, cluster):
        session = cluster.create_session()
        k = _key(19)
        await session.delete(k).execute()
        await session.upsert(k).bin("cat").set_to(PRODUCTS).execute()

        tree = await _collect(
            session, k, "cat",
            lambda b: b.on_map_key("items")
            .on_each_child_where(
                Exp.lt(_field("price", ExpType.FLOAT), Exp.val(11.0))
            )
            .collect_matching_tree(),
        )
        # Still shaped like the source (a map holding a list), not a flat list.
        assert isinstance(tree, dict)
        assert [p["name"] for p in tree["items"]] == ["Widget", "Gizmo"]


# ---------------------------------------------------------------------------
# Loop-variable forms. A predicate reads the current element through a loop
# variable, and there is one typed accessor per particle type plus three parts
# (value, map key, index). Only the int/map/float value forms were exercised
# anywhere before this.
# ---------------------------------------------------------------------------
MIXED = {
    "ints": [1, 5, 9],
    "strs": ["apple", "banana"],
    "bools": [True, False, True],
    "blobs": [b"\x01", b"\xff"],
    "lists": [[1, 2], [3, 4, 5]],
    "m": {"a": 1, "b": 2, "c": 3},
}


class TestLoopVariableParts:
    """A predicate can read the element's key or index, not just its value."""

    async def test_filter_on_the_map_key(self, cluster):
        session = cluster.create_session()
        k = _key(20)
        await session.delete(k).execute()
        await session.upsert(k).put(MIXED).execute()

        assert await _collect(
            session, k, "m",
            lambda b: b.on_each_child_where(
                Exp.eq(Exp.string_loop_var(LoopVarPart.MAP_KEY), Exp.val("b"))
            ).collect_values(),
        ) == [2]

    async def test_filter_on_the_list_index(self, cluster):
        session = cluster.create_session()
        k = _key(21)
        await session.delete(k).execute()
        await session.upsert(k).put(MIXED).execute()

        assert await _collect(
            session, k, "ints",
            lambda b: b.on_each_child_where(
                Exp.ge(Exp.int_loop_var(LoopVarPart.INDEX), Exp.val(1))
            ).collect_values(),
        ) == [5, 9]


class TestTypedLoopVariables:
    """One accessor per particle type."""

    async def test_string_loop_variable(self, cluster):
        session = cluster.create_session()
        k = _key(22)
        await session.delete(k).execute()
        await session.upsert(k).put(MIXED).execute()

        assert await _collect(
            session, k, "strs",
            lambda b: b.on_each_child_where(
                Exp.eq(Exp.string_loop_var(LoopVarPart.VALUE), Exp.val("apple"))
            ).collect_values(),
        ) == ["apple"]

    async def test_bool_loop_variable(self, cluster):
        session = cluster.create_session()
        k = _key(23)
        await session.delete(k).execute()
        await session.upsert(k).put(MIXED).execute()

        assert await _collect(
            session, k, "bools",
            lambda b: b.on_each_child_where(
                Exp.eq(Exp.bool_loop_var(LoopVarPart.VALUE), Exp.val(True))
            ).collect_values(),
        ) == [True, True]

    async def test_blob_loop_variable(self, cluster):
        session = cluster.create_session()
        k = _key(24)
        await session.delete(k).execute()
        await session.upsert(k).put(MIXED).execute()

        assert await _collect(
            session, k, "blobs",
            lambda b: b.on_each_child_where(
                Exp.eq(Exp.blob_loop_var(LoopVarPart.VALUE), Exp.val(b"\xff"))
            ).collect_values(),
        ) == [b"\xff"]

    async def test_list_loop_variable_reads_the_nested_collection(self, cluster):
        """The element is itself a list, so its size is what the filter asks."""
        session = cluster.create_session()
        k = _key(25)
        await session.delete(k).execute()
        await session.upsert(k).put(MIXED).execute()

        assert await _collect(
            session, k, "lists",
            lambda b: b.on_each_child_where(
                Exp.gt(
                    Exp.list_size(Exp.list_loop_var(LoopVarPart.VALUE), []),
                    Exp.val(2),
                )
            ).collect_values(),
        ) == [[3, 4, 5]]


class TestPathArithmetic:
    """``modify_by`` computes the new value from the old one."""

    async def test_subtract_from_every_element(self, cluster):
        session = cluster.create_session()
        k = _key(26)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [10, 20, 30]}).execute()

        minus_5 = Exp.num_sub([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(5)])
        await session.update(k).bin("nums").on_each_child().modify_by(minus_5).execute()
        assert (await _bins(session, k))["nums"] == [5, 15, 25]

    async def test_divide_every_element(self, cluster):
        session = cluster.create_session()
        k = _key(27)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [10, 20, 30]}).execute()

        halved = Exp.num_div([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(2)])
        await session.update(k).bin("nums").on_each_child().modify_by(halved).execute()
        assert (await _bins(session, k))["nums"] == [5, 10, 15]

    async def test_compound_arithmetic_on_matching_elements_only(self, cluster):
        """(v * 2) + 1, applied only where v > 5."""
        session = cluster.create_session()
        k = _key(28)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [3, 7, 11]}).execute()

        expr = Exp.num_add([
            Exp.num_mul([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(2)]),
            Exp.val(1),
        ])
        await (
            session.update(k).bin("nums")
            .on_each_child_where(_value_over(5)).modify_by(expr).execute()
        )
        assert (await _bins(session, k))["nums"] == [3, 15, 23]


class TestPathRemovalVariants:
    """Removal across maps, indexes and nested paths."""

    async def test_remove_map_entries_by_key(self, cluster):
        session = cluster.create_session()
        k = _key(29)
        await session.delete(k).execute()
        await session.upsert(k).put({"m": {"a": 1, "b": 2, "c": 3}}).execute()

        await (
            session.update(k).bin("m")
            .on_each_child_where(
                Exp.eq(Exp.string_loop_var(LoopVarPart.MAP_KEY), Exp.val("b"))
            )
            .remove_matches().execute()
        )
        assert (await _bins(session, k))["m"] == {"a": 1, "c": 3}

    async def test_remove_list_elements_by_index(self, cluster):
        session = cluster.create_session()
        k = _key(30)
        await session.delete(k).execute()
        await session.upsert(k).put({"nums": [10, 20, 30, 40]}).execute()

        await (
            session.update(k).bin("nums")
            .on_each_child_where(
                Exp.ge(Exp.int_loop_var(LoopVarPart.INDEX), Exp.val(2))
            )
            .remove_matches().execute()
        )
        assert (await _bins(session, k))["nums"] == [10, 20]

    async def test_remove_every_map_entry(self, cluster):
        session = cluster.create_session()
        k = _key(31)
        await session.delete(k).execute()
        await session.upsert(k).put({"m": {"a": 1, "b": 2}}).execute()

        await session.update(k).bin("m").on_each_child().remove_matches().execute()
        assert (await _bins(session, k))["m"] == {}

    async def test_remove_deep_inside_a_nested_path(self, cluster):
        session = cluster.create_session()
        k = _key(32)
        await session.delete(k).execute()
        await session.upsert(k).put(
            {"d": {"x": [1, 9, 2], "y": [3, 8]}}
        ).execute()

        await (
            session.update(k).bin("d")
            .on_map_key("x")
            .on_each_child_where(_value_over(5))
            .remove_matches().execute()
        )
        bins = await _bins(session, k)
        assert bins["d"]["x"] == [1, 2]
        assert bins["d"]["y"] == [3, 8]   # the sibling branch is untouched


class TestDeeplyNestedStructures:
    """Three levels of collection, filtered at the leaf."""

    async def test_three_level_path(self, cluster):
        session = cluster.create_session()
        k = _key(33)
        await session.delete(k).execute()
        await session.upsert(k).put(
            {"root": {"teams": [{"scores": [1, 9]}, {"scores": [2, 8]}]}}
        ).execute()

        assert sorted(await _collect(
            session, k, "root",
            lambda b: b.on_map_key("teams")
            .on_each_child()
            .on_map_key("scores")
            .on_each_child_where(_value_over(5))
            .collect_values(),
        )) == [8, 9]


class TestRemainingLoopVariableTypes:
    """The accessors not covered above: nil, GeoJSON, and key ordering.

    ``hll_loop_var`` is deliberately absent. The reference's two HLL cases do
    not exercise it either -- their expressions use the map and string loop
    variables, and they assert only that the record is non-null -- so porting
    them would add a test that cannot fail.
    """

    async def test_lexicographic_range_over_map_keys(self, cluster):
        """Keys before 'c': a comparison, not the equality tested earlier."""
        session = cluster.create_session()
        k = _key(34)
        await session.delete(k).execute()
        await session.upsert(k).put(
            {"m": {"apple": 1.5, "banana": 0.75, "cherry": 2.25}}
        ).execute()

        assert sorted(await _collect(
            session, k, "m",
            lambda b: b.on_each_child_where(
                Exp.lt(Exp.string_loop_var(LoopVarPart.MAP_KEY), Exp.val("c"))
            ).collect_values(),
        )) == [0.75, 1.5]

    async def test_nil_loop_variable_selects_the_absent_values(self, cluster):
        session = cluster.create_session()
        k = _key(35)
        await session.delete(k).execute()
        await session.upsert(k).put({"vals": [1, None, 3]}).execute()

        assert await _collect(
            session, k, "vals",
            lambda b: b.on_each_child_where(
                Exp.eq(Exp.nil_loop_var(LoopVarPart.VALUE), Exp.val(None))
            ).collect_values(no_fail=True),
        ) == [None]

    async def test_geojson_loop_variable_filters_by_region(self, cluster):
        """Only the point inside the circle is selected."""
        session = cluster.create_session()
        k = _key(36)
        await session.delete(k).execute()
        await session.upsert(k).put({
            "geos": [
                GeoJSON('{"type":"Point","coordinates":[-122.0,37.0]}'),
                GeoJSON('{"type":"Point","coordinates":[-74.0,40.0]}'),
            ]
        }).execute()

        near_new_york = Exp.geo_compare(
            Exp.geo_json_loop_var(LoopVarPart.VALUE),
            Exp.geo_val(
                '{"type":"AeroCircle","coordinates":[[-74.0,40.0],100000]}'
            ),
        )
        selected = await _collect(
            session, k, "geos",
            lambda b: b.on_each_child_where(near_new_york).collect_values(
                no_fail=True,
            ),
        )
        assert len(selected) == 1
        assert "-74" in str(selected[0])
