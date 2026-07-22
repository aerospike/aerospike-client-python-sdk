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

"""Unit tests for the AEL IN operator."""

import pytest
from aerospike_async import CTX, ExpType, ListReturnType, MapReturnType
from aerospike_sdk import Exp, parse_ael
from aerospike_sdk.ael.exceptions import AelParseException


# ---------------------------------------------------------------------------
# Literal values in literal lists
# ---------------------------------------------------------------------------
class TestInLiteral:

    def test_string_literal_in_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_val("gold"),
            Exp.list_val(["gold", "silver"]),
            [],
        )
        assert parse_ael('"gold" in ["gold", "silver"]') == expected

    def test_int_literal_in_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_val(100),
            Exp.list_val([100, 200, 300]),
            [],
        )
        assert parse_ael("100 in [100, 200, 300]") == expected

    def test_float_literal_in_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_val(1.5),
            Exp.list_val([1.0, 2.0, 3.0]),
            [],
        )
        assert parse_ael("1.5 in [1.0, 2.0, 3.0]") == expected

    def test_bool_literal_in_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.bool_val(True),
            Exp.list_val([True, False]),
            [],
        )
        assert parse_ael("true in [true, false]") == expected

    def test_list_literal_in_list_of_lists(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_val([1, 2, 3]),
            Exp.list_val([[2, 3, 4], [3, 4, 5], [1, 2, 3], [1, 2]]),
            [],
        )
        assert parse_ael("[1,2,3] in [[2,3,4], [3,4,5], [1,2,3], [1,2]]") == expected

    def test_list_bin_in_list_of_lists(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_bin("listBin"),
            Exp.list_val([[2, 3, 4], [3, 4, 5], [1, 2, 3], [1, 2]]),
            [],
        )
        assert parse_ael("$.listBin in [[2,3,4], [3,4,5], [1,2,3], [1,2]]") == expected

    def test_map_literal_in_list_of_maps(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_val({1: "a"}),
            Exp.list_val([{1: "a"}, {2: "b"}]),
            [],
        )
        assert parse_ael('{1: "a"} in [{1: "a"}, {2: "b"}]') == expected

    def test_map_bin_in_list_of_maps(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_bin("mapBin"),
            Exp.list_val([{1: "a"}, {2: "b"}]),
            [],
        )
        assert parse_ael('$.mapBin in [{1: "a"}, {2: "b"}]') == expected

    def test_bin_in_string_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.list_val(["Bob", "Mary"]),
            [],
        )
        assert parse_ael('$.name in ["Bob", "Mary"]') == expected

    def test_bin_in_int_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("age"),
            Exp.list_val([18, 21, 65]),
            [],
        )
        assert parse_ael("$.age in [18, 21, 65]") == expected

    def test_bin_in_float_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_bin("score"),
            Exp.list_val([1.0, 2.5]),
            [],
        )
        assert parse_ael("$.score in [1.0, 2.5]") == expected

    def test_bin_in_bool_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.bool_bin("isActive"),
            Exp.list_val([True, False]),
            [],
        )
        assert parse_ael("$.isActive in [true, false]") == expected

    def test_nested_path_in_list_literal(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("rateType"),
                Exp.map_bin("rooms"),
                [CTX.map_key("room1"), CTX.map_key("rates")],
            ),
            Exp.list_val(["RACK_RATE", "DISCOUNT"]),
            [],
        )
        assert parse_ael('$.rooms.room1.rates.rateType in ["RACK_RATE", "DISCOUNT"]') == expected

    def test_explicit_int_bin_in_list_designator(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("name"),
            Exp.list_bin("binName"),
            [],
        )
        assert parse_ael("$.name.get(type: INT) in $.binName.[]") == expected

    def test_in_with_single_element_list(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.list_val(["Bob"]),
            [],
        )
        assert parse_ael('$.name in ["Bob"]') == expected

    def test_in_with_negative_ints(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("val"),
            Exp.list_val([-1, 0, 1]),
            [],
        )
        assert parse_ael("$.val in [-1, 0, 1]") == expected

    def test_in_with_negative_floats(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_bin("val"),
            Exp.list_val([-1.5, 0.0, 1.5]),
            [],
        )
        assert parse_ael("$.val in [-1.5, 0.0, 1.5]") == expected

    def test_in_with_hex_binary_literals(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("val"),
            Exp.list_val([255, 5, 42]),
            [],
        )
        assert parse_ael("$.val in [0xFF, 0b101, 42]") == expected


# ---------------------------------------------------------------------------
# Literal / bin values in a bin (right side is a bin reference)
# ---------------------------------------------------------------------------
class TestInBin:

    def test_string_literal_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_val("gold"),
            Exp.list_bin("allowedStatuses"),
            [],
        )
        assert parse_ael('"gold" in $.allowedStatuses') == expected

    def test_int_literal_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_val(100),
            Exp.list_bin("allowedValues"),
            [],
        )
        assert parse_ael("100 in $.allowedValues") == expected

    def test_float_literal_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_val(1.5),
            Exp.list_bin("scores"),
            [],
        )
        assert parse_ael("1.5 in $.scores") == expected

    def test_bool_literal_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.bool_val(True),
            Exp.list_bin("flags"),
            [],
        )
        assert parse_ael("true in $.flags") == expected

    def test_list_literal_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_val([1, 2, 3]),
            Exp.list_bin("listOfLists"),
            [],
        )
        assert parse_ael("[1, 2, 3] in $.listOfLists") == expected

    def test_map_literal_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_val({1: "a"}),
            Exp.list_bin("mapItems"),
            [],
        )
        assert parse_ael('{1: "a"} in $.mapItems') == expected

    def test_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("itemType"),
            Exp.list_bin("allowedItems"),
            [],
        )
        assert parse_ael("$.itemType.get(type: INT) in $.allowedItems") == expected

    def test_bin_in_nested_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("intBin"),
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("allowedNames"),
                Exp.map_bin("rooms"),
                [CTX.map_key("config")],
            ),
            [],
        )
        assert parse_ael("$.intBin.get(type: INT) in $.rooms.config.allowedNames") == expected


# ---------------------------------------------------------------------------
# Placeholder values in IN expressions
# ---------------------------------------------------------------------------
class TestInPlaceholder:

    def test_placeholder_as_left_operand(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_val("gold"),
            Exp.list_val(["gold", "silver"]),
            [],
        )
        assert parse_ael('?0 in ["gold", "silver"]', "gold") == expected

    def test_int_placeholder_as_left_operand(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_val(100),
            Exp.list_val([100, 200, 300]),
            [],
        )
        assert parse_ael("?0 in [100, 200, 300]", 100) == expected

    def test_float_placeholder_as_left_operand(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_val(1.5),
            Exp.list_val([1.0, 2.0, 3.0]),
            [],
        )
        assert parse_ael("?0 in [1.0, 2.0, 3.0]", 1.5) == expected

    def test_bool_placeholder_as_left_operand(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.bool_val(True),
            Exp.list_val([True, False]),
            [],
        )
        assert parse_ael("?0 in [true, false]", True) == expected

    def test_list_placeholder_as_left_operand(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_val([1, 2, 3]),
            Exp.list_val([[1, 2, 3], [4, 5, 6]]),
            [],
        )
        assert parse_ael("?0 in [[1,2,3], [4,5,6]]", [1, 2, 3]) == expected

    def test_map_placeholder_as_left_operand(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_val({1: "a"}),
            Exp.list_val([{1: "a"}, {2: "b"}]),
            [],
        )
        assert parse_ael('?0 in [{1: "a"}, {2: "b"}]', {1: "a"}) == expected

    def test_placeholder_as_right_operand(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.list_val(["Bob", "Mary"]),
            [],
        )
        assert parse_ael(
            '$.name.get(type: STRING) in ?0',
            ["Bob", "Mary"],
        ) == expected

    def test_int_list_placeholder_as_right(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("age"),
            Exp.list_val([1, 2, 3]),
            [],
        )
        assert parse_ael("$.age.get(type: INT) in ?0", [1, 2, 3]) == expected

    def test_float_list_placeholder_as_right(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_bin("score"),
            Exp.list_val([1.5, 2.5]),
            [],
        )
        assert parse_ael("$.score.get(type: FLOAT) in ?0", [1.5, 2.5]) == expected

    def test_bool_list_placeholder_as_right(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.bool_bin("isActive"),
            Exp.list_val([True, False]),
            [],
        )
        assert parse_ael("$.isActive.get(type: BOOL) in ?0", [True, False]) == expected

    def test_list_of_lists_placeholder_as_right(self):
        outer = [[1, 2, 3], [4, 5, 6]]
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_bin("listBin"),
            Exp.list_val(outer),
            [],
        )
        assert parse_ael("$.listBin.get(type: LIST) in ?0", outer) == expected

    def test_map_list_placeholder_as_right(self):
        map_list = [{1: "a"}, {2: "b"}]
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_bin("mapBin"),
            Exp.list_val(map_list),
            [],
        )
        assert parse_ael("$.mapBin.get(type: MAP) in ?0", map_list) == expected

    def test_empty_list_placeholder_as_right(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("intBin1"),
            Exp.list_val([]),
            [],
        )
        assert parse_ael("$.intBin1.get(type: INT) in ?0", []) == expected

    def test_both_placeholders(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_val("gold"),
            Exp.list_val(["gold", "silver"]),
            [],
        )
        assert parse_ael("?0 in ?1", "gold", ["gold", "silver"]) == expected


# ---------------------------------------------------------------------------
# Explicit types on left / right operand
# ---------------------------------------------------------------------------
class TestInExplicitType:

    # --- Explicit type on left, right is a literal list ---

    def test_explicit_list_type_on_right_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("intBin1"),
            Exp.list_bin("tags"),
            [],
        )
        assert parse_ael("$.intBin1.get(type: INT) in $.tags.get(type: LIST)") == expected

    def test_explicit_int_in_int_list(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("age"),
            Exp.list_val([1, 2]),
            [],
        )
        assert parse_ael("$.age.get(type: INT) in [1, 2]") == expected

    def test_explicit_string_in_string_list(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.list_val(["a"]),
            [],
        )
        assert parse_ael('$.name.get(type: STRING) in ["a"]') == expected

    def test_explicit_int_compatible_with_float_list(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("val"),
            Exp.list_val([1.5, 2.5]),
            [],
        )
        assert parse_ael("$.val.get(type: INT) in [1.5, 2.5]") == expected

    def test_explicit_float_compatible_with_int_list(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_bin("val"),
            Exp.list_val([1, 2]),
            [],
        )
        assert parse_ael("$.val.get(type: FLOAT) in [1, 2]") == expected

    def test_explicit_float_in_float_list(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_bin("val"),
            Exp.list_val([1.5, 2.5]),
            [],
        )
        assert parse_ael("$.val.get(type: FLOAT) in [1.5, 2.5]") == expected

    def test_explicit_bool_in_bool_list(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.bool_bin("flag"),
            Exp.list_val([True, False]),
            [],
        )
        assert parse_ael("$.flag.get(type: BOOL) in [true, false]") == expected

    # --- Explicit type on left, right is a bin ---

    def test_explicit_string_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.name.get(type: STRING) in $.list") == expected

    def test_explicit_int_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("val"),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.val.get(type: INT) in $.list") == expected

    def test_explicit_float_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_bin("val"),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.val.get(type: FLOAT) in $.list") == expected

    def test_explicit_bool_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.bool_bin("flag"),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.flag.get(type: BOOL) in $.list") == expected

    def test_explicit_list_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_bin("items"),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.items.get(type: LIST) in $.list") == expected

    def test_explicit_map_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_bin("item"),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.item.get(type: MAP) in $.list") == expected

    # --- Explicit type on left, right is a path ---

    def test_explicit_string_bin_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("tags"), Exp.map_bin("items"), [],
            ),
            [],
        )
        assert parse_ael("$.name.get(type: STRING) in $.items.tags") == expected

    def test_explicit_int_bin_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("val"),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("tags"), Exp.map_bin("items"), [],
            ),
            [],
        )
        assert parse_ael("$.val.get(type: INT) in $.items.tags") == expected

    def test_explicit_float_bin_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.float_bin("val"),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("tags"), Exp.map_bin("items"), [],
            ),
            [],
        )
        assert parse_ael("$.val.get(type: FLOAT) in $.items.tags") == expected

    def test_explicit_bool_bin_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.bool_bin("flag"),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("tags"), Exp.map_bin("items"), [],
            ),
            [],
        )
        assert parse_ael("$.flag.get(type: BOOL) in $.items.tags") == expected

    def test_explicit_list_bin_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_bin("items"),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("tags"), Exp.map_bin("items"), [],
            ),
            [],
        )
        assert parse_ael("$.items.get(type: LIST) in $.items.tags") == expected

    def test_explicit_map_bin_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_bin("item"),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("tags"), Exp.map_bin("items"), [],
            ),
            [],
        )
        assert parse_ael("$.item.get(type: MAP) in $.items.tags") == expected

    # --- Cast on left ---

    def test_cast_int_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.to_int(Exp.float_bin("val")),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.val.asInt() in $.list") == expected

    def test_cast_float_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.to_float(Exp.int_bin("val")),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.val.asFloat() in $.list") == expected

    def test_cast_int_bin_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.to_int(Exp.float_bin("val")),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("tags"), Exp.map_bin("items"), [],
            ),
            [],
        )
        assert parse_ael("$.val.asInt() in $.items.tags") == expected

    def test_cast_float_bin_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.to_float(Exp.int_bin("val")),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("tags"), Exp.map_bin("items"), [],
            ),
            [],
        )
        assert parse_ael("$.val.asFloat() in $.items.tags") == expected

    # --- Explicit type on left PATH_OPERAND ---

    def test_explicit_path_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("name"), Exp.map_bin("rooms"), [],
            ),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.rooms.name.get(type: STRING) in $.list") == expected

    def test_explicit_path_in_path(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("name"), Exp.map_bin("rooms"), [],
            ),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("list"), Exp.map_bin("rooms2"), [],
            ),
            [],
        )
        assert parse_ael("$.rooms.name.get(type: STRING) in $.rooms2.list") == expected

    # --- Explicit type on both sides ---

    def test_explicit_bin_in_explicit_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("val"),
            Exp.list_bin("tags"),
            [],
        )
        assert parse_ael("$.val.get(type: INT) in $.tags.get(type: LIST)") == expected

    # --- Explicit type with placeholder right ---

    def test_explicit_bin_in_placeholder(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.list_val(["Bob"]),
            [],
        )
        assert parse_ael("$.name.get(type: STRING) in ?0", ["Bob"]) == expected

    def test_explicit_path_in_placeholder(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("name"), Exp.map_bin("rooms"), [],
            ),
            Exp.list_val(["Bob"]),
            [],
        )
        assert parse_ael("$.rooms.name.get(type: STRING) in ?0", ["Bob"]) == expected

    # --- Explicit type with list designator right ---

    def test_explicit_bin_in_list_designator(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("name"),
            Exp.list_bin("binName"),
            [],
        )
        assert parse_ael("$.name.get(type: INT) in $.binName.[]") == expected

    # --- Placeholder left (concrete value) ---

    def test_pos_both_placeholders(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_val("gold"),
            Exp.list_val(["gold", "silver"]),
            [],
        )
        assert parse_ael("?0 in ?1", "gold", ["gold", "silver"]) == expected

    def test_pos_placeholder_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_val(42),
            Exp.list_bin("bin"),
            [],
        )
        assert parse_ael("?0 in $.bin", 42) == expected

    # --- Variable left (concrete value) ---

    def test_pos_variable_in_bin(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.int_val(1)),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.var("x"),
                Exp.list_bin("list"),
                [],
            ),
        ])
        assert parse_ael("let (x = 1) then (${x} in $.list)") == expected

    # --- PATH_OPERAND with type designator ---

    def test_list_designator_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_bin("items"),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.items.[] in $.list") == expected

    def test_map_designator_bin_in_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.map_bin("item"),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.item.{} in $.list") == expected

    # --- PATH_OPERAND with COUNT/SIZE function ---

    def test_count_path_in_list_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_size(
                Exp.list_get_by_index(
                    ListReturnType.VALUE, ExpType.LIST,
                    Exp.int_val(0), Exp.list_bin("items"), [],
                ),
                [],
            ),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.items.[0].count() in $.list") == expected

    def test_count_list_bin(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.list_size(Exp.list_bin("items"), []),
            Exp.list_bin("list"),
            [],
        )
        assert parse_ael("$.items.[].count() in $.list") == expected

    # --- Negative: explicit non-LIST type on right ---

    def test_neg_explicit_int_type_on_right_bin(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in $.tags.get(type: INT)")

    def test_neg_explicit_string_type_on_right_bin(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in $.tags.get(type: STRING)")

    def test_neg_nested_path_explicit_string_on_right(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in $.tags.nested.get(type: STRING)")

    # --- Negative: type mismatch between explicit left and list element type ---

    def test_neg_explicit_string_in_int_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael('$.name.get(type: STRING) in [1, 2]')

    def test_neg_explicit_int_in_string_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael('$.age.get(type: INT) in ["a", "b"]')

    def test_neg_explicit_bool_in_int_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael("$.flag.get(type: BOOL) in [1, 2]")

    def test_neg_explicit_int_in_bool_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael("$.val.get(type: INT) in [true, false]")

    def test_neg_explicit_float_in_string_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael('$.val.get(type: FLOAT) in ["a", "b"]')

    def test_neg_explicit_float_in_bool_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael("$.val.get(type: FLOAT) in [true, false]")

    def test_neg_explicit_string_in_float_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael("$.name.get(type: STRING) in [1.5, 2.5]")

    def test_neg_explicit_string_in_bool_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael("$.name.get(type: STRING) in [true, false]")

    def test_neg_explicit_bool_in_string_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael('$.flag.get(type: BOOL) in ["a", "b"]')

    def test_neg_explicit_bool_in_float_list(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael("$.flag.get(type: BOOL) in [1.5, 2.5]")


# ---------------------------------------------------------------------------
# Composite expressions: IN combined with AND, OR, NOT, LET, WHEN
# ---------------------------------------------------------------------------
class TestInComposite:

    def test_in_with_and(self):
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("cost"), Exp.int_val(50)),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("status"),
                Exp.list_val(["active", "pending"]),
                [],
            ),
        ])
        assert parse_ael(
            '$.cost > 50 and $.status in ["active", "pending"]'
        ) == expected

    def test_in_with_or(self):
        expected = Exp.or_([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("status"),
                Exp.list_val(["active"]),
                [],
            ),
            Exp.gt(Exp.int_bin("priority"), Exp.int_val(5)),
        ])
        assert parse_ael('$.status in ["active"] or $.priority > 5') == expected

    def test_complex_expression_with_in(self):
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("cost"), Exp.int_val(50)),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.int_bin("status"),
                Exp.list_bin("allowedStatuses"),
                [],
            ),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_val("available"),
                Exp.list_bin("bookableStates"),
                [],
            ),
        ])
        assert parse_ael(
            '$.cost > 50 and $.status.get(type: INT) in $.allowedStatuses'
            ' and "available" in $.bookableStates'
        ) == expected

    def test_in_with_parentheses(self):
        expected = Exp.and_([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("name"),
                Exp.list_val(["Bob"]),
                [],
            ),
            Exp.gt(Exp.int_bin("age"), Exp.int_val(18)),
        ])
        assert parse_ael('($.name in ["Bob"]) and $.age > 18') == expected

    def test_in_inside_let(self):
        expected = Exp.exp_let([
            Exp.def_("allowed", Exp.list_val(["Bob", "Mary"])),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("name"),
                Exp.var("allowed"),
                [],
            ),
        ])
        assert parse_ael(
            "let (allowed = [\"Bob\", \"Mary\"])"
            " then ($.name.get(type: STRING) in ${allowed})"
        ) == expected

    def test_in_inside_when_condition(self):
        expected = Exp.cond([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("name"),
                Exp.list_val(["Bob"]),
                [],
            ),
            Exp.string_val("VIP"),
            Exp.string_val("regular"),
        ])
        assert parse_ael(
            'when($.name.get(type: STRING) in ["Bob"] => "VIP",'
            ' default => "regular")'
        ) == expected

    def test_not_wrapping_in(self):
        expected = Exp.not_(
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("name"),
                Exp.list_val(["Bob", "Mary"]),
                [],
            )
        )
        assert parse_ael('not($.name in ["Bob", "Mary"])') == expected

    def test_nested_let_outer_list_var(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.list_val(["a", "b"])),
            Exp.exp_let([
                Exp.def_("y", Exp.int_val(3)),
                Exp.list_get_by_value(
                    ListReturnType.EXISTS,
                    Exp.string_bin("name"),
                    Exp.var("x"),
                    [],
                ),
            ]),
        ])
        assert parse_ael(
            'let (x = ["a", "b"]) then'
            ' (let (y = 3) then ($.name.get(type: STRING) in ${x}))'
        ) == expected

    def test_nested_let_shadowed_var(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.int_val(1)),
            Exp.exp_let([
                Exp.def_("x", Exp.list_val(["a"])),
                Exp.list_get_by_value(
                    ListReturnType.EXISTS,
                    Exp.string_bin("name"),
                    Exp.var("x"),
                    [],
                ),
            ]),
        ])
        assert parse_ael(
            'let (x = 1) then'
            ' (let (x = ["a"]) then ($.name.get(type: STRING) in ${x}))'
        ) == expected

    def test_nested_let_var_bound_to_var(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.list_val([1, 2])),
            Exp.exp_let([
                Exp.def_("y", Exp.var("x")),
                Exp.list_get_by_value(
                    ListReturnType.EXISTS,
                    Exp.int_val(1),
                    Exp.var("y"),
                    [],
                ),
            ]),
        ])
        assert parse_ael(
            'let (x = [1, 2]) then'
            ' (let (y = ${x}) then (1 in ${y}))'
        ) == expected

    def test_neg_transitive_var_indirection(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael(
                'let (x = 1) then'
                ' (let (y = ${x}) then ("foo" in ${y}))'
            )

    def test_when_scalar_branches_allowed_conservatively(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.cond([
                Exp.bool_val(True), Exp.int_val(1),
                Exp.int_val(2),
            ])),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_val("foo"),
                Exp.var("x"),
                [],
            ),
        ])
        assert parse_ael(
            'let (x = when(true => 1, default => 2))'
            ' then ("foo" in ${x})'
        ) == expected

    def test_arithmetic_expr_as_left_in(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.num_add([Exp.int_bin("a"), Exp.int_val(5)]),
            Exp.list_val([10, 20, 30]),
            [],
        )
        assert parse_ael("$.a + 5 in [10, 20, 30]") == expected

    def test_in_with_int_type_in_when_cond(self):
        expected = Exp.cond([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.int_bin("age"),
                Exp.list_val([18, 21]),
                [],
            ),
            Exp.string_val("eligible"),
            Exp.string_val("ineligible"),
        ])
        assert parse_ael(
            'when($.age.get(type: INT) in [18, 21] => "eligible",'
            ' default => "ineligible")'
        ) == expected

    def test_multiple_in_conditions_in_when(self):
        expected = Exp.cond([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("role"),
                Exp.list_val(["admin"]),
                [],
            ),
            Exp.int_val(1),
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("role"),
                Exp.list_val(["user"]),
                [],
            ),
            Exp.int_val(2),
            Exp.int_val(0),
        ])
        assert parse_ael(
            'when($.role in ["admin"] => 1,'
            ' $.role in ["user"] => 2,'
            ' default => 0)'
        ) == expected

    def test_mixed_in_and_comparison_in_when(self):
        expected = Exp.cond([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("name"),
                Exp.list_val(["Bob", "Mary"]),
                [],
            ),
            Exp.string_val("known"),
            Exp.gt(Exp.int_bin("age"), Exp.int_val(65)),
            Exp.string_val("senior"),
            Exp.string_val("other"),
        ])
        assert parse_ael(
            'when($.name in ["Bob", "Mary"] => "known",'
            ' $.age > 65 => "senior",'
            ' default => "other")'
        ) == expected

    def test_in_with_bin_right_in_when_cond(self):
        expected = Exp.cond([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.int_bin("status"),
                Exp.list_bin("allowedStatuses"),
                [],
            ),
            Exp.string_val("ok"),
            Exp.string_val("rejected"),
        ])
        assert parse_ael(
            'when($.status.get(type: INT) in $.allowedStatuses'
            ' => "ok", default => "rejected")'
        ) == expected

    def test_when_result_with_in_condition(self):
        expected = Exp.eq(
            Exp.string_bin("label"),
            Exp.cond([
                Exp.list_get_by_value(
                    ListReturnType.EXISTS,
                    Exp.string_bin("name"),
                    Exp.list_val(["Bob"]),
                    [],
                ),
                Exp.string_val("VIP"),
                Exp.string_val("regular"),
            ]),
        )
        assert parse_ael(
            '$.label.get(type: STRING) =='
            ' (when($.name in ["Bob"] => "VIP", default => "regular"))'
        ) == expected

    def test_in_inside_when_with_let_variable(self):
        expected = Exp.exp_let([
            Exp.def_("allowed", Exp.list_val(["Bob", "Mary"])),
            Exp.cond([
                Exp.list_get_by_value(
                    ListReturnType.EXISTS,
                    Exp.string_bin("name"),
                    Exp.var("allowed"),
                    [],
                ),
                Exp.string_val("found"),
                Exp.string_val("missing"),
            ]),
        ])
        assert parse_ael(
            'let (allowed = ["Bob", "Mary"]) then'
            ' (when($.name.get(type: STRING) in ${allowed} => "found",'
            ' default => "missing"))'
        ) == expected

    def test_in_inside_when_with_placeholder(self):
        expected = Exp.cond([
            Exp.list_get_by_value(
                ListReturnType.EXISTS,
                Exp.string_bin("name"),
                Exp.list_val(["Bob"]),
                [],
            ),
            Exp.string_val("match"),
            Exp.string_val("no match"),
        ])
        assert parse_ael(
            'when($.name.get(type: STRING) in ?0 => "match",'
            ' default => "no match")',
            ["Bob"],
        ) == expected


# ---------------------------------------------------------------------------
# Negative tests (error / rejection cases)
# ---------------------------------------------------------------------------
class TestInNegative:

    # --- Right operand is not a list ---

    def test_neg_right_operand_string(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('$.name in "Bob"')

    def test_neg_right_operand_int(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in 42")

    def test_neg_right_operand_float(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in 1.5")

    def test_neg_right_operand_bool(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in true")

    def test_neg_right_operand_map(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('$.name in {"a": 1}')

    def test_neg_right_operand_metadata(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in $.ttl()")

    # --- Mixed types in literal list ---

    def test_neg_mixed_int_and_string_in_list(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael('$.bin in [1, "hello"]')

    def test_neg_mixed_bool_and_int_in_list(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael("$.bin in [true, 42]")

    def test_neg_mixed_float_and_string_in_list(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael('$.bin in [1.5, "hello"]')

    def test_neg_mixed_int_and_float_in_list(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael("$.bin in [1, 1.5]")

    def test_neg_literal_in_mixed_type_list(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael('"x" in [1, "y"]')

    def test_neg_path_in_mixed_type_list(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael('$.rooms.room1.name in [1, "y"]')

    def test_neg_variable_in_mixed_type_list(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael('let (x = 1) then (${x} in [1, "y"])')

    # --- Placeholder resolves to non-list right operand ---

    def test_neg_placeholder_resolves_to_str(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in ?0", "Bob")

    def test_neg_placeholder_resolves_to_int(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in ?0", 42)

    def test_neg_placeholder_resolves_to_float(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in ?0", 1.5)

    def test_neg_placeholder_resolves_to_bool(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name in ?0", True)

    def test_neg_placeholder_resolves_to_map(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael("$.name in ?0", {"a": 1})

    def test_neg_placeholder_in_mixed_type_list(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael('?0 in [1, "y"]', 42)

    def test_neg_mixed_type_list_via_placeholder(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael(
                '$.name.get(type: STRING) in ?0',
                [1, "hello"],
            )

    # --- Ambiguous left operand (no explicit type, right is not a literal list) ---

    def test_neg_bin_in_bin_ambiguous(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael("$.itemType in $.allowedItems")

    def test_neg_bin_in_path_ambiguous(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael("$.name in $.rooms.room1.name")

    def test_neg_bin_in_list_designator_ambiguous(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael("$.name in $.binName.[]")

    def test_neg_bin_in_placeholder_ambiguous(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael("$.name in ?0", [])

    def test_neg_path_in_bin_ambiguous(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael("$.rooms.room1.name in $.list")

    def test_neg_path_in_path_ambiguous(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael("$.rooms.room1.a in $.rooms.room2.b")

    def test_neg_path_in_placeholder_ambiguous(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael("$.rooms.room1.name in ?0", [])

    def test_neg_bin_in_variable_ambiguous(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael('let (x = ["a"]) then ($.name in ${x})')

    # --- Variable bound to non-list as right operand ---

    def test_neg_var_bound_to_int(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = 1) then ("100" in ${x})')

    def test_neg_var_bound_to_float(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = 1.5) then ("100" in ${x})')

    def test_neg_var_bound_to_bool(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = true) then ("100" in ${x})')

    def test_neg_var_bound_to_string(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = "hello") then ("100" in ${x})')

    def test_neg_var_bound_to_map(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = {"a": 1}) then ("100" in ${x})')

    def test_neg_var_bound_to_metadata(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = $.ttl()) then ("100" in ${x})')

    def test_neg_var_bound_to_arithmetic_expr(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = 1 + 2) then ("foo" in ${x})')

    def test_neg_var_bound_to_function_expr(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = abs(1)) then ("foo" in ${x})')

    def test_neg_var_bound_to_explicit_int_bin(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = $.someBin.get(type: INT)) then ("foo" in ${x})')

    def test_neg_var_bound_to_explicit_str_path(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = $.a.b.get(type: STRING)) then ("foo" in ${x})')

    # --- Nested LET variable validation ---

    def test_neg_nested_let_outer_non_list_var(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = 1) then (let (y = [1, 2]) then ("foo" in ${x}))')

    def test_neg_nested_shadowed_var_let_scalar(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = [1]) then (let (x = 1) then ("foo" in ${x}))')

    def test_neg_not_wrapping_in_let_scalar_var(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = 1) then (not("foo" in ${x}))')

    def test_neg_var_bound_to_count_path(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('let (x = $.a.[].count()) then ("foo" in ${x})')

    def test_neg_var_def_let_in_scalar_var(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("let (x = 5, y = ($.bin.get(type: INT) in ${x})) then (${y} == true)")

    # --- Explicit bin in non-list placeholder ---

    def test_neg_explicit_bin_in_not_list(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("$.name.get(type: STRING) in ?0", "Bob")

    def test_neg_placeholder_in_not_list(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael("?0 in ?1", "gold", "notAList")

    # --- Missing placeholder values ---

    def test_neg_placeholder_missing_value(self):
        with pytest.raises(AelParseException, match="no placeholder values provided"):
            parse_ael("$.name.get(type: STRING) in ?0")

    def test_neg_placeholder_index_out_of_bounds(self):
        with pytest.raises(AelParseException, match="Missing value for placeholder"):
            parse_ael("$.name.get(type: STRING) in ?1", 42)

    # --- IN inside WHEN (regression) ---

    def test_neg_ambiguous_left_in_when_cond(self):
        with pytest.raises(AelParseException, match="cannot infer the type"):
            parse_ael('when($.name in $.allowedNames => "ok", default => "no")')

    def test_neg_non_list_right_in_when_cond(self):
        with pytest.raises(AelParseException, match="IN operation requires a List"):
            parse_ael('when($.name.get(type: STRING) in "Bob" => "ok", default => "no")')

    def test_neg_mixed_type_list_in_when_cond(self):
        with pytest.raises(AelParseException, match="same type"):
            parse_ael('when($.name in [1, "hello"] => "ok", default => "no")')


# ---------------------------------------------------------------------------
# Grammar conflict: the keyword 'in' used in non-IN contexts
# ---------------------------------------------------------------------------
class TestInGrammarConflicts:

    def test_case_insensitive_in(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.string_bin("name"),
            Exp.list_val(["Bob"]),
            [],
        )
        assert parse_ael('$.name IN ["Bob"]') == expected
        assert parse_ael('$.name In ["Bob"]') == expected
        assert parse_ael('$.name iN ["Bob"]') == expected
        assert parse_ael('$.name in ["Bob"]') == expected

    def test_bin_named_in_equality(self):
        expected = Exp.eq(Exp.int_bin("in"), Exp.int_val(5))
        assert parse_ael("$.in == 5") == expected

    def test_bin_named_in_in_list(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_bin("in"),
            Exp.list_val([1, 2]),
            [],
        )
        assert parse_ael("$.in in [1, 2]") == expected

    def test_map_key_named_in_nested(self):
        expected = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("in"),
                Exp.map_bin("map"),
                [CTX.map_key("a")],
            ),
            Exp.int_val(5),
        )
        assert parse_ael("$.map.a.in > 5") == expected

    def test_simple_map_key_named_in(self):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("in"),
                Exp.map_bin("list"),
                [],
            ),
            Exp.int_val(5),
        )
        assert parse_ael("$.list.in == 5") == expected

    def test_list_value_named_in(self):
        expected = Exp.eq(
            Exp.list_get_by_value(
                ListReturnType.VALUE,
                Exp.string_val("in"),
                Exp.list_bin("listBin"),
                [],
            ),
            Exp.string_val("hello"),
        )
        assert parse_ael('$.listBin.[=in].get(type: STRING) == "hello"') == expected

    def test_list_value_named_in_upper_case(self):
        expected = Exp.eq(
            Exp.list_get_by_value(
                ListReturnType.VALUE,
                Exp.string_val("IN"),
                Exp.list_bin("listBin"),
                [],
            ),
            Exp.string_val("hello"),
        )
        assert parse_ael('$.listBin.[=IN].get(type: STRING) == "hello"') == expected

    def test_map_value_named_in(self):
        expected = Exp.eq(
            Exp.map_get_by_value(
                MapReturnType.VALUE,
                Exp.string_val("in"),
                Exp.map_bin("mapBin"),
                [],
            ),
            Exp.string_val("hello"),
        )
        assert parse_ael('$.mapBin.{=in}.get(type: STRING) == "hello"') == expected

    def test_map_value_named_in_upper_case(self):
        expected = Exp.eq(
            Exp.map_get_by_value(
                MapReturnType.VALUE,
                Exp.string_val("IN"),
                Exp.map_bin("mapBin"),
                [],
            ),
            Exp.string_val("hello"),
        )
        assert parse_ael('$.mapBin.{=IN}.get(type: STRING) == "hello"') == expected

    def test_map_key_range_start_in(self):
        expected = Exp.map_get_by_key_range(
            MapReturnType.VALUE,
            Exp.string_val("in"),
            Exp.string_val("z"),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{in-z}") == expected

    def test_map_key_range_end_in(self):
        expected = Exp.map_get_by_key_range(
            MapReturnType.VALUE,
            Exp.string_val("a"),
            Exp.string_val("in"),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{a-in}") == expected

    def test_map_key_range_open_end_in(self):
        expected = Exp.map_get_by_key_range(
            MapReturnType.VALUE,
            Exp.string_val("in"),
            None,
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{in-}") == expected

    def test_inverted_key_range_start_in(self):
        expected = Exp.map_get_by_key_range(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.string_val("in"),
            Exp.string_val("z"),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{!in-z}") == expected

    def test_inverted_key_range_end_in(self):
        expected = Exp.map_get_by_key_range(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.string_val("a"),
            Exp.string_val("in"),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{!a-in}") == expected

    def test_map_key_list_with_in(self):
        expected = Exp.map_get_by_key_list(
            MapReturnType.VALUE,
            Exp.list_val(["in", "z"]),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{in,z}") == expected

    def test_map_key_list_only_in(self):
        expected = Exp.map_get_by_key_list(
            MapReturnType.VALUE,
            Exp.list_val(["in"]),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{in}") == expected

    def test_inverted_key_list_with_in(self):
        expected = Exp.map_get_by_key_list(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.list_val(["in", "z"]),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{!in,z}") == expected

    def test_relative_index_with_key_in(self):
        expected = Exp.map_get_by_key_relative_index_range_count(
            MapReturnType.VALUE,
            Exp.string_val("in"),
            Exp.int_val(0),
            Exp.int_val(1),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{0:1~in}") == expected

    def test_inverted_relative_index_key_in(self):
        expected = Exp.map_get_by_key_relative_index_range_count(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.string_val("in"),
            Exp.int_val(0),
            Exp.int_val(1),
            Exp.map_bin("mapBin"),
            [],
        )
        assert parse_ael("$.mapBin.{!0:1~in}") == expected
