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

"""Unit tests for AEL map expressions. Order matches MapExpressionsTests."""

from aerospike_async import CTX, ExpType, MapReturnType
from aerospike_sdk import Exp, parse_ael


class TestMapExpressions:
    """Test map expressions."""

    def test_map_one_level_expressions(self):
        """One-level map key: int and string, implicit and get(type)/asInt()."""
        expected_int = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("a"),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.a == 200")
        assert result == expected_int
        result = parse_ael("$.mapBin1.a.get(type: INT) == 200")
        assert result == expected_int
        result = parse_ael("$.mapBin1.a.get(type: INT, return: VALUE) == 200")
        assert result == expected_int

        expected_str = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("a"),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_ael('$.mapBin1.a == "stringVal"')
        assert result == expected_str
        result = parse_ael('$.mapBin1.a.get(type: STRING) == "stringVal"')
        assert result == expected_str
        result = parse_ael('$.mapBin1.a.get(type: STRING, return: VALUE) == "stringVal"')
        assert result == expected_str

    def test_map_nested_level_expressions(self):
        """Nested map keys: a.bb.bcc with int and string."""
        expected_gt = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.a.bb.bcc > 200")
        assert result == expected_gt
        result = parse_ael("$.mapBin1.a.bb.bcc.get(type: INT) > 200")
        assert result == expected_gt
        result = parse_ael("$.mapBin1.a.bb.bcc.get(type: INT, return: VALUE) > 200")
        assert result == expected_gt

        expected_eq = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_ael('$.mapBin1.a.bb.bcc == "stringVal"')
        assert result == expected_eq
        result = parse_ael('$.mapBin1.a.bb.bcc.get(type: STRING) == "stringVal"')
        assert result == expected_eq
        result = parse_ael('$.mapBin1.a.bb.bcc.get(type: STRING, return: VALUE) == "stringVal"')
        assert result == expected_eq

    def test_quoted_string_in_expression_path(self):
        """Quoted map keys in path: \"bb\", 'bb', \"127.0.0.1\", spaces."""
        expected = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.a.bb.bcc.get(type: INT) > 200")
        assert result == expected
        result = parse_ael('$.mapBin1.a."bb".bcc.get(type: INT) > 200')
        assert result == expected
        result = parse_ael("$.mapBin1.a.'bb'.bcc.get(type: INT) > 200")
        assert result == expected

        expected_ip = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("127.0.0.1")],
            ),
            Exp.int_val(200),
        )
        result = parse_ael('$.mapBin1."127.0.0.1".bcc.get(type: INT) > 200')
        assert result == expected_ip
        result = parse_ael("$.mapBin1.'127.0.0.1'.bcc.get(type: INT) > 200")
        assert result == expected_ip

        expected_spaces = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("127 0 0 1")],
            ),
            Exp.int_val(200),
        )
        result = parse_ael('$.mapBin1."127 0 0 1".bcc.get(type: INT) > 200')
        assert result == expected_spaces
        result = parse_ael("$.mapBin1.'127 0 0 1'.bcc.get(type: INT) > 200")
        assert result == expected_spaces

    def test_map_size(self):
        """$.mapBin1.{}.count() uses map_size; $.mapBin1.count() defaults to list_size."""
        result = parse_ael("$.mapBin1.{}.count() > 200")
        expected_map = Exp.gt(
            Exp.map_size(Exp.map_bin("mapBin1"), []),
            Exp.int_val(200),
        )
        assert result == expected_map

        result = parse_ael("$.mapBin1.count() > 200")
        expected_list = Exp.gt(
            Exp.list_size(Exp.list_bin("mapBin1"), []),
            Exp.int_val(200),
        )
        assert result == expected_list

    def test_nested_map_size(self):
        """$.mapBin1.a.{}.count() uses map_size; $.mapBin1.a.count() defaults to list_size."""
        expected_map = Exp.eq(
            Exp.map_size(
                Exp.map_get_by_key(
                    MapReturnType.VALUE,
                    ExpType.MAP,
                    Exp.string_val("a"),
                    Exp.map_bin("mapBin1"),
                    [],
                ),
                [],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.a.{}.count() == 200")
        assert result == expected_map

        expected_list = Exp.eq(
            Exp.list_size(
                Exp.map_get_by_key(
                    MapReturnType.VALUE,
                    ExpType.LIST,
                    Exp.string_val("a"),
                    Exp.map_bin("mapBin1"),
                    [],
                ),
                [],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.a.count() == 200")
        assert result == expected_list

    def test_nested_map_size_with_context(self):
        """$.mapBin1.a.b.{}.count() uses map_size; $.mapBin1.a.b.count() defaults to list_size."""
        expected_map = Exp.eq(
            Exp.map_size(
                Exp.map_get_by_key(
                    MapReturnType.VALUE,
                    ExpType.MAP,
                    Exp.string_val("b"),
                    Exp.map_bin("mapBin1"),
                    [CTX.map_key("a")],
                ),
                [],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.a.b.{}.count() == 200")
        assert result == expected_map

        expected_list = Exp.eq(
            Exp.list_size(
                Exp.map_get_by_key(
                    MapReturnType.VALUE,
                    ExpType.LIST,
                    Exp.string_val("b"),
                    Exp.map_bin("mapBin1"),
                    [CTX.map_key("a")],
                ),
                [],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.a.b.count() == 200")
        assert result == expected_list

    def test_map_by_index(self):
        """$.mapBin1.{0} with int and string, get(type)/asInt()."""
        expected_int = Exp.eq(
            Exp.map_get_by_index(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.mapBin1.{0} == 100")
        assert result == expected_int
        result = parse_ael("$.mapBin1.{0}.get(type: INT) == 100")
        assert result == expected_int
        result = parse_ael("$.mapBin1.{0}.get(type: INT, return: VALUE) == 100")
        assert result == expected_int

        expected_str = Exp.eq(
            Exp.map_get_by_index(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.int_val(0),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.string_val("value"),
        )
        result = parse_ael("$.mapBin1.{0} == 'value'")
        assert result == expected_str
        result = parse_ael("$.mapBin1.{0}.get(type: STRING) == 'value'")
        assert result == expected_str
        result = parse_ael("$.mapBin1.{0}.get(type: STRING, return: VALUE) == 'value'")
        assert result == expected_str

    def test_map_by_value(self):
        """$.mapBin1.{=100} == 100 with get(type)/asInt()."""
        expected = Exp.eq(
            Exp.map_get_by_value(
                MapReturnType.VALUE,
                Exp.int_val(100),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.mapBin1.{=100} == 100")
        assert result == expected
        result = parse_ael("$.mapBin1.{=100}.get(type: INT) == 100")
        assert result == expected
        result = parse_ael("$.mapBin1.{=100}.get(type: INT, return: VALUE) == 100")
        assert result == expected

    def test_map_by_value_count(self):
        """$.mapBin1.{=100}.count() and {}.count()."""
        expected = Exp.gt(
            Exp.map_get_by_value(
                MapReturnType.COUNT,
                Exp.int_val(100),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.int_val(0),
        )
        result = parse_ael("$.mapBin1.{=100}.count() > 0")
        assert result == expected
        result = parse_ael("$.mapBin1.{=100}.{}.count() > 0")
        assert result == expected

    def test_map_by_rank(self):
        """$.mapBin1.{#-1} with get(type)/asInt()."""
        expected = Exp.eq(
            Exp.map_get_by_rank(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(-1),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.mapBin1.{#-1} == 100")
        assert result == expected
        result = parse_ael("$.mapBin1.{#-1}.get(type: INT) == 100")
        assert result == expected
        result = parse_ael("$.mapBin1.{#-1}.get(type: INT, return: VALUE) == 100")
        assert result == expected

    def test_map_by_rank_with_nesting(self):
        """$.mapBin1.a.{#-1} with get(type)/asInt()."""
        expected = Exp.eq(
            Exp.map_get_by_rank(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(-1),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a")],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.mapBin1.a.{#-1} == 100")
        assert result == expected
        result = parse_ael("$.mapBin1.a.{#-1}.get(type: INT) == 100")
        assert result == expected
        result = parse_ael("$.mapBin1.a.{#-1}.get(type: INT, return: VALUE) == 100")
        assert result == expected

    def test_nested_lists_with_different_context_types(self):
        """$.mapBin1.{5}.{#-1} (map index then rank) and {=100} value."""
        expected_rank = Exp.eq(
            Exp.map_get_by_rank(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.int_val(-1),
                Exp.map_bin("mapBin1"),
                [CTX.map_index(5)],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_ael('$.mapBin1.{5}.{#-1} == "stringVal"')
        assert result == expected_rank
        result = parse_ael('$.mapBin1.{5}.{#-1}.get(type: STRING) == "stringVal"')
        assert result == expected_rank

        expected_value = Exp.eq(
            Exp.map_get_by_value(
                MapReturnType.VALUE,
                Exp.int_val(100),
                Exp.map_bin("mapBin1"),
                [CTX.map_index(5), CTX.map_rank(-1)],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.{5}.{#-1}.{=100} == 200")
        assert result == expected_value

    def test_map_key_range(self):
        """$.mapBin1.{a-c}, {!a-c}, {a-} with quoted variants."""
        expected = Exp.map_get_by_key_range(
            MapReturnType.VALUE,
            Exp.string_val("a"),
            Exp.string_val("c"),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{a-c}")
        assert result == expected
        result = parse_ael('$.mapBin1.{"a"-"c"}')
        assert result == expected

        expected_inv = Exp.map_get_by_key_range(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.string_val("a"),
            Exp.string_val("c"),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{!a-c}")
        assert result == expected_inv
        result = parse_ael('$.mapBin1.{!"a"-"c"}')
        assert result == expected_inv

        expected_open = Exp.map_get_by_key_range(
            MapReturnType.VALUE,
            Exp.string_val("a"),
            None,
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{a-}")
        assert result == expected_open
        result = parse_ael('$.mapBin1.{"a"-}')
        assert result == expected_open

    def test_map_key_list(self):
        """$.mapBin1.{a,b,c} and {!a,b,c}."""
        expected = Exp.map_get_by_key_list(
            MapReturnType.VALUE,
            Exp.list_val(["a", "b", "c"]),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{a,b,c}")
        assert result == expected
        result = parse_ael('$.mapBin1.{"a","b","c"}')
        assert result == expected

        expected_inv = Exp.map_get_by_key_list(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.list_val(["a", "b", "c"]),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{!a,b,c}")
        assert result == expected_inv
        result = parse_ael('$.mapBin1.{!"a","b","c"}')
        assert result == expected_inv

    def test_map_index_range(self):
        """$.mapBin1.{1:3}, {-3:1}, {!2:4}, {1:}."""
        expected = Exp.map_get_by_index_range_count(
            MapReturnType.VALUE,
            Exp.int_val(1),
            Exp.int_val(2),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{1:3}")
        assert result == expected

        expected_neg = Exp.map_get_by_index_range_count(
            MapReturnType.VALUE,
            Exp.int_val(-3),
            Exp.int_val(4),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{-3:1}")
        assert result == expected_neg

        expected_inv = Exp.map_get_by_index_range_count(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.int_val(2),
            Exp.int_val(2),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{!2:4}")
        assert result == expected_inv

        expected_open = Exp.map_get_by_index_range(
            MapReturnType.VALUE,
            Exp.int_val(1),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{1:}")
        assert result == expected_open

    def test_map_value_list(self):
        """$.mapBin1.{=a,b,c}, {=1,2,3}, {!=a,b,c}."""
        expected_str = Exp.map_get_by_value_list(
            MapReturnType.VALUE,
            Exp.list_val(["a", "b", "c"]),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{=a,b,c}")
        assert result == expected_str
        result = parse_ael('$.mapBin1.{="a","b","c"}')
        assert result == expected_str

        expected_int = Exp.map_get_by_value_list(
            MapReturnType.VALUE,
            Exp.list_val([1, 2, 3]),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{=1,2,3}")
        assert result == expected_int

        expected_inv = Exp.map_get_by_value_list(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.list_val(["a", "b", "c"]),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{!=a,b,c}")
        assert result == expected_inv
        result = parse_ael('$.mapBin1.{!="a","b","c"}')
        assert result == expected_inv

    def test_map_value_range(self):
        """$.mapBin1.{=111:334}, {!=10:20}, {=111:}."""
        expected = Exp.map_get_by_value_range(
            MapReturnType.VALUE,
            Exp.int_val(111),
            Exp.int_val(334),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{=111:334}")
        assert result == expected

        expected_inv = Exp.map_get_by_value_range(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.int_val(10),
            Exp.int_val(20),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{!=10:20}")
        assert result == expected_inv

        expected_open = Exp.map_get_by_value_range(
            MapReturnType.VALUE,
            Exp.int_val(111),
            None,
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{=111:}")
        assert result == expected_open

    def test_map_rank_range(self):
        """$.mapBin1.{#0:3}, {!#0:3}, {#-3:}, {5}.{#-3:}."""
        expected = Exp.map_get_by_rank_range_count(
            MapReturnType.VALUE,
            Exp.int_val(0),
            Exp.int_val(3),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{#0:3}")
        assert result == expected

        expected_inv = Exp.map_get_by_rank_range_count(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.int_val(0),
            Exp.int_val(3),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{!#0:3}")
        assert result == expected_inv

        expected_open = Exp.map_get_by_rank_range(
            MapReturnType.VALUE,
            Exp.int_val(-3),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{#-3:}")
        assert result == expected_open

        expected_ctx = Exp.map_get_by_rank_range(
            MapReturnType.VALUE,
            Exp.int_val(-3),
            Exp.map_bin("mapBin1"),
            [CTX.map_index(5)],
        )
        result = parse_ael("$.mapBin1.{5}.{#-3:}")
        assert result == expected_ctx

    def test_map_rank_range_relative(self):
        """$.mapBin1.{#-1:1~10}, {!#-1:1~10}, {#-2:~10}."""
        expected = Exp.map_get_by_value_relative_rank_range_count(
            MapReturnType.VALUE,
            Exp.int_val(10),
            Exp.int_val(-1),
            Exp.int_val(2),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{#-1:1~10}")
        assert result == expected

        expected_inv = Exp.map_get_by_value_relative_rank_range_count(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.int_val(10),
            Exp.int_val(-1),
            Exp.int_val(2),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{!#-1:1~10}")
        assert result == expected_inv

        expected_open = Exp.map_get_by_value_relative_rank_range(
            MapReturnType.VALUE,
            Exp.int_val(10),
            Exp.int_val(-2),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{#-2:~10}")
        assert result == expected_open

    def test_map_index_range_relative(self):
        """$.mapBin1.{0:1~a}, {!0:1~a}, {0:~a}."""
        expected = Exp.map_get_by_key_relative_index_range_count(
            MapReturnType.VALUE,
            Exp.string_val("a"),
            Exp.int_val(0),
            Exp.int_val(1),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{0:1~a}")
        assert result == expected

        expected_inv = Exp.map_get_by_key_relative_index_range_count(
            MapReturnType.VALUE | MapReturnType.INVERTED,
            Exp.string_val("a"),
            Exp.int_val(0),
            Exp.int_val(1),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{!0:1~a}")
        assert result == expected_inv

        expected_open = Exp.map_get_by_key_relative_index_range(
            MapReturnType.VALUE,
            Exp.string_val("a"),
            Exp.int_val(0),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.{0:~a}")
        assert result == expected_open

    def test_map_return_types(self):
        """get(return: COUNT), get(return: ORDERED_MAP), get(return: RANK)."""
        expected_count = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.COUNT,
                ExpType.INT,
                Exp.string_val("a"),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.int_val(5),
        )
        result = parse_ael("$.mapBin1.a.get(type: INT, return: COUNT) == 5")
        assert result == expected_count

        expected_ordered = Exp.map_get_by_key(
            MapReturnType.ORDERED_MAP,
            ExpType.STRING,
            Exp.string_val("a"),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.a.get(return: ORDERED_MAP)")
        assert result == expected_ordered

        expected_rank = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.RANK,
                ExpType.INT,
                Exp.string_val("a"),
                Exp.map_bin("mapBin1"),
                [],
            ),
            Exp.int_val(5),
        )
        result = parse_ael("$.mapBin1.a.get(type: INT, return: RANK) == 5")
        assert result == expected_rank

        # UNORDERED_MAP — the unordered counterpart of ORDERED_MAP. Both are
        # PAC v3 additions and must round-trip through the AEL pipeline.
        expected_unordered = Exp.map_get_by_key(
            MapReturnType.UNORDERED_MAP,
            ExpType.STRING,
            Exp.string_val("a"),
            Exp.map_bin("mapBin1"),
            [],
        )
        result = parse_ael("$.mapBin1.a.get(return: UNORDERED_MAP)")
        assert result == expected_unordered
