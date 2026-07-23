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

"""Unit tests for AEL list expressions."""

import pytest
from aerospike_sdk import CTX, Exp, ExpType, ListReturnType, parse_ael
from aerospike_sdk.ael.exceptions import AelParseException


class TestListExpressions:
    """Test list expressions."""

    def test_list_by_index_integer(self):
        """$.listBin1.[0] == 100 and explicit get/asInt variants."""
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[0] == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[0].get(type: INT) == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[0].get(type: INT, return: VALUE) == 100")
        assert result == expected

    def test_list_by_index_other_types(self):
        """$.listBin1.[0] with STRING and BOOL."""
        expected_str = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.STRING,
                Exp.int_val(0),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_ael('$.listBin1.[0] == "stringVal"')
        assert result == expected_str
        result = parse_ael('$.listBin1.[0].get(type: STRING) == "stringVal"')
        assert result == expected_str
        result = parse_ael('$.listBin1.[0].get(type: STRING, return: VALUE) == "stringVal"')
        assert result == expected_str

        expected_bool = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.BOOL,
                Exp.int_val(0),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.bool_val(True),
        )
        result = parse_ael("$.listBin1.[0] == true")
        assert result == expected_bool
        result = parse_ael("$.listBin1.[0].get(type: BOOL) == true")
        assert result == expected_bool
        result = parse_ael("$.listBin1.[0].get(type: BOOL, return: VALUE) == true")
        assert result == expected_bool

    def test_list_by_value(self):
        """$.listBin1.[=100] == 100 and variants."""
        expected = Exp.eq(
            Exp.list_get_by_value(
                ListReturnType.VALUE,
                Exp.int_val(100),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[=100] == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[=100].get(type: INT) == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[=100].get(type: INT, return: VALUE) == 100")
        assert result == expected

    def test_list_by_value_count(self):
        """$.listBin1.[=100].count() > 0."""
        expected = Exp.gt(
            Exp.list_get_by_value(
                ListReturnType.COUNT,
                Exp.int_val(100),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.int_val(0),
        )
        result = parse_ael("$.listBin1.[=100].count() > 0")
        assert result == expected
        result = parse_ael("$.listBin1.[=100].[].count() > 0")
        assert result == expected

    def test_list_by_rank(self):
        """$.listBin1.[#-1] == 100 and variants."""
        expected = Exp.eq(
            Exp.list_get_by_rank(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(-1),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[#-1] == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[#-1].get(type: INT) == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[#-1].get(type: INT, return: VALUE) == 100")
        assert result == expected

    def test_list_bin_element_equals_nested(self):
        """$.listBin1.[0].[0].[0] == 100 (triple nested index)."""
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("listBin1"),
                [CTX.list_index(0), CTX.list_index(0)],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[0].[0].[0] == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[0].[0].[0].get(type: INT) == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[0].[0].[0].get(type: INT, return: VALUE) == 100")
        assert result == expected

    def test_list_size(self):
        """$.listBin1.[].count() == 1 and $.listBin1.count() == 1."""
        expected = Exp.eq(
            Exp.list_size(Exp.list_bin("listBin1"), []),
            Exp.int_val(1),
        )
        result = parse_ael("$.listBin1.[].count() == 1")
        assert result == expected
        result = parse_ael("$.listBin1.count() == 1")
        assert result == expected

    def test_nested_list_size(self):
        """$.listBin1.[1].[].count() == 100 and .[1].count() == 100."""
        expected = Exp.eq(
            Exp.list_size(
                Exp.list_get_by_index(
                    ListReturnType.VALUE,
                    ExpType.LIST,
                    Exp.int_val(1),
                    Exp.list_bin("listBin1"),
                    [],
                ),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[1].[].count() == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[1].count() == 100")
        assert result == expected

    def test_nested_list_size_with_context(self):
        """$.listBin1.[1].[2].[].count() == 100 and .[1].[2].count() == 100."""
        expected = Exp.eq(
            Exp.list_size(
                Exp.list_get_by_index(
                    ListReturnType.VALUE,
                    ExpType.LIST,
                    Exp.int_val(2),
                    Exp.list_bin("listBin1"),
                    [CTX.list_index(1)],
                ),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[1].[2].[].count() == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[1].[2].count() == 100")
        assert result == expected

    def test_nested_lists(self):
        """$.listBin1.[5].[1].get(type: STRING) == \"stringVal\"."""
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE,
                ExpType.STRING,
                Exp.int_val(1),
                Exp.list_bin("listBin1"),
                [CTX.list_index(5)],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_ael('$.listBin1.[5].[1].get(type: STRING) == "stringVal"')
        assert result == expected

    def test_nested_lists_with_different_context_types(self):
        """Nested list rank and list rank + value."""
        expected_rank = Exp.eq(
            Exp.list_get_by_rank(
                ListReturnType.VALUE,
                ExpType.STRING,
                Exp.int_val(-1),
                Exp.list_bin("listBin1"),
                [CTX.list_index(5)],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_ael('$.listBin1.[5].[#-1] == "stringVal"')
        assert result == expected_rank
        result = parse_ael('$.listBin1.[5].[#-1].get(type: STRING) == "stringVal"')
        assert result == expected_rank

        expected_value = Exp.eq(
            Exp.list_get_by_value(
                ListReturnType.VALUE,
                Exp.int_val(100),
                Exp.list_bin("listBin1"),
                [CTX.list_index(5), CTX.list_rank(-1)],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.listBin1.[5].[#-1].[=100] == 200")
        assert result == expected_value

    def test_list_bin_element_count(self):
        """$.listBin1.[0].count() == 100 and .[0].[].count() == 100."""
        expected = Exp.eq(
            Exp.list_size(
                Exp.list_get_by_index(
                    ListReturnType.VALUE,
                    ExpType.LIST,
                    Exp.int_val(0),
                    Exp.list_bin("listBin1"),
                    [],
                ),
                [],
            ),
            Exp.int_val(100),
        )
        result = parse_ael("$.listBin1.[0].count() == 100")
        assert result == expected
        result = parse_ael("$.listBin1.[0].[].count() == 100")
        assert result == expected

    def test_negative_syntax_list(self):
        """Invalid list index syntax raises."""
        with pytest.raises(AelParseException):
            parse_ael('$.listBin1.[stringValue] == 100')

    def test_list_index_range(self):
        """$.listBin1.[1:3], [-3:1], [!2:4], [1:]."""
        expected_1_3 = Exp.list_get_by_index_range_count(
            ListReturnType.VALUE,
            Exp.int_val(1),
            Exp.int_val(2),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[1:3]")
        assert result == expected_1_3

        expected_neg = Exp.list_get_by_index_range_count(
            ListReturnType.VALUE,
            Exp.int_val(-3),
            Exp.int_val(4),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[-3:1]")
        assert result == expected_neg

        expected_inv = Exp.list_get_by_index_range_count(
            ListReturnType.VALUE | ListReturnType.INVERTED,
            Exp.int_val(2),
            Exp.int_val(2),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[!2:4]")
        assert result == expected_inv

        expected_open = Exp.list_get_by_index_range(
            ListReturnType.VALUE,
            Exp.int_val(1),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[1:]")
        assert result == expected_open

    def test_list_value_list(self):
        """$.listBin1.[=a,b,c], [=1,2,3], [!=a,b,c]."""
        expected_str = Exp.list_get_by_value_list(
            ListReturnType.VALUE,
            Exp.list_val(["a", "b", "c"]),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[=a,b,c]")
        assert result == expected_str
        result = parse_ael('$.listBin1.[="a","b","c"]')
        assert result == expected_str

        expected_int = Exp.list_get_by_value_list(
            ListReturnType.VALUE,
            Exp.list_val([1, 2, 3]),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[=1,2,3]")
        assert result == expected_int

        expected_inv = Exp.list_get_by_value_list(
            ListReturnType.VALUE | ListReturnType.INVERTED,
            Exp.list_val(["a", "b", "c"]),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[!=a,b,c]")
        assert result == expected_inv
        result = parse_ael('$.listBin1.[!="a","b","c"]')
        assert result == expected_inv

    def test_list_value_range(self):
        """$.listBin1.[=111:334], [!=10:20], [=111:]."""
        expected = Exp.list_get_by_value_range(
            ListReturnType.VALUE,
            Exp.int_val(111),
            Exp.int_val(334),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[=111:334]")
        assert result == expected

        expected_inv = Exp.list_get_by_value_range(
            ListReturnType.VALUE | ListReturnType.INVERTED,
            Exp.int_val(10),
            Exp.int_val(20),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[!=10:20]")
        assert result == expected_inv

        expected_open = Exp.list_get_by_value_range(
            ListReturnType.VALUE,
            Exp.int_val(111),
            None,
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[=111:]")
        assert result == expected_open

    def test_list_rank_range(self):
        """$.listBin1.[#0:3], [!#0:3], [#-3:], [5].[#-3:]."""
        expected = Exp.list_get_by_rank_range_count(
            ListReturnType.VALUE,
            Exp.int_val(0),
            Exp.int_val(3),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[#0:3]")
        assert result == expected

        expected_inv = Exp.list_get_by_rank_range_count(
            ListReturnType.VALUE | ListReturnType.INVERTED,
            Exp.int_val(0),
            Exp.int_val(3),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[!#0:3]")
        assert result == expected_inv

        expected_open = Exp.list_get_by_rank_range(
            ListReturnType.VALUE,
            Exp.int_val(-3),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[#-3:]")
        assert result == expected_open

        expected_ctx = Exp.list_get_by_rank_range(
            ListReturnType.VALUE,
            Exp.int_val(-3),
            Exp.list_bin("listBin1"),
            [CTX.list_index(5)],
        )
        result = parse_ael("$.listBin1.[5].[#-3:]")
        assert result == expected_ctx

    def test_list_rank_range_relative(self):
        """$.listBin1.[#-3:-1~b], [!#-3:-1~b], [#-3:~b]."""
        expected = Exp.list_get_by_value_relative_rank_range_count(
            ListReturnType.VALUE,
            Exp.string_val("b"),
            Exp.int_val(-3),
            Exp.int_val(2),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[#-3:-1~b]")
        assert result == expected

        expected_inv = Exp.list_get_by_value_relative_rank_range_count(
            ListReturnType.VALUE | ListReturnType.INVERTED,
            Exp.string_val("b"),
            Exp.int_val(-3),
            Exp.int_val(2),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[!#-3:-1~b]")
        assert result == expected_inv

        expected_open = Exp.list_get_by_value_relative_rank_range(
            ListReturnType.VALUE,
            Exp.string_val("b"),
            Exp.int_val(-3),
            Exp.list_bin("listBin1"),
            [],
        )
        result = parse_ael("$.listBin1.[#-3:~b]")
        assert result == expected_open

    def test_list_return_types(self):
        """get(return: COUNT), get(return: EXISTS), get(return: INDEX)."""
        expected_count = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.COUNT,
                ExpType.LIST,
                Exp.int_val(0),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.int_val(5),
        )
        result = parse_ael("$.listBin1.[0].get(return: COUNT) == 5")
        assert result == expected_count

        expected_exists = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.EXISTS,
                ExpType.BOOL,
                Exp.int_val(0),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.bool_val(True),
        )
        result = parse_ael("$.listBin1.[0].get(return: EXISTS) == true")
        assert result == expected_exists

        expected_index = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.INDEX,
                ExpType.INT,
                Exp.int_val(0),
                Exp.list_bin("listBin1"),
                [],
            ),
            Exp.int_val(1),
        )
        result = parse_ael("$.listBin1.[0].get(return: INDEX) == 1")
        assert result == expected_index
