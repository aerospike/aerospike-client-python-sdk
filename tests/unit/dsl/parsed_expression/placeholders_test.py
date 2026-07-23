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

"""Unit tests for AEL placeholder expressions. Order matches PlaceholdersTests.

"""

import pytest

from aerospike_sdk import (
    CTX,
    ExpType,
    Filter,
    ListReturnType,
    MapReturnType,
    AelParseException,
    Exp,
    Index,
    IndexContext,
    IndexTypeEnum,
    parse_ael,
    parse_ael_with_index,
)

NAMESPACE = "test"


class TestPlaceholders:
    """Test placeholder expressions."""

    def test_int_placeholder(self):
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))
        assert parse_ael("$.intBin1 > ?0", 100) == expected

    def test_string_placeholder(self):
        expected = Exp.gt(Exp.string_bin("strBin1"), Exp.string_val("str"))
        assert parse_ael("$.strBin1 > ?0", "str") == expected

        expected_quoted = Exp.gt(Exp.string_bin("strBin1"), Exp.string_val("'str'"))
        assert parse_ael("$.strBin1 > ?0", "'str'") == expected_quoted

        expected_double = Exp.gt(Exp.string_bin("strBin1"), Exp.string_val('"str"'))
        assert parse_ael("$.strBin1 > ?0", '"str"') == expected_double

    def test_blob_placeholder(self):
        data = bytes([1, 2, 3])
        expected = Exp.gt(
            Exp.blob_bin("blobBin1"),
            Exp.blob_val(list(data)),
        )
        assert parse_ael("$.blobBin1.get(type: BLOB) > ?0", data) == expected

    def test_float_placeholder(self):
        expected = Exp.gt(Exp.float_bin("floatBin"), Exp.float_val(3.14))
        assert parse_ael("$.floatBin > ?0", 3.14) == expected

    def test_bool_placeholder(self):
        expected = Exp.eq(Exp.bool_bin("active"), Exp.bool_val(True))
        assert parse_ael("$.active == ?0", True) == expected

    def test_multiple_placeholders(self):
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(200))
        ])
        assert parse_ael("$.intBin1 > ?0 and $.intBin2 > ?1", 100, 200) == expected

    def test_placeholder_in_arithmetic(self):
        expected = Exp.gt(Exp.num_add([Exp.int_bin("apples"), Exp.int_val(5)]), Exp.int_val(10))
        assert parse_ael("($.apples + ?0) > ?1", 5, 10) == expected

    def test_placeholder_with_metadata(self):
        expected = Exp.le(Exp.ttl(), Exp.int_val(86400))
        assert parse_ael("$.ttl() <= ?0", 86400) == expected

    def test_placeholder_in_when(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.eq(Exp.int_bin("who"), Exp.int_val(2)),
            Exp.string_val("fred"),
            Exp.string_val("other")
        ])
        assert parse_ael(
            "when ($.who == ?0 => ?1, $.who == ?2 => ?3, default => ?4)",
            1, "bob", 2, "fred", "other",
        ) == expected

    def test_missing_placeholder_value_raises_error(self):
        with pytest.raises(AelParseException, match="no placeholder values provided"):
            parse_ael("$.intBin1 > ?0")

    def test_missing_second_placeholder_raises_error(self):
        with pytest.raises(AelParseException, match="Missing value for placeholder \\?1"):
            parse_ael("$.intBin1 > ?0 and $.intBin2 > ?1", 100)

    def test_extra_placeholder_values_ignored(self):
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))
        assert parse_ael("$.intBin1 > ?0", 100, 200) == expected

    def test_int_bin_gt_has_index(self):
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(NAMESPACE, indexes)
        result = parse_ael_with_index("$.intBin1 > ?0", ctx, (100,))
        assert result.filter is not None
        assert str(result.filter) == str(Filter.range("intBin1", 101, 2**63 - 1))
        assert result.exp is None

    def test_int_bin_gt_and_all_indexes(self):
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(NAMESPACE, indexes)
        result = parse_ael_with_index(
            "$.intBin1 > ?0 and $.intBin2 > ?1",
            ctx,
            (100, 100),
        )
        assert result.filter is not None
        assert str(result.filter) == str(Filter.range("intBin2", 101, 2**63 - 1))
        expected_exp = Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))
        assert result.exp == expected_exp

    def test_arithmetic_expression_with_index(self):
        indexes = [
            Index(bin="apples", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=1),
            Index(bin="bananas", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(NAMESPACE, indexes)
        result = parse_ael_with_index(
            "($.apples + ?0) > ?1",
            ctx,
            (5, 10),
        )
        assert result.filter is not None
        assert str(result.filter) == str(Filter.range("apples", 6, 2**63 - 1))
        assert result.exp is None

    def test_map_nested_exp(self):
        expected_int = Exp.gt(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.INT,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.int_val(200),
        )
        result = parse_ael("$.mapBin1.a.bb.bcc > ?0", 200)
        assert result == expected_int
        result = parse_ael("$.mapBin1.a.bb.bcc.get(type: INT) > ?0", 200)
        assert result == expected_int
        result = parse_ael("$.mapBin1.a.bb.bcc.get(type: INT, return: VALUE) > ?0", 200)
        assert result == expected_int

        expected_str = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE,
                ExpType.STRING,
                Exp.string_val("bcc"),
                Exp.map_bin("mapBin1"),
                [CTX.map_key("a"), CTX.map_key("bb")],
            ),
            Exp.string_val("stringVal"),
        )
        result = parse_ael('$.mapBin1.a.bb.bcc == ?0', "stringVal")
        assert result == expected_str
        result = parse_ael('$.mapBin1.a.bb.bcc.get(type: STRING) == ?0', "stringVal")
        assert result == expected_str
        result = parse_ael(
            '$.mapBin1.a.bb.bcc.get(type: STRING, return: VALUE) == ?0',
            "stringVal",
        )
        assert result == expected_str

    def test_nested_lists_exp_with_different_context_types(self):
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
        result = parse_ael('$.listBin1.[5].[#-1] == ?0', "stringVal")
        assert result == expected_rank
        result = parse_ael('$.listBin1.[5].[#-1].get(type: STRING) == ?0', "stringVal")
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
        result = parse_ael("$.listBin1.[5].[#-1].[=100] == ?0", 200)
        assert result == expected_value

    def test_fourth_degree_complicated_explicit_float(self):
        expected = Exp.gt(
            Exp.num_add([
                Exp.num_add([Exp.float_bin("apples"), Exp.float_bin("bananas")]),
                Exp.num_add([Exp.float_bin("oranges"), Exp.float_bin("acai")]),
            ]),
            Exp.float_val(10.5),
        )
        result = parse_ael(
            "(($.apples.get(type: FLOAT) + $.bananas.get(type: FLOAT))"
            " + ($.oranges.get(type: FLOAT) + $.acai.get(type: FLOAT))) > ?0",
            10.5,
        )
        assert result == expected

    def test_complicated_when_explicit_type_string(self):
        expected = Exp.eq(
            Exp.string_bin("a"),
            Exp.cond([
                Exp.eq(Exp.int_bin("b"), Exp.int_val(1)), Exp.string_bin("a1"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(2)), Exp.string_bin("a2"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(3)), Exp.string_bin("a3"),
                Exp.string_val("hello"),
            ]),
        )
        result = parse_ael(
            "$.a.get(type: STRING) == "
            "(when($.b == ?0 => $.a1.get(type: STRING),"
            " $.b == ?1 => $.a2.get(type: STRING),"
            " $.b == ?2 => $.a3.get(type: STRING),"
            " default => ?3))",
            1, 2, 3, "hello",
        )
        assert result == expected

    def test_exclusive_no_indexes(self):
        expected = Exp.xor([
            Exp.eq(Exp.string_bin("hand"), Exp.string_val("stand")),
            Exp.eq(Exp.string_bin("pun"), Exp.string_val("done")),
        ])
        result = parse_ael(
            'exclusive($.hand == ?0, $.pun == ?1)',
            "stand", "done",
        )
        assert result == expected
