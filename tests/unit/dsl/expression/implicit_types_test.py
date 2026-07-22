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

"""Unit tests for AEL implicit type inference."""

from aerospike_sdk import Exp, parse_ael


class TestImplicitTypes:
    """Test implicit type inference in AEL expressions."""

    def test_float_comparison(self):
        """Test $.floatBin1 >= 100.25."""
        expected = Exp.ge(Exp.float_bin("floatBin1"), Exp.float_val(100.25))
        result = parse_ael("$.floatBin1 >= 100.25")
        assert result == expected

    def test_boolean_comparison_true(self):
        """Test $.boolBin1 == true."""
        expected = Exp.eq(Exp.bool_bin("boolBin1"), Exp.bool_val(True))
        result = parse_ael("$.boolBin1 == true")
        assert result == expected

    def test_boolean_comparison_false_first(self):
        """Test false == $.boolBin1."""
        expected = Exp.eq(Exp.bool_val(False), Exp.bool_bin("boolBin1"))
        result = parse_ael("false == $.boolBin1")
        assert result == expected

    def test_boolean_comparison_not_equals(self):
        """Test $.boolBin1 != false."""
        expected = Exp.ne(Exp.bool_bin("boolBin1"), Exp.bool_val(False))
        result = parse_ael("$.boolBin1 != false")
        assert result == expected

    def test_bin_boolean_implicit_logical_and(self):
        """Test $.boolBin1 and $.boolBin2."""
        expected = Exp.and_([Exp.bool_bin("boolBin1"), Exp.bool_bin("boolBin2")])
        result = parse_ael("$.boolBin1 and $.boolBin2")
        assert result == expected

    def test_bin_boolean_implicit_logical_or(self):
        """Test $.boolBin1 or $.boolBin2."""
        expected = Exp.or_([Exp.bool_bin("boolBin1"), Exp.bool_bin("boolBin2")])
        result = parse_ael("$.boolBin1 or $.boolBin2")
        assert result == expected

    def test_bin_boolean_implicit_logical_not(self):
        """Test not($.boolBin1)."""
        expected = Exp.not_(Exp.bool_bin("boolBin1"))
        result = parse_ael("not($.boolBin1)")
        assert result == expected

    def test_bin_boolean_implicit_logical_exclusive(self):
        """Test exclusive($.boolBin1, $.boolBin2)."""
        expected = Exp.xor([Exp.bool_bin("boolBin1"), Exp.bool_bin("boolBin2")])
        result = parse_ael("exclusive($.boolBin1, $.boolBin2)")
        assert result == expected

    def test_implicit_default_int_comparison(self):
        """Test $.intBin1 < $.intBin2."""
        expected = Exp.lt(Exp.int_bin("intBin1"), Exp.int_bin("intBin2"))
        result = parse_ael("$.intBin1 < $.intBin2")
        assert result == expected

    def test_second_degree_implicit_casting_float(self):
        """Test ($.apples + $.bananas) > 10.5 - float literal infers float bins."""
        expected = Exp.gt(
            Exp.num_add([Exp.float_bin("apples"), Exp.float_bin("bananas")]), Exp.float_val(10.5)
        )
        result = parse_ael("($.apples + $.bananas) > 10.5")
        assert result == expected

    def test_second_degree_float_first_implicit_casting(self):
        """Test mixed float and int arithmetic."""
        expected = Exp.and_([
            Exp.gt(
                Exp.num_add([Exp.float_bin("apples"), Exp.float_bin("bananas")]),
                Exp.float_val(10.5)
            ),
            Exp.le(
                Exp.num_add([Exp.int_bin("oranges"), Exp.int_bin("grapes")]),
                Exp.int_val(5)
            )
        ])
        result = parse_ael("($.apples + $.bananas) > 10.5 and ($.oranges + $.grapes) <= 5")
        assert result == expected

    def test_second_degree_int_first_implicit_casting(self):
        """Test int first, then float arithmetic."""
        expected = Exp.and_([
            Exp.gt(
                Exp.num_add([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
                Exp.int_val(5)
            ),
            Exp.le(
                Exp.num_add([Exp.float_bin("oranges"), Exp.float_bin("grapes")]),
                Exp.float_val(10.5)
            )
        ])
        result = parse_ael("($.apples + $.bananas) > 5 and ($.oranges + $.grapes) <= 10.5")
        assert result == expected

    def test_third_degree_default_int(self):
        """Test (($.apples + $.bananas) + $.oranges) > 10."""
        expected = Exp.gt(
            Exp.num_add([
                Exp.num_add([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
                Exp.int_bin("oranges")
            ]),
            Exp.int_val(10)
        )
        result = parse_ael("(($.apples + $.bananas) + $.oranges) > 10")
        assert result == expected

    def test_third_degree_implicit_casting_float(self):
        """Test (($.apples + $.bananas) + $.oranges) > 10.5."""
        expected = Exp.gt(
            Exp.num_add([
                Exp.num_add([Exp.float_bin("apples"), Exp.float_bin("bananas")]),
                Exp.float_bin("oranges")
            ]),
            Exp.float_val(10.5)
        )
        result = parse_ael("(($.apples + $.bananas) + $.oranges) > 10.5")
        assert result == expected

    def test_fourth_degree_default_int(self):
        """Test (($.apples + $.bananas) + ($.oranges + $.acai)) > 10."""
        expected = Exp.gt(
            Exp.num_add([
                Exp.num_add([Exp.int_bin("apples"), Exp.int_bin("bananas")]),
                Exp.num_add([Exp.int_bin("oranges"), Exp.int_bin("acai")])
            ]),
            Exp.int_val(10)
        )
        result = parse_ael("(($.apples + $.bananas) + ($.oranges + $.acai)) > 10")
        assert result == expected

    def test_fourth_degree_implicit_casting_float(self):
        """Test (($.apples + $.bananas) + ($.oranges + $.acai)) > 10.5."""
        expected = Exp.gt(
            Exp.num_add([
                Exp.num_add([Exp.float_bin("apples"), Exp.float_bin("bananas")]),
                Exp.num_add([Exp.float_bin("oranges"), Exp.float_bin("acai")])
            ]),
            Exp.float_val(10.5)
        )
        result = parse_ael("(($.apples + $.bananas) + ($.oranges + $.acai)) > 10.5")
        assert result == expected

    def test_complicated_when_implicit_type_int(self):
        """Test when expression with implicit int type."""
        expected = Exp.eq(
            Exp.int_bin("a"),
            Exp.cond([
                Exp.eq(Exp.int_bin("b"), Exp.int_val(1)), Exp.int_bin("a1"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(2)), Exp.int_bin("a2"),
                Exp.eq(Exp.int_bin("b"), Exp.int_val(3)), Exp.int_bin("a3"),
                Exp.num_add([Exp.int_bin("a4"), Exp.int_val(1)])
            ])
        )
        result = parse_ael(
            "$.a == (when($.b == 1 => $.a1, $.b == 2 => $.a2, $.b == 3 => $.a3, default => $.a4+1))"
        )
        assert result == expected
