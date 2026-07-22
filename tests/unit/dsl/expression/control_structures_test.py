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

"""Unit tests for AEL control structures (when/let)."""

import pytest

from aerospike_sdk import Exp, parse_ael
from aerospike_sdk.ael.exceptions import AelParseException


class TestWhenExpressions:
    """Test when (conditional) expressions."""

    def test_when_single_condition(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.string_val("other")
        ])
        result = parse_ael('when ($.who == 1 => "bob", default => "other")')
        assert result == expected

    def test_when_single_condition_no_spaces(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.string_val("other")
        ])
        result = parse_ael('when($.who == 1 => "bob", default => "other")')
        assert result == expected

    def test_when_single_quotes(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.string_val("other")
        ])
        result = parse_ael("when($.who == 1 => 'bob', default => 'other')")
        assert result == expected

    def test_when_using_the_result_string_bin_equals_when(self):
        """String bin equals when expression: $.stringBin1.get(type: STRING) == (when (...))."""
        expected = Exp.eq(
            Exp.string_bin("stringBin1"),
            Exp.cond([
                Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
                Exp.string_val("bob"),
                Exp.string_val("other")
            ])
        )
        result = parse_ael(
            '$.stringBin1.get(type: STRING) == (when ($.who == 1 => "bob", default => "other"))'
        )
        assert result == expected

    def test_when_using_the_result_numeric_in_comparison(self):
        """When expression in comparison: (when (...) => 10, 20) > 15."""
        expected = Exp.gt(
            Exp.cond(
                [Exp.eq(Exp.int_bin("who"), Exp.int_val(1)), Exp.int_val(10), Exp.int_val(20)]
            ),
            Exp.int_val(15),
        )
        result = parse_ael('(when($.who == 1 => 10, default => 20)) > 15')
        assert result == expected

    def test_when_as_operand_no_parens(self):
        """when(...) == value without extra parens (control structures are operands)."""
        expected = Exp.eq(
            Exp.cond([
                Exp.eq(Exp.int_bin("A"), Exp.int_val(0)),
                Exp.num_add([Exp.int_bin("D"), Exp.int_bin("E")]),
                Exp.eq(Exp.int_bin("A"), Exp.int_val(1)),
                Exp.num_sub([Exp.int_bin("D"), Exp.int_bin("E")]),
                Exp.eq(Exp.int_bin("A"), Exp.int_val(2)),
                Exp.num_mul([Exp.int_bin("D"), Exp.int_bin("E")]),
                Exp.int_val(-1)
            ]),
            Exp.int_val(2)
        )
        result = parse_ael(
            "when($.A == 0 => $.D + $.E, "
            "$.A == 1 => $.D - $.E, "
            "$.A == 2 => $.D * $.E, "
            "default => -1) == 2"
        )
        assert result == expected

    def test_not_wrapping_when_comparison(self):
        """not(when(...) == value) — control structures composable in operand position."""
        expected = Exp.not_(
            Exp.eq(
                Exp.cond([
                    Exp.eq(Exp.int_bin("A"), Exp.int_val(0)),
                    Exp.num_add([Exp.int_bin("D"), Exp.int_bin("E")]),
                    Exp.eq(Exp.int_bin("A"), Exp.int_val(1)),
                    Exp.num_sub([Exp.int_bin("D"), Exp.int_bin("E")]),
                    Exp.int_val(-1)
                ]),
                Exp.int_val(2)
            )
        )
        result = parse_ael(
            "not(when($.A == 0 => $.D + $.E, $.A == 1 => $.D - $.E, default => -1) == 2)"
        )
        assert result == expected

    def test_when_multiple_conditions(self):
        """Multiple condition/result pairs plus default."""
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("who"), Exp.int_val(1)),
            Exp.string_val("bob"),
            Exp.eq(Exp.int_bin("who"), Exp.int_val(2)),
            Exp.string_val("fred"),
            Exp.string_val("other")
        ])
        result = parse_ael('when ($.who == 1 => "bob", $.who == 2 => "fred", default => "other")')
        assert result == expected

    def test_when_numeric_actions(self):
        expected = Exp.cond([
            Exp.eq(Exp.int_bin("category"), Exp.int_val(1)),
            Exp.int_val(100),
            Exp.eq(Exp.int_bin("category"), Exp.int_val(2)),
            Exp.int_val(200),
            Exp.int_val(0)
        ])
        result = parse_ael("when ($.category == 1 => 100, $.category == 2 => 200, default => 0)")
        assert result == expected


class TestLetExpressions:
    """Test let/then (variable binding) expressions."""

    def test_let_single_variable(self):
        expected = Exp.exp_let(
            [Exp.def_("x", Exp.int_val(1)), Exp.num_add([Exp.var("x"), Exp.int_val(1)])]
        )
        result = parse_ael("let (x = 1) then (${x} + 1)")
        assert result == expected

    def test_let_multiple_variables(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.int_val(1)),
            Exp.def_("y", Exp.num_add([Exp.var("x"), Exp.int_val(1)])),
            Exp.num_add([Exp.var("x"), Exp.var("y")])
        ])
        result = parse_ael("let (x = 1, y = ${x} + 1) then (${x} + ${y})")
        assert result == expected

    def test_let_no_spaces(self):
        expected = Exp.exp_let([
            Exp.def_("x", Exp.int_val(1)),
            Exp.def_("y", Exp.num_add([Exp.var("x"), Exp.int_val(1)])),
            Exp.num_add([Exp.var("x"), Exp.var("y")])
        ])
        result = parse_ael("let(x = 1, y = ${x}+1) then(${x}+${y})")
        assert result == expected

    def test_let_bin_reference(self):
        expected = Exp.exp_let([
            Exp.def_("total", Exp.num_add([Exp.int_bin("a"), Exp.int_bin("b")])),
            Exp.num_mul([Exp.var("total"), Exp.int_val(2)])
        ])
        result = parse_ael("let (total = $.a + $.b) then (${total} * 2)")
        assert result == expected

    def test_let_in_comparison(self):
        expected = Exp.gt(
            Exp.exp_let(
                [Exp.def_("sum", Exp.num_add([Exp.int_bin("a"), Exp.int_bin("b")])), Exp.var("sum")]
            ),
            Exp.int_val(100),
        )
        result = parse_ael("(let (sum = $.a + $.b) then (${sum})) > 100")
        assert result == expected

    def test_let_as_operand_no_parens(self):
        """let(...) then(...) > value without extra parens."""
        expected = Exp.gt(
            Exp.exp_let(
                [Exp.def_("sum", Exp.num_add([Exp.int_bin("a"), Exp.int_bin("b")])), Exp.var("sum")]
            ),
            Exp.int_val(100),
        )
        result = parse_ael("let (sum = $.a + $.b) then (${sum}) > 100")
        assert result == expected

    def test_variable_reference_syntax(self):
        expected = Exp.exp_let([Exp.def_("myVar", Exp.int_val(42)), Exp.var("myVar")])
        result = parse_ael("let (myVar = 42) then (${myVar})")
        assert result == expected


class TestEofRejection:
    """Trailing garbage after a valid expression must be rejected."""

    def test_trailing_garbage_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("$.x == 1 blah")

    def test_trailing_operator_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("$.x == 1 +")


class TestCaseInsensitiveIn:
    """IN keyword is case-insensitive."""

    def test_lowercase_in(self):
        result = parse_ael("1 in $.list.get(type: LIST)")
        assert result is not None

    def test_uppercase_in(self):
        result = parse_ael("1 IN $.list.get(type: LIST)")
        assert result is not None

    def test_mixed_case_in(self):
        result = parse_ael("1 In $.list.get(type: LIST)")
        assert result is not None


class TestInAsBinName:
    """A bin named 'in' should be usable."""

    def test_bin_named_in(self):
        expected = Exp.eq(Exp.int_bin("in"), Exp.int_val(5))
        result = parse_ael("$.in == 5")
        assert result == expected


class TestCaseInsensitiveHexBinary:
    """Hex and binary prefixes are case-insensitive."""

    def test_uppercase_hex(self):
        expected = Exp.eq(Exp.int_bin("x"), Exp.int_val(0xFF))
        result = parse_ael("$.x == 0XFF")
        assert result == expected

    def test_uppercase_binary(self):
        expected = Exp.eq(Exp.int_bin("x"), Exp.int_val(0b1010))
        result = parse_ael("$.x == 0B1010")
        assert result == expected


class TestLeadingDotFloat:
    """Leading-dot float syntax (.5 = 0.5)."""

    def test_leading_dot_float(self):
        expected = Exp.eq(Exp.float_bin("x"), Exp.float_val(0.5))
        result = parse_ael("$.x == .5")
        assert result == expected

    def test_leading_dot_float_larger(self):
        expected = Exp.gt(Exp.float_bin("val"), Exp.float_val(0.75))
        result = parse_ael("$.val > .75")
        assert result == expected


class TestUnaryExpressions:
    """Phase 2: unary minus, unary plus, precedence of unary vs power."""

    def test_unary_minus_int_literal(self):
        expected = Exp.eq(Exp.int_bin("x"), Exp.int_val(-5))
        result = parse_ael("$.x == -5")
        assert result == expected

    def test_unary_minus_float_literal(self):
        expected = Exp.eq(Exp.float_bin("x"), Exp.float_val(-3.14))
        result = parse_ael("$.x == -3.14")
        assert result == expected

    def test_unary_minus_hex(self):
        expected = Exp.eq(Exp.int_val(-0xFF), Exp.int_val(-255))
        result = parse_ael("-0xFF == -255")
        assert result == expected

    def test_unary_minus_leading_dot_float(self):
        expected = Exp.eq(Exp.float_bin("x"), Exp.float_val(-0.5))
        result = parse_ael("$.x == -.5")
        assert result == expected

    def test_unary_plus_identity(self):
        expected = Exp.eq(Exp.int_bin("x"), Exp.int_val(5))
        result = parse_ael("$.x == +5")
        assert result == expected

    def test_unary_minus_bin(self):
        """Unary minus on a bin expression uses num_mul(bin, -1)."""
        expected = Exp.gt(Exp.num_mul([Exp.int_bin("x"), Exp.int_val(-1)]), Exp.int_val(0))
        result = parse_ael("-$.x > 0")
        assert result == expected

    def test_unary_minus_higher_precedence_than_power(self):
        """-2.0 ** 2.0 should parse as (-2.0) ** 2.0, not -(2.0 ** 2.0)."""
        expected = Exp.eq(Exp.num_pow(Exp.float_val(-2.0), Exp.float_val(2.0)), Exp.float_val(4.0))
        result = parse_ael("-2.0 ** 2.0 == 4.0")
        assert result == expected


class TestPrecedenceOrder:
    """Verify the full operator precedence chain: unary > power > mult > add > shift > bitwise > comparison."""

    def test_shift_lower_than_add(self):
        """1 + 2 << 3 should parse as (1 + 2) << 3."""
        expected = Exp.eq(
            Exp.int_lshift(Exp.num_add([Exp.int_val(1), Exp.int_val(2)]), Exp.int_val(3)),
            Exp.int_val(24),
        )
        result = parse_ael("(1 + 2 << 3) == 24")
        assert result == expected

    def test_bitwise_lower_than_shift(self):
        """1 << 2 & 0xFF should parse as (1 << 2) & 0xFF."""
        expected = Exp.eq(
            Exp.int_and([Exp.int_lshift(Exp.int_val(1), Exp.int_val(2)), Exp.int_val(0xFF)]),
            Exp.int_val(4),
        )
        result = parse_ael("(1 << 2 & 0xFF) == 4")
        assert result == expected

    def test_add_higher_than_shift(self):
        """1 << 2 + 1 should parse as 1 << (2 + 1) = 8."""
        expected = Exp.eq(
            Exp.int_lshift(Exp.int_val(1), Exp.num_add([Exp.int_val(2), Exp.int_val(1)])),
            Exp.int_val(8),
        )
        result = parse_ael("(1 << 2 + 1) == 8")
        assert result == expected

    def test_pow_higher_than_bitwise_and(self):
        """2 ** 3 & 5 should parse as (2 ** 3) & 5 = 8 & 5 = 0."""
        expected = Exp.eq(
            Exp.int_and([Exp.num_pow(Exp.int_val(2), Exp.int_val(3)), Exp.int_val(5)]),
            Exp.int_val(0),
        )
        result = parse_ael("(2 ** 3 & 5) == 0")
        assert result == expected

    def test_bitwise_not_at_unary_level(self):
        """~-5 should parse as ~(-5) = 4."""
        expected = Exp.eq(Exp.int_not(Exp.int_val(-5)), Exp.int_val(4))
        result = parse_ael("(~-5) == 4")
        assert result == expected


class TestLeadingDotFloatRejection:
    """Leading-dot hex/binary (.0xFF, .0b11) and malformed floats must be rejected."""

    def test_leading_dot_hex_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael(".0x10 == 0.0")

    def test_leading_dot_hex_rejected_right(self):
        with pytest.raises(AelParseException):
            parse_ael("0.0 == .0x10")

    def test_leading_dot_hex_minus_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("-.0x10 == 0.0")

    def test_leading_dot_hex_plus_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("+.0x10 == 0.0")

    def test_leading_dot_binary_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael(".0b11 == 0.0")

    def test_leading_dot_binary_rejected_right(self):
        with pytest.raises(AelParseException):
            parse_ael("0.0 == .0b11")

    def test_leading_dot_binary_minus_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("-.0b11 == 0.0")

    def test_leading_dot_binary_plus_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("+.0b11 == 0.0")

    def test_double_dot_float_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("..37 == 0.0")

    def test_double_dot_float_rejected_right(self):
        with pytest.raises(AelParseException):
            parse_ael("0.0 == ..37")

    def test_embedded_dot_float_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael(".3.7 == 0.0")

    def test_embedded_dot_float_rejected_right(self):
        with pytest.raises(AelParseException):
            parse_ael("0.0 == .3.7")

    def test_trailing_dot_float_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("10. == 10.0")


class TestNumericLiteralEdgeCases:
    """Unary sign edge cases, double negation, spaceless operators."""

    def test_double_negation(self):
        """--5 should parse as -(-5) = 5."""
        expected = Exp.eq(Exp.int_val(5), Exp.int_val(5))
        result = parse_ael("--5 == 5")
        assert result == expected

    def test_double_unary_plus(self):
        """++5 should parse as +(+5) = 5."""
        expected = Exp.eq(Exp.int_val(5), Exp.int_val(5))
        result = parse_ael("++5 == 5")
        assert result == expected

    def test_double_unary_plus_on_right(self):
        expected = Exp.eq(Exp.int_val(5), Exp.int_val(5))
        result = parse_ael("5 == ++5")
        assert result == expected

    def test_unary_plus_parenthesized(self):
        """+(5) is a no-op."""
        expected = Exp.eq(Exp.int_val(5), Exp.int_val(5))
        result = parse_ael("+(5) == 5")
        assert result == expected

    def test_subtraction_of_negative(self):
        """5 - -3 should parse as 5 - (-3) = 8."""
        expected = Exp.eq(Exp.num_sub([Exp.int_val(5), Exp.int_val(-3)]), Exp.int_val(8))
        result = parse_ael("(5 - -3) == 8")
        assert result == expected

    def test_unary_plus_hex(self):
        expected = Exp.eq(Exp.int_val(0xFF), Exp.int_val(255))
        result = parse_ael("+0xff == 255")
        assert result == expected

    def test_unary_plus_binary(self):
        expected = Exp.eq(Exp.int_val(0b1010), Exp.int_val(10))
        result = parse_ael("+0b1010 == 10")
        assert result == expected

    def test_unary_plus_float(self):
        expected = Exp.eq(Exp.float_val(3.14), Exp.float_val(3.14))
        result = parse_ael("+3.14 == 3.14")
        assert result == expected

    def test_negative_float(self):
        expected = Exp.eq(Exp.float_val(-34.1), Exp.float_val(-34.1))
        result = parse_ael("-34.1 == -34.1")
        assert result == expected

    def test_leading_dot_float_negative(self):
        expected = Exp.eq(Exp.float_val(-0.37), Exp.float_val(-0.37))
        result = parse_ael("-.37 == -0.37")
        assert result == expected

    def test_leading_dot_float_plus(self):
        expected = Exp.eq(Exp.float_val(0.37), Exp.float_val(0.37))
        result = parse_ael("+.37 == 0.37")
        assert result == expected

    def test_leading_dot_float_in_expression(self):
        expected = Exp.eq(Exp.num_add([Exp.float_val(0.5), Exp.float_val(0.5)]), Exp.float_val(1.0))
        result = parse_ael("(.5 + .5) == 1.0")
        assert result == expected

    def test_leading_dot_float_zero(self):
        expected = Exp.eq(Exp.float_val(0.0), Exp.float_val(0.0))
        result = parse_ael(".0 == 0.0")
        assert result == expected

    def test_spaceless_addition(self):
        """5+3 must tokenize as INT(5) + INT(3), not INT(5) INT(+3)."""
        expected = Exp.eq(Exp.num_add([Exp.int_val(5), Exp.int_val(3)]), Exp.int_val(8))
        result = parse_ael("(5+3) == 8")
        assert result == expected

    def test_spaceless_subtraction(self):
        """5-3 must tokenize as INT(5) - INT(3), not INT(5) INT(-3)."""
        expected = Exp.eq(Exp.num_sub([Exp.int_val(5), Exp.int_val(3)]), Exp.int_val(2))
        result = parse_ael("(5-3) == 2")
        assert result == expected

    def test_spaceless_mixed(self):
        expected = Exp.eq(
            Exp.num_add([Exp.num_sub([Exp.int_val(10), Exp.int_val(3)]), Exp.int_val(1)]),
            Exp.int_val(8),
        )
        result = parse_ael("(10-3+1) == 8")
        assert result == expected

    def test_invalid_hex_digits_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("0xGG == 0")

    def test_invalid_binary_digits_rejected(self):
        with pytest.raises(AelParseException):
            parse_ael("0b2 == 0")
