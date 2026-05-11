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

"""Unit tests for AEL bin-name handling.

A ``binPart`` accepts:

* a plain identifier (``$.foo``),
* an ``@``-containing identifier (``$.name@host``, ``$.@attr``),
* a quoted name with otherwise-illegal characters (``$."my-bin"``),
* the ``in`` keyword (lowercased),
* a reserved keyword (``$.true``, ``$.and``, ``$.when``...).

A bin name containing the substring ``null`` (case-insensitive) is rejected.
"""

import pytest
from aerospike_async import ExpType, ListReturnType, MapReturnType
from aerospike_sdk import Exp, parse_ael
from aerospike_sdk.ael.exceptions import AelParseException


class TestCharsetAndQuoting:
    """Bin names with ``@`` and bin names supplied via quoted strings."""

    @pytest.mark.parametrize("expr,bin_name", [
        ("$.name@host == 5", "name@host"),
        ("$.@attr == 5", "@attr"),
        ("$.name@ == 5", "name@"),
        ("$.@ == 5", "@"),
        ("$.123@ == 5", "123@"),
    ])
    def test_at_in_bin_name(self, expr, bin_name):
        assert parse_ael(expr) == Exp.eq(Exp.int_bin(bin_name), Exp.int_val(5))

    @pytest.mark.parametrize("expr,bin_name", [
        ('$."my-bin" == 5', "my-bin"),
        ("$.'my-bin' == 5", "my-bin"),
        ("$.'$price' == 5", "$price"),
        ('$."has spaces" == 5', "has spaces"),
    ])
    def test_quoted_bin_name(self, expr, bin_name):
        assert parse_ael(expr) == Exp.eq(Exp.int_bin(bin_name), Exp.int_val(5))

    def test_quoted_unquoted_equivalence(self):
        """Plain, double-quoted, and single-quoted forms produce the same expression."""
        a = parse_ael("$.myBin == 5")
        b = parse_ael('$."myBin" == 5')
        c = parse_ael("$.'myBin' == 5")
        assert a == b == c

    def test_at_bin_with_map_key(self):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.INT,
                Exp.string_val("key"), Exp.map_bin("name@host"), [],
            ),
            Exp.int_val(5),
        )
        assert parse_ael("$.name@host.key == 5") == expected

    def test_at_bin_with_list_index(self):
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE, ExpType.INT,
                Exp.int_val(0), Exp.list_bin("@attr"), [],
            ),
            Exp.int_val(5),
        )
        assert parse_ael("$.@attr.[0] == 5") == expected

    @pytest.mark.parametrize("expr", ['$."" == 5', "$.'' == 5"])
    def test_empty_quoted_bin_rejected(self, expr):
        with pytest.raises(AelParseException, match="must not be empty"):
            parse_ael(expr)

    @pytest.mark.parametrize("expr", [
        "$.123 == 5",            # digit-only bin name not allowed
        "let(x@ = 5) then (${x} + 1)",  # @ in variable definition
    ])
    def test_invalid_bin_or_var_form(self, expr):
        with pytest.raises(AelParseException):
            parse_ael(expr)


class TestKeywordCollision:
    """A reserved keyword can be used as a bin name without quoting."""

    @pytest.mark.parametrize("keyword", [
        "true", "false", "and", "or", "not", "let", "then", "when",
        "default", "exclusive", "get", "type", "return", "remove",
        "insert", "set", "append", "increment", "clear", "sort",
    ])
    def test_keyword_as_bin_name(self, keyword):
        expected = Exp.eq(Exp.int_bin(keyword), Exp.int_val(1))
        assert parse_ael(f"$.{keyword} == 1") == expected

    def test_compound_and_with_keyword_bins(self):
        expected = Exp.and_([
            Exp.eq(Exp.int_bin("and"), Exp.int_val(1)),
            Exp.eq(Exp.int_bin("or"), Exp.int_val(2)),
        ])
        assert parse_ael("$.and == 1 and $.or == 2") == expected

    def test_compound_not_with_keyword_bin(self):
        expected = Exp.and_([
            Exp.eq(Exp.int_bin("not"), Exp.int_val(1)),
            Exp.not_(Exp.eq(Exp.int_bin("x"), Exp.int_val(2))),
        ])
        assert parse_ael("$.not == 1 and not($.x == 2)") == expected

    def test_keyword_bin_with_map_key(self):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.INT,
                Exp.string_val("key"), Exp.map_bin("true"), [],
            ),
            Exp.int_val(5),
        )
        assert parse_ael("$.true.key == 5") == expected

    @pytest.mark.parametrize("keyword", ["when", "default", "let", "then", "and", "or"])
    def test_keyword_bin_with_list_index(self, keyword):
        expected = Exp.eq(
            Exp.list_get_by_index(
                ListReturnType.VALUE, ExpType.INT,
                Exp.int_val(0), Exp.list_bin(keyword), [],
            ),
            Exp.int_val(1),
        )
        assert parse_ael(f"$.{keyword}.[0] == 1") == expected

    @pytest.mark.parametrize("keyword", ["when", "default", "and", "or", "let", "then", "unknown", "error"])
    def test_quoted_keyword_as_map_key(self, keyword):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.INT,
                Exp.string_val(keyword), Exp.map_bin("mapBin"), [],
            ),
            Exp.int_val(1),
        )
        assert parse_ael(f"$.mapBin.'{keyword}' == 1") == expected


class TestNullBinNameRejection:
    """Bin names containing ``null`` (case-insensitive) are reserved."""

    @pytest.mark.parametrize("expr", [
        "$.null == 5",
        "$.NULL == 5",
        "$.nUlL == 5",
        "$.nullify == 5",
        "$.mynull == 5",
        "$.myNullVar == 5",
        "$.my_null_bin == 5",
        '$."my-null-key" == 5',
        '$."null" == 5',
        "$.'null' == 5",
        "$.'NULL_BIN' == 5",
        "$.null@ == 5",
        "$.@null == 5",
        '$."null@host" == 5',
    ])
    def test_null_bin_rejected(self, expr):
        with pytest.raises(AelParseException, match="null"):
            parse_ael(expr)

    @pytest.mark.parametrize("expr,bin_name", [
        ("$.nul == 5", "nul"),
        ("$.Nul == 5", "Nul"),
        ("$.nu_ll == 5", "nu_ll"),
        ("$.lnul == 5", "lnul"),
        ("$.nu@ll == 5", "nu@ll"),
    ])
    def test_null_substring_not_present(self, expr, bin_name):
        # 'null' as a literal substring is what's reserved — surrounding
        # decoration that breaks the substring is still allowed.
        assert parse_ael(expr) == Exp.eq(Exp.int_bin(bin_name), Exp.int_val(5))
