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

"""Unit tests for typed map keys in AEL paths.

Map keys are typed at parse time:

* a digit-only segment after a path dot (``$.bin.55``) is an ``int``,
* a hex/binary segment (``$.bin.0xff``, ``$.bin.0b101``) is an ``int``,
* a signed segment (``$.bin.-3``, ``$.bin.+5``) is a signed ``int``,
* a quoted segment (``$.bin."55"``) is the literal ``str`` ``"55"``,
* an INT inside a brace context (``$.bin.{1-5}``, ``$.bin.{1,2,3}``) keeps
  its integer type.
"""

import pytest
from aerospike_async import CTX, ExpType, MapReturnType
from aerospike_sdk import Exp, parse_ael


def _map_get_by_int_key(bin_name: str, key: int, ctx=None):
    return Exp.map_get_by_key(
        MapReturnType.VALUE, ExpType.INT,
        Exp.int_val(key), Exp.map_bin(bin_name), list(ctx) if ctx else [],
    )


def _map_get_by_str_key(bin_name: str, key: str, ctx=None):
    return Exp.map_get_by_key(
        MapReturnType.VALUE, ExpType.INT,
        Exp.string_val(key), Exp.map_bin(bin_name), list(ctx) if ctx else [],
    )


class TestDigitOnlyPathKeys:
    """``$.bin.<digits>`` is an integer-typed map key."""

    @pytest.mark.parametrize("expr,key", [
        ("$.m.55 == 10", 55),
        ("$.m.1 == 10", 1),
        ("$.m.0 == 10", 0),
        ("$.m.007 == 10", 7),
        ("$.bin.12345678910 == 1", 12345678910),
    ])
    def test_digit_path_key(self, expr, key):
        bin_name = expr[expr.index("$.") + 2:expr.index(".", 2)]
        rhs = int(expr.rsplit("==", 1)[1].strip())
        assert parse_ael(expr) == Exp.eq(_map_get_by_int_key(bin_name, key), Exp.int_val(rhs))

    @pytest.mark.parametrize("expr", [
        '$.m."55" == "val"',
        "$.m.'55' == 'val'",
    ])
    def test_quoted_digit_is_string_key(self, expr):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.STRING,
                Exp.string_val("55"), Exp.map_bin("m"), [],
            ),
            Exp.string_val("val"),
        )
        assert parse_ael(expr) == expected

    def test_int_key_vs_string_key_differ(self):
        int_form = parse_ael("$.m.55 == 10")
        str_form = parse_ael('$.m."55" == 10')
        assert int_form != str_form

    def test_mixed_digit_then_string_key(self):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.INT,
                Exp.string_val("key"), Exp.map_bin("bin"),
                [CTX.map_key(55)],
            ),
            Exp.int_val(10),
        )
        assert parse_ael("$.bin.55.key == 10") == expected

    def test_int_key_with_get_type(self):
        expected = Exp.eq(_map_get_by_int_key("m", 55), Exp.int_val(10))
        assert parse_ael("$.m.55.get(type: INT) == 10") == expected


class TestSignedPathKeys:
    """``$.bin.+5``, ``$.bin.-3`` produce signed-integer keys."""

    @pytest.mark.parametrize("expr,key", [
        ("$.m.+5 == 10", 5),
        ("$.m.-3 == 10", -3),
        ("$.m.+0 == 10", 0),
        ("$.m.-0 == 10", 0),
    ])
    def test_signed_path_key(self, expr, key):
        assert parse_ael(expr) == Exp.eq(_map_get_by_int_key("m", key), Exp.int_val(10))


class TestHexBinaryPathKeys:
    """``$.bin.0xff`` and ``$.bin.0b101`` produce integer-typed keys."""

    @pytest.mark.parametrize("expr,key", [
        ("$.m.0xff == 1", 255),
        ("$.m.0XFF == 1", 255),
        ("$.m.0x0 == 1", 0),
        ("$.m.0b101 == 1", 5),
        ("$.m.0B11 == 1", 3),
        ("$.m.0xdeadbeef == 1", 0xDEADBEEF),
    ])
    def test_hex_binary_path_key(self, expr, key):
        assert parse_ael(expr) == Exp.eq(_map_get_by_int_key("m", key), Exp.int_val(1))

    def test_hex_followed_by_string_key(self):
        expected = Exp.eq(
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.INT,
                Exp.string_val("name"), Exp.map_bin("m"),
                [CTX.map_key(0xff)],
            ),
            Exp.int_val(1),
        )
        assert parse_ael("$.m.0xff.name == 1") == expected


class TestBraceContextKeys:
    """INT keys inside ``{...}`` map-key contexts (range, list, relative)."""

    def test_int_key_range(self):
        expected = Exp.eq(
            Exp.map_get_by_key_range(
                MapReturnType.VALUE,
                Exp.int_val(1), Exp.int_val(5),
                Exp.map_bin("m"), [],
            ),
            Exp.int_val(100),
        )
        assert parse_ael("$.m.{1-5} == 100") == expected

    def test_int_key_list(self):
        expected = Exp.eq(
            Exp.map_get_by_key_list(
                MapReturnType.VALUE,
                Exp.list_val([1, 2, 3]),
                Exp.map_bin("m"), [],
            ),
            Exp.int_val(100),
        )
        assert parse_ael("$.m.{1,2,3} == 100") == expected

    def test_string_key_range_unchanged(self):
        # Regression guard: existing string ranges must keep working.
        expected = Exp.eq(
            Exp.map_get_by_key_range(
                MapReturnType.VALUE,
                Exp.string_val("a"), Exp.string_val("c"),
                Exp.map_bin("m"), [],
            ),
            Exp.int_val(100),
        )
        assert parse_ael("$.m.{a-c} == 100") == expected
