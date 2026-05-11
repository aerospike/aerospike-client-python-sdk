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

"""Unit tests for the AEL ``exists()`` path function.

A bare bin path lowers to ``bin_exists``. A CDT path lowers to the underlying
``*_get_by_*`` op with EXISTS return-type and BOOL value-type — the same
shape produced by ``.get(return: EXISTS)``.
"""

import pytest
from aerospike_async import CTX, ExpType, ListReturnType, MapReturnType
from aerospike_sdk import Exp, parse_ael
from aerospike_sdk.ael.exceptions import AelParseException


class TestBinExists:
    """``$.bin.exists()`` — bare bin existence."""

    def test_bin_exists(self):
        assert parse_ael("$.a.exists()") == Exp.bin_exists("a")

    def test_two_bin_exists_and(self):
        expected = Exp.and_([Exp.bin_exists("a"), Exp.bin_exists("b")])
        assert parse_ael("$.a.exists() and $.b.exists()") == expected

    def test_bin_exists_or_comparison(self):
        expected = Exp.or_([
            Exp.bin_exists("a"),
            Exp.gt(Exp.int_bin("b"), Exp.int_val(5)),
        ])
        assert parse_ael("$.a.exists() or $.b > 5") == expected

    def test_not_bin_exists(self):
        assert parse_ael("not($.a.exists())") == Exp.not_(Exp.bin_exists("a"))

    def test_nested_logical_bin_exists(self):
        expected = Exp.or_([
            Exp.and_([Exp.bin_exists("a"), Exp.bin_exists("b")]),
            Exp.gt(Exp.int_bin("c"), Exp.int_val(0)),
        ])
        assert parse_ael("($.a.exists() and $.b.exists()) or $.c > 0") == expected


class TestCdtExists:
    """CDT-path ``exists()`` lowers to the matching get-op with EXISTS."""

    def test_map_key_exists(self):
        expected = Exp.map_get_by_key(
            MapReturnType.EXISTS, ExpType.BOOL,
            Exp.string_val("a"), Exp.map_bin("mapbin"), [],
        )
        assert parse_ael("$.mapbin.a.exists()") == expected

    def test_list_index_exists(self):
        expected = Exp.list_get_by_index(
            ListReturnType.EXISTS, ExpType.BOOL,
            Exp.int_val(0), Exp.list_bin("listbin"), [],
        )
        assert parse_ael("$.listbin.[0].exists()") == expected

    def test_nested_map_key_exists(self):
        expected = Exp.map_get_by_key(
            MapReturnType.EXISTS, ExpType.BOOL,
            Exp.string_val("b"), Exp.map_bin("mapbin"),
            [CTX.map_key("a")],
        )
        assert parse_ael("$.mapbin.a.b.exists()") == expected

    def test_deeply_nested_map_exists(self):
        expected = Exp.map_get_by_key(
            MapReturnType.EXISTS, ExpType.BOOL,
            Exp.string_val("c"), Exp.map_bin("m"),
            [CTX.map_key("a"), CTX.map_key("b")],
        )
        assert parse_ael("$.m.a.b.c.exists()") == expected

    def test_list_value_exists(self):
        expected = Exp.list_get_by_value(
            ListReturnType.EXISTS,
            Exp.int_val(100), Exp.list_bin("lb"), [],
        )
        assert parse_ael("$.lb.[=100].exists()") == expected

    def test_list_rank_exists(self):
        expected = Exp.list_get_by_rank(
            ListReturnType.EXISTS, ExpType.BOOL,
            Exp.int_val(-1), Exp.list_bin("lb"), [],
        )
        assert parse_ael("$.lb.[#-1].exists()") == expected

    def test_map_value_exists(self):
        expected = Exp.map_get_by_value(
            MapReturnType.EXISTS,
            Exp.int_val(100), Exp.map_bin("mb"), [],
        )
        assert parse_ael("$.mb.{=100}.exists()") == expected

    def test_map_rank_exists(self):
        expected = Exp.map_get_by_rank(
            MapReturnType.EXISTS, ExpType.BOOL,
            Exp.int_val(0), Exp.map_bin("mb"), [],
        )
        assert parse_ael("$.mb.{#0}.exists()") == expected

    def test_map_index_exists(self):
        expected = Exp.map_get_by_index(
            MapReturnType.EXISTS, ExpType.BOOL,
            Exp.int_val(0), Exp.map_bin("mb"), [],
        )
        assert parse_ael("$.mb.{0}.exists()") == expected


class TestMixedExists:
    """``exists()`` mixed with logical, comparison, and control structures."""

    def test_metadata_and_cdt_exists(self):
        expected = Exp.and_([
            Exp.lt(Exp.ttl(), Exp.int_val(3600)),
            Exp.map_get_by_key(
                MapReturnType.EXISTS, ExpType.BOOL,
                Exp.string_val("a"), Exp.map_bin("mapbin"), [],
            ),
        ])
        assert parse_ael("$.ttl() < 3600 and $.mapbin.a.exists()") == expected

    def test_bin_exists_and_cdt_exists(self):
        expected = Exp.and_([
            Exp.bin_exists("a"),
            Exp.map_get_by_key(
                MapReturnType.EXISTS, ExpType.BOOL,
                Exp.string_val("key"), Exp.map_bin("mapbin"), [],
            ),
        ])
        assert parse_ael("$.a.exists() and $.mapbin.key.exists()") == expected

    def test_cdt_exists_in_logical_and(self):
        expected = Exp.and_([
            Exp.map_get_by_key(
                MapReturnType.EXISTS, ExpType.BOOL,
                Exp.string_val("a"), Exp.map_bin("mapbin"), [],
            ),
            Exp.gt(Exp.int_bin("x"), Exp.int_val(0)),
        ])
        assert parse_ael("$.mapbin.a.exists() and $.x > 0") == expected

    def test_not_cdt_exists(self):
        expected = Exp.not_(
            Exp.map_get_by_key(
                MapReturnType.EXISTS, ExpType.BOOL,
                Exp.string_val("a"), Exp.map_bin("mapbin"), [],
            )
        )
        assert parse_ael("not($.mapbin.a.exists())") == expected

    def test_exists_in_when_condition(self):
        # The when-default branch is ``0`` so PSDK's type inference picks INT
        # for the consequent's value-type. The condition itself must lower to
        # bin_exists, which is what this test guards.
        expected = Exp.cond([
            Exp.bin_exists("mapbin"),
            Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.INT,
                Exp.string_val("a"), Exp.map_bin("mapbin"), [],
            ),
            Exp.int_val(0),
        ])
        actual = parse_ael("when($.mapbin.exists() => $.mapbin.a, default => 0)")
        assert actual == expected


class TestExistsAsBoolean:
    """``exists()`` is a boolean expression, usable in comparisons."""

    def test_bin_exists_in_comparison(self):
        expected = Exp.eq(Exp.bin_exists("a"), Exp.bool_val(True))
        assert parse_ael("$.a.exists() == true") == expected

    def test_bin_exists_gt_is_valid_expression(self):
        expected = Exp.gt(Exp.bin_exists("a"), Exp.int_val(0))
        assert parse_ael("$.a.exists() > 0") == expected


class TestExistsNegative:
    """Malformed ``exists()`` invocations must not parse."""

    @pytest.mark.parametrize("expr", [
        "$.a.exists(1)",
        "$.a.exists ()",
        "$.a.exists( )",
        "$.mapbin.a.exists( )",
    ])
    def test_invalid_exists_call(self, expr):
        with pytest.raises(AelParseException):
            parse_ael(expr)
