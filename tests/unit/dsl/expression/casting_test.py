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

"""Unit tests for AEL casting expressions."""

import pytest
from aerospike_sdk import ExpType, ListReturnType, MapReturnType

from aerospike_sdk import AelParseException, Exp, parse_ael


class TestBinCasting:
    """Test bin-level asInt()/asFloat() — always generates toInt/toFloat wrappers."""

    def test_bin_as_int(self):
        """$.bin.asInt() == 1 → eq(toInt(floatBin("bin")), val(1))."""
        expected = Exp.eq(Exp.to_int(Exp.float_bin("bin")), Exp.int_val(1))
        assert parse_ael("$.bin.asInt() == 1") == expected

    def test_bin_as_float(self):
        """$.bin.asFloat() == 1.0 → eq(toFloat(intBin("bin")), val(1.0))."""
        expected = Exp.eq(Exp.to_float(Exp.int_bin("bin")), Exp.float_val(1.0))
        assert parse_ael("$.bin.asFloat() == 1.0") == expected

    def test_bin_as_int_in_comparison(self):
        """$.intBin1 > $.floatBin1.asInt() → gt(intBin1, toInt(floatBin(floatBin1)))."""
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.to_int(Exp.float_bin("floatBin1")))
        assert parse_ael("$.intBin1 > $.floatBin1.asInt()") == expected
        assert parse_ael("$.intBin1.get(type: INT) > $.floatBin1.asInt()") == expected

    def test_bin_as_float_in_comparison(self):
        """$.intBin1.get(type: INT) > $.intBin2.asFloat() → gt(intBin1, toFloat(intBin(intBin2)))."""
        expected = Exp.gt(Exp.int_bin("intBin1"), Exp.to_float(Exp.int_bin("intBin2")))
        assert parse_ael("$.intBin1.get(type: INT) > $.intBin2.asFloat()") == expected

    def test_bin_as_float_in_addition(self):
        expected = Exp.eq(
            Exp.num_add([Exp.to_float(Exp.int_bin("binA")), Exp.float_val(4.0)]),
            Exp.float_val(5.0),
        )
        assert parse_ael("$.binA.asFloat() + 4.0 == 5.0") == expected

    def test_bin_as_int_in_subtraction(self):
        expected = Exp.eq(
            Exp.num_sub([Exp.to_int(Exp.float_bin("binA")), Exp.int_val(3)]),
            Exp.int_val(0),
        )
        assert parse_ael("$.binA.asInt() - 3 == 0") == expected

    def test_negative_invalid_types_comparison(self):
        """$.stringBin1.get(type: STRING) > $.intBin2.asFloat() raises Cannot compare."""
        with pytest.raises(AelParseException, match="Cannot compare STRING to FLOAT"):
            parse_ael("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()")


class TestLiteralCasting:
    """Test operandCast: numeric literal '.asInt()' / '.asFloat()' casts."""

    def test_int_literal_to_float(self):
        expected = Exp.eq(Exp.float_val(28.0), Exp.float_val(28.0))
        assert parse_ael("28.asFloat() == 28.0") == expected

    def test_float_literal_to_int(self):
        expected = Exp.eq(Exp.int_val(27), Exp.int_val(27))
        assert parse_ael("27.0.asInt() == 27") == expected

    def test_negative_int_to_float(self):
        expected = Exp.eq(Exp.float_val(-5.0), Exp.float_val(-5.0))
        assert parse_ael("-5.asFloat() == -5.0") == expected

    def test_negative_float_to_int(self):
        expected = Exp.eq(Exp.int_val(-5), Exp.int_val(-5))
        assert parse_ael("-5.5.asInt() == -5") == expected

    def test_zero_int_to_float(self):
        expected = Exp.eq(Exp.float_val(0.0), Exp.float_val(0.0))
        assert parse_ael("0.asFloat() == 0.0") == expected

    def test_zero_float_to_int(self):
        expected = Exp.eq(Exp.int_val(0), Exp.int_val(0))
        assert parse_ael("0.0.asInt() == 0") == expected

    def test_leading_dot_float_to_int(self):
        expected = Exp.eq(Exp.int_val(0), Exp.int_val(0))
        assert parse_ael(".37.asInt() == 0") == expected

    def test_hex_to_float(self):
        expected = Exp.eq(Exp.float_val(255.0), Exp.float_val(255.0))
        assert parse_ael("0xFF.asFloat() == 255.0") == expected

    def test_identity_int_cast(self):
        expected = Exp.eq(Exp.int_val(42), Exp.int_val(42))
        assert parse_ael("42.asInt() == 42") == expected

    def test_identity_float_cast(self):
        expected = Exp.eq(Exp.float_val(3.14), Exp.float_val(3.14))
        assert parse_ael("3.14.asFloat() == 3.14") == expected

    def test_cast_in_arithmetic(self):
        expected = Exp.eq(
            Exp.num_add([Exp.float_val(5.0), Exp.float_val(3.0)]),
            Exp.float_val(8.0),
        )
        assert parse_ael("(5.asFloat() + 3.asFloat()) == 8.0") == expected

    def test_cast_compared_to_bin(self):
        expected = Exp.gt(Exp.int_bin("age"), Exp.int_val(18))
        assert parse_ael("$.age > 18.asInt()") == expected

    def test_large_int_to_float(self):
        expected = Exp.lt(Exp.float_val(float(-9223372036854775808)), Exp.float_val(0.0))
        assert parse_ael("-9223372036854775808.asFloat() < 0.0") == expected

    def test_cast_to_float_compared_to_string_raises(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael('28.asFloat() == "hello"')

    def test_cast_to_int_compared_to_string_raises(self):
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael('28.0.asInt() == "hello"')


class TestCDTCasting:
    """Test asInt()/asFloat() on CDT list and map paths — wraps in toInt/toFloat."""

    def test_list_index_as_int(self):
        expected = Exp.eq(
            Exp.to_int(Exp.list_get_by_index(
                ListReturnType.VALUE, ExpType.FLOAT,
                Exp.int_val(0), Exp.list_bin("listBin1"), [],
            )),
            Exp.int_val(100),
        )
        assert parse_ael("$.listBin1.[0].asInt() == 100") == expected

    def test_list_index_as_float(self):
        expected = Exp.eq(
            Exp.to_float(Exp.list_get_by_index(
                ListReturnType.VALUE, ExpType.INT,
                Exp.int_val(0), Exp.list_bin("listBin1"), [],
            )),
            Exp.float_val(100.0),
        )
        assert parse_ael("$.listBin1.[0].asFloat() == 100.0") == expected

    def test_list_rank_as_int(self):
        expected = Exp.eq(
            Exp.to_int(Exp.list_get_by_rank(
                ListReturnType.VALUE, ExpType.FLOAT,
                Exp.int_val(-1), Exp.list_bin("listBin1"), [],
            )),
            Exp.int_val(100),
        )
        assert parse_ael("$.listBin1.[#-1].asInt() == 100") == expected

    def test_map_key_as_int(self):
        expected = Exp.eq(
            Exp.to_int(Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.FLOAT,
                Exp.string_val("a"), Exp.map_bin("mapBin1"), [],
            )),
            Exp.int_val(200),
        )
        assert parse_ael("$.mapBin1.a.asInt() == 200") == expected

    def test_map_key_as_float(self):
        expected = Exp.eq(
            Exp.to_float(Exp.map_get_by_key(
                MapReturnType.VALUE, ExpType.INT,
                Exp.string_val("a"), Exp.map_bin("mapBin1"), [],
            )),
            Exp.float_val(200.0),
        )
        assert parse_ael("$.mapBin1.a.asFloat() == 200.0") == expected

    def test_map_index_as_int(self):
        expected = Exp.eq(
            Exp.to_int(Exp.map_get_by_index(
                MapReturnType.VALUE, ExpType.FLOAT,
                Exp.int_val(0), Exp.map_bin("mapBin1"), [],
            )),
            Exp.int_val(100),
        )
        assert parse_ael("$.mapBin1.{0}.asInt() == 100") == expected

    def test_list_value_as_int(self):
        expected = Exp.eq(
            Exp.to_int(Exp.list_get_by_value(
                ListReturnType.VALUE,
                Exp.int_val(100), Exp.list_bin("listBin1"), [],
            )),
            Exp.int_val(100),
        )
        assert parse_ael("$.listBin1.[=100].asInt() == 100") == expected

    def test_list_value_as_float(self):
        expected = Exp.eq(
            Exp.to_float(Exp.list_get_by_value(
                ListReturnType.VALUE,
                Exp.int_val(100), Exp.list_bin("listBin1"), [],
            )),
            Exp.float_val(100.0),
        )
        assert parse_ael("$.listBin1.[=100].asFloat() == 100.0") == expected

    def test_list_rank_as_float(self):
        expected = Exp.eq(
            Exp.to_float(Exp.list_get_by_rank(
                ListReturnType.VALUE, ExpType.INT,
                Exp.int_val(-1), Exp.list_bin("listBin1"), [],
            )),
            Exp.float_val(100.0),
        )
        assert parse_ael("$.listBin1.[#-1].asFloat() == 100.0") == expected

    def test_map_index_as_float(self):
        expected = Exp.eq(
            Exp.to_float(Exp.map_get_by_index(
                MapReturnType.VALUE, ExpType.INT,
                Exp.int_val(0), Exp.map_bin("mapBin1"), [],
            )),
            Exp.float_val(100.0),
        )
        assert parse_ael("$.mapBin1.{0}.asFloat() == 100.0") == expected

    def test_map_value_as_int(self):
        expected = Exp.eq(
            Exp.to_int(Exp.map_get_by_value(
                MapReturnType.VALUE,
                Exp.int_val(100), Exp.map_bin("mapBin1"), [],
            )),
            Exp.int_val(100),
        )
        assert parse_ael("$.mapBin1.{=100}.asInt() == 100") == expected

    def test_map_rank_as_int(self):
        expected = Exp.eq(
            Exp.to_int(Exp.map_get_by_rank(
                MapReturnType.VALUE, ExpType.FLOAT,
                Exp.int_val(-1), Exp.map_bin("mapBin1"), [],
            )),
            Exp.int_val(100),
        )
        assert parse_ael("$.mapBin1.{#-1}.asInt() == 100") == expected

    def test_nested_map_rank_as_int(self):
        from aerospike_sdk import CTX
        expected = Exp.eq(
            Exp.to_int(Exp.map_get_by_rank(
                MapReturnType.VALUE, ExpType.FLOAT,
                Exp.int_val(-1), Exp.map_bin("mapBin1"),
                [CTX.map_key("a")],
            )),
            Exp.int_val(100),
        )
        assert parse_ael("$.mapBin1.a.{#-1}.asInt() == 100") == expected
