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

"""Unit tests for explicit types filter generation."""

import re

import pytest

from aerospike_sdk import (
    Filter,
    AelParseException,
    Exp,
    Index,
    IndexContext,
    IndexTypeEnum,
    parse_ael_with_index,
)

MAX = 2**63 - 1


def _index_ctx():
    """Index context with intBin1 (numeric) and stringBin1 (string)."""
    indexes = [
        Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace="test", bin_values_ratio=1),
        Index(bin="stringBin1", index_type=IndexTypeEnum.STRING, namespace="test", bin_values_ratio=1),
    ]
    return IndexContext.of("test", indexes)


def _index_ctx_blob():
    """Index context including blobBin1 (BLOB) for blob comparison tests."""
    indexes = [
        Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace="test", bin_values_ratio=1),
        Index(bin="stringBin1", index_type=IndexTypeEnum.STRING, namespace="test", bin_values_ratio=1),
        Index(bin="blobBin1", index_type=IndexTypeEnum.BLOB, namespace="test", bin_values_ratio=1),
    ]
    return IndexContext.of("test", indexes)


def _assert_range_filter(result, bin_name: str, range_min: int, range_max: int) -> None:
    """Assert result.filter equals Filter.range(bin_name, range_min, range_max) by value."""
    assert result.filter is not None
    expected = Filter.range(bin_name, range_min, range_max)
    assert str(result.filter) == str(expected), f"{result.filter!r} != {expected!r}"


def _assert_equal_filter(result, bin_name: str, value) -> None:
    """Assert result.filter equals Filter.equal(bin_name, value) by value."""
    assert result.filter is not None
    expected = Filter.equal(bin_name, value)
    assert str(result.filter) == str(expected), f"{result.filter!r} != {expected!r}"


class TestExplicitTypesFilters:
    """Test filter generation with explicit type expressions."""

    def test_explicit_int_parsed(self):
        """$.intBin1.get(type: INT) > 5 parses to correct Exp."""
        result = parse_ael_with_index("$.intBin1.get(type: INT) > 5")
        assert result.filter is None
        assert result.exp == Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(5))


class TestIntegerComparison:
    """Explicit type INT with range filter."""

    def test_integer_comparison_no_index(self):
        """Namespace and indexes must be given to create a Filter; no index context → filter None."""
        result = parse_ael_with_index("$.intBin1.get(type: INT) > 5")
        assert result.filter is None
        assert result.exp is not None

    def test_integer_comparison_with_index(self):
        """$.intBin1.get(type: INT) > 5 with index context → range(intBin1, 6, MAX)."""
        result = parse_ael_with_index("$.intBin1.get(type: INT) > 5", _index_ctx())
        _assert_range_filter(result, "intBin1", 6, MAX)
        assert result.exp is None

    def test_integer_comparison_value_op_bin(self):
        """5 < $.intBin1.get(type: INT) with index context → range(intBin1, 6, MAX)."""
        result = parse_ael_with_index("5 < $.intBin1.get(type: INT)", _index_ctx())
        _assert_range_filter(result, "intBin1", 6, MAX)
        assert result.exp is None

    def test_two_integer_bins_comparison(self):
        """$.intBin1.get(type: INT) == $.intBin2.get(type: INT) — bin-op-bin; filter is None."""
        result = parse_ael_with_index(
            "$.intBin1.get(type: INT) == $.intBin2.get(type: INT)",
            _index_ctx(),
        )
        assert result.filter is None


class TestStringComparison:
    """Explicit type STRING with equal filter."""

    def test_string_comparison_double_quotes(self):
        """$.stringBin1.get(type: STRING) == \"yes\" with index context → equal(stringBin1, 'yes')."""
        result = parse_ael_with_index('$.stringBin1.get(type: STRING) == "yes"', _index_ctx())
        _assert_equal_filter(result, "stringBin1", "yes")
        assert result.exp is None

    def test_string_comparison_single_quotes(self):
        """$.stringBin1.get(type: STRING) == 'yes' with index context → equal(stringBin1, 'yes')."""
        result = parse_ael_with_index("$.stringBin1.get(type: STRING) == 'yes'", _index_ctx())
        _assert_equal_filter(result, "stringBin1", "yes")
        assert result.exp is None

    def test_string_comparison_value_op_bin_double_quotes(self):
        """\"yes\" == $.stringBin1.get(type: STRING) with index context → equal(stringBin1, 'yes')."""
        result = parse_ael_with_index('"yes" == $.stringBin1.get(type: STRING)', _index_ctx())
        _assert_equal_filter(result, "stringBin1", "yes")
        assert result.exp is None

    def test_string_comparison_value_op_bin_single_quotes(self):
        """'yes' == $.stringBin1.get(type: STRING) with index context → equal(stringBin1, 'yes')."""
        result = parse_ael_with_index("'yes' == $.stringBin1.get(type: STRING)", _index_ctx())
        _assert_equal_filter(result, "stringBin1", "yes")
        assert result.exp is None

    def test_two_string_bins_comparison(self):
        """$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING) — bin-op-bin; filter is None."""
        result = parse_ael_with_index(
            '$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)',
            _index_ctx(),
        )
        assert result.filter is None


class TestStringComparisonNegative:
    """String constant must be quoted."""

    def test_string_constant_must_be_quoted(self):
        """$.stringBin1.get(type: STRING) == yes — unquoted string raises AelParseException."""
        with pytest.raises(AelParseException, match=r"Unable to parse right operand|Right operand cannot be None|Could not parse given AEL|mismatched input"):
            parse_ael_with_index("$.stringBin1.get(type: STRING) == yes")


class TestBlobComparison:
    """Explicit type BLOB with base64-encoded string literal."""

    def test_blob_comparison_bin_op_value(self):
        """$.blobBin1.get(type: BLOB) == \"AQID\" with index context → equal(blobBin1, b'\\x01\\x02\\x03')."""
        result = parse_ael_with_index(
            '$.blobBin1.get(type: BLOB) == "AQID"',
            _index_ctx_blob(),
        )
        _assert_equal_filter(result, "blobBin1", b"\x01\x02\x03")
        assert result.exp is None

    def test_blob_comparison_value_op_bin(self):
        """\"AQID\" == $.blobBin1.get(type: BLOB) with index context → equal(blobBin1, b'\\x01\\x02\\x03')."""
        result = parse_ael_with_index(
            '"AQID" == $.blobBin1.get(type: BLOB)',
            _index_ctx_blob(),
        )
        _assert_equal_filter(result, "blobBin1", b"\x01\x02\x03")
        assert result.exp is None

    def test_two_blob_bins_comparison(self):
        """$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB) — bin-op-bin; filter is None."""
        result = parse_ael_with_index(
            "$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)",
            _index_ctx_blob(),
        )
        assert result.filter is None


class TestFloatComparison:
    """No float support in secondary index filter; filter is None."""

    def test_float_comparison_bin_op_value(self):
        """$.floatBin1.get(type: FLOAT) == 1.5 — no float filter support."""
        result = parse_ael_with_index("$.floatBin1.get(type: FLOAT) == 1.5")
        assert result.filter is None
        assert result.exp is not None

    def test_float_comparison_value_op_bin(self):
        """1.5 == $.floatBin1.get(type: FLOAT) — no float filter support."""
        result = parse_ael_with_index("1.5 == $.floatBin1.get(type: FLOAT)")
        assert result.filter is None
        assert result.exp is not None

    def test_two_float_bins_comparison(self):
        """$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT) — bin-op-bin; filter is None."""
        result = parse_ael_with_index(
            "$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)",
        )
        assert result.filter is None


class TestBooleanComparison:
    """No boolean support in secondary index filter; filter is None."""

    def test_boolean_comparison_bin_op_value(self):
        """$.boolBin1.get(type: BOOL) == true — no boolean filter support."""
        result = parse_ael_with_index("$.boolBin1.get(type: BOOL) == true")
        assert result.filter is None
        assert result.exp is not None

    def test_boolean_comparison_value_op_bin(self):
        """true == $.boolBin1.get(type: BOOL) — no boolean filter support."""
        result = parse_ael_with_index("true == $.boolBin1.get(type: BOOL)")
        assert result.filter is None
        assert result.exp is not None


class TestNegativeBooleanComparison:
    """BOOL compared to INT raises AelParseException."""

    def test_cannot_compare_bool_to_int(self):
        """$.boolBin1.get(type: BOOL) == 5 raises Cannot compare BOOL to INT."""
        with pytest.raises(AelParseException, match="Cannot compare BOOL to INT"):
            parse_ael_with_index("$.boolBin1.get(type: BOOL) == 5")


class TestListComparison:
    """List comparison: not supported by secondary index filter; filter is None or raises."""

    def test_list_comparison_constant_on_right_side(self):
        """$.listBin1.get(type: LIST) == [100] — not supported by secondary index filter."""
        result = parse_ael_with_index("$.listBin1.get(type: LIST) == [100]")
        assert result.filter is None

    def test_list_comparison_constant_on_right_side_negative(self):
        """$.listBin1.get(type: LIST) == [yes, of course] — unquoted list elements raise or filter is None."""
        try:
            result = parse_ael_with_index("$.listBin1.get(type: LIST) == [yes, of course]")
            assert result.filter is None
        except AelParseException as e:
            assert re.search(
                r"Unable to parse list operand|Unsupported operand type|Could not parse given AEL expression input|line \d+:\d+|extraneous input|no viable alternative",
                str(e),
            )

    def test_list_comparison_constant_on_left_side(self):
        """[100] == $.listBin1.get(type: LIST) — not supported by secondary index filter."""
        result = parse_ael_with_index("[100] == $.listBin1.get(type: LIST)")
        assert result.filter is None

    def test_list_comparison_constant_on_left_side_negative(self):
        """[yes, of course] == $.listBin1.get(type: LIST) — invalid list raises."""
        with pytest.raises(AelParseException, match=r"Could not parse given AEL expression input|Failed to parse AEL expression: visitor returned None|line \d+:\d+|no viable alternative|extraneous input"):
            parse_ael_with_index("[yes, of course] == $.listBin1.get(type: LIST)")


class TestMapComparison:
    """Map comparison not supported by secondary index filter; filter is None or raises."""

    def test_map_comparison_constant_on_right_side(self):
        """$.mapBin1.get(type: MAP) == {100:100} — not supported by secondary index filter."""
        result = parse_ael_with_index("$.mapBin1.get(type: MAP) == {100:100}")
        assert result.filter is None

    def test_map_comparison_constant_on_right_side_negative(self):
        """$.mapBin1.get(type: MAP) == {yes, of course} — invalid map raises or filter is None."""
        try:
            result = parse_ael_with_index("$.mapBin1.get(type: MAP) == {yes, of course}")
            assert result.filter is None
        except AelParseException as e:
            assert re.search(
                r"Unable to parse map operand|Map constants in expressions|Could not parse given AEL expression input|line \d+:\d+|extraneous input|no viable alternative",
                str(e),
            )

    def test_map_comparison_constant_on_left_side(self):
        """{100:100} == $.mapBin1.get(type: MAP) — not supported by secondary index filter."""
        result = parse_ael_with_index("{100:100} == $.mapBin1.get(type: MAP)")
        assert result.filter is None

    def test_map_comparison_constant_on_left_side_negative(self):
        """{yes, of course} == $.mapBin1.get(type: MAP) — invalid map raises."""
        with pytest.raises(AelParseException, match=r"Could not parse given AEL expression input|Failed to parse AEL expression: visitor returned None|line \d+:\d+|no viable alternative|extraneous input"):
            parse_ael_with_index("{yes, of course} == $.mapBin1.get(type: MAP)")
