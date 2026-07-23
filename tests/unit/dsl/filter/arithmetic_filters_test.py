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

"""Unit tests for arithmetic filter generation.

Each parseFilterAndCompare or assert filter null is a separate test for clear failure isolation.
"""

from aerospike_sdk import Filter, Index, IndexContext, IndexTypeEnum, parse_ael_with_index

MIN = -(2**63)
MAX = 2**63 - 1


def _assert_range_filter(result, bin_name: str, range_min: int, range_max: int) -> None:
    """Assert result.filter equals Filter.range(bin_name, range_min, range_max) by value."""
    assert result.filter is not None
    expected = Filter.range(bin_name, range_min, range_max)
    assert str(result.filter) == str(expected), f"{result.filter!r} != {expected!r}"


def _index_ctx():
    """Index context with apples and bananas numeric indexes."""
    indexes = [
        Index(bin="apples", index_type=IndexTypeEnum.NUMERIC, namespace="test1", bin_values_ratio=1),
        Index(bin="bananas", index_type=IndexTypeEnum.NUMERIC, namespace="test1", bin_values_ratio=1),
    ]
    return IndexContext.of("test1", indexes)


class TestArithmeticFiltersAdd:
    """Filter generation for addition in range expressions."""

    def test_add_1(self):
        """Two bins: ($.apples + $.bananas) > 10 — not supported by secondary index filter."""
        result = parse_ael_with_index("($.apples + $.bananas) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_add_2(self):
        """($.apples + 5) > 10 → range(apples, 6, MAX)."""
        result = parse_ael_with_index("($.apples + 5) > 10", _index_ctx())
        _assert_range_filter(result, "apples", 10 - 5 + 1, MAX)
        assert result.exp is None

    def test_add_3(self):
        """($.apples + 5) >= 10 → range(apples, 5, MAX)."""
        result = parse_ael_with_index("($.apples + 5) >= 10", _index_ctx())
        _assert_range_filter(result, "apples", 10 - 5, MAX)
        assert result.exp is None

    def test_add_4(self):
        """($.apples + 5) < 10 → range(apples, MIN, 4)."""
        result = parse_ael_with_index("($.apples + 5) < 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 10 - 5 - 1)
        assert result.exp is None

    def test_add_5(self):
        """($.apples + 5) <= 10 → range(apples, MIN, 5)."""
        result = parse_ael_with_index("($.apples + 5) <= 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 10 - 5)
        assert result.exp is None

    def test_add_6(self):
        """(9 + $.bananas) > 10 → range(bananas, 2, MAX)."""
        result = parse_ael_with_index("(9 + $.bananas) > 10", _index_ctx())
        _assert_range_filter(result, "bananas", 10 - 9 + 1, MAX)
        assert result.exp is None

    def test_add_7(self):
        """(9 + $.bananas) >= 10 → range(bananas, 1, MAX)."""
        result = parse_ael_with_index("(9 + $.bananas) >= 10", _index_ctx())
        _assert_range_filter(result, "bananas", 10 - 9, MAX)
        assert result.exp is None

    def test_add_8(self):
        """(9 + $.bananas) < 10 → range(bananas, MIN, 0)."""
        result = parse_ael_with_index("(9 + $.bananas) < 10", _index_ctx())
        _assert_range_filter(result, "bananas", MIN, 10 - 9 - 1)
        assert result.exp is None

    def test_add_9(self):
        """(9 + $.bananas) <= 10 → range(bananas, MIN, 1)."""
        result = parse_ael_with_index("(9 + $.bananas) <= 10", _index_ctx())
        _assert_range_filter(result, "bananas", MIN, 10 - 9)
        assert result.exp is None

    def test_add_10(self):
        """(5.2 + $.bananas) > 10.2 — float not supported by secondary index filter."""
        result = parse_ael_with_index("(5.2 + $.bananas) > 10.2")
        assert result.filter is None
        assert result.exp is not None

    def test_add_11(self):
        """($.apples + $.bananas + 5) > 10 — not supported by current grammar."""
        result = parse_ael_with_index("($.apples + $.bananas + 5) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None


class TestArithmeticFiltersSub:
    """Filter generation for subtraction."""

    def test_sub_1(self):
        """($.apples - $.bananas) > 10 — not supported by secondary index filter."""
        result = parse_ael_with_index("($.apples - $.bananas) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_sub_2(self):
        """($.apples - 5) > 10 → range(apples, 16, MAX)."""
        result = parse_ael_with_index("($.apples - 5) > 10", _index_ctx())
        _assert_range_filter(result, "apples", 10 + 5 + 1, MAX)
        assert result.exp is None

    def test_sub_3(self):
        """($.apples - 5) >= 10 → range(apples, 15, MAX)."""
        result = parse_ael_with_index("($.apples - 5) >= 10", _index_ctx())
        _assert_range_filter(result, "apples", 10 + 5, MAX)
        assert result.exp is None

    def test_sub_4(self):
        """($.apples - 5) < 10 → range(apples, MIN, 14)."""
        result = parse_ael_with_index("($.apples - 5) < 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 10 + 5 - 1)
        assert result.exp is None

    def test_sub_5(self):
        """($.apples - 5) <= 10 → range(apples, MIN, 15)."""
        result = parse_ael_with_index("($.apples - 5) <= 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 10 + 5)
        assert result.exp is None

    def test_sub_6(self):
        """($.apples - 5) > -10 → range(apples, -4, MAX)."""
        result = parse_ael_with_index("($.apples - 5) > -10", _index_ctx())
        _assert_range_filter(result, "apples", -10 + 5 + 1, MAX)
        assert result.exp is None

    def test_sub_7(self):
        """($.apples - 5) >= -10 → range(apples, -5, MAX)."""
        result = parse_ael_with_index("($.apples - 5) >= -10", _index_ctx())
        _assert_range_filter(result, "apples", -10 + 5, MAX)
        assert result.exp is None

    def test_sub_8(self):
        """($.apples - 5) < -10 → range(apples, MIN, -6)."""
        result = parse_ael_with_index("($.apples - 5) < -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -10 + 5 - 1)
        assert result.exp is None

    def test_sub_9(self):
        """($.apples - 5) <= -10 → range(apples, MIN, -5)."""
        result = parse_ael_with_index("($.apples - 5) <= -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -10 + 5)
        assert result.exp is None

    def test_sub_10(self):
        """(9 - $.bananas) > 10 → range(bananas, MIN, -2)."""
        result = parse_ael_with_index("(9 - $.bananas) > 10", _index_ctx())
        _assert_range_filter(result, "bananas", MIN, 9 - 10 - 1)
        assert result.exp is None

    def test_sub_11(self):
        """(9 - $.bananas) >= 10 → range(bananas, MIN, -1)."""
        result = parse_ael_with_index("(9 - $.bananas) >= 10", _index_ctx())
        _assert_range_filter(result, "bananas", MIN, 9 - 10)
        assert result.exp is None

    def test_sub_12(self):
        """(9 - $.bananas) < 10 → range(bananas, 0, MAX)."""
        result = parse_ael_with_index("(9 - $.bananas) < 10", _index_ctx())
        _assert_range_filter(result, "bananas", 9 - 10 + 1, MAX)
        assert result.exp is None

    def test_sub_13(self):
        """(9 - $.bananas) <= 10 → range(bananas, 1, MAX)."""
        result = parse_ael_with_index("(9 - $.bananas) <= 10", _index_ctx())
        _assert_range_filter(result, "bananas", 9 - 10, MAX)
        assert result.exp is None

    def test_sub_14(self):
        """($.apples - $.bananas) > 10 (no index ctx) — filter null."""
        result = parse_ael_with_index("($.apples - $.bananas) > 10")
        assert result.filter is None

    def test_sub_15(self):
        """($.apples - $.bananas - 5) > 10 — not supported by current grammar."""
        result = parse_ael_with_index("($.apples - $.bananas - 5) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None


class TestArithmeticFiltersMul:
    """Filter generation for multiplication."""

    def test_mul_1(self):
        """($.apples * $.bananas) > 10 — not supported by secondary index filter."""
        result = parse_ael_with_index("($.apples * $.bananas) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_mul_2(self):
        """($.apples * 5) > 10 → range(apples, 3, MAX)."""
        result = parse_ael_with_index("($.apples * 5) > 10", _index_ctx())
        _assert_range_filter(result, "apples", 10 // 5 + 1, MAX)
        assert result.exp is None

    def test_mul_3(self):
        """($.apples * 5) >= 10 → range(apples, 2, MAX)."""
        result = parse_ael_with_index("($.apples * 5) >= 10", _index_ctx())
        _assert_range_filter(result, "apples", 10 // 5, MAX)
        assert result.exp is None

    def test_mul_4(self):
        """($.apples * 5) < 10 → range(apples, MIN, 1)."""
        result = parse_ael_with_index("($.apples * 5) < 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 10 // 5 - 1)
        assert result.exp is None

    def test_mul_5(self):
        """($.apples * 5) <= 10 → range(apples, MIN, 2)."""
        result = parse_ael_with_index("($.apples * 5) <= 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 10 // 5)
        assert result.exp is None

    def test_mul_6(self):
        """(9 * $.bananas) > 10 → range(bananas, 2, MAX)."""
        result = parse_ael_with_index("(9 * $.bananas) > 10", _index_ctx())
        _assert_range_filter(result, "bananas", 10 // 9 + 1, MAX)
        assert result.exp is None

    def test_mul_7(self):
        """(9 * $.bananas) >= 10 → range(bananas, 2, MAX)."""
        result = parse_ael_with_index("(9 * $.bananas) >= 10", _index_ctx())
        _assert_range_filter(result, "bananas", 10 // 9, MAX)
        assert result.exp is None

    def test_mul_8(self):
        """(9 * $.bananas) < 10 → range(bananas, MIN, 1)."""
        result = parse_ael_with_index("(9 * $.bananas) < 10", _index_ctx())
        _assert_range_filter(result, "bananas", MIN, 10 // 9 - 1)
        assert result.exp is None

    def test_mul_9(self):
        """(9 * $.bananas) <= 10 → range(bananas, MIN, 1)."""
        result = parse_ael_with_index("(9 * $.bananas) <= 10", _index_ctx())
        _assert_range_filter(result, "bananas", MIN, 10 // 9)
        assert result.exp is None

    def test_mul_10(self):
        """($.apples * -5) > 10 → range(apples, MIN, -3)."""
        result = parse_ael_with_index("($.apples * -5) > 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 10 // (-5) - 1)
        assert result.exp is None

    def test_mul_11(self):
        """($.apples * -5) >= 10 → range(apples, MIN, -2)."""
        result = parse_ael_with_index("($.apples * -5) >= 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 10 // (-5))
        assert result.exp is None

    def test_mul_12(self):
        """($.apples * -5) < 10 → range(apples, -1, MAX)."""
        result = parse_ael_with_index("($.apples * -5) < 10", _index_ctx())
        _assert_range_filter(result, "apples", 10 // (-5) + 1, MAX)
        assert result.exp is None

    def test_mul_13(self):
        """($.apples * -5) <= 10 → range(apples, -2, MAX)."""
        result = parse_ael_with_index("($.apples * -5) <= 10", _index_ctx())
        _assert_range_filter(result, "apples", 10 // (-5), MAX)
        assert result.exp is None

    def test_mul_14(self):
        """(0 * $.bananas) > 10 — cannot divide by zero, filter null."""
        result = parse_ael_with_index("(0 * $.bananas) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_mul_15(self):
        """(9 * $.bananas) > 0 → range(bananas, 1, MAX)."""
        result = parse_ael_with_index("(9 * $.bananas) > 0", _index_ctx())
        _assert_range_filter(result, "bananas", 0 // 9 + 1, MAX)
        assert result.exp is None

    def test_mul_16(self):
        """($.apples * $.bananas - 5) > 10 — not supported by current grammar."""
        result = parse_ael_with_index("($.apples * $.bananas - 5) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None


class TestArithmeticFiltersDivTwoBins:
    """Division with two bins."""

    def test_div_two_bins_1(self):
        """($.apples / $.bananas) <= 10 — not supported by secondary index filter."""
        result = parse_ael_with_index("($.apples / $.bananas) <= 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None


class TestArithmeticFiltersDivBinDividedLeftLarger:
    """Bin is dividend, divisor constant larger than result."""

    def test_div_bin_divided_left_larger_1(self):
        """($.apples / 50) > 10 → range(apples, 501, MAX)."""
        result = parse_ael_with_index("($.apples / 50) > 10", _index_ctx())
        _assert_range_filter(result, "apples", 50 * 10 + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_larger_2(self):
        """($.apples / 50) >= 10 → range(apples, 500, MAX)."""
        result = parse_ael_with_index("($.apples / 50) >= 10", _index_ctx())
        _assert_range_filter(result, "apples", 50 * 10, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_larger_3(self):
        """($.apples / 50) < 10 → range(apples, MIN, 499)."""
        result = parse_ael_with_index("($.apples / 50) < 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 50 * 10 - 1)
        assert result.exp is None

    def test_div_bin_divided_left_larger_4(self):
        """($.apples / 50) <= 10 → range(apples, MIN, 500)."""
        result = parse_ael_with_index("($.apples / 50) <= 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 50 * 10)
        assert result.exp is None

    def test_div_bin_divided_left_larger_5(self):
        """($.apples / -50) > 10 → range(apples, MIN, -501)."""
        result = parse_ael_with_index("($.apples / -50) > 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -50 * 10 - 1)
        assert result.exp is None

    def test_div_bin_divided_left_larger_6(self):
        """($.apples / -50) >= 10 → range(apples, MIN, -500)."""
        result = parse_ael_with_index("($.apples / -50) >= 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -50 * 10)
        assert result.exp is None

    def test_div_bin_divided_left_larger_7(self):
        """($.apples / -50) < 10 → range(apples, -499, MAX)."""
        result = parse_ael_with_index("($.apples / -50) < 10", _index_ctx())
        _assert_range_filter(result, "apples", -50 * 10 + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_larger_8(self):
        """($.apples / -50) <= 10 → range(apples, -500, MAX)."""
        result = parse_ael_with_index("($.apples / -50) <= 10", _index_ctx())
        _assert_range_filter(result, "apples", -50 * 10, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_larger_9(self):
        """($.apples / 50) > -10 → range(apples, -499, MAX)."""
        result = parse_ael_with_index("($.apples / 50) > -10", _index_ctx())
        _assert_range_filter(result, "apples", 50 * (-10) + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_larger_10(self):
        """($.apples / 50) >= -10 → range(apples, -500, MAX)."""
        result = parse_ael_with_index("($.apples / 50) >= -10", _index_ctx())
        _assert_range_filter(result, "apples", 50 * (-10), MAX)
        assert result.exp is None

    def test_div_bin_divided_left_larger_11(self):
        """($.apples / 50) < -10 → range(apples, MIN, -501)."""
        result = parse_ael_with_index("($.apples / 50) < -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 50 * (-10) - 1)
        assert result.exp is None

    def test_div_bin_divided_left_larger_12(self):
        """($.apples / 50) <= -10 → range(apples, MIN, -500)."""
        result = parse_ael_with_index("($.apples / 50) <= -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 50 * (-10))
        assert result.exp is None

    def test_div_bin_divided_left_larger_13(self):
        """($.apples / -50) > -10 → range(apples, MIN, 499)."""
        result = parse_ael_with_index("($.apples / -50) > -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -50 * (-10) - 1)
        assert result.exp is None

    def test_div_bin_divided_left_larger_14(self):
        """($.apples / -50) >= -10 → range(apples, MIN, 500)."""
        result = parse_ael_with_index("($.apples / -50) >= -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, (-10) * (-50))
        assert result.exp is None

    def test_div_bin_divided_left_larger_15(self):
        """($.apples / -50) < -10 → range(apples, 501, MAX)."""
        result = parse_ael_with_index("($.apples / -50) < -10", _index_ctx())
        _assert_range_filter(result, "apples", (-10) * (-50) + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_larger_16(self):
        """($.apples / -50) <= -10 → range(apples, 500, MAX)."""
        result = parse_ael_with_index("($.apples / -50) <= -10", _index_ctx())
        _assert_range_filter(result, "apples", (-10) * (-50), MAX)
        assert result.exp is None


class TestArithmeticFiltersDivBinDividedLeftSmaller:
    """Bin is dividend, divisor constant smaller."""

    def test_div_bin_divided_left_smaller_1(self):
        """($.apples / 5) > 10 → range(apples, 51, MAX)."""
        result = parse_ael_with_index("($.apples / 5) > 10", _index_ctx())
        _assert_range_filter(result, "apples", 5 * 10 + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_2(self):
        """($.apples / 5) >= 10 → range(apples, 50, MAX)."""
        result = parse_ael_with_index("($.apples / 5) >= 10", _index_ctx())
        _assert_range_filter(result, "apples", 5 * 10, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_3(self):
        """($.apples / 5) < 10 → range(apples, MIN, 49)."""
        result = parse_ael_with_index("($.apples / 5) < 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 5 * 10 - 1)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_4(self):
        """($.apples / 5) <= 10 → range(apples, MIN, 50)."""
        result = parse_ael_with_index("($.apples / 5) <= 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 5 * 10)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_5(self):
        """($.apples / -5) > 10 → range(apples, MIN, -51)."""
        result = parse_ael_with_index("($.apples / -5) > 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -5 * 10 - 1)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_6(self):
        """($.apples / -5) >= 10 → range(apples, MIN, -50)."""
        result = parse_ael_with_index("($.apples / -5) >= 10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -5 * 10)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_7(self):
        """($.apples / -5) < 10 → range(apples, -49, MAX)."""
        result = parse_ael_with_index("($.apples / -5) < 10", _index_ctx())
        _assert_range_filter(result, "apples", -5 * 10 + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_8(self):
        """($.apples / -5) <= 10 → range(apples, -50, MAX)."""
        result = parse_ael_with_index("($.apples / -5) <= 10", _index_ctx())
        _assert_range_filter(result, "apples", -5 * 10, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_9(self):
        """($.apples / 5) > -10 → range(apples, -49, MAX)."""
        result = parse_ael_with_index("($.apples / 5) > -10", _index_ctx())
        _assert_range_filter(result, "apples", 5 * (-10) + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_10(self):
        """($.apples / 5) >= -10 → range(apples, -50, MAX)."""
        result = parse_ael_with_index("($.apples / 5) >= -10", _index_ctx())
        _assert_range_filter(result, "apples", 5 * (-10), MAX)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_11(self):
        """($.apples / 5) < -10 → range(apples, MIN, -51)."""
        result = parse_ael_with_index("($.apples / 5) < -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 5 * (-10) - 1)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_12(self):
        """($.apples / 5) <= -10 → range(apples, MIN, -50)."""
        result = parse_ael_with_index("($.apples / 5) <= -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 5 * (-10))
        assert result.exp is None

    def test_div_bin_divided_left_smaller_13(self):
        """($.apples / -5) > -10 → range(apples, MIN, 49)."""
        result = parse_ael_with_index("($.apples / -5) > -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, (-10) * (-5) - 1)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_14(self):
        """($.apples / -5) >= -10 → range(apples, MIN, 50)."""
        result = parse_ael_with_index("($.apples / -5) >= -10", _index_ctx())
        _assert_range_filter(result, "apples", MIN, (-10) * (-5))
        assert result.exp is None

    def test_div_bin_divided_left_smaller_15(self):
        """($.apples / -5) < -10 → range(apples, 51, MAX)."""
        result = parse_ael_with_index("($.apples / -5) < -10", _index_ctx())
        _assert_range_filter(result, "apples", (-10) * (-5) + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_smaller_16(self):
        """($.apples / -5) <= -10 → range(apples, 50, MAX)."""
        result = parse_ael_with_index("($.apples / -5) <= -10", _index_ctx())
        _assert_range_filter(result, "apples", (-10) * (-5), MAX)
        assert result.exp is None


class TestArithmeticFiltersDivBinDividedLeftEquals:
    """Bin is dividend, divisor constant equals result."""

    def test_div_bin_divided_left_equals_1(self):
        """($.apples / 5) > 5 → range(apples, 26, MAX)."""
        result = parse_ael_with_index("($.apples / 5) > 5", _index_ctx())
        _assert_range_filter(result, "apples", 5 * 5 + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_equals_2(self):
        """($.apples / 5) >= 5 → range(apples, 25, MAX)."""
        result = parse_ael_with_index("($.apples / 5) >= 5", _index_ctx())
        _assert_range_filter(result, "apples", 5 * 5, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_equals_3(self):
        """($.apples / 5) < 5 → range(apples, MIN, 24)."""
        result = parse_ael_with_index("($.apples / 5) < 5", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 5 * 5 - 1)
        assert result.exp is None

    def test_div_bin_divided_left_equals_4(self):
        """($.apples / 5) <= 5 → range(apples, MIN, 25)."""
        result = parse_ael_with_index("($.apples / 5) <= 5", _index_ctx())
        _assert_range_filter(result, "apples", MIN, 5 * 5)
        assert result.exp is None

    def test_div_bin_divided_left_equals_5(self):
        """($.apples / -5) > -5 → range(apples, MIN, 24)."""
        result = parse_ael_with_index("($.apples / -5) > -5", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -5 * (-5) - 1)
        assert result.exp is None

    def test_div_bin_divided_left_equals_6(self):
        """($.apples / -5) >= -5 → range(apples, MIN, 25)."""
        result = parse_ael_with_index("($.apples / -5) >= -5", _index_ctx())
        _assert_range_filter(result, "apples", MIN, -5 * (-5))
        assert result.exp is None

    def test_div_bin_divided_left_equals_7(self):
        """($.apples / -5) < -5 → range(apples, 26, MAX)."""
        result = parse_ael_with_index("($.apples / -5) < -5", _index_ctx())
        _assert_range_filter(result, "apples", -5 * (-5) + 1, MAX)
        assert result.exp is None

    def test_div_bin_divided_left_equals_8(self):
        """($.apples / -5) <= -5 → range(apples, 25, MAX)."""
        result = parse_ael_with_index("($.apples / -5) <= -5", _index_ctx())
        _assert_range_filter(result, "apples", -5 * (-5), MAX)
        assert result.exp is None


class TestArithmeticFiltersDivBinDivisorLeftLarger:
    """Bin is divisor, dividend constant larger."""

    def test_div_bin_divisor_left_larger_1(self):
        """(90 / $.bananas) > 10 → range(bananas, 1, 8)."""
        result = parse_ael_with_index("(90 / $.bananas) > 10", _index_ctx())
        _assert_range_filter(result, "bananas", 1, 90 // 10 - 1)
        assert result.exp is None

    def test_div_bin_divisor_left_larger_2(self):
        """(90 / $.bananas) >= 10 → range(bananas, 1, 9)."""
        result = parse_ael_with_index("(90 / $.bananas) >= 10", _index_ctx())
        _assert_range_filter(result, "bananas", 1, 90 // 10)
        assert result.exp is None

    def test_div_bin_divisor_left_larger_3(self):
        """(90 / $.bananas) < 10 — not supported by secondary index filter."""
        result = parse_ael_with_index("(90 / $.bananas) < 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_larger_4(self):
        """(90 / $.bananas) <= 10 — not supported by secondary index filter."""
        result = parse_ael_with_index("(90 / $.bananas) <= 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_larger_5(self):
        """(90 / $.bananas) > -10 — not supported by secondary index filter."""
        result = parse_ael_with_index("(90 / $.bananas) > -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_larger_6(self):
        """(90 / $.bananas) >= -10 — not supported by secondary index filter."""
        result = parse_ael_with_index("(90 / $.bananas) >= -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_larger_7(self):
        """(90 / $.bananas) < -10 → range(bananas, -8, -1)."""
        result = parse_ael_with_index("(90 / $.bananas) < -10", _index_ctx())
        _assert_range_filter(result, "bananas", 90 // (-10) + 1, -1)
        assert result.exp is None

    def test_div_bin_divisor_left_larger_8(self):
        """(90 / $.bananas) <= -10 → range(bananas, -9, -1)."""
        result = parse_ael_with_index("(90 / $.bananas) <= -10", _index_ctx())
        _assert_range_filter(result, "bananas", 90 // (-10), -1)
        assert result.exp is None

    def test_div_bin_divisor_left_larger_9(self):
        """(-90 / $.bananas) > 10 → range(bananas, -8, -1)."""
        result = parse_ael_with_index("(-90 / $.bananas) > 10", _index_ctx())
        _assert_range_filter(result, "bananas", -90 // 10 + 1, -1)
        assert result.exp is None

    def test_div_bin_divisor_left_larger_10(self):
        """(90 / $.bananas) >= 10 (duplicate) → range(bananas, 1, 9)."""
        result = parse_ael_with_index("(90 / $.bananas) >= 10", _index_ctx())
        _assert_range_filter(result, "bananas", 1, 90 // 10)
        assert result.exp is None

    def test_div_bin_divisor_left_larger_11(self):
        """(-90 / $.bananas) < 10 — not supported by secondary index filter."""
        result = parse_ael_with_index("(-90 / $.bananas) < 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_larger_12(self):
        """(-90 / $.bananas) <= 10 — not supported by secondary index filter."""
        result = parse_ael_with_index("(-90 / $.bananas) <= 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_larger_13(self):
        """(-90 / $.bananas) > -10 — not supported by secondary index filter."""
        result = parse_ael_with_index("(-90 / $.bananas) > -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_larger_14(self):
        """(-90 / $.bananas) >= -10 — not supported by secondary index filter."""
        result = parse_ael_with_index("(-90 / $.bananas) >= -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_larger_15(self):
        """(-90 / $.bananas) < -10 → range(bananas, 1, 8)."""
        result = parse_ael_with_index("(-90 / $.bananas) < -10", _index_ctx())
        _assert_range_filter(result, "bananas", 1, -90 // (-10) - 1)
        assert result.exp is None

    def test_div_bin_divisor_left_larger_16(self):
        """(-90 / $.bananas) <= -10 → range(bananas, 1, 9)."""
        result = parse_ael_with_index("(-90 / $.bananas) <= -10", _index_ctx())
        _assert_range_filter(result, "bananas", 1, -90 // (-10))
        assert result.exp is None


class TestArithmeticFiltersDivBinDivisorLeftSmaller:
    """Bin is divisor, dividend constant smaller — all yield null."""

    def test_div_bin_divisor_left_smaller_1(self):
        """(9 / $.bananas) > 10 — no integer numbers."""
        result = parse_ael_with_index("(9 / $.bananas) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_2(self):
        """(9 / $.bananas) >= 10 — no integer numbers."""
        result = parse_ael_with_index("(9 / $.bananas) >= 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_3(self):
        """(9 / $.bananas) < 10 — maximal range is all numbers."""
        result = parse_ael_with_index("(9 / $.bananas) < 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_4(self):
        """(9 / $.bananas) <= 10 — maximal range is all numbers."""
        result = parse_ael_with_index("(9 / $.bananas) <= 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_5(self):
        """(9 / $.bananas) > -10 — maximal range is all numbers."""
        result = parse_ael_with_index("(9 / $.bananas) > -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_6(self):
        """(9 / $.bananas) >= -10 — maximal range is all numbers."""
        result = parse_ael_with_index("(9 / $.bananas) >= -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_7(self):
        """(9 / $.bananas) < -10 — no integer numbers."""
        result = parse_ael_with_index("(9 / $.bananas) < -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_8(self):
        """(9 / $.bananas) <= -10 — no integer numbers."""
        result = parse_ael_with_index("(9 / $.bananas) <= -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_9(self):
        """(-9 / $.bananas) > 10 — no integer numbers."""
        result = parse_ael_with_index("(-9 / $.bananas) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_10(self):
        """(-9 / $.bananas) >= 10 — no integer numbers."""
        result = parse_ael_with_index("(-9 / $.bananas) >= 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_11(self):
        """(-9 / $.bananas) < 10 — maximal range is all numbers."""
        result = parse_ael_with_index("(-9 / $.bananas) < 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_12(self):
        """(-9 / $.bananas) <= 10 — maximal range is all numbers."""
        result = parse_ael_with_index("(-9 / $.bananas) <= 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_13(self):
        """(-9 / $.bananas) > -10 — maximal range is all numbers."""
        result = parse_ael_with_index("(-9 / $.bananas) > -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_14(self):
        """(-9 / $.bananas) >= -10 — maximal range is all numbers."""
        result = parse_ael_with_index("(-9 / $.bananas) >= -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_15(self):
        """(-9 / $.bananas) < -10 — no integer numbers."""
        result = parse_ael_with_index("(-9 / $.bananas) < -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_16(self):
        """(-9 / $.bananas) <= -10 — no integer numbers."""
        result = parse_ael_with_index("(-9 / $.bananas) <= -10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_17(self):
        """(0 / $.bananas) > 10 — maximal range is all numbers."""
        result = parse_ael_with_index("(0 / $.bananas) > 10", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_smaller_18(self):
        """(9 / $.bananas) > 0 — no integer numbers (cannot divide by zero for filter)."""
        result = parse_ael_with_index("(9 / $.bananas) > 0", _index_ctx())
        assert result.filter is None
        assert result.exp is not None


class TestArithmeticFiltersDivBinDivisorLeftEquals:
    """Bin is divisor, dividend constant equals divisor."""

    def test_div_bin_divisor_left_equals_1(self):
        """(90 / $.bananas) > 90 — no integer numbers."""
        result = parse_ael_with_index("(90 / $.bananas) > 90", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_equals_2(self):
        """(90 / $.bananas) >= 90 → range(bananas, 1, 1)."""
        result = parse_ael_with_index("(90 / $.bananas) >= 90", _index_ctx())
        _assert_range_filter(result, "bananas", 90 // 90, 90 // 90)
        assert result.exp is None

    def test_div_bin_divisor_left_equals_3(self):
        """(90 / $.bananas) < 90 — maximal range is all numbers."""
        result = parse_ael_with_index("(90 / $.bananas) < 90", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_equals_4(self):
        """(90 / $.bananas) <= 90 — maximal range is all numbers."""
        result = parse_ael_with_index("(90 / $.bananas) <= 90", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_equals_5(self):
        """(-90 / $.bananas) > -90 — maximal range is all numbers."""
        result = parse_ael_with_index("(-90 / $.bananas) > -90", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_equals_6(self):
        """(-90 / $.bananas) >= -90 — maximal range is all numbers."""
        result = parse_ael_with_index("(-90 / $.bananas) >= -90", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_equals_7(self):
        """(-90 / $.bananas) < -90 — no integer numbers."""
        result = parse_ael_with_index("(-90 / $.bananas) < -90", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_div_bin_divisor_left_equals_8(self):
        """(-90 / $.bananas) <= -90 → range(bananas, 1, 1)."""
        result = parse_ael_with_index("(-90 / $.bananas) <= -90", _index_ctx())
        _assert_range_filter(result, "bananas", 1, 90 // 90)
        assert result.exp is None
