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

"""Unit tests for bin filter generation.

Each parseFilterAndCompare or assert filter null is a separate test for clear failure isolation.
"""

import pytest

from aerospike_sdk import (
    Filter,
    AelParseException,
    Index,
    IndexContext,
    IndexTypeEnum,
    parse_ael_with_index,
)

MIN = -(2**63)
MAX = 2**63 - 1


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


def _index_ctx():
    """Index context with intBin1 (numeric) and stringBin1 (string)."""
    indexes = [
        Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace="test", bin_values_ratio=1),
        Index(bin="stringBin1", index_type=IndexTypeEnum.STRING, namespace="test", bin_values_ratio=1),
    ]
    return IndexContext.of("test", indexes)


def _index_ctx_int_bins():
    """Index context with intBin1 (ratio 0) and intBin2 (ratio 1). Higher cardinality chosen for Filter."""
    indexes = [
        Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace="test", bin_values_ratio=0),
        Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace="test", bin_values_ratio=1),
    ]
    return IndexContext.of("test", indexes)


class TestBinFiltersGT:
    """Filter generation for greater-than."""

    def test_bin_gt_1(self):
        """$.intBin1 > 100 → range(intBin1, 101, MAX)."""
        result = parse_ael_with_index("$.intBin1 > 100", _index_ctx())
        _assert_range_filter(result, "intBin1", 101, MAX)
        assert result.exp is None

    def test_bin_gt_2(self):
        """$.intBin1 > -100 → range(intBin1, -99, MAX)."""
        result = parse_ael_with_index("$.intBin1 > -100", _index_ctx())
        _assert_range_filter(result, "intBin1", -99, MAX)
        assert result.exp is None

    def test_bin_gt_3(self):
        """$.stringBin1 > 'text' — string comparison not supported by secondary index Filter."""
        result = parse_ael_with_index("$.stringBin1 > 'text'", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_bin_gt_4(self):
        """$.stringBin1 > \"text\" — string comparison not supported by secondary index Filter."""
        result = parse_ael_with_index('$.stringBin1 > "text"', _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_bin_gt_5(self):
        """100 < $.intBin1 — same as $.intBin1 > 100 → range(intBin1, 101, MAX)."""
        result = parse_ael_with_index("100 < $.intBin1", _index_ctx())
        _assert_range_filter(result, "intBin1", 101, MAX)
        assert result.exp is None

    def test_bin_gt_6(self):
        """'text' > $.stringBin1 — string comparison not supported by secondary index Filter."""
        result = parse_ael_with_index("'text' > $.stringBin1", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_bin_gt_7(self):
        """\"text\" > $.stringBin1 — string comparison not supported by secondary index Filter."""
        result = parse_ael_with_index('"text" > $.stringBin1', _index_ctx())
        assert result.filter is None
        assert result.exp is not None


class TestBinFiltersGTLogicalCombinations:
    """AND with two indexed bins; no indexes."""

    def test_bin_gt_logical_combinations_1(self):
        """$.intBin1 > 100 and $.intBin2 < 1000 with indexes → Filter on intBin2 (higher cardinality), range(intBin2, MIN, 999)."""
        result = parse_ael_with_index(
            "$.intBin1 > 100 and $.intBin2 < 1000",
            _index_ctx_int_bins(),
        )
        _assert_range_filter(result, "intBin2", MIN, 999)
        assert result.exp is not None

    def test_bin_gt_logical_combinations_2(self):
        """$.intBin1 > 100 and $.intBin2 < 1000 with no indexes → filter None, full expression as exp."""
        result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 < 1000")
        assert result.filter is None
        assert result.exp is not None


class TestBinFiltersGE:
    """Filter generation for greater-than-or-equal."""

    def test_bin_ge_1(self):
        """$.intBin1 >= 100 → range(intBin1, 100, MAX)."""
        result = parse_ael_with_index("$.intBin1 >= 100", _index_ctx())
        _assert_range_filter(result, "intBin1", 100, MAX)
        assert result.exp is None

    def test_bin_ge_2(self):
        """100 <= $.intBin1 — same as $.intBin1 >= 100 → range(intBin1, 100, MAX)."""
        result = parse_ael_with_index("100 <= $.intBin1", _index_ctx())
        _assert_range_filter(result, "intBin1", 100, MAX)
        assert result.exp is None


class TestBinFiltersLT:
    """Filter generation for less-than."""

    def test_bin_lt_1(self):
        """$.intBin1 < 100 → range(intBin1, MIN, 99)."""
        result = parse_ael_with_index("$.intBin1 < 100", _index_ctx())
        _assert_range_filter(result, "intBin1", MIN, 99)
        assert result.exp is None

    def test_bin_lt_2(self):
        """100 > $.intBin1 — same as $.intBin1 < 100 → range(intBin1, MIN, 99)."""
        result = parse_ael_with_index("100 > $.intBin1", _index_ctx())
        _assert_range_filter(result, "intBin1", MIN, 99)
        assert result.exp is None


class TestBinFiltersLE:
    """Filter generation for less-than-or-equal."""

    def test_bin_le_1(self):
        """$.intBin1 <= 100 → range(intBin1, MIN, 100)."""
        result = parse_ael_with_index("$.intBin1 <= 100", _index_ctx())
        _assert_range_filter(result, "intBin1", MIN, 100)
        assert result.exp is None

    def test_bin_le_2(self):
        """100 >= $.intBin1 — same as $.intBin1 <= 100 → range(intBin1, MIN, 100)."""
        result = parse_ael_with_index("100 >= $.intBin1", _index_ctx())
        _assert_range_filter(result, "intBin1", MIN, 100)
        assert result.exp is None


class TestBinFiltersEQ:
    """Filter generation for equality."""

    def test_bin_eq_1(self):
        """$.intBin1 == 100 → equal(intBin1, 100)."""
        result = parse_ael_with_index("$.intBin1 == 100", _index_ctx())
        _assert_equal_filter(result, "intBin1", 100)
        assert result.exp is None

    def test_bin_eq_2(self):
        """100 == $.intBin1 — same as $.intBin1 == 100 → equal(intBin1, 100)."""
        result = parse_ael_with_index("100 == $.intBin1", _index_ctx())
        _assert_equal_filter(result, "intBin1", 100)
        assert result.exp is None

    def test_bin_eq_3(self):
        """$.stringBin1 == 'text' → equal(stringBin1, 'text')."""
        result = parse_ael_with_index("$.stringBin1 == 'text'", _index_ctx())
        _assert_equal_filter(result, "stringBin1", "text")
        assert result.exp is None

    def test_bin_eq_4(self):
        """$.stringBin1 == \"text\" → equal(stringBin1, 'text')."""
        result = parse_ael_with_index('$.stringBin1 == "text"', _index_ctx())
        _assert_equal_filter(result, "stringBin1", "text")
        assert result.exp is None


class TestBinFiltersNOTEQ:
    """NOT EQUAL is not supported by secondary index filter; filter is None."""

    def test_bin_noteq_1(self):
        """$.intBin1 != 100 — not supported by secondary index filter."""
        result = parse_ael_with_index("$.intBin1 != 100", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_bin_noteq_2(self):
        """$.stringBin1 != 'text' — not supported by secondary index filter."""
        result = parse_ael_with_index("$.stringBin1 != 'text'", _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_bin_noteq_3(self):
        """$.stringBin1 != \"text\" — not supported by secondary index filter."""
        result = parse_ael_with_index('$.stringBin1 != "text"', _index_ctx())
        assert result.filter is None
        assert result.exp is not None

    def test_bin_noteq_4(self):
        """100 != $.intBin1 — not supported by secondary index filter."""
        result = parse_ael_with_index("100 != $.intBin1", _index_ctx())
        assert result.filter is None

    def test_bin_noteq_5(self):
        """100 != 'text' — cross-type comparison rejected."""
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael_with_index("100 != 'text'", _index_ctx())

    def test_bin_noteq_6(self):
        """100 != "text" — cross-type comparison rejected."""
        with pytest.raises(AelParseException, match="Cannot compare"):
            parse_ael_with_index('100 != "text"', _index_ctx())
