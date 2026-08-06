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

"""Unit tests for logical parsed expressions (filter + exp from AEL)."""

from aerospike_sdk import (
    Exp,
    Index,
    IndexContext,
    IndexTypeEnum,
    parse_ael_with_index,
)


class TestFilterGeneration:
    """Test filter generation from AEL."""

    NAMESPACE = "test"

    def test_and_no_indexes(self):
        """Test AND expression with no indexes returns only Exp."""
        result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 > 100")

        assert result.filter is None
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_and_all_indexes(self):
        """Test AND with all bins indexed - highest cardinality becomes Filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 > 100", ctx)

        assert result.filter is not None
        expected_exp = Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100))
        assert result.exp == expected_exp

    def test_and_one_index(self):
        """Test AND with only one bin indexed - that one becomes Filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 > 100", ctx)

        assert result.filter is not None
        expected_exp = Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100))
        assert result.exp == expected_exp

    def test_and_and_no_indexes(self):
        """Test triple AND with no indexes returns only Exp."""
        result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100")

        assert result.filter is None
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_and_and_all_indexes(self):
        """Test triple AND - highest cardinality becomes Filter, rest becomes Exp."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", ctx
        )

        assert result.filter is not None
        expected_exp = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        assert result.exp == expected_exp

    def test_or_no_indexes(self):
        """Test OR expression with no indexes - cannot use Filter."""
        result = parse_ael_with_index("$.intBin1 > 100 or $.intBin2 > 100")

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_or_all_indexes(self):
        """Test OR with all bins indexed - still cannot use Filter (OR semantics)."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 or $.intBin2 > 100", ctx)

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_simple_gt_with_index(self):
        """Test simple > comparison with matching index."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100", ctx)

        assert result.filter is not None
        assert result.exp is None

    def test_simple_ge_with_index(self):
        """Test simple >= comparison with matching index."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 >= 100", ctx)

        assert result.filter is not None
        assert result.exp is None

    def test_simple_lt_with_index(self):
        """Test simple < comparison with matching index."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 < 100", ctx)

        assert result.filter is not None
        assert result.exp is None

    def test_simple_le_with_index(self):
        """Test simple <= comparison with matching index."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 <= 100", ctx)

        assert result.filter is not None
        assert result.exp is None

    def test_simple_eq_with_index(self):
        """Test simple == comparison with matching index."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 == 100", ctx)

        assert result.filter is not None
        assert result.exp is None

    def test_string_eq_with_index(self):
        """Test string == comparison with STRING index."""
        indexes = [
            Index(bin="stringBin1", index_type=IndexTypeEnum.STRING, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index('$.stringBin1 == "text"', ctx)

        assert result.filter is not None
        assert result.exp is None

    def test_string_gt_no_filter(self):
        """Test string > comparison - not supported by secondary index."""
        indexes = [
            Index(bin="stringBin1", index_type=IndexTypeEnum.STRING, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index('$.stringBin1 > "text"', ctx)

        assert result.filter is None
        assert result.exp is not None

    def test_no_matching_index_type(self):
        """Test when index type doesn't match expression type."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.STRING, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100", ctx)

        assert result.filter is None
        assert result.exp is not None

    def test_namespace_filtering(self):
        """Test that indexes with wrong namespace are ignored."""
        indexes = [
            Index(
                bin="intBin1",
                index_type=IndexTypeEnum.NUMERIC,
                namespace="other_ns",
                bin_values_ratio=1,
            ),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100", ctx)

        assert result.filter is None
        assert result.exp is not None

    def test_same_cardinality_alphabetical(self):
        """Test that same cardinality chooses alphabetically."""
        indexes = [
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 > 100", ctx)

        assert result.filter is not None
        expected_exp = Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100))
        assert result.exp == expected_exp

    def test_and_all_indexes_no_cardinality(self):
        """Alphabetical selection when no cardinality."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 > 100", ctx)

        assert result.filter is not None
        expected_exp = Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100))
        assert result.exp == expected_exp

    def test_and_or_or_no_indexes(self):
        """(a AND b) OR c OR d - top level OR."""
        result = parse_ael_with_index(
            "$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 > 100 or $.intBin4 > 100"
        )

        assert result.filter is None
        expected = Exp.or_([
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            ]),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_or_and_and_no_indexes(self):
        """a OR (b AND c AND d)."""
        result = parse_ael_with_index(
            "$.intBin1 > 100 or $.intBin2 > 100 and $.intBin3 > 100 and $.intBin4 > 100"
        )

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_and_or_indexed(self):
        """(a AND b) OR c, all indexed, no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 > 100", ctx)

        assert result.filter is None
        expected = Exp.or_([
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            ]),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_and_prioritized_or_indexed(self):
        """a AND (b OR c), a can be filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)", ctx
        )

        assert result.filter is not None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_or_prioritized_or_indexed(self):
        """a OR (b OR c), no filter possible."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100)", ctx
        )

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_or_prioritized_and_indexed(self):
        """a OR (b AND c), no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100)", ctx
        )

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_and_or_prioritized_and_indexed(self):
        """(a AND b) OR (c AND d) - top level OR."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 and $.intBin4 > 100) or ($.intBin2 > 100 and $.intBin1 > 100)", ctx
        )

        assert result.filter is None
        expected = Exp.or_([
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
            ]),
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_or_and_prioritized_or_indexed(self):
        """(a OR b) AND (c OR d) - both OR children excluded."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 or $.intBin1 > 100)", ctx
        )

        assert result.filter is None
        expected = Exp.and_([
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
            ]),
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_or_and_prioritized_and_indexed(self):
        """(a OR b) AND (c AND d) - intBin2 has highest cardinality."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 and $.intBin1 > 100)", ctx
        )

        assert result.filter is not None
        expected = Exp.and_([
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
            ]),
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_or_one_index(self):
        """OR with one indexed, still no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 or $.intBin2 > 100", ctx)

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_or_or_all_indexes(self):
        """Triple OR, no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100", ctx)

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_and_and_two_indexes(self):
        """Triple AND, two indexed."""
        indexes = [
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", ctx
        )

        assert result.filter is not None
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_and_and_all_indexes_same_cardinality(self):
        """Same cardinality chooses alphabetically."""
        indexes = [
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=100),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=100),
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=100),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", ctx
        )

        assert result.filter is not None
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_and_and_all_indexes_no_cardinality(self):
        """No cardinality - alphabetically intBin1."""
        indexes = [
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", ctx
        )

        assert result.filter is not None
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_and_prioritized_or_indexed_same_cardinality(self):
        """intBin3 AND (intBin2 OR intBin1) - only intBin3 filterable."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)", ctx
        )

        assert result.filter is not None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_parenthesized_expression(self):
        """Test that parenthesized expressions parse correctly."""
        indexes = [
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result1 = parse_ael_with_index(
            "$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)", ctx
        )
        result2 = parse_ael_with_index(
            "($.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100))", ctx
        )

        assert result1.filter is not None
        assert result2.filter is not None
        assert result1.exp == result2.exp

    def test_and_and_all_indexes_partial_data(self):
        """Tests partial index data - missing namespace won't match."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.STRING, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.STRING, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 > 100", ctx
        )

        assert result.filter is not None
        expected = Exp.and_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_or_or_all_indexes_same_cardinality(self):
        """OR cannot use filter even with same cardinality."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index("$.intBin1 > 100 or $.intBin2 > 100 or $.intBin3 > 100", ctx)

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_and_prioritized_or_indexed_no_cardinality(self):
        """intBin3 is the only filterable one."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin3 > 100 and ($.intBin2 > 100 or $.intBin1 > 100)", ctx
        )

        assert result.filter is not None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_or_prioritized_or_indexed_same_cardinality(self):
        """OR with nested OR - no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin3 > 100 or ($.intBin2 > 100 or $.intBin1 > 100)", ctx
        )

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_or_prioritized_and_indexed_same_cardinality(self):
        """OR with nested AND - no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "$.intBin3 > 100 or ($.intBin2 > 100 and $.intBin1 > 100)", ctx
        )

        assert result.filter is None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_and_or_prioritized_and_indexed_same_cardinality(self):
        """(AND) OR (AND) - top level OR, no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 and $.intBin4 > 100) or ($.intBin2 > 100 and $.intBin1 > 100)", ctx
        )

        assert result.filter is None
        expected = Exp.or_([
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
            ]),
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_or_and_prioritized_or_indexed_same_cardinality(self):
        """(OR) AND (OR) - both excluded, no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 or $.intBin1 > 100)", ctx
        )

        assert result.filter is None
        expected = Exp.and_([
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
            ]),
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_or_and_prioritized_and_indexed_same_cardinality(self):
        """(OR) AND (AND) - intBin1 filterable."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 or $.intBin4 > 100) and ($.intBin2 > 100 and $.intBin1 > 100)", ctx
        )

        assert result.filter is not None
        expected = Exp.and_([
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
            ]),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_prioritized_or_prioritized_and_and_indexed_with_filter(self):
        """(a OR b AND c) AND d - intBin1 has highest cardinality."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100", ctx
        )

        assert result.filter is not None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_or_prioritized_and_and_indexed_with_only_filter(self):
        """intBin2 has highest cardinality but is under OR, intBin1 is the only filterable."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100", ctx
        )

        assert result.filter is not None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_or_prioritized_and_and_indexed_same_cardinality(self):
        """intBin1 is the only filterable (others under OR)."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "($.intBin3 > 100 or $.intBin4 > 100 and $.intBin2 > 100) and $.intBin1 > 100", ctx
        )

        assert result.filter is not None
        expected = Exp.or_([
            Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
            Exp.and_([
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
            ]),
        ])
        assert result.exp == expected

    def test_prioritized_or_prioritized_and_and_indexed_with_filter_per_cardinality(self):
        """intBin2 has highest cardinality AND is filterable."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=1),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE, bin_values_ratio=0),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        result = parse_ael_with_index(
            "(($.intBin3 > 100 or $.intBin4 > 100) and $.intBin2 > 100) and $.intBin1 > 100", ctx
        )

        assert result.filter is not None
        expected = Exp.and_([
            Exp.or_([
                Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
            ]),
            Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_complex_6_bin_expression_with_filter(self):
        """Complex 6-bin expression - intBin1 filterable."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin5", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin6", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        ael = (
            "(($.intBin3 > 100 or $.intBin4 > 100) or ($.intBin5 > 100 and $.intBin6 > 100)) "
            "and ($.intBin2 > 100 and $.intBin1 > 100)"
        )
        result = parse_ael_with_index(ael, ctx)

        assert result.filter is not None
        expected = Exp.and_([
            Exp.or_([
                Exp.or_([
                    Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                    Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
                ]),
                Exp.and_([
                    Exp.gt(Exp.int_bin("intBin5"), Exp.int_val(100)),
                    Exp.gt(Exp.int_bin("intBin6"), Exp.int_val(100)),
                ]),
            ]),
            Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
        ])
        assert result.exp == expected

    def test_complex_8_bin_expression_no_filter(self):
        """Complex 8-bin expression - all parts in OR, no filter."""
        indexes = [
            Index(bin="intBin1", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin2", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin3", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin4", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin5", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin6", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin7", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
            Index(bin="intBin8", index_type=IndexTypeEnum.NUMERIC, namespace=self.NAMESPACE),
        ]
        ctx = IndexContext.of(self.NAMESPACE, indexes)

        ael = (
            "(($.intBin3 > 100 or $.intBin4 > 100) or ($.intBin5 > 100 and $.intBin6 > 100)) "
            "and (($.intBin2 > 100 and $.intBin1 > 100) or ($.intBin7 > 100 and $.intBin8 > 100))"
        )
        result = parse_ael_with_index(ael, ctx)

        assert result.filter is None
        expected = Exp.and_([
            Exp.or_([
                Exp.or_([
                    Exp.gt(Exp.int_bin("intBin3"), Exp.int_val(100)),
                    Exp.gt(Exp.int_bin("intBin4"), Exp.int_val(100)),
                ]),
                Exp.and_([
                    Exp.gt(Exp.int_bin("intBin5"), Exp.int_val(100)),
                    Exp.gt(Exp.int_bin("intBin6"), Exp.int_val(100)),
                ]),
            ]),
            Exp.or_([
                Exp.and_([
                    Exp.gt(Exp.int_bin("intBin2"), Exp.int_val(100)),
                    Exp.gt(Exp.int_bin("intBin1"), Exp.int_val(100)),
                ]),
                Exp.and_([
                    Exp.gt(Exp.int_bin("intBin7"), Exp.int_val(100)),
                    Exp.gt(Exp.int_bin("intBin8"), Exp.int_val(100)),
                ]),
            ]),
        ])
        assert result.exp == expected

    def test_exclusive_no_indexes(self):
        """exclusive() operator with no indexes."""
        result = parse_ael_with_index('exclusive($.hand == "stand", $.pun == "done")')

        assert result.filter is None
        expected = Exp.xor([
            Exp.eq(Exp.string_bin("hand"), Exp.string_val("stand")),
            Exp.eq(Exp.string_bin("pun"), Exp.string_val("done")),
        ])
        assert result.exp == expected
