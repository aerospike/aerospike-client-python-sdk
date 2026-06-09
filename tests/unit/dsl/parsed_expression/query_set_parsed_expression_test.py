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

"""Filter-generation tests for set-aware secondary-index selection.

When :class:`IndexContext` is given a ``query_set``, indexes defined on a
different set must be excluded from filter selection. Indexes with no set
name (cross-set / null set) remain eligible regardless of ``query_set``.
"""

from aerospike_async import Filter
from aerospike_sdk import (
    Exp,
    Index,
    IndexContext,
    IndexTypeEnum,
    parse_ael_with_index,
)

NAMESPACE = "test"
QUERY_SET = "testScan"


def _idx(bin_name: str, *, set_name=None, ratio=0, name=None) -> Index:
    return Index(
        bin=bin_name,
        index_type=IndexTypeEnum.NUMERIC,
        namespace=NAMESPACE,
        bin_values_ratio=ratio,
        set_name=set_name,
        name=name,
    )


class TestQuerySetExcludesOtherSetIndexes:
    """Indexes defined on a different set must not be selected."""

    def test_eq_bin_indexed_only_on_other_set_no_si_filter(self):
        # intBin2 has an index on "orders"; query is on "customers".
        # The eq on intBin2 must NOT produce a SI filter.
        indexes = [
            _idx("intBin2", set_name="orders", ratio=1),
        ]
        ctx = IndexContext.with_query_set(NAMESPACE, "customers", indexes)
        result = parse_ael_with_index("$.intBin2 == 100", ctx)
        assert result.filter is None
        # The remaining Exp is the eq itself — no SI filter could absorb it.
        assert result.exp == Exp.eq(Exp.int_bin("intBin2"), Exp.int_val(100))

    def test_eq_bin_indexed_on_query_set_uses_si_filter(self):
        # Index on the query set — eq becomes the SI filter.
        indexes = [
            _idx("intBin2", set_name=QUERY_SET, ratio=1),
        ]
        ctx = IndexContext.with_query_set(NAMESPACE, QUERY_SET, indexes)
        result = parse_ael_with_index("$.intBin2 == 100", ctx)
        assert result.filter is not None
        assert str(result.filter) == str(Filter.equal("intBin2", 100))


class TestCardinalityRespectsQuerySet:
    """The most-selective index *within* the matching set wins, not globally."""

    def test_without_query_set_picks_highest_cardinality_globally(self):
        catalog = [
            _idx("intBin1", set_name="other", ratio=200, name="idx_bin1"),
            _idx("intBin2", set_name=QUERY_SET, ratio=100, name="idx_bin2"),
            _idx("intBin3", set_name=QUERY_SET, ratio=100, name="idx_bin3"),
        ]
        ctx = IndexContext.of(NAMESPACE, catalog)  # no query_set: any wins
        result = parse_ael_with_index(
            "$.intBin1 > 50 and $.intBin2 > 50 and $.intBin3 > 50", ctx,
        )
        assert result.filter is not None
        assert str(result.filter) == str(Filter.range("intBin1", 51, 2**63 - 1))

    def test_with_query_set_picks_highest_cardinality_within_set(self):
        catalog = [
            _idx("intBin1", set_name="other", ratio=200, name="idx_bin1"),
            _idx("intBin2", set_name=QUERY_SET, ratio=100, name="idx_bin2"),
            _idx("intBin3", set_name=QUERY_SET, ratio=100, name="idx_bin3"),
        ]
        ctx = IndexContext.with_query_set(NAMESPACE, QUERY_SET, catalog)
        result = parse_ael_with_index(
            "$.intBin1 > 50 and $.intBin2 > 50 and $.intBin3 > 50", ctx,
        )
        # intBin2 wins ties alphabetically among matching-set indexes.
        assert result.filter is not None
        assert str(result.filter) == str(Filter.range("intBin2", 51, 2**63 - 1))


class TestQuerySetWithIndexNameHint:
    """``hint_index_name`` resolves only among indexes on the matching set."""

    def test_index_name_hint_picks_matching_set_index(self):
        # Two indexes named differently on bin 'age': one on 'set' (ageidx),
        # one on the query set (age_idx). The hint must pick the latter.
        catalog = [
            _idx("age", set_name="set", ratio=50, name="ageidx"),
            _idx("age", set_name=QUERY_SET, ratio=10, name="age_idx"),
            _idx("score", set_name=QUERY_SET, ratio=5, name="score_idx"),
        ]
        ctx = IndexContext.with_query_set(NAMESPACE, QUERY_SET, catalog)
        result = parse_ael_with_index(
            "$.age > 18 and $.score > 0 and $.flag == 1",
            ctx,
            hint_index_name="age_idx",
        )
        assert result.filter is not None
        # Filter targets bin 'age' via the named index lookup.
        assert "age" in str(result.filter)
        expected_exp = Exp.and_([
            Exp.gt(Exp.int_bin("score"), Exp.int_val(0)),
            Exp.eq(Exp.int_bin("flag"), Exp.int_val(1)),
        ])
        assert result.exp == expected_exp


class TestNullSetIndexIsCrossSet:
    """An index with no ``set_name`` is eligible regardless of ``query_set``."""

    def test_cross_set_index_used_under_any_query_set(self):
        # No set_name on the only index. Any query_set should still see it.
        indexes = [_idx("intBin1", set_name=None, ratio=10)]
        ctx = IndexContext.with_query_set(NAMESPACE, QUERY_SET, indexes)
        result = parse_ael_with_index("$.intBin1 == 5", ctx)
        assert result.filter is not None
        assert str(result.filter) == str(Filter.equal("intBin1", 5))

    def test_blank_query_set_disables_filtering(self):
        # An empty/blank query_set must normalize to None — no filtering.
        indexes = [_idx("intBin1", set_name="other", ratio=10)]
        ctx = IndexContext.with_query_set(NAMESPACE, "  ", indexes)
        assert ctx.query_set is None
        result = parse_ael_with_index("$.intBin1 == 5", ctx)
        assert result.filter is not None
        assert str(result.filter) == str(Filter.equal("intBin1", 5))
