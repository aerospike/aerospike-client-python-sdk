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

"""Unit tests for the ``set_name`` / ``query_set`` plumbing on Index and
IndexContext, plus the ``IndexTypeEnum.INTEGER`` ↔ ``NUMERIC`` equivalence.

These cover the type-level behavior — normalization, factory contracts, and
the :meth:`IndexContext.index_matches_query_set` predicate. Filter-generation
end-to-end tests live in :mod:`query_set_parsed_expression_test`.
"""

import pytest

from aerospike_async import IndexType as PacIndexType
from aerospike_sdk import Index, IndexContext, IndexTypeEnum

NAMESPACE = "test"


def _idx(*, set_name=None, name=None, ratio=0, index_type=IndexTypeEnum.NUMERIC) -> Index:
    return Index(
        bin="age",
        index_type=index_type,
        namespace=NAMESPACE,
        bin_values_ratio=ratio,
        set_name=set_name,
        name=name,
    )


class TestIndexSetName:
    """:attr:`Index.set_name` is optional and normalized."""

    def test_default_set_name_is_none(self):
        assert _idx().set_name is None

    def test_set_name_preserved(self):
        assert _idx(set_name="mySet").set_name == "mySet"

    @pytest.mark.parametrize("blank", ["", "   ", "\t", "\n"])
    def test_blank_set_name_normalized_to_none(self, blank):
        assert _idx(set_name=blank).set_name is None


class TestIndexContextQuerySet:
    """:attr:`IndexContext.query_set` is optional and normalized."""

    def test_of_does_not_set_query_set(self):
        ctx = IndexContext.of(NAMESPACE, [_idx(set_name="mySet")])
        assert ctx.query_set is None

    def test_with_query_set_stores_value(self):
        ctx = IndexContext.with_query_set(NAMESPACE, "scan", [_idx(set_name="scan")])
        assert ctx.query_set == "scan"

    @pytest.mark.parametrize("blank", [None, "", "  ", "\n"])
    def test_blank_query_set_normalized_to_none(self, blank):
        ctx = IndexContext.with_query_set(NAMESPACE, blank, [_idx()])
        assert ctx.query_set is None


class TestIndexMatchesQuerySet:
    """The ``index_matches_query_set`` predicate covers cross-set semantics."""

    def test_no_query_set_matches_any_index(self):
        # query_set=None → all indexes are eligible regardless of set_name
        for set_name in [None, "", "anySet"]:
            assert IndexContext.index_matches_query_set(_idx(set_name=set_name), None) is True

    def test_no_index_set_is_cross_set(self):
        # An index with no set_name remains eligible under any query_set
        assert IndexContext.index_matches_query_set(_idx(set_name=None), "anySet") is True

    def test_matching_set_eligible(self):
        assert IndexContext.index_matches_query_set(_idx(set_name="scan"), "scan") is True

    def test_mismatched_set_excluded(self):
        assert IndexContext.index_matches_query_set(_idx(set_name="other"), "scan") is False


class TestIntegerNumericEquivalence:
    """``INTEGER`` is the modern preferred name (server 8.1.2+); ``NUMERIC``
    is retained as a back-compat alias. Both lower to PAC ``IndexType.NUMERIC``
    and are interchangeable for filter selection."""

    def test_integer_normalizes_to_numeric_internally(self):
        # Index stores NUMERIC after construction so the existing equality-
        # based filter selection loop doesn't need a special predicate.
        idx = _idx(index_type=IndexTypeEnum.INTEGER)
        assert idx.index_type == IndexTypeEnum.NUMERIC

    def test_numeric_unchanged_after_construction(self):
        idx = _idx(index_type=IndexTypeEnum.NUMERIC)
        assert idx.index_type == IndexTypeEnum.NUMERIC

    @pytest.mark.parametrize("variant", [IndexTypeEnum.INTEGER, IndexTypeEnum.NUMERIC])
    def test_both_lower_to_pac_numeric(self, variant):
        assert variant.to_aerospike() == PacIndexType.NUMERIC
