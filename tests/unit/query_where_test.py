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

"""Unit tests for QueryBuilder and SyncQueryBuilder where() overloads.

Tests the two forms: where(str) and where(FilterExpression).
"""

from aerospike_sdk import Exp, parse_ael
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.sync.operations.query import SyncQueryBuilder


def _query_builder():
    """Return a QueryBuilder with a fake client (no real connection)."""
    return QueryBuilder(client=object(), namespace="test", set_name="unit_test")


class TestQueryBuilderWhere:
    """Test QueryBuilder.where() overloads."""

    def test_where_ael_string_sets_filter_expression(self):
        """where(str) parses AEL and sets _filter_expression."""
        builder = _query_builder()
        expected = parse_ael("$.age > 20")
        result = builder.where("$.age > 20")
        assert result is builder
        assert builder._filter_expression == expected

    def test_where_ael_fstring_sets_filter_expression(self):
        """where(str) with f-string interpolation."""
        builder = _query_builder()
        age = 21
        expected = parse_ael("$.age > 21")
        result = builder.where(f"$.age > {age}")
        assert result is builder
        assert builder._filter_expression == expected

    def test_where_filter_expression_sets_filter_expression(self):
        """where(FilterExpression) stores the expression directly."""
        builder = _query_builder()
        exp = Exp.gt(Exp.int_bin("a"), Exp.int_val(100))
        result = builder.where(exp)
        assert result is builder
        assert builder._filter_expression is exp

    def test_where_filter_expression_chains(self):
        """where(Exp) can be chained with other builder methods."""
        builder = _query_builder()
        exp = Exp.eq(Exp.string_bin("name"), Exp.string_val("Bob"))
        builder.where(exp).bins(["name"])
        assert builder._filter_expression is exp
        assert builder._bins == ["name"]


class TestSyncQueryBuilderWhere:
    """Test SyncQueryBuilder.where() overloads (same behavior as QueryBuilder)."""

    def _sync_builder(self):
        """Return a SyncQueryBuilder with fake deps (no real connection)."""
        return SyncQueryBuilder(
            client=object(),
            namespace="test",
            set_name="unit_test",
        )

    def test_where_ael_string_sets_filter_expression(self):
        """where(str) parses AEL and sets _filter_expression on the delegate."""
        builder = self._sync_builder()
        expected = parse_ael("$.age > 20")
        result = builder.where("$.age > 20")
        assert result is builder
        assert builder._filter_expression == expected

    def test_where_filter_expression_sets_filter_expression(self):
        """where(FilterExpression) stores the expression directly."""
        builder = self._sync_builder()
        exp = Exp.gt(Exp.int_bin("a"), Exp.int_val(100))
        result = builder.where(exp)
        assert result is builder
        assert builder._filter_expression is exp
