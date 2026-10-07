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

"""Unit tests for QueryHint dataclass and QueryBuilder.with_hint()."""

import pytest
from aerospike_sdk import Behavior, Filter, QueryDuration

from aerospike_sdk import QueryHint
from aerospike_sdk.aio.operations.query import QueryBuilder


def _query_builder():
    """Return a QueryBuilder with a fake client (no real connection)."""
    return QueryBuilder(client=object(), namespace="test", set_name="unit_test", behavior=Behavior.DEFAULT)


class TestQueryHintValidation:
    """QueryHint.__post_init__ mutual-exclusivity validation."""

    def test_index_name_only(self):
        hint = QueryHint(index_name="my_idx")
        assert hint.index_name == "my_idx"
        assert hint.query_duration is None

    def test_query_duration_only(self):
        hint = QueryHint(query_duration=QueryDuration.SHORT)
        assert hint.query_duration == QueryDuration.SHORT

    def test_index_name_with_query_duration(self):
        hint = QueryHint(index_name="idx", query_duration=QueryDuration.LONG)
        assert hint.index_name == "idx"
        assert hint.query_duration == QueryDuration.LONG

    def test_all_none_is_valid(self):
        hint = QueryHint()
        assert hint.index_name is None
        assert hint.query_duration is None

    def test_bin_name_is_not_accepted(self):
        """It never selected an index: its only effect was skipping the
        planner, and with it the scan policy."""
        with pytest.raises(TypeError, match="bin_name"):
            QueryHint(bin_name="age")

    def test_fields_are_keyword_only(self):
        """Keyword-only fields can be added later without shifting positions."""
        with pytest.raises(TypeError):
            QueryHint("age_idx")

    def test_hard_hint_without_index_name_raises(self):
        with pytest.raises(ValueError, match="hard_hint requires index_name"):
            QueryHint(hard_hint=True)

    def test_allow_scans_with_where_and_hard_hint_allowed(self):
        hint = QueryHint(
            index_name="age_idx",
            allow_scans_with_where=False,
            hard_hint=True,
        )
        assert hint.allow_scans_with_where is False
        assert hint.hard_hint is True

    def test_allow_scans_with_where_defaults_to_none(self):
        # Tri-state: unset inherits the Behavior setting, not a bare bool.
        assert QueryHint().allow_scans_with_where is None
        assert QueryHint(allow_scans_with_where=False).allow_scans_with_where is False

    def test_frozen(self):
        hint = QueryHint(index_name="idx")
        # `setattr` instead of direct `hint.index_name = ...` to bypass static
        # analyzers (PyCharm `PyDataclass`, mypy `misc`) that flag the
        # intentional frozen-dataclass mutation. Runtime behavior is
        # identical: any attribute assignment raises `FrozenInstanceError`
        # (which is an `AttributeError` subclass).
        with pytest.raises(AttributeError):
            setattr(hint, "index_name", "other")


class TestWithHint:
    """QueryBuilder.with_hint() storage and validation."""

    def test_stores_hint(self):
        builder = _query_builder()
        hint = QueryHint(query_duration=QueryDuration.SHORT)
        result = builder.with_hint(hint)
        assert result is builder
        assert builder._query_hint is hint

    def test_double_call_raises(self):
        builder = _query_builder()
        builder.with_hint(QueryHint(query_duration=QueryDuration.SHORT))
        with pytest.raises(ValueError, match="once per query"):
            builder.with_hint(QueryHint(query_duration=QueryDuration.LONG))

    def test_chains_with_where(self):
        builder = _query_builder()
        result = (
            builder
            .where("$.age > 30")
            .with_hint(QueryHint(query_duration=QueryDuration.SHORT))
        )
        assert result is builder
        assert builder._query_hint is not None
        assert builder._where_ael == "$.age > 30"
        assert builder._filter_expression is None

    def test_where_stores_ael_string(self):
        builder = _query_builder()
        builder.where("$.age > 30")
        assert builder._where_ael == "$.age > 30"


class TestExplicitFilter:
    """QueryBuilder.filter() attaches one authoritative secondary-index filter."""

    def test_filter_is_sent_unchanged(self):
        chosen = Filter.equal("age", 30)
        statement = _query_builder().filter(chosen)._build_statement()
        assert len(statement.filters) == 1
        assert str(statement.filters[0]) == str(chosen)

    def test_second_filter_raises(self):
        builder = _query_builder().filter(Filter.equal("age", 30))
        with pytest.raises(ValueError, match="filter\\(\\) can only be called once"):
            builder.filter(Filter.equal("age", 31))

    def test_index_name_hint_does_not_rewrite_explicit_filter(self):
        chosen = Filter.equal("age", 30)
        statement = (
            _query_builder()
            .filter(chosen)
            .with_hint(QueryHint(index_name="other_idx"))
            ._build_statement()
        )
        assert str(statement.filters[0]) == str(chosen)

    def test_duration_hint_leaves_explicit_filter_alone(self):
        chosen = Filter.range_by_index("score_idx", 10, 100)
        statement = (
            _query_builder()
            .filter(chosen)
            .with_hint(QueryHint(query_duration=QueryDuration.SHORT))
            ._build_statement()
        )
        assert str(statement.filters[0]) == str(chosen)

    def test_no_filter_leaves_statement_unfiltered(self):
        assert _query_builder()._build_statement().filters is None
