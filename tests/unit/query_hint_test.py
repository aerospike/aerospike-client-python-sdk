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
from aerospike_sdk import Filter, QueryDuration

from aerospike_sdk import (
    Exp,
    QueryHint,
)
from aerospike_sdk.aio.operations.query import QueryBuilder, _FilterRecord


def _query_builder():
    """Return a QueryBuilder with a fake client (no real connection)."""
    return QueryBuilder(client=object(), namespace="test", set_name="unit_test")


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
        assert hint.bin_name is None
        assert hint.query_duration is None

    def test_bin_name_only(self):
        hint = QueryHint(bin_name="alt_bin")
        assert hint.bin_name == "alt_bin"
        assert hint.index_name is None

    def test_bin_name_with_query_duration(self):
        hint = QueryHint(bin_name="b", query_duration=QueryDuration.SHORT)
        assert hint.bin_name == "b"
        assert hint.query_duration == QueryDuration.SHORT

    def test_index_name_and_bin_name_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            QueryHint(index_name="idx", bin_name="b")

    def test_hard_hint_without_index_name_raises(self):
        with pytest.raises(ValueError, match="hard_hint requires index_name"):
            QueryHint(hard_hint=True)

    def test_require_index_and_hard_hint_allowed(self):
        hint = QueryHint(
            index_name="age_idx",
            require_index=True,
            hard_hint=True,
        )
        assert hint.require_index is True
        assert hint.hard_hint is True

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

    def test_where_filter_expression_clears_ael_string(self):
        builder = _query_builder()
        builder.where("$.age > 30")
        builder.where(Exp.gt(Exp.int_bin("age"), Exp.int_val(30)))
        assert builder._where_ael is None


class TestFilterRecord:
    """_FilterRecord.rebuild_for_hint() reconstruction."""

    def test_rebuild_with_index_name(self):
        record = _FilterRecord(
            filter=Filter.equal("age", 30),
            method="equal",
            identifier="age",
            args=(30,),
        )
        hint = QueryHint(index_name="age_idx")
        rebuilt = record.rebuild_for_hint(hint)
        expected = Filter.equal_by_index("age_idx", 30)
        assert str(rebuilt) == str(expected)

    def test_rebuild_range_with_index_name(self):
        record = _FilterRecord(
            filter=Filter.range("score", 10, 100),
            method="range",
            identifier="score",
            args=(10, 100),
        )
        hint = QueryHint(index_name="score_idx")
        rebuilt = record.rebuild_for_hint(hint)
        expected = Filter.range_by_index("score_idx", 10, 100)
        assert str(rebuilt) == str(expected)

    def test_rebuild_no_hint_override_returns_original(self):
        orig = Filter.equal("age", 30)
        record = _FilterRecord(
            filter=orig,
            method="equal",
            identifier="age",
            args=(30,),
        )
        hint = QueryHint(query_duration=QueryDuration.SHORT)
        rebuilt = record.rebuild_for_hint(hint)
        assert rebuilt is orig

    def test_rebuild_without_metadata_raises(self):
        record = _FilterRecord(filter=Filter.equal("age", 30))
        hint = QueryHint(index_name="age_idx")
        with pytest.raises(ValueError, match="pre-built Filter"):
            record.rebuild_for_hint(hint)
