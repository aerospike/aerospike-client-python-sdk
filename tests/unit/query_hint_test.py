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
from aerospike_async import Filter, QueryDuration

from aerospike_sdk import (
    Exp,
    Index,
    IndexContext,
    IndexTypeEnum,
    QueryHint,
    parse_ael_with_index,
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
        assert hint.bin_name is None
        assert hint.query_duration is None

    def test_bin_name_only(self):
        hint = QueryHint(bin_name="alt_bin")
        assert hint.bin_name == "alt_bin"
        assert hint.index_name is None

    def test_query_duration_only(self):
        hint = QueryHint(query_duration=QueryDuration.SHORT)
        assert hint.query_duration == QueryDuration.SHORT

    def test_index_name_with_query_duration(self):
        hint = QueryHint(index_name="idx", query_duration=QueryDuration.LONG)
        assert hint.index_name == "idx"
        assert hint.query_duration == QueryDuration.LONG

    def test_bin_name_with_query_duration(self):
        hint = QueryHint(bin_name="b", query_duration=QueryDuration.SHORT)
        assert hint.bin_name == "b"
        assert hint.query_duration == QueryDuration.SHORT

    def test_all_none_is_valid(self):
        hint = QueryHint()
        assert hint.index_name is None
        assert hint.bin_name is None
        assert hint.query_duration is None

    def test_index_name_and_bin_name_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            QueryHint(index_name="idx", bin_name="b")

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
        assert builder._filter_expression is not None

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

    def test_rebuild_with_bin_name(self):
        record = _FilterRecord(
            filter=Filter.equal("age", 30),
            method="equal",
            identifier="age",
            args=(30,),
        )
        hint = QueryHint(bin_name="alt_age")
        rebuilt = record.rebuild_for_hint(hint)
        expected = Filter.equal("alt_age", 30)
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


class TestFilterGenHintOverrides:
    """parse_ael_with_index with hint_index_name and hint_bin_name overrides."""

    def test_hint_index_name_produces_by_index_filter(self):
        ctx = IndexContext.of("test", [
            Index(bin="age", index_type=IndexTypeEnum.NUMERIC,
                  namespace="test", bin_values_ratio=1),
        ])
        result = parse_ael_with_index(
            "$.age == 30", ctx, hint_index_name="age_idx",
        )
        assert result.filter is not None
        expected = Filter.equal_by_index("age_idx", 30)
        assert str(result.filter) == str(expected)

    def test_hint_bin_name_overrides_filter_bin(self):
        ctx = IndexContext.of("test", [
            Index(bin="age", index_type=IndexTypeEnum.NUMERIC,
                  namespace="test", bin_values_ratio=1),
        ])
        result = parse_ael_with_index(
            "$.age > 10", ctx, hint_bin_name="alt_age",
        )
        assert result.filter is not None
        expected = Filter.range("alt_age", 11, 2**63 - 1)
        assert str(result.filter) == str(expected)

    def test_no_hint_uses_default_bin(self):
        ctx = IndexContext.of("test", [
            Index(bin="age", index_type=IndexTypeEnum.NUMERIC,
                  namespace="test", bin_values_ratio=1),
        ])
        result = parse_ael_with_index("$.age == 42", ctx)
        assert result.filter is not None
        expected = Filter.equal("age", 42)
        assert str(result.filter) == str(expected)

    def test_hint_index_name_range_ge(self):
        ctx = IndexContext.of("test", [
            Index(bin="score", index_type=IndexTypeEnum.NUMERIC,
                  namespace="test", bin_values_ratio=1),
        ])
        result = parse_ael_with_index(
            "$.score >= 50", ctx, hint_index_name="score_idx",
        )
        assert result.filter is not None
        expected = Filter.range_by_index("score_idx", 50, 2**63 - 1)
        assert str(result.filter) == str(expected)
