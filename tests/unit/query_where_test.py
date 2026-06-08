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

import pytest
from aerospike_async import FilterExpression

from aerospike_sdk import Exp, parse_ael
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.pac_sdk_client_attr import PAC_CLIENT_ATTR_SDK_SUPPORTS_SERVER_COMPILED_AEL
from aerospike_sdk.sync.operations.query import SyncQueryBuilder

from tests.pac_compat import skip_if_pac_lacks_from_server_compiled_ael


def _query_builder(**kwargs):
    """Return a QueryBuilder with a fake client (no real connection)."""
    client = kwargs.pop("client", None)
    if kwargs.pop("supports_server_compiled_ael", False):
        if client is None:
            client = object()
        setattr(client, PAC_CLIENT_ATTR_SDK_SUPPORTS_SERVER_COMPILED_AEL, True)
    elif client is None:
        client = object()
    return QueryBuilder(
        client=client,
        namespace="test",
        set_name="unit_test",
        **kwargs,
    )


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

    def test_where_server_compiled_when_supported(self) -> None:
        """where(str) uses server-compiled path when builder flag is set."""
        skip_if_pac_lacks_from_server_compiled_ael()
        builder = _query_builder(supports_server_compiled_ael=True)
        expected_parse = parse_ael("$.age > 20")
        builder.where("$.age > 20")
        assert builder._filter_expression != expected_parse


class TestSyncQueryBuilderWhere:
    """Test SyncQueryBuilder.where() overloads (same behavior as QueryBuilder)."""

    def _sync_builder(self):
        """Return a SyncQueryBuilder with fake deps (no real connection)."""
        return SyncQueryBuilder(
            async_client=object(),
            namespace="test",
            set_name="unit_test",
            loop_manager=object(),
        )

    def test_where_ael_string_sets_filter_expression(self):
        """where(str) parses AEL and sets _filter_expression on the delegate."""
        builder = self._sync_builder()
        expected = parse_ael("$.age > 20")
        result = builder.where("$.age > 20")
        assert result is builder
        assert builder._qb._filter_expression == expected

    def test_where_filter_expression_sets_filter_expression(self):
        """where(FilterExpression) stores the expression directly."""
        builder = self._sync_builder()
        exp = Exp.gt(Exp.int_bin("a"), Exp.int_val(100))
        result = builder.where(exp)
        assert result is builder
        assert builder._qb._filter_expression is exp
