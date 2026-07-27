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

"""Unit tests for list expressions in filter context."""

import pytest

from aerospike_sdk import CollectionIndexType, CTX, Exp, Filter, Index, IndexContext, IndexTypeEnum, parse_ael_with_index

NAMESPACE = "test1"

INDEXES = [
    Index(bin="listBin1", index_type=IndexTypeEnum.NUMERIC, namespace=NAMESPACE),
    Index(
        bin="listBin1",
        index_type=IndexTypeEnum.STRING,
        namespace=NAMESPACE,
        collection_index_type=CollectionIndexType.LIST,
    ),
    Index(
        bin="listBin1",
        index_type=IndexTypeEnum.STRING,
        namespace=NAMESPACE,
        collection_index_type=CollectionIndexType.LIST,
        ctx=[CTX.list_index(5)],
    ),
    Index(
        bin="listBin1",
        index_type=IndexTypeEnum.STRING,
        namespace=NAMESPACE,
        collection_index_type=CollectionIndexType.LIST,
        ctx=[CTX.list_value(5)],
    ),
    Index(
        bin="listBin1",
        index_type=IndexTypeEnum.STRING,
        namespace=NAMESPACE,
        collection_index_type=CollectionIndexType.LIST,
        ctx=[CTX.list_index(5), CTX.list_index(1)],
    ),
    Index(
        bin="listBin1",
        index_type=IndexTypeEnum.STRING,
        namespace=NAMESPACE,
        collection_index_type=CollectionIndexType.LIST,
        ctx=[CTX.list_value(5), CTX.list_rank(10)],
    ),
]
INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES)


def _assert_equal_filter(result, bin_name: str, value) -> None:
    """Assert result.filter equals Filter.equal(bin_name, value) by value."""
    assert result.filter is not None
    expected = Filter.equal(bin_name, value)
    assert str(result.filter) == str(expected), f"{result.filter!r} != {expected!r}"


def _assert_equal_filter_with_ctx(result, bin_name: str, value, ctx_list: list) -> None:
    """Assert result.filter equals Filter.equal(bin_name, value).context(ctx_list)."""
    expected = Filter.equal(bin_name, value).context(ctx_list)
    assert result.filter is not None
    assert str(result.filter) == str(expected), f"{result.filter!r} != {expected!r}"


class TestListExpressionsFilters:
    """Test filter generation for list expressions."""

    def test_list_expression_no_filter(self):
        """List CDT expressions do not produce secondary index Filters."""
        result = parse_ael_with_index("$.listBin.[0] == 100")
        assert result.filter is None
        assert result.exp is not None

    def test_list_expression(self):
        """$.listBin1 == \"stringVal\" and .get(type: STRING) variants → equal(listBin1, 'stringVal')."""
        result = parse_ael_with_index('$.listBin1 == "stringVal"', INDEX_FILTER_INPUT)
        _assert_equal_filter(result, "listBin1", "stringVal")
        result = parse_ael_with_index(
            '$.listBin1.get(type: STRING) == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter(result, "listBin1", "stringVal")
        result = parse_ael_with_index(
            '$.listBin1.get(type: STRING, return: VALUE) == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter(result, "listBin1", "stringVal")

    def test_list_expression_nested_one_level(self):
        """$.listBin1.[5] == \"stringVal\" and .get(type: STRING) variants → equal(listBin1, 'stringVal', list_index(5))."""
        expected_ctx = [CTX.list_index(5)]
        result = parse_ael_with_index(
            '$.listBin1.[5] == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter_with_ctx(result, "listBin1", "stringVal", expected_ctx)
        result = parse_ael_with_index(
            '$.listBin1.[5].get(type: STRING) == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter_with_ctx(result, "listBin1", "stringVal", expected_ctx)
        result = parse_ael_with_index(
            '$.listBin1.[5].get(type: STRING, return: VALUE) == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter_with_ctx(result, "listBin1", "stringVal", expected_ctx)

    def test_list_expression_nested_two_levels(self):
        """$.listBin1.[5].[1] == \"stringVal\" and [=5].[#10] variants → equal with two-level ctx."""
        expected_ctx_5_1 = [CTX.list_index(5), CTX.list_index(1)]
        result = parse_ael_with_index(
            '$.listBin1.[5].[1] == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter_with_ctx(result, "listBin1", "stringVal", expected_ctx_5_1)
        result = parse_ael_with_index(
            '$.listBin1.[5].[1].get(type: STRING) == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter_with_ctx(result, "listBin1", "stringVal", expected_ctx_5_1)
        result = parse_ael_with_index(
            '$.listBin1.[5].[1].get(type: STRING, return: VALUE) == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter_with_ctx(result, "listBin1", "stringVal", expected_ctx_5_1)

        expected_ctx_value_rank = [CTX.list_value(5), CTX.list_rank(10)]
        result = parse_ael_with_index(
            '$.listBin1.[=5].[#10] == "stringVal"',
            INDEX_FILTER_INPUT,
        )
        _assert_equal_filter_with_ctx(result, "listBin1", "stringVal", expected_ctx_value_rank)
