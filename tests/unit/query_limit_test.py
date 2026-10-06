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

"""Unit tests for QueryBuilder.limit()."""

import pytest

from aerospike_sdk.aio.operations.query import QueryBuilder


def _query_builder():
    """Return a QueryBuilder with a fake client (no real connection)."""
    return QueryBuilder(client=object(), namespace="test", set_name="unit_test")


def test_limit_sets_policy_cap():
    builder = _query_builder().limit(100)
    assert builder._policy.max_records == 100


@pytest.mark.parametrize("value", [0, -1])
def test_limit_rejects_non_positive(value):
    """No limit is the default; there is no value that means "unlimited"."""
    with pytest.raises(ValueError, match="Limit must be > 0"):
        _query_builder().limit(value)


def test_max_records_is_not_a_builder_method():
    assert not hasattr(_query_builder(), "max_records")
