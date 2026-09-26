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

"""Unit tests for tabular writes: ``session.upsert(dataset).bins(...).row(...)``.

Each row must become one write spec on the shared query builder, carrying the
op type, the dataset-scoped key, one put per bin, and any per-row TTL or
generation, so the batch it produces is indistinguishable from a hand-chained
``upsert(key).put(...)`` sequence.
"""

from datetime import timedelta

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.aio.operations.query import (
    DataSetWriteBuilder,
    QueryBuilder,
    RowWriteBuilder,
)

USERS = DataSet.of("test", "unit_rows")


def _builder(op_type: str = "upsert") -> DataSetWriteBuilder:
    qb = QueryBuilder(client=object(), namespace="test", set_name="unit_rows")
    return DataSetWriteBuilder(qb, op_type, USERS)


def _specs(rows: RowWriteBuilder):
    rows._qb._finalize_current_spec()
    return rows._qb._specs


class TestRows:
    def test_bins_opens_the_row_builder(self):
        assert isinstance(_builder().bins("name", "age"), RowWriteBuilder)

    def test_each_row_is_one_spec_with_one_put_per_bin(self):
        rows = _builder().bins("name", "age").row(1, "Tim", 312).row(2, "Bob", 25)
        specs = _specs(rows)
        assert [s.keys for s in specs] == [[USERS.id(1)], [USERS.id(2)]]
        assert all(s.op_type == "upsert" for s in specs)
        assert all(len(s.operations) == 2 for s in specs)

    def test_rows_accepts_any_iterable(self):
        people = ((i, f"user{i}", 20 + i) for i in range(4))
        specs = _specs(_builder().bins("name", "age").rows(people))
        assert [s.keys[0].value for s in specs] == [0, 1, 2, 3]

    def test_ids_may_be_strings_or_bytes(self):
        specs = _specs(_builder().bins("v").row("alpha", 1).row(b"\x01", 2))
        assert specs[0].keys[0].value == "alpha"
        assert specs[1].keys[0].value == b"\x01"

    @pytest.mark.parametrize("op_type", ["insert", "update", "replace"])
    def test_verb_carries_through(self, op_type):
        specs = _specs(_builder(op_type).bins("v").row(1, 1))
        assert specs[0].op_type == op_type

    def test_value_count_must_match_the_bins(self):
        with pytest.raises(ValueError, match="2 values for 3 bins"):
            _builder().bins("name", "age", "city").row(1, "Tim", 312)

    def test_bin_names_must_be_strings(self):
        with pytest.raises(TypeError, match="bin names must be str"):
            _builder().bins("name", 42)


class TestPerRowVerbs:
    def test_ttl_applies_to_the_latest_row_only(self):
        rows = (
            _builder().bins("v")
            .row(1, 1)
            .row(2, 2).expire_record_after(timedelta(hours=1))
            .row(3, 3)
        )
        assert [s.ttl_seconds for s in _specs(rows)] == [None, 3600, None]

    def test_generation_applies_to_the_latest_row_only(self):
        rows = _builder().bins("v").row(1, 1).ensure_generation_is(7).row(2, 2)
        assert [s.generation for s in _specs(rows)] == [7, None]

    def test_named_ttl_sentinels(self):
        rows = (
            _builder().bins("v")
            .row(1, 1).never_expire()
            .row(2, 2).with_no_change_in_expiration()
            .row(3, 3).expiry_from_server_default()
        )
        assert [s.ttl_seconds for s in _specs(rows)] == [-1, -2, 0]

    def test_per_row_verb_before_any_row_is_rejected(self):
        with pytest.raises(TypeError, match="call row\\(\\) first"):
            _builder().bins("v").expire_record_after_seconds(60)
        with pytest.raises(TypeError, match="call row\\(\\) first"):
            _builder().bins("v").ensure_generation_is(3)

    def test_default_ttl_covers_rows_without_their_own(self):
        rows = (
            _builder().bins("v")
            .default_expire_record_after_seconds(300)
            .row(1, 1)
            .row(2, 2).expire_record_after_seconds(60)
        )
        assert [s.ttl_seconds for s in _specs(rows)] == [300, 60]


class TestExecuteGuards:
    async def test_execute_without_rows_is_rejected(self):
        with pytest.raises(ValueError, match="no rows"):
            await _builder().bins("v").execute()

    def test_stream_without_rows_is_rejected(self):
        with pytest.raises(ValueError, match="no rows"):
            _builder().bins("v").stream()
