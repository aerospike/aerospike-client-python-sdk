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
# License for the specific language governing permissions and limitations
# under the License.

"""Unit tests for the sync query builder's diagnostic helpers."""

from types import SimpleNamespace

from aerospike_sdk.sync.operations.query import _describe_specs


def _spec(**overrides):
    base = dict(
        op_type="upsert",
        keys=[object(), object()],
        operations=[object()],
        filter_expression=None,
        generation=None,
        ttl_seconds=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class TestDescribeSpecs:

    def test_missing_specs_attribute(self):
        assert _describe_specs(object()) == "qb=None"

    def test_keyless_builder_summary(self):
        qb = SimpleNamespace(
            _specs=[],
            _namespace="test",
            _set_name="users",
            _operations=[object()],
            _where_ael="$.age > 21",
            _filter_records=None,
        )
        text = _describe_specs(qb)
        assert "keyless" in text
        assert "ns='test'" in text
        assert "ops=1" in text
        assert "where_ael=True" in text

    def test_spec_summary_lists_each_spec(self):
        qb = SimpleNamespace(_specs=[_spec(), _spec(op_type="delete", keys=[object()])])
        text = _describe_specs(qb)
        assert text.startswith("specs=2:")
        assert "spec0(op_type='upsert' keys=2" in text
        assert "spec1(op_type='delete' keys=1" in text

    def test_spec_summary_flags_filter_and_ttl(self):
        qb = SimpleNamespace(
            _specs=[_spec(filter_expression=object(), generation=3, ttl_seconds=60)],
        )
        text = _describe_specs(qb)
        assert "filter_expression=True" in text
        assert "gen=3" in text
        assert "ttl=60" in text
