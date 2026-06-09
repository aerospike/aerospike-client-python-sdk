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

"""Unit tests for ``set_to_geo_json(...)`` on the four bin builders."""

from aerospike_async import GeoJSON, Key

from aerospike_sdk.aio.operations.batch import (
    BatchBinBuilder,
    BatchKeyOperationBuilder,
    BatchOperationBuilder,
    BatchOpType,
)
from aerospike_sdk.aio.operations.query import (
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
)


POINT = '{"type":"Point","coordinates":[-122.4,37.7]}'


def _make_qb() -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="unit")


def _make_key() -> Key:
    return Key("test", "unit", 1)


class TestWriteBinBuilderSetToGeoJson:

    def test_queues_put_with_geojson_value(self):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        wbb = WriteBinBuilder(segment, "loc")

        result = wbb.set_to_geo_json(POINT)

        assert result is segment
        assert len(qb._operations) == 1
        # The queued Operation.put wraps a GeoJSON value, not a raw str.
        # We can't introspect Operation internals directly, but the call
        # would fail if PAC rejected the type.

    def test_chaining_to_next_bin(self):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        result = (
            WriteBinBuilder(segment, "loc")
            .set_to_geo_json(POINT)
            .bin("name").set_to("alpha")
        )
        assert result is segment
        assert len(qb._operations) == 2


class TestSyncWriteBinBuilderSetToGeoJson:

    def test_method_exists_and_returns_segment(self):
        # SyncWriteBinBuilder wraps the async builder; just verify the
        # method exists and is callable. Behavior is covered by the
        # async test above plus the integration test.
        from aerospike_sdk.sync.operations.query import SyncWriteBinBuilder
        assert hasattr(SyncWriteBinBuilder, "set_to_geo_json")
        assert callable(SyncWriteBinBuilder.set_to_geo_json)


class TestBatchBinBuilderSetToGeoJson:

    def test_queues_put_with_geojson_value(self):
        bob = BatchOperationBuilder(client=object())
        key_op = BatchKeyOperationBuilder(bob, _make_key(), BatchOpType.UPSERT)
        bbb = BatchBinBuilder(key_op, "loc")

        result = bbb.set_to_geo_json(POINT)

        assert result is key_op
        assert "loc" in key_op._bins
        assert isinstance(key_op._bins["loc"], GeoJSON)
        assert len(key_op._operations) == 1


class TestSyncBatchBinBuilderSetToGeoJson:

    def test_method_exists_and_is_callable(self):
        from aerospike_sdk.sync.operations.batch import SyncBatchBinBuilder
        assert hasattr(SyncBatchBinBuilder, "set_to_geo_json")
        assert callable(SyncBatchBinBuilder.set_to_geo_json)
