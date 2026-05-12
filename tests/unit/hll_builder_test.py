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

"""Cross-builder coverage tests for the HLL method surface.

Verifies that all 11 HLL operations (5 writes + 6 reads) are wired correctly
onto each builder, that ``hll_init`` / ``hll_add`` / ``hll_set_union`` accept
the four write-flag keywords, and that mutual exclusion of ``create_only``
vs ``update_only`` surfaces a ``ValueError`` at the builder level.
"""

import pytest

from aerospike_async import Key

from aerospike_sdk import HllConfig
from aerospike_sdk.aio.operations.batch import (
    BatchBinBuilder,
    BatchKeyOperationBuilder,
    BatchOperationBuilder,
    BatchOpType,
)
from aerospike_sdk.aio.operations.query import (
    QueryBinBuilder,
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
)
from aerospike_sdk.sync.operations.batch import SyncBatchBinBuilder
from aerospike_sdk.sync.operations.query import SyncWriteBinBuilder


HLL_WRITE_METHODS = (
    "hll_init", "hll_add", "hll_set_union", "hll_fold", "hll_refresh_count",
)
HLL_READ_METHODS = (
    "hll_get_count", "hll_describe", "hll_get_union",
    "hll_get_union_count", "hll_get_intersect_count", "hll_get_similarity",
)


# ---------------------------------------------------------------------------
# Builder API surface
# ---------------------------------------------------------------------------

class TestAllBuildersHaveAllHllMethods:
    """Every builder that exposes HLL ops must expose all 11."""

    @pytest.mark.parametrize("builder_cls", [
        WriteBinBuilder, SyncWriteBinBuilder,
        BatchBinBuilder, SyncBatchBinBuilder,
    ])
    @pytest.mark.parametrize("method", HLL_WRITE_METHODS + HLL_READ_METHODS)
    def test_write_capable_builder_has_method(self, builder_cls, method):
        assert hasattr(builder_cls, method), f"{builder_cls.__name__} missing {method}"
        assert callable(getattr(builder_cls, method))

    @pytest.mark.parametrize("method", HLL_READ_METHODS)
    def test_query_bin_builder_has_read_method(self, method):
        # QueryBinBuilder is read-only and has just the 6 read methods.
        assert hasattr(QueryBinBuilder, method)
        assert callable(getattr(QueryBinBuilder, method))


# ---------------------------------------------------------------------------
# WriteBinBuilder — operation queuing and flag wiring
# ---------------------------------------------------------------------------

def _make_wbb(bin_name: str = "h"):
    qb = QueryBuilder(client=object(), namespace="test", set_name="unit")
    qb._single_key = Key("test", "unit", 1)
    segment = WriteSegmentBuilder(qb)
    return WriteBinBuilder(segment, bin_name), segment


class TestWriteBuilderFlagWiring:

    def test_hll_init_accepts_each_flag(self):
        wbb, segment = _make_wbb()
        wbb.hll_init(HllConfig.of(14))
        wbb.hll_init(HllConfig.of(14), create_only=True)
        wbb.hll_init(HllConfig.of(14), update_only=True)
        wbb.hll_init(HllConfig.of(14), no_fail=True)
        wbb.hll_init(HllConfig.of(14), allow_fold=True)
        assert len(segment._qb._operations) == 5

    def test_hll_init_combines_flags(self):
        wbb, segment = _make_wbb()
        wbb.hll_init(HllConfig.of(14), create_only=True, no_fail=True, allow_fold=True)
        assert len(segment._qb._operations) == 1

    def test_hll_add_with_config_kwarg(self):
        wbb, segment = _make_wbb()
        wbb.hll_add([1, 2, 3], config=HllConfig.of(12))
        wbb.hll_add([4, 5, 6])  # no config → inherits existing precision
        assert len(segment._qb._operations) == 2

    def test_hll_set_union_with_flags(self):
        wbb, segment = _make_wbb()
        wbb.hll_set_union([b"\x00\x01"], create_only=True, no_fail=True)
        assert len(segment._qb._operations) == 1

    @pytest.mark.parametrize("method_name,first_arg", [
        ("hll_init", HllConfig.of(14)),
        ("hll_add", [1, 2, 3]),
        ("hll_set_union", [b"\x00\x01"]),
    ])
    def test_mutual_exclusion_raises_value_error(self, method_name, first_arg):
        wbb, _ = _make_wbb()
        method = getattr(wbb, method_name)
        with pytest.raises(ValueError, match="mutually exclusive"):
            method(first_arg, create_only=True, update_only=True)


# ---------------------------------------------------------------------------
# BatchBinBuilder — HLL methods newly added in this work
# ---------------------------------------------------------------------------

class TestBatchBuilderHll:

    def _make_bbb(self):
        bob = BatchOperationBuilder(client=object())
        key_op = BatchKeyOperationBuilder(bob, Key("test", "unit", 1), BatchOpType.UPSERT)
        return BatchBinBuilder(key_op, "h"), key_op

    def test_hll_init_queues_one_op(self):
        bbb, key_op = self._make_bbb()
        result = bbb.hll_init(HllConfig.of(14), no_fail=True)
        assert result is key_op
        assert len(key_op._operations) == 1

    def test_hll_add_with_config(self):
        bbb, key_op = self._make_bbb()
        bbb.hll_add(["x", "y"], config=HllConfig.of(12))
        assert len(key_op._operations) == 1

    def test_all_reads_queue_ops(self):
        bbb, key_op = self._make_bbb()
        bbb.hll_get_count()
        bbb.hll_describe()
        bbb.hll_get_union([b"\x00"])
        bbb.hll_get_union_count([b"\x00"])
        bbb.hll_get_intersect_count([b"\x00"])
        bbb.hll_get_similarity([b"\x00"])
        assert len(key_op._operations) == 6

    def test_mutual_exclusion_raises(self):
        bbb, _ = self._make_bbb()
        with pytest.raises(ValueError, match="mutually exclusive"):
            bbb.hll_init(HllConfig.of(14), create_only=True, update_only=True)
