# Copyright 2026 Aerospike, Inc.
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

"""Unit tests for the partition-restriction helpers on the query builder.

``PartitionFilter.by_range`` takes ``(begin, count)``, while the builder
helpers speak in partition ids and exclusive end bounds — these tests pin
the conversion by asserting the ``begin``/``count`` of the filter actually
built, so a bound-vs-count mixup cannot come back silently.
"""

import pytest

from aerospike_sdk.aio.operations.query import QueryBuilder


def _query_builder(**kwargs):
    """Return a QueryBuilder with a fake client (no real connection)."""
    client = kwargs.pop("client", None)
    if client is None:
        client = object()
    return QueryBuilder(
        client=client,
        namespace="test",
        set_name="unit_test",
        **kwargs,
    )


class TestOnPartitionRange:
    """on_partition_range(start_incl, end_excl) → by_range(start, end - start)."""

    def test_first_half(self):
        builder = _query_builder().on_partition_range(0, 2048)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (0, 2048)

    def test_second_half_stays_inside_partition_space(self):
        """The common (2048, 4096) pattern must cover 2048-4095, not run past 4096."""
        builder = _query_builder().on_partition_range(2048, 4096)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (2048, 2048)
        assert pf.begin + pf.count <= 4096

    def test_interior_range(self):
        builder = _query_builder().on_partition_range(1024, 2048)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (1024, 1024)

    def test_narrow_range(self):
        builder = _query_builder().on_partition_range(100, 200)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (100, 100)

    def test_invalid_bounds_raise(self):
        with pytest.raises(ValueError):
            _query_builder().on_partition_range(-1, 10)
        with pytest.raises(ValueError):
            _query_builder().on_partition_range(0, 4097)
        with pytest.raises(ValueError):
            _query_builder().on_partition_range(10, 10)


class TestOnPartition:
    """on_partition(p) targets exactly one partition."""

    def test_single_partition(self):
        builder = _query_builder().on_partition(100)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (100, 1)

    def test_last_partition(self):
        """part_id 4095 is valid and must not step past the partition space."""
        builder = _query_builder().on_partition(4095)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (4095, 1)

    def test_out_of_range_raises(self):
        with pytest.raises(ValueError):
            _query_builder().on_partition(4096)


class TestOnPartitions:
    """on_partitions(*ids) → by_id for one id, contiguous by_range otherwise."""

    def test_single_id_uses_by_id(self):
        builder = _query_builder().on_partitions(7)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (7, 1)

    def test_contiguous_ids(self):
        builder = _query_builder().on_partitions(1000, 1001, 1002)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (1000, 3)

    def test_contiguous_ids_order_independent(self):
        builder = _query_builder().on_partitions(3, 1, 2)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (1, 3)

    def test_non_contiguous_ids_span_min_to_max(self):
        """Non-contiguous ids collapse to the [min, max] span (documented
        limitation of the single-span filter model): the gap partitions are
        included, but nothing outside the requested min/max is."""
        builder = _query_builder().on_partitions(4, 6, 8)
        pf = builder._partition_filter
        assert (pf.begin, pf.count) == (4, 5)
