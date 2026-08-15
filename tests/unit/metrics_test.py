# Copyright 2025-2026 Aerospike, Inc.
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

"""Unit tests for the metrics policy mapping and derived histogram math."""

from dataclasses import dataclass
from typing import List

import pytest

from aerospike_sdk import LatencyType, LatencyUnit, MetricsPolicy, Sampler
from aerospike_sdk.metrics import _merge_histograms


class TestMetricsPolicy:

    def test_defaults_are_the_sdk_metrics_defaults(self):
        mp = MetricsPolicy()
        assert mp.latency_unit == LatencyUnit.MILLISECONDS
        assert mp.latency_columns == 7
        assert mp.latency_shift == 1
        assert mp.sampler == Sampler.all()
        assert mp.labels == []

    def test_no_histogram_type_knob(self):
        # Logarithmic range layout is the only supported bucket scheme;
        # the policy deliberately has no histogram-type field.
        assert not hasattr(MetricsPolicy(), "histogram_type")

    def test_shift_maps_to_pac_base(self):
        assert MetricsPolicy(latency_shift=1)._to_pac().latency_base == 2
        assert MetricsPolicy(latency_shift=3)._to_pac().latency_base == 8

    def test_fields_pass_through_to_pac(self):
        mp = MetricsPolicy(
            latency_unit=LatencyUnit.MICROSECONDS,
            latency_columns=18,
            sampler=Sampler.probability(0.25),
            labels=[{"team": "billing"}],
        )
        pac = mp._to_pac()
        assert pac.latency_unit == LatencyUnit.MICROSECONDS
        assert pac.latency_columns == 18
        assert pac.sampler.range == 1_000_000
        assert pac.sampler.threshold == 250_000
        assert pac.labels == [{"team": "billing"}]

    def test_shift_below_one_rejected(self):
        with pytest.raises(ValueError):
            MetricsPolicy(latency_shift=0)


class TestLatencyType:

    def test_five_way_members(self):
        assert {t.value for t in LatencyType} == {
            "conn",
            "write",
            "read",
            "batch",
            "query",
        }


@dataclass
class _FakeHistogram:
    buckets: List[int]
    count: int
    min: int
    max: int
    sum: float


class TestDerivedHistogramMerge:

    def test_merges_same_shape(self):
        merged = _merge_histograms(
            [
                _FakeHistogram([3, 1, 0], count=4, min=1, max=5, sum=10.0),
                _FakeHistogram([1, 0, 2], count=3, min=0, max=9, sum=14.0),
            ],
            LatencyUnit.MILLISECONDS,
        )
        assert merged.buckets == (4, 1, 2)
        assert merged.count == 7
        assert merged.min == 0
        assert merged.max == 9
        assert merged.sum == 24.0
        assert merged.average == 24.0 / 7
        assert merged.latency_unit == LatencyUnit.MILLISECONDS

    def test_skips_empty_and_missing(self):
        merged = _merge_histograms(
            [
                None,
                _FakeHistogram([0, 0], count=0, min=0, max=0, sum=0.0),
                _FakeHistogram([2, 1], count=3, min=1, max=4, sum=6.0),
            ],
            LatencyUnit.MICROSECONDS,
        )
        assert merged.buckets == (2, 1)
        assert merged.count == 3

    def test_skips_mismatched_shapes(self):
        # Mirrors the core's aggregation contract: shape-mismatched
        # histograms are ignored rather than mis-summed.
        merged = _merge_histograms(
            [
                _FakeHistogram([2, 1], count=3, min=1, max=4, sum=6.0),
                _FakeHistogram([1, 1, 1], count=3, min=1, max=8, sum=11.0),
            ],
            LatencyUnit.MILLISECONDS,
        )
        assert merged.buckets == (2, 1)
        assert merged.count == 3

    def test_empty_input_yields_zeroed_view(self):
        merged = _merge_histograms([], LatencyUnit.MILLISECONDS)
        assert merged.buckets == ()
        assert merged.count == 0
        assert merged.average == 0.0
