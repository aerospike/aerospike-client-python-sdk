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

from aerospike_sdk import (
    CommandType,
    LatencyType,
    LatencyUnit,
    MetricsPolicy,
    MetricsSnapshot,
    Sampler,
)
from aerospike_sdk.metrics.snapshot import _merge_histograms


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

    def test_shift_reaches_the_underlying_policy(self):
        """The shift is passed down; the multiplier is derived from it there."""
        assert MetricsPolicy(latency_shift=1)._to_pac().latency_shift == 1
        assert MetricsPolicy(latency_shift=1)._to_pac().latency_base == 2
        assert MetricsPolicy(latency_shift=3)._to_pac().latency_shift == 3
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


class _FakeNode:
    def __init__(self, name, address, host):
        self.name, self.address, self.host = name, address, host


class _FakePacSnapshot:
    """Stands in for the underlying client's snapshot object."""

    def __init__(self, doc):
        self._doc = doc
        self.total_nodes = 1
        self.open_connections = 3
        self.exceeded_max_retries = 1
        self.exceeded_total_timeout = 2
        self.nodes = {}
        self.cluster_aggregated = None

    def to_dict(self):
        return self._doc


_RAW = {
    "cluster-aggregated-metrics": {
        "latency-unit": "us",
        "labels": [{"node": "BB9", "host": "127.0.0.1:3010", "cluster": "c1",
                    "app-id": "billing", "owner": "platform"}],
    },
    "127.0.0.1:3010": {
        "open-connections": 3,
        "connections-successful": 7,
        "closed-connections": 2,
        "detailed-resultcode-counts": {
            # Serialized display names, as a real snapshot carries them.
            "test": {"Get": {"ok": 4, "Timeout": 1, "Hot key": 2, "Bin type error": 3}}
        },
        "detailed-metrics": {
            "test": {
                "Get": {
                    "bytes-received": {"buckets": [1], "count": 1, "sum": 120.0},
                    "bytes-sent": {"buckets": [1], "count": 1, "sum": 80.0},
                    "connection-aq": {"buckets": [5, 1], "count": 6, "sum": 6.0},
                    "latency": {"buckets": [0, 4], "count": 4, "sum": 40.0},
                },
                "Put": {
                    "latency": {"buckets": [2, 0], "count": 2, "sum": 10.0},
                },
            }
        },
    },
}


class TestCommandGroupingContract:
    """The command->latency-group map is keyed by serialized names.

    ``_COMMAND_LATENCY_GROUP`` is built from ``str(CommandType.X)``, and the
    canonical snapshot looks commands up by the name the client core puts in
    the raw tree. If a command is renamed below us, or a new one appears, the
    lookup silently misses and that command drops out of its group with no
    error anywhere. These tests fail instead.
    """

    def test_every_command_the_client_exposes_is_grouped(self):
        """A new CommandType must be assigned a group, not silently ignored."""
        from aerospike_sdk.metrics.snapshot import _COMMAND_LATENCY_GROUP

        exposed = {
            str(getattr(CommandType, name))
            for name in dir(CommandType)
            if not name.startswith("_") and name != "NONE"
        }
        missing = exposed - set(_COMMAND_LATENCY_GROUP)
        assert not missing, f"commands with no latency group: {sorted(missing)}"

    def test_no_grouped_command_is_unknown_to_the_client(self):
        """A renamed command must not leave a dead key behind."""
        from aerospike_sdk.metrics.snapshot import _COMMAND_LATENCY_GROUP

        exposed = {
            str(getattr(CommandType, name))
            for name in dir(CommandType)
            if not name.startswith("_")
        }
        stale = set(_COMMAND_LATENCY_GROUP) - exposed
        assert not stale, f"grouped names the client no longer has: {sorted(stale)}"

    def test_group_values_are_the_canonical_group_names(self):
        from aerospike_sdk.metrics.snapshot import _COMMAND_LATENCY_GROUP

        assert set(_COMMAND_LATENCY_GROUP.values()) <= {
            "conn", "read", "write", "batch", "query",
        }

    def test_serialized_names_are_not_the_enum_member_names(self):
        """Guards the assumption the map is built on.

        ``str(CommandType.BATCH_READ)`` is ``"BatchRead"``, not
        ``"BATCH_READ"`` -- the same display-vs-member-name distinction that
        made ``key_busy`` read zero when it matched ``"KeyBusy"`` instead of
        the serialized ``"Hot key"``.
        """
        assert str(CommandType.BATCH_READ) == "BatchRead"
        assert str(CommandType.GET_HEADER) != "GET_HEADER"


class TestCanonicalSnapshot:
    """The structured export shape, independent of the raw serialization."""

    def _snapshot(self, policy=None, nodes=None):
        return MetricsSnapshot(_FakePacSnapshot(_RAW), policy=policy, nodes=nodes)

    def test_top_level_fields(self):
        doc = self._snapshot(policy=MetricsPolicy(latency_columns=9, latency_shift=3)).to_canonical_dict()
        assert doc["client_type"] == "python"
        assert doc["latency_unit"] == "microseconds"
        assert doc["latency_columns"] == 9
        assert doc["latency_shift"] == 3
        assert doc["cluster_name"] == "c1"
        assert doc["app_id"] == "billing"
        assert doc["timestamp"].endswith("+00:00")

    def test_reserved_labels_are_promoted_not_duplicated(self):
        """Identity labels become their own fields and leave the user map alone."""
        doc = self._snapshot().to_canonical_dict()
        assert doc["labels"] == {"owner": "platform"}

    def test_shape_falls_back_to_defaults_when_no_policy_known(self):
        """The schema is fixed: an exporter never has to test for the key."""
        doc = self._snapshot().to_canonical_dict()
        assert doc["latency_columns"] == MetricsPolicy().latency_columns
        assert doc["latency_shift"] == MetricsPolicy().latency_shift

    def test_node_identity_joins_the_live_node(self):
        node = _FakeNode("BB9", "10.0.0.1", ("127.0.0.1", 3010))
        doc = self._snapshot(nodes=[node]).to_canonical_dict()
        entry = doc["nodes"][0]
        assert entry["name"] == "BB9"
        assert entry["port"] == 3010

    def test_node_without_a_live_match_still_reports(self):
        entry = self._snapshot().to_canonical_dict()["nodes"][0]
        assert entry["address"] == "127.0.0.1"
        assert entry["port"] == 3010

    def test_connections_omit_the_unavailable_split(self):
        conns = self._snapshot().to_canonical_dict()["nodes"][0]["connections"]
        assert conns == {"opened": 7, "closed": 2, "open": 3}
        assert "in_use" not in conns and "in_pool" not in conns

    def test_namespace_counters(self):
        ns = self._snapshot().to_canonical_dict()["nodes"][0]["namespaces"][0]
        assert ns["name"] == "test"
        assert ns["errors"] == 6        # every non-ok code
        assert ns["timeouts"] == 1
        assert ns["key_busy"] == 2
        assert ns["bytes_in"] == 120
        assert ns["bytes_out"] == 80

    def test_latency_grouped_by_command_family(self):
        ns = self._snapshot().to_canonical_dict()["nodes"][0]["namespaces"][0]
        assert ns["latency"]["read"] == [0, 4]      # Get
        assert ns["latency"]["write"] == [2, 0]     # Put
        assert ns["latency"]["conn"] == [5, 1]      # connection acquisition

