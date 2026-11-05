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

import copy
from dataclasses import dataclass
from datetime import timedelta
from typing import List

import time

import pytest
from types import SimpleNamespace

from aerospike_sdk.aio.cluster import Cluster as AsyncCluster
from aerospike_sdk.cluster_shared import ClusterBase
from aerospike_sdk.exceptions import AerospikeError, ResultCode
from aerospike_sdk.policy.system_settings import MetricsSettings, SystemSettings
from aerospike_sdk.sync.cluster import Cluster as SyncCluster
from aerospike_sdk.metrics import (
    CommandType,
    LatencyType,
    LatencyUnit,
    MetricsPolicy,
    MetricsSnapshot,
    Sampler,
)
from aerospike_sdk.metrics.snapshot import _COMMAND_LATENCY_GROUP, _merge_histograms, ProcessSampler
from aerospike_sdk.metrics.export import _NodeCloseTracker


class TestMetricsPolicy:

    def test_defaults_are_the_sdk_metrics_defaults(self):
        mp = MetricsPolicy()
        assert mp.latency_unit == LatencyUnit.MILLISECONDS
        assert mp.latency_columns == 7
        assert mp.latency_shift == 1
        assert mp.sampler == Sampler.all()
        assert mp.labels == {}

    def test_no_histogram_type_knob(self):
        # Logarithmic range layout is the only supported bucket scheme;
        # the policy deliberately has no histogram-type field.
        assert not hasattr(MetricsPolicy(), "histogram_type")

    def test_shift_reaches_the_underlying_policy(self):
        """The shift is passed down; the multiplier is derived from it there."""
        assert MetricsPolicy(latency_shift=1)._to_pnc().latency_shift == 1
        assert MetricsPolicy(latency_shift=1)._to_pnc().latency_base == 2
        assert MetricsPolicy(latency_shift=3)._to_pnc().latency_shift == 3
        assert MetricsPolicy(latency_shift=3)._to_pnc().latency_base == 8

    def test_fields_pass_through_to_pnc(self):
        mp = MetricsPolicy(
            latency_unit=LatencyUnit.MICROSECONDS,
            latency_columns=18,
            sampler=Sampler.probability(0.25),
            labels={"team": "billing"},
        )
        pnc = mp._to_pnc()
        assert pnc.latency_unit == LatencyUnit.MICROSECONDS
        assert pnc.latency_columns == 18
        assert pnc.sampler.range == 1_000_000
        assert pnc.sampler.threshold == 250_000
        assert pnc.labels == [{"team": "billing"}]

    def test_no_labels_pass_down_as_no_label_maps(self):
        assert MetricsPolicy()._to_pnc().labels == []

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


class _FakePncSnapshot:
    """Stands in for the underlying client's snapshot object."""

    def __init__(self, doc):
        self._doc = doc
        self.total_nodes = 1
        self.exceeded_max_retries = 1
        self.exceeded_total_timeout = 2
        self.nodes = {}
        self.cluster_aggregated = None

    def to_dict(self):
        return self._doc


_RAW = {
    "cluster_aggregated_metrics": {
        "latency_unit": "us",
        "labels": [{"node": "BB9", "host": "127.0.0.1:3010", "cluster": "c1",
                    "app_id": "billing", "owner": "platform"}],
    },
    "127.0.0.1:3010": {
        "open_connections": 3,
        "connections_successful": 7,
        "closed_connections": 2,
        "detailed_resultcode_counts": {
            # Serialized display names, as a real snapshot carries them.
            "test": {"Get": {"ok": 4, "Timeout": 1, "Hot key": 2, "Bin type error": 3}}
        },
        "detailed_metrics": {
            "test": {
                "Get": {
                    "bytes_received": {"buckets": [1], "count": 1, "sum": 120.0},
                    "bytes_sent": {"buckets": [1], "count": 1, "sum": 80.0},
                    "connection_aq": {"buckets": [5, 1], "count": 6, "sum": 6.0},
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

        exposed = {
            str(getattr(CommandType, name))
            for name in dir(CommandType)
            if not name.startswith("_") and name != "NONE"
        }
        missing = exposed - set(_COMMAND_LATENCY_GROUP)
        assert not missing, f"commands with no latency group: {sorted(missing)}"

    def test_no_grouped_command_is_unknown_to_the_client(self):
        """A renamed command must not leave a dead key behind."""

        exposed = {
            str(getattr(CommandType, name))
            for name in dir(CommandType)
            if not name.startswith("_")
        }
        stale = set(_COMMAND_LATENCY_GROUP) - exposed
        assert not stale, f"grouped names the client no longer has: {sorted(stale)}"

    def test_group_values_are_the_canonical_group_names(self):
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
        return MetricsSnapshot(_FakePncSnapshot(_RAW), policy=policy, nodes=nodes)

    def test_top_level_fields(self):
        doc = MetricsSnapshot(
            _FakePncSnapshot(_RAW),
            policy=MetricsPolicy(latency_columns=9, latency_shift=3),
        ).to_canonical_dict()
        assert doc["client_type"] == "python-sdk"
        assert "conn_type" not in doc
        assert doc["latency_unit"] == "microseconds"
        assert doc["latency_columns"] == 9
        assert doc["latency_shift"] == 3
        assert doc["cluster_name"] == "c1"
        assert doc["app_id"] == "billing"
        assert doc["timestamp"].endswith("+00:00")

    def test_shared_document_has_the_fixed_v1_key_sets(self):
        doc = self._snapshot().to_canonical_dict()
        assert set(doc) == {
            "timestamp", "metrics_enabled", "operational_metrics_enabled",
            "usage_metrics_enabled", "cluster_name", "client_type",
            "client_version", "app_id", "labels", "total_nodes",
            "recover_queue_size", "invalid_node_count",
            "delay_queue_timeout_count", "open_connections", "command_count",
            "command_retries", "cpu_percent", "memory_bytes",
            "batch_partial_failure", "cluster_latencies",
            "validation_open_failure", "validation_tls_handshake_failure",
            "validation_auth_failure", "nodes", "nodes_departed", "usage",
            "latency_unit", "latency_columns", "latency_shift",
        }
        node = doc["nodes"][0]
        assert set(node) == {
            "node_name", "node_address", "node_port", "connections",
            "conn_latency", "connection_open_failure",
            "connection_tls_handshake_failure", "connection_auth_failure",
            "namespaces",
        }
        assert set(node["connections"]) == {"in_use", "in_pool", "opened", "closed"}
        assert set(node["namespaces"][0]) == {
            "namespace", "errors", "client_timeouts", "server_timeouts",
            "key_busy", "record_too_big", "device_overload", "bytes_in",
            "bytes_out", "latencies",
        }

    def test_native_blocked_shared_fields_are_stable_zero_placeholders(self):
        doc = self._snapshot().to_canonical_dict()
        assert doc["delay_queue_timeout_count"] == 0
        assert doc["batch_partial_failure"] == 0
        assert doc["validation_open_failure"] == 0
        assert doc["validation_tls_handshake_failure"] == 0
        assert doc["validation_auth_failure"] == 0
        assert doc["nodes"][0]["namespaces"][0]["client_timeouts"] == 0

    def test_snapshot_is_immutable_and_copies_python_owned_inputs(self):
        policy = MetricsPolicy(latency_columns=9)
        node = _FakeNode("original", "127.0.0.1:3010", ("127.0.0.1", 3010))
        usage = {"feature.shape.point": 1}
        snapshot = MetricsSnapshot(
            _FakePncSnapshot(_RAW), policy=policy, nodes=[node], usage=usage
        )

        policy.latency_columns = 3
        node.name = "changed"
        usage["feature.shape.point"] = 99

        doc = snapshot.to_canonical_dict()
        assert doc["latency_columns"] == 9
        assert doc["nodes"][0]["node_name"] == "original"
        assert snapshot.usage == {"feature.shape.point": 1}
        with pytest.raises(AttributeError, match="immutable"):
            snapshot._departed = ("127.0.0.1:3010",)
        with pytest.raises(AttributeError, match="immutable"):
            del snapshot._departed

        returned_nodes = snapshot.nodes
        returned_nodes["changed"] = object()
        assert "changed" not in snapshot.nodes

        first_timestamp = snapshot.to_canonical_dict()["timestamp"]
        assert snapshot.to_canonical_dict()["timestamp"] == first_timestamp

    def test_tier_flags_follow_what_was_collecting(self):
        on = MetricsSnapshot(
            _FakePncSnapshot(_RAW),
            policy=MetricsPolicy(operational_enabled=True),
            metrics_enabled=True,
            usage_enabled=False,
        ).to_canonical_dict()
        assert on["metrics_enabled"] is True
        assert on["operational_metrics_enabled"] is True
        assert on["usage_metrics_enabled"] is False

    def test_tier_flags_are_off_while_collection_is_off(self):
        """A pull taken after disable reports frozen counters, not live tiers."""
        snapshot = MetricsSnapshot(
            _FakePncSnapshot(_RAW),
            policy=MetricsPolicy(operational_enabled=True, usage_enabled=True),
            metrics_enabled=False,
            usage_enabled=True,
        )
        assert snapshot.metrics_enabled is False
        assert snapshot.operational_metrics_enabled is False
        assert snapshot.usage_metrics_enabled is False

    def _no_app_id_snapshot(self, app_id=None):
        """A snapshot whose underlying client reports no application identity."""
        raw = copy.deepcopy(_RAW)
        labels = raw["cluster_aggregated_metrics"]["labels"][0]
        del labels["app_id"]
        return MetricsSnapshot(_FakePncSnapshot(raw), app_id=app_id)

    def test_app_id_falls_back_to_the_authenticated_user(self):
        """Unnamed applications still group by the identity the server knows."""
        doc = self._no_app_id_snapshot(app_id="analytics_svc").to_canonical_dict()
        assert doc["app_id"] == "analytics_svc"

    def test_app_id_is_never_empty(self):
        """No application name and no authentication: a placeholder, not ``""``."""
        doc = self._no_app_id_snapshot().to_canonical_dict()
        assert doc["app_id"] == "not-set"

    def test_declared_app_id_wins_over_the_fallback(self):
        doc = self._snapshot().to_canonical_dict()
        assert doc["app_id"] == "billing"

    def test_reserved_labels_are_promoted_not_duplicated(self):
        """Identity labels become their own fields and leave the user map alone."""
        doc = self._snapshot().to_canonical_dict()
        assert doc["labels"] == {"owner": "platform"}

    def test_shape_falls_back_to_defaults_when_no_policy_known(self):
        """The schema is fixed: an exporter never has to test for the key."""
        doc = self._snapshot().to_canonical_dict()
        assert doc["latency_columns"] == MetricsPolicy().latency_columns
        assert doc["latency_shift"] == MetricsPolicy().latency_shift

    def test_canonical_usage_exposes_only_promoted_short_names(self):
        snapshot = MetricsSnapshot(
            _FakePncSnapshot(_RAW),
            usage={
                "feature.api.blocking": 4,
                "feature.shape.point": 3,
                "feature.filter.exp": 2,
                "feature.transaction": 9,
            },
        )
        usage = snapshot.to_canonical_dict()["usage"]
        assert usage["blocking"] == 4
        assert usage["shape_point"] == 3
        assert usage["filter_exp"] == 2
        assert "feature.transaction" not in usage
        assert set(usage) == {
            "blocking", "deferred", "background",
            "shape_point", "shape_batch", "shape_query",
            "object_mapping_read", "object_mapping_write",
            "filter_ael", "filter_exp",
        }

    def test_cluster_latencies_are_full_whole_call_summaries(self):
        class Aggregate:
            latency_unit = LatencyUnit.MICROSECONDS
            transaction_retry_count = 0

            @staticmethod
            def command_histogram(command):
                return _FakeHistogram([1, 2], count=3, min=2, max=8, sum=12.0)

        pnc = _FakePncSnapshot(_RAW)
        pnc.cluster_aggregated = Aggregate()
        latencies = MetricsSnapshot(pnc).to_canonical_dict()["cluster_latencies"]
        assert [latency["type"] for latency in latencies] == [
            "read", "write", "batch", "query",
        ]
        assert all(
            set(latency) == {"type", "bucket_counts", "min", "max", "sum", "count"}
            for latency in latencies
        )
        assert all(latency["count"] > 0 for latency in latencies)

    def test_node_identity_joins_the_live_node(self):
        node = _FakeNode("BB9", "10.0.0.1", ("127.0.0.1", 3010))
        doc = self._snapshot(nodes=[node]).to_canonical_dict()
        entry = doc["nodes"][0]
        assert entry["node_name"] == "BB9"
        assert entry["node_port"] == 3010

    def test_node_without_a_live_match_still_reports(self):
        entry = self._snapshot().to_canonical_dict()["nodes"][0]
        assert entry["node_address"] == "127.0.0.1"
        assert entry["node_port"] == 3010

    def test_connections_report_only_the_v1_pool_set(self):
        conns = self._snapshot().to_canonical_dict()["nodes"][0]["connections"]
        assert conns == {
            "opened": 7, "closed": 2, "in_use": 0, "in_pool": 0,
        }

    def test_connection_failure_causes_are_reported_beside_the_rollup(self):
        """TLS and auth name causes of `open_failure`; they do not partition it."""
        raw = {**_RAW, "127.0.0.1:3010": dict(
            _RAW["127.0.0.1:3010"],
            **{"connections_failed": 5, "connections_error_tls": 3,
               "connections_error_auth": 1},
        )}
        node = MetricsSnapshot(_FakePncSnapshot(raw)).to_canonical_dict()["nodes"][0]
        assert node["connection_open_failure"] == 5
        assert node["connection_tls_handshake_failure"] == 3
        assert node["connection_auth_failure"] == 1

    def test_pool_occupancy_split_is_reported(self):
        raw = {**_RAW, "127.0.0.1:3010": dict(
            _RAW["127.0.0.1:3010"],
            **{"connections_in_use": 2, "connections_in_pool": 6},
        )}
        conns = MetricsSnapshot(
            _FakePncSnapshot(raw)
        ).to_canonical_dict()["nodes"][0]["connections"]
        assert (conns["in_use"], conns["in_pool"]) == (2, 6)

    def test_connection_open_failures_are_reported(self):
        raw = {**_RAW, "127.0.0.1:3010": dict(_RAW["127.0.0.1:3010"], **{"connections_failed": 4})}
        snapshot = MetricsSnapshot(_FakePncSnapshot(raw))
        node = snapshot.to_canonical_dict()["nodes"][0]
        assert node["connection_open_failure"] == 4

    def test_cluster_gauges_core_now_reports_are_mapped(self):
        """Pool occupancy, recover depth and invalid-node count reach the cluster section."""
        raw = dict(_RAW, connections_in_use=2, connections_in_pool=6,
                   recover_queue_size=1, nodes_invalid=3)
        doc = MetricsSnapshot(_FakePncSnapshot(raw)).to_canonical_dict()
        assert doc["recover_queue_size"] == 1
        assert doc["total_nodes"] == 1
        assert doc["invalid_node_count"] == 3

    def test_pool_exhaustion_is_mapped(self):
        """A poll against a full pool with no idle socket counts as pool_exhausted."""
        raw = {**_RAW, "127.0.0.1:3010": dict(_RAW["127.0.0.1:3010"], connections_pool_empty=3)}
        node = MetricsSnapshot(_FakePncSnapshot(raw)).to_canonical_dict()["nodes"][0]
        assert "pool_exhausted" not in node["connections"]

    def test_command_timeouts_are_split_by_who_reported_them(self):
        """Only server TIMEOUT answers can currently be attributed to a namespace."""
        # The fake reports exceeded_max_retries=1 and exceeded_total_timeout=2;
        # the namespace fixture carries one server TIMEOUT answer.
        doc = self._snapshot().to_canonical_dict()
        assert "command_timeout_client" not in doc
        assert doc["nodes"][0]["namespaces"][0]["server_timeouts"] == 1

    def test_node_error_rate_is_mapped(self):
        raw = {**_RAW, "127.0.0.1:3010": dict(_RAW["127.0.0.1:3010"], error_rate=4)}
        node = MetricsSnapshot(_FakePncSnapshot(raw)).to_canonical_dict()["nodes"][0]
        assert "error_rate" not in node

    def test_process_samples_reach_the_cluster_section(self):
        doc = MetricsSnapshot(
            _FakePncSnapshot(_RAW), process=(12.5, 1048576)
        ).to_canonical_dict()
        assert doc["cpu_percent"] == 12.5
        assert doc["memory_bytes"] == 1048576

    def test_process_samples_are_omitted_when_not_taken(self):
        doc = self._snapshot().to_canonical_dict()
        assert doc["cpu_percent"] == 0.0 and doc["memory_bytes"] == 0

    def test_namespace_splits_the_named_error_causes(self):
        """Record-too-big and device-overload are broken out beside key_busy."""
        raw = {**_RAW, "127.0.0.1:3010": dict(_RAW["127.0.0.1:3010"], detailed_resultcode_counts={
            "test": {"Put": {"ok": 4, "Timeout": 1, "Hot key": 2,
                             "Record too big": 5, "Device overload": 3}},
        })}
        ns = MetricsSnapshot(_FakePncSnapshot(raw)).to_canonical_dict()["nodes"][0]["namespaces"][0]
        assert ns["record_too_big"] == 5
        assert ns["device_overload"] == 3
        assert ns["key_busy"] == 2 and ns["server_timeouts"] == 1
        # Every failure here has a named cause, so none is left for `errors`.
        assert ns["errors"] == 0

    def test_command_count_reaches_the_cluster_section(self):
        snapshot = MetricsSnapshot(_FakePncSnapshot(_RAW), command_count=42)
        doc = snapshot.to_canonical_dict()
        assert doc["command_count"] == 42
        assert snapshot.command_count == 42

    def test_command_count_is_zero_when_nobody_counted(self):
        doc = self._snapshot().to_canonical_dict()
        assert doc["command_count"] == 0

    def test_retry_count_sums_the_per_node_counters(self):
        raw = dict(_RAW)
        raw["cluster_aggregated_metrics"] = dict(
            _RAW["cluster_aggregated_metrics"], **{"transaction_retry_count": 9}
        )
        doc = MetricsSnapshot(_FakePncSnapshot(raw)).to_canonical_dict()
        assert doc["command_retries"] == 9

    def test_retry_count_defaults_to_zero(self):
        doc = self._snapshot().to_canonical_dict()
        assert doc["command_retries"] == 0
        assert self._snapshot().command_retries == 0

    def test_deferred_close_reasons_are_not_reported(self):
        raw = {**_RAW, "127.0.0.1:3010": dict(
            _RAW["127.0.0.1:3010"],
            **{"connections_idle_dropped": 5, "connections_closed_error": 2,
               "connections_closed_node_removed": 1},
        )}
        conns = MetricsSnapshot(
            _FakePncSnapshot(raw)
        ).to_canonical_dict()["nodes"][0]["connections"]
        assert set(conns) == {"in_use", "in_pool", "opened", "closed"}

    def test_nodes_departed_is_empty_on_a_plain_pull(self):
        """Departure is tracked by the export timer, not by pulls."""
        doc = self._snapshot().to_canonical_dict()
        assert doc["nodes_departed"] == []

    def test_a_departed_node_moves_to_nodes_departed(self):
        # An empty live-node list: the one measured host has left the cluster.
        original = self._snapshot(nodes=[])
        snapshot = original._with_departed(_NodeCloseTracker())
        assert original.to_canonical_dict()["nodes_departed"] == []
        doc = snapshot.to_canonical_dict()
        assert doc["nodes"] == []
        assert [n["node_address"] for n in doc["nodes_departed"]] == ["127.0.0.1"]

    def test_departure_remains_pending_until_drained(self):
        """On-demand peeks do not consume the final node sample."""

        tracker = _NodeCloseTracker()
        self._snapshot(nodes=[])._with_departed(tracker)
        second = self._snapshot(nodes=[])._with_departed(tracker)
        doc = second.to_canonical_dict()
        assert doc["nodes"] == []
        assert len(doc["nodes_departed"]) == 1
        tracker.drain(second._departed_hosts)
        third = self._snapshot(nodes=[])._with_departed(tracker).to_canonical_dict()
        assert third["nodes_departed"] == []

    def test_namespace_counters(self):
        ns = self._snapshot().to_canonical_dict()["nodes"][0]["namespaces"][0]
        assert ns["namespace"] == "test"
        assert ns["errors"] == 3        # the unnamed causes only
        assert ns["server_timeouts"] == 1
        assert ns["client_timeouts"] == 0
        assert ns["key_busy"] == 2
        assert ns["bytes_in"] == 120
        assert ns["bytes_out"] == 80

    def test_latency_grouped_by_command_family(self):
        ns = self._snapshot().to_canonical_dict()["nodes"][0]["namespaces"][0]
        latency = {item["type"]: item for item in ns["latencies"]}
        assert latency["read"]["bucket_counts"] == [0, 4]
        assert latency["read"]["count"] == 4
        assert latency["write"]["bucket_counts"] == [2, 0]
        assert "conn" not in latency
        assert self._snapshot().to_canonical_dict()["nodes"][0]["conn_latency"][
            "bucket_counts"
        ] == [5, 1]


class TestProcessSampler:
    """The SDK's own process samples: CPU over the interval, resident memory now."""

    def test_samples_are_a_percent_and_bytes(self):
        sampler = ProcessSampler()
        cpu, rss = sampler.sample()
        assert 0.0 <= cpu <= 100.0 * 64
        assert isinstance(rss, int) and rss > 0

    def test_cpu_reflects_work_since_the_previous_sample(self):
        sampler = ProcessSampler()
        sampler.sample()
        deadline = time.monotonic() + 0.05
        while time.monotonic() < deadline:
            pass
        cpu, _ = sampler.sample()
        assert cpu > 0.0



class TestConfigFileOwnsTheSwitch:
    """`enable_metrics()` / `disable_metrics()` defer to a file that sets `metrics.enabled`."""

    @staticmethod
    def _cluster(monitor, enabled):
        metrics = None if enabled is None else MetricsSettings(enabled=enabled)
        return SimpleNamespace(
            _sdk_client=SimpleNamespace(
                _sdk_config_monitor=monitor,
                _sdk_settings=SystemSettings(metrics=metrics),
            ),
        )

    def test_a_file_that_sets_enabled_refuses_the_api(self):
        for enabled in (True, False):
            with pytest.raises(AerospikeError) as info:
                ClusterBase._check_metrics_switch_is_free(
                    self._cluster(object(), enabled), "enable_metrics"
                )
            assert info.value.result_code == ResultCode.PARAMETER_ERROR
            assert "enable_metrics()" in str(info.value)
            assert "metrics.enabled" in str(info.value)

    def test_a_file_silent_on_enabled_leaves_the_switch_to_code(self):
        ClusterBase._check_metrics_switch_is_free(self._cluster(object(), None), "enable_metrics")

    def test_no_file_means_no_owner(self):
        """Settings given in code are not re-applied, so they cannot undo the API."""
        ClusterBase._check_metrics_switch_is_free(self._cluster(None, False), "disable_metrics")


class TestMetricsDeliveryRefresh:
    @pytest.mark.parametrize("cluster_type", [SyncCluster, AsyncCluster])
    def test_delivery_only_refresh_preserves_the_active_collection_policy(
        self, cluster_type,
    ):
        base = MetricsPolicy(
            operational_enabled=True,
            latency_columns=17,
            labels={"environment": "prod"},
            usage_enabled=True,
        )
        applied = []
        cluster = SimpleNamespace(
            _metrics_policy=base,
            metrics_enabled=lambda: True,
            _enable_metrics=applied.append,
        )

        cluster_type._apply_metrics_settings(
            cluster,
            MetricsSettings(export_interval=timedelta(seconds=5)),
        )

        assert len(applied) == 1
        effective = applied[0]
        assert effective.operational_enabled is True
        assert effective.latency_columns == 17
        assert effective.labels == {"environment": "prod"}
        assert effective.usage_enabled is True

    @pytest.mark.parametrize("cluster_type", [SyncCluster, AsyncCluster])
    def test_silent_refresh_does_not_enable_collection(self, cluster_type):
        applied = []
        cluster = SimpleNamespace(
            _metrics_policy=MetricsPolicy(),
            metrics_enabled=lambda: False,
            _enable_metrics=applied.append,
        )
        cluster_type._apply_metrics_settings(
            cluster,
            MetricsSettings(export_interval=timedelta(seconds=5)),
        )
        assert applied == []
