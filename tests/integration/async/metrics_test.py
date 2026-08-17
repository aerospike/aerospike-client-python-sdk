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

"""Integration tests for cluster metrics (enable, snapshot, derived views)."""

import pytest

from aerospike_sdk import CommandType, LatencyType, LatencyUnit, MetricsPolicy, Sampler
from aerospike_sdk.dataset import DataSet

from tests.integration.namespace import general_namespace


@pytest.fixture(scope="module")
async def metrics_cluster(aerospike_host, make_cluster_definition):
    """Module-scoped cluster so metrics state isn't shared with other suites."""
    async with await make_cluster_definition(aerospike_host).connect() as c:
        yield c


# The core builds node metrics with its default histogram shape
# (microseconds / 24 columns); enabling with the same shape keeps every part
# of the snapshot populated. The ms-default detail path is pinned by the
# xfail below until the core reshape fix ships.
_SHAPE_SAFE = MetricsPolicy(latency_unit=LatencyUnit.MICROSECONDS, latency_columns=24)


async def _do_some_ops(cluster, count):
    session = cluster.create_session()
    ds = DataSet.of(general_namespace(), "sdk_metrics")
    for i in range(count):
        await session.upsert(ds.id(i)).put({"n": i}).execute()
        result = await session.query(ds.id(i)).execute()
        first = await result.first_or_raise()
        assert first.is_ok


class TestMetricsLifecycle:

    async def test_enable_disable_round_trip(self, metrics_cluster):
        assert metrics_cluster.metrics_enabled() is False
        metrics_cluster.enable_metrics()
        assert metrics_cluster.metrics_enabled() is True
        metrics_cluster.disable_metrics()
        assert metrics_cluster.metrics_enabled() is False

    async def test_snapshot_before_enable_is_empty(self, metrics_cluster):
        snapshot = await metrics_cluster.metrics()
        assert snapshot.total_nodes >= 1
        assert snapshot.latency(LatencyType.READ).count == 0


class TestMetricsSnapshot:

    async def test_snapshot_and_derived_views(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        await _do_some_ops(metrics_cluster, count=5)

        snapshot = await metrics_cluster.metrics()
        assert snapshot.total_nodes >= 1
        assert snapshot.open_connections >= 1
        assert len(snapshot.nodes) == snapshot.total_nodes

        agg = snapshot.cluster_aggregated
        assert agg.latency_unit == LatencyUnit.MICROSECONDS

        # Derived views sum the canonical per-command histograms.
        reads = snapshot.latency(LatencyType.READ)
        writes = snapshot.latency(LatencyType.WRITE)
        assert reads.count >= 5
        assert writes.count >= 5
        assert sum(reads.buckets) == reads.count
        assert reads.latency_unit == LatencyUnit.MICROSECONDS

        # Derived READ equals the sum of its constituent categories.
        constituent = sum(
            agg.command_histogram(ct).count
            for ct in (CommandType.GET, CommandType.GET_HEADER, CommandType.EXISTS)
        )
        assert reads.count == constituent

        # Connection-acquisition view draws from the detailed metrics.
        conn = snapshot.latency(LatencyType.CONN)
        assert conn.count >= 1

        # Canonical detail is reachable through the snapshot.
        assert general_namespace() in agg.detailed_namespaces()
        # Chained writes execute as operate commands; reads fast-path as gets.
        detail = agg.detailed_metric(general_namespace(), CommandType.GET)
        assert detail is not None
        assert detail.latency.count >= 5

        metrics_cluster.disable_metrics()

    async def test_per_node_derived_view(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        await _do_some_ops(metrics_cluster, count=2)

        snapshot = await metrics_cluster.metrics()
        host = next(iter(snapshot.nodes))
        node_reads = snapshot.latency(LatencyType.READ, node=host)
        assert node_reads.count >= 0  # single node clusters: same as aggregate
        with pytest.raises(KeyError):
            snapshot.latency(LatencyType.READ, node="10.0.0.1:9999")
        metrics_cluster.disable_metrics()

    # Bucket-placement pair: microseconds resolve what milliseconds collapse.
    # Both tests assert against the <=1 boundary only — bucket 0 means the
    # same thing in the current core bucket math and in the range layout the
    # spec migrates to (interior bucket indices shift by one between the
    # two), so these survive that core change without edits.

    async def test_millisecond_buckets_collapse_fast_ops(self, metrics_cluster):
        metrics_cluster.enable_metrics(MetricsPolicy())  # milliseconds / 7
        await _do_some_ops(metrics_cluster, count=5)

        hist = (
            await metrics_cluster.metrics()
        ).cluster_aggregated.command_histogram(CommandType.GET)
        assert hist.count >= 5
        # Local round trips are sub-millisecond; at least the fastest op
        # lands in the <= 1 ms bucket.
        assert hist.buckets[0] >= 1
        assert hist.min <= 1
        metrics_cluster.disable_metrics()

    async def test_microsecond_buckets_resolve_fast_ops(self, metrics_cluster):
        metrics_cluster.enable_metrics(
            MetricsPolicy(latency_unit=LatencyUnit.MICROSECONDS, latency_columns=18)
        )
        await _do_some_ops(metrics_cluster, count=5)

        hist = (
            await metrics_cluster.metrics()
        ).cluster_aggregated.command_histogram(CommandType.GET)
        assert hist.count >= 5
        # A network round trip is never <= 1 microsecond: everything the
        # millisecond view collapsed into bucket 0 spreads above it here.
        assert hist.buckets[0] == 0
        assert sum(hist.buckets[1:]) == hist.count
        assert hist.min >= 2
        metrics_cluster.disable_metrics()

    async def test_sampler_never_gates_command_metrics(self, metrics_cluster):
        policy = MetricsPolicy(
            latency_unit=LatencyUnit.MICROSECONDS,
            latency_columns=24,
            sampler=Sampler.never(),
        )
        # Baseline after enabling: a histogram-shape change on enable resets
        # the accumulated counts, so capture from the post-reshape state.
        metrics_cluster.enable_metrics(policy)
        before = (await metrics_cluster.metrics()).latency(LatencyType.READ).count
        await _do_some_ops(metrics_cluster, count=3)

        snapshot = await metrics_cluster.metrics()
        assert snapshot.latency(LatencyType.READ).count == before
        metrics_cluster.disable_metrics()

    async def test_to_dict_stable_names(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        await _do_some_ops(metrics_cluster, count=1)

        d = (await metrics_cluster.metrics()).to_dict()
        assert d["total-nodes"] >= 1
        agg = d["cluster-aggregated-metrics"]
        assert agg["latency-unit"] == "us"
        assert agg["get-metrics"]["count"] >= 1
        metrics_cluster.disable_metrics()

    @pytest.mark.xfail(
        strict=True,
        reason="core reshape bug: detail created after a column-count change keeps "
        "the stale shape and aggregation drops it; fixed in core (unreleased) — "
        "remove this marker on the next PAC pin bump",
    )
    async def test_default_policy_detailed_metrics(
        self, aerospike_host, make_cluster_definition
    ):
        # A fresh cluster so the detail slots are created lazily AFTER the
        # ms/7 enable — the exact path the core bug loses.
        async with await make_cluster_definition(aerospike_host).connect() as c:
            c.enable_metrics()  # default: milliseconds / 7 columns
            await _do_some_ops(c, count=3)

            snapshot = await c.metrics()
            detail = snapshot.cluster_aggregated.detailed_metric(
                general_namespace(), CommandType.GET
            )
            assert detail is not None
            assert detail.latency.count >= 3
