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
from aerospike_sdk.metrics.export import AsyncMetricsExportTimer

from tests.integration.namespace import general_namespace


@pytest.fixture(scope="module")
async def metrics_cluster(aerospike_host, make_cluster_definition):
    """Module-scoped cluster so metrics state isn't shared with other suites."""
    async with await make_cluster_definition(aerospike_host).connect() as c:
        yield c


# The core builds node metrics with its default histogram shape
# (microseconds / 24 columns); enabling with the same shape keeps every part of
# the snapshot populated, since a shape change resets the accumulated counts.
# The ms-default detail path is covered by test_default_policy_detailed_metrics.
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
        # lands in the first bucket, which holds everything under 1 ms.
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

    async def test_labels_reach_the_snapshot(self, metrics_cluster):
        """User labels are merged with the identity labels the core stamps."""
        metrics_cluster.enable_metrics(
            MetricsPolicy(labels=[{"team": "billing", "region": "us-west"}])
        )
        await _do_some_ops(metrics_cluster, count=2)

        document = (await metrics_cluster.metrics()).to_canonical_dict()
        assert document["labels"]["team"] == "billing"
        assert document["labels"]["region"] == "us-west"
        # Node identity is promoted to its own fields rather than left in the
        # user's label map.
        assert "node" not in document["labels"]
        assert "host" not in document["labels"]
        metrics_cluster.disable_metrics()

    async def test_batch_records_the_batch_command_types(self, metrics_cluster):
        """A multi-key call is counted as batch, not as its individual keys."""
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "sdk_metrics_batch")
        keys = [ds.id(f"b{i}") for i in range(5)]
        for key in keys:
            await session.upsert(key).put({"n": 1}).execute()

        result = await session.query(*keys).execute()
        rows = [row async for row in result]
        assert len(rows) == 5

        agg = (await metrics_cluster.metrics()).cluster_aggregated
        batch_reads = agg.command_histogram(CommandType.BATCH_READ)
        assert batch_reads.count >= 1, "a multi-key read should record as BATCH_READ"
        # One batch call is one measurement, not one per key.
        assert batch_reads.count < len(keys)
        metrics_cluster.disable_metrics()


class TestMetricsExport:
    """Snapshots reaching an exporter, against a live cluster."""

    async def test_exporter_receives_pushed_snapshots(self, metrics_cluster):
        """The timer pushes without anyone calling metrics()."""
        received = []

        class Recording:
            async def on_enable(self, cluster, settings):
                pass

            async def on_snapshot(self, snapshot):
                received.append(snapshot)

            async def on_node_close(self, host, snapshot):
                pass

            async def on_disable(self, cluster):
                pass

        metrics_cluster.metrics_exporter = Recording()
        try:
            metrics_cluster.enable_metrics(_SHAPE_SAFE)
            # The configured interval is 30s; drive one push directly rather
            # than waiting for it.
            await metrics_cluster._export_timer._export_once()
            assert len(received) == 1
            assert received[0].to_canonical_dict()["client_type"] == "python"
        finally:
            metrics_cluster.disable_metrics()
            metrics_cluster.metrics_exporter = None

    async def test_raw_tier_exposes_parsing_and_connection_acquisition(
        self, metrics_cluster
    ):
        """The per-command tier carries two histograms nothing else does.

        ``parsing`` and ``connection_aq`` are absent from the canonical
        document and from the log file, so this is the only route to
        client-side response handling and pool acquisition time. The guide
        documents them; this pins that they are actually populated.
        """
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "raw_tier")
        for i in range(30):
            await session.upsert(ds.id(i)).put({"n": i}).execute()
            await (await session.query(ds.id(i)).execute()).first()

        snapshot = await metrics_cluster.metrics()
        agg = snapshot.cluster_aggregated
        namespaces = agg.detailed_namespaces()
        assert general_namespace() in namespaces

        detail = agg.detailed_metric(general_namespace(), CommandType.GET)
        assert detail is not None, "reads were issued but Get has no detail"
        assert detail.latency.count >= 30
        for field in ("latency", "parsing", "connection_aq", "bytes_sent",
                      "bytes_received"):
            histogram = getattr(detail, field)
            assert histogram is not None, f"{field} missing"
            for attr in ("count", "min", "max", "sum", "average", "buckets"):
                assert hasattr(histogram, attr), f"{field}.{attr} missing"

        # Neither reaches the canonical document -- the reason this tier exists.
        document = snapshot.to_canonical_dict()
        flattened = repr(document)
        assert "parsing" not in flattened
        assert "connection_aq" not in flattened

        # An upsert records as Operate, not Put; the guide calls this out.
        assert agg.detailed_metric(general_namespace(), CommandType.OPERATE) is not None
        assert agg.detailed_metric(general_namespace(), CommandType.PUT) is None

        # The same surface exists per node, not only on the roll-up.
        node = next(iter(snapshot.nodes.values()))
        assert general_namespace() in node.detailed_namespaces()
        assert node.detailed_metric(general_namespace(), CommandType.GET) is not None
        metrics_cluster.disable_metrics()

    async def test_canonical_snapshot_across_multiple_nodes(self, metrics_cluster):
        """Per-node structure holds when there is more than one node.

        Single-node coverage cannot catch a per-node view that accidentally
        aggregates, reuses one node's values, or collides on a dict key --
        every such bug looks correct with one node in the list. Skips rather
        than passes vacuously when the cluster is single-node.
        """
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "multi_node")
        # Enough keys to spread across partitions, so every node does work.
        for i in range(400):
            await session.upsert(ds.id(i)).put({"n": i}).execute()
            await (await session.query(ds.id(i)).execute()).first()

        document = (await metrics_cluster.metrics()).to_canonical_dict()
        nodes = document["nodes"]
        if len(nodes) < 2:
            pytest.skip(f"cluster has {len(nodes)} node(s); needs 2+")

        # Identity is distinct per node, and the dict key cannot collide.
        names = [n["name"] for n in nodes]
        hosts = [f"{n['address']}:{n['port']}" for n in nodes]
        assert len(set(names)) == len(names), f"duplicate node names: {names}"
        assert len(set(hosts)) == len(hosts), f"duplicate hosts: {hosts}"

        # Every node reports its own namespace block and its own connections.
        for node in nodes:
            assert node["namespaces"], f"node {node['name']} has no namespace block"
            assert set(node["connections"]) == {"opened", "closed", "open"}

        # Work is spread, not attributed to one node: at least two nodes
        # recorded read latency. A view that aggregated would put it all on one.
        with_reads = [
            n["name"]
            for n in nodes
            for ns in n["namespaces"]
            if sum((ns.get("latency") or {}).get("read") or []) > 0
        ]
        assert len(set(with_reads)) >= 2, (
            f"read latency recorded on {set(with_reads)} of {len(nodes)} nodes; "
            "per-node attribution looks collapsed"
        )

        # Cluster totals agree with the per-node rows they summarize.
        assert document["cluster"]["total_nodes"] == len(nodes)
        assert document["cluster"]["open_connections"] == sum(
            n["connections"]["open"] for n in nodes
        )
        metrics_cluster.disable_metrics()

    async def test_node_departure_reaches_the_exporter(self, metrics_cluster):
        """A host the cluster no longer lists fires ``on_node_close`` once.

        Departure is judged by joining the snapshot's hosts against the live
        node list, because a snapshot keeps every host it ever saw. Stopping a
        real node mid-suite would be destructive and racy, so the live list is
        narrowed instead -- everything downstream of it (the export tick, the
        tracker, the exporter call, the snapshot handed over) is the real path.
        """
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "node_close")
        for i in range(5):
            await session.upsert(ds.id(i)).put({"n": i}).execute()

        class Recorder:
            def __init__(self):
                self.closed = []
                self.snapshots = 0

            async def on_enable(self, cluster, settings):
                pass

            async def on_snapshot(self, snapshot):
                self.snapshots += 1

            async def on_node_close(self, host, snapshot):
                self.closed.append((host, snapshot))

            async def on_disable(self, cluster):
                pass

        recorder = Recorder()
        snapshot = await metrics_cluster.metrics()
        hosts = [
            f"{n['address']}:{n['port']}"
            for n in snapshot.to_canonical_dict().get("nodes", [])
        ]
        assert hosts, "snapshot reported no nodes; cannot test departure"

        # The underlying client is a compiled extension type with read-only
        # attributes, so the live-node source is replaced at the cluster seam
        # the timer reads it through. metrics() still returns the real
        # snapshot.
        class _NoLiveNodes:
            async def nodes(self):
                return []

        class _EmptiedCluster:
            def __init__(self, real):
                self._real = real
                self._sdk_client = type(
                    "_Stub", (), {"underlying_client": _NoLiveNodes()}
                )()

            async def metrics(self):
                return await self._real.metrics()

        timer = AsyncMetricsExportTimer(_EmptiedCluster(metrics_cluster), recorder, 3600.0)

        await timer._export_once()
        assert recorder.snapshots == 1
        assert sorted(h for h, _ in recorder.closed) == sorted(hosts)
        # The snapshot handed over is the real one, so a file exporter can
        # write that node's final line from it.
        _, handed = recorder.closed[0]
        assert handed.to_canonical_dict()["nodes"]

        # Fires once per host, not once per export tick.
        await timer._export_once()
        assert sorted(h for h, _ in recorder.closed) == sorted(hosts)
        metrics_cluster.disable_metrics()

    async def test_mismatched_exporter_protocol_is_rejected(self, metrics_cluster):
        """A sync exporter on the async cluster fails at assignment, not later."""

        class SyncShaped:
            def on_enable(self, cluster, settings): ...
            def on_snapshot(self, snapshot): ...
            def on_node_close(self, host, snapshot): ...
            def on_disable(self, cluster): ...

        with pytest.raises(TypeError, match="AsyncMetricsExporter"):
            metrics_cluster.metrics_exporter = SyncShaped()

    async def test_learn_metrics_file_is_written_and_parses(
        self, metrics_cluster, tmp_path
    ):
        """The built-in exporter's output matches the header it emits."""
        from aerospike_sdk.metrics.export import LearnMetricsFileExporter

        exporter = LearnMetricsFileExporter(str(tmp_path))
        exporter.on_enable(None, None)
        try:
            metrics_cluster.enable_metrics(_SHAPE_SAFE)
            await _do_some_ops(metrics_cluster, count=3)
            exporter.on_snapshot(await metrics_cluster.metrics())
        finally:
            exporter.on_disable(None)
            metrics_cluster.disable_metrics()

        files = list(tmp_path.glob("metrics-*.log"))
        assert len(files) == 1
        lines = files[0].read_text().splitlines()
        assert lines[0].startswith("header(1)")
        cluster_line = lines[1]
        # Every segment the header advertises is present and closed.
        for segment in ("cluster[", "node[", "namespace[", "latency("):
            assert segment in cluster_line
        assert cluster_line.count("[") == cluster_line.count("]")
        assert "python" in cluster_line

