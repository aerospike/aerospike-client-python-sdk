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

from aerospike_sdk.metrics import CommandType, LatencyType, LatencyUnit, MetricsPolicy, Sampler
import os

from aerospike_sdk import UDFLang
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.metrics.export import AsyncMetricsExportTimer

from tests.integration.namespace import general_namespace


@pytest.fixture(scope="module")
async def metrics_cluster(aerospike_host, make_cluster_definition):
    """Module-scoped cluster so metrics state isn't shared with other suites."""
    async with make_cluster_definition(aerospike_host).connect() as c:
        yield c


# The core builds node metrics with its default histogram shape
# (microseconds / 24 columns); enabling with the same shape keeps every part of
# the snapshot populated, since a shape change resets the accumulated counts.
# The ms-default detail path is covered by test_default_policy_detailed_metrics.
_SHAPE_SAFE = MetricsPolicy(
    operational_enabled=True,
    latency_unit=LatencyUnit.MICROSECONDS,
    latency_columns=24,
)


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
        metrics_cluster.enable_metrics(
            MetricsPolicy(operational_enabled=True)  # milliseconds / 7
        )
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
            MetricsPolicy(
                operational_enabled=True,
                latency_unit=LatencyUnit.MICROSECONDS,
                latency_columns=18,
            )
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
            operational_enabled=True,
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
        assert d["total_nodes"] >= 1
        agg = d["cluster_aggregated_metrics"]
        assert agg["latency_unit"] == "us"
        assert agg["get_metrics"]["count"] >= 1
        metrics_cluster.disable_metrics()

    async def test_operational_off_records_only_the_always_on_gauges(
        self, aerospike_host, make_cluster_definition
    ):
        """Enabling metrics does not by itself start measuring commands."""
        async with make_cluster_definition(aerospike_host).connect() as c:
            c.enable_metrics(MetricsPolicy())
            await _do_some_ops(c, count=3)

            snapshot = await c.metrics()
            agg = snapshot.cluster_aggregated
            assert agg.command_histogram(CommandType.GET).count == 0
            assert agg.detailed_metric(general_namespace(), CommandType.GET) is None
            # The always-on tier is not gated with it.
            assert snapshot.open_connections >= 1
            assert snapshot.total_nodes >= 1

    async def test_record_udf_call_is_counted(self, aerospike_host, make_cluster_definition):
        """A record UDF call counts like any other deferred point call, plus its feature."""
        lua = os.path.join(os.path.dirname(__file__), "..", "udf", "record_example.lua")
        async with make_cluster_definition(aerospike_host).connect() as cluster:
            task = await cluster.register_udf_from_file(
                os.path.normpath(lua), "record_example.lua", UDFLang.LUA
            )
            await task.wait_till_complete(sleep_time=0.2, timeout=10.0)
            cluster.enable_metrics(MetricsPolicy(usage_enabled=True))
            session = cluster.create_session()
            key = DataSet.of(general_namespace(), "sdk_metrics_udf").id("k")
            await session.upsert(key).put({"n": 1}).execute()
            await session.execute_udf(key).function("record_example", "readBin").passing("n").execute()
            snapshot = await cluster.metrics()
            counts = snapshot.usage
            assert counts.get("feature.udf.record", 0) == 1, counts
            assert counts.get("feature.api.deferred", 0) == 2, counts
            assert counts.get("feature.shape.point", 0) == 2, counts
            assert snapshot.command_count == 2

    async def test_default_policy_detailed_metrics(
        self, aerospike_host, make_cluster_definition
    ):
        # A fresh cluster so the detail slots are created lazily AFTER the
        # ms/7 enable — the exact path the core bug loses.
        async with make_cluster_definition(aerospike_host).connect() as c:
            # Default shape: milliseconds / 7 columns.
            c.enable_metrics(MetricsPolicy(operational_enabled=True))
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
            MetricsPolicy(
                operational_enabled=True,
                labels=[{"team": "billing", "region": "us-west"}],
            )
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


    async def test_canonical_byte_totals_reflect_the_payload(self, metrics_cluster):
        """`bytes_in` is summed by this SDK, not read from a field.

        Bounded by the payload rather than pinned to a total: the framing
        overhead tracks bin-name length and record metadata, so an equality
        would fail on changes unrelated to the measurement. The bounds still
        catch both real failures -- summing to zero, and accumulating across
        commands instead of per command.
        """
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        payload, reads = 4096, 5
        session = metrics_cluster.create_session()
        key = DataSet.of(general_namespace(), "canonical_bytes").id("k1")
        await session.upsert(key).put({"blob": "x" * payload}).execute()
        for _ in range(reads):
            await session.get(key)

        document = (await metrics_cluster.metrics()).to_canonical_dict()
        namespaces = [
            ns
            for node in document["nodes"]
            for ns in node["namespaces"]
            if ns["name"] == general_namespace()
        ]
        assert namespaces, "the namespace produced no canonical entry"
        bytes_in = sum(ns["bytes_in"] for ns in namespaces)
        assert bytes_in >= reads * payload
        assert bytes_in < (reads + 2) * (payload + 512)


class TestMetricsExport:
    """Snapshots reaching an exporter, against a live cluster."""

    async def test_exporters_receive_pushed_snapshots(self, metrics_cluster):
        """The timer pushes to every registered exporter without anyone calling metrics()."""
        first, second = [], []

        class Recording:
            def __init__(self, into):
                self._into = into

            async def export(self, snapshot):
                self._into.append(snapshot)

        a, b = Recording(first), Recording(second)
        metrics_cluster.add_exporter(a)
        metrics_cluster.add_exporter(b)
        try:
            metrics_cluster.enable_metrics(_SHAPE_SAFE)
            # The configured interval is 30s; drive one push directly rather
            # than waiting for it.
            await metrics_cluster._export_timer._export_once()
            assert len(first) == 1 and len(second) == 1
            doc = first[0].to_canonical_dict()
            assert doc["client_type"] == "python"
            assert doc["nodes_departed"] == []
        finally:
            metrics_cluster.disable_metrics()
            metrics_cluster.remove_exporter(a)
            metrics_cluster.remove_exporter(b)

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

        # Byte histograms carry totals, not just sample counts. Asserting
        # the count alone cannot tell a measured total from one that
        # summed to zero.
        assert detail.bytes_sent.sum > 0
        assert detail.bytes_received.sum > 0

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
        assert document["cluster"]["nodes"]["active"] == len(nodes)
        cluster_conns = document["cluster"]["connections"]
        assert cluster_conns["open"] == sum(n["connections"]["open"] for n in nodes)
        assert cluster_conns["opened"] == sum(n["connections"]["opened"] for n in nodes)
        assert cluster_conns["closed"] == sum(n["connections"]["closed"] for n in nodes)
        metrics_cluster.disable_metrics()

    async def test_node_departure_reaches_nodes_departed(self, metrics_cluster):
        """A host the cluster no longer lists appears in ``nodes_departed`` once.

        Departure is judged by joining the snapshot's hosts against the live
        node list, because a snapshot keeps every host it ever saw. Stopping a
        real node mid-suite would be destructive and racy, so the live list is
        narrowed instead -- everything downstream of it (the export tick, the
        tracker, the departed section, the exporter call) is the real path.
        """
        from aerospike_sdk.metrics import MetricsSnapshot

        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "node_close")
        for i in range(5):
            await session.upsert(ds.id(i)).put({"n": i}).execute()

        snapshot = await metrics_cluster.metrics()
        hosts = [
            f"{n['address']}:{n['port']}"
            for n in snapshot.to_canonical_dict().get("nodes", [])
        ]
        assert hosts, "snapshot reported no nodes; cannot test departure"

        received = []

        class Recorder:
            async def export(self, snapshot):
                received.append(snapshot.to_canonical_dict())

        recorder = Recorder()

        # The live-node list rides on the snapshot itself, so the seam is the
        # cluster's metrics(): rebuild the real snapshot with an emptied node
        # list and let the real timer do everything downstream.
        class _EmptiedCluster:
            def __init__(self, real):
                self._real = real
                self._exporters = [recorder]

            async def metrics(self):
                real = await self._real.metrics()
                return MetricsSnapshot(real._pac, policy=real._policy, nodes=[])

        timer = AsyncMetricsExportTimer(_EmptiedCluster(metrics_cluster), 3600.0)

        await timer._export_once()
        assert len(received) == 1
        departed = [f"{n['address']}:{n['port']}" for n in received[0]["nodes_departed"]]
        assert sorted(departed) == sorted(hosts)
        # A departed node is no longer claimed as a member.
        assert received[0]["nodes"] == []
        # The final counters ride along, so a file exporter can write the
        # node's last line from them.
        assert received[0]["nodes_departed"][0]["connections"]["opened"] >= 0

        # Reported once per host, not once per export tick.
        await timer._export_once()
        assert received[1]["nodes_departed"] == []
        metrics_cluster.disable_metrics()

    async def test_mismatched_exporter_protocol_is_rejected(self, metrics_cluster):
        """A sync exporter on the async cluster fails at registration, not later."""

        class SyncShaped:
            def export(self, snapshot): ...

        with pytest.raises(TypeError, match="AsyncMetricsExporter"):
            metrics_cluster.add_exporter(SyncShaped())

    async def test_learn_metrics_file_is_written_and_parses(
        self, metrics_cluster, tmp_path
    ):
        """The built-in exporter's output matches the header it emits."""
        from aerospike_sdk.metrics.export import LearnMetricsFileExporter

        exporter = LearnMetricsFileExporter(str(tmp_path))
        try:
            metrics_cluster.enable_metrics(_SHAPE_SAFE)
            await _do_some_ops(metrics_cluster, count=3)
            exporter.export(await metrics_cluster.metrics())
        finally:
            exporter.close()
            metrics_cluster.disable_metrics()

        files = list(tmp_path.glob("metrics-*.log"))
        assert len(files) == 1
        lines = files[0].read_text().splitlines()
        assert lines[0].startswith("header(3)")
        cluster_line = lines[1]
        # Every segment the header advertises is present and closed.
        for segment in ("cluster[", "node[", "namespace[", "latency("):
            assert segment in cluster_line
        assert cluster_line.count("[") == cluster_line.count("]")
        assert "python" in cluster_line



class TestCommandCount:
    """The SDK-counted cluster command total: one per data-path API call."""

    async def test_counts_once_per_user_call(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "cmd_count")
        base = (await metrics_cluster.metrics()).command_count

        for i in range(3):
            await session.upsert(ds.id(i)).put({"n": i}).execute()
        single = await session.query(ds.id(0)).execute()
        assert (await single.first_or_raise()).is_ok
        batch = await session.query(ds.id(0), ds.id(1), ds.id(2)).execute()
        assert len(await batch.collect()) == 3

        after = (await metrics_cluster.metrics()).command_count
        # Three writes, one point read, one batch read: the batch is one
        # call however many keys it carries.
        assert after - base == 5

        doc = (await metrics_cluster.metrics()).to_canonical_dict()
        assert doc["cluster"]["command_count"] == after
        # Derived from the per-node counters the client core keeps.
        assert doc["cluster"]["command_retries"] >= 0
        assert "closed_idle" in doc["nodes"][0]["connections"]
        metrics_cluster.disable_metrics()

    async def test_disabled_metrics_freeze_the_count(self, metrics_cluster):
        metrics_cluster.enable_metrics(_SHAPE_SAFE)
        session = metrics_cluster.create_session()
        ds = DataSet.of(general_namespace(), "cmd_count_off")
        await session.upsert(ds.id(1)).put({"n": 1}).execute()
        metrics_cluster.disable_metrics()

        frozen = (await metrics_cluster.metrics()).command_count
        await session.upsert(ds.id(2)).put({"n": 2}).execute()
        assert (await metrics_cluster.metrics()).command_count == frozen
