#!/usr/bin/env python3
"""Client metrics: collecting, reading, and exporting.

Covers the three things an application does with metrics — turn collection on,
read a snapshot, and push snapshots somewhere on an interval.

Part 1 enables collection, runs traffic, and reads the derived latency views.
Part 2 turns on feature-usage counters, which answer "which parts of the SDK
does this application actually use" rather than "how fast is it".
Part 3 registers an exporter and watches it receive a pushed snapshot.

Bucket boundaries and sampler granularity have caveats worth knowing before
you act on the numbers; see the "Known limitations" section of
``docs/guide/metrics.md``.
"""

import asyncio

import _env
from aerospike_sdk import (
    CommandType,
    DataSet,
    LatencyType,
    LatencyUnit,
    MetricsPolicy,
    Sampler,
)


async def _traffic(cluster, count: int) -> None:
    """Write and read *count* records so there is something to measure."""
    session = cluster.create_session()
    dataset = DataSet.of("test", "metrics_example")
    for i in range(count):
        await session.upsert(dataset.id(i)).put({"n": i}).execute()
        result = await session.query(dataset.id(i)).execute()
        await result.first_or_raise()


async def part1_collect_and_read(cluster) -> None:
    """Enable collection, run traffic, and read the snapshot."""
    print("\n=== 1. Collecting latency ===")

    # Microseconds here because a local cluster answers in well under a
    # millisecond, where the default millisecond buckets would put every
    # sample in the first column.
    cluster.enable_metrics(
        MetricsPolicy(latency_unit=LatencyUnit.MICROSECONDS, latency_columns=24)
    )
    await _traffic(cluster, 25)

    snapshot = await cluster.metrics()
    print(f"  nodes={snapshot.total_nodes} open_connections={snapshot.open_connections}")

    for kind in (LatencyType.READ, LatencyType.WRITE):
        histogram = snapshot.latency(kind)
        if histogram.count:
            print(
                f"  {kind.name:5s} count={histogram.count:3d} "
                f"avg={histogram.average:7.1f}{histogram.latency_unit}"
            )

    # The five-way grouping above is derived; the canonical detail is per
    # command type.
    aggregated = snapshot.cluster_aggregated
    gets = aggregated.command_histogram(CommandType.GET)
    print(f"  GET command histogram: count={gets.count}")

    # A fractional sampler records less. Re-enabling with a different shape
    # discards the samples collected so far.
    cluster.enable_metrics(
        MetricsPolicy(
            latency_unit=LatencyUnit.MICROSECONDS,
            latency_columns=24,
            sampler=Sampler.probability(0.1),
        )
    )
    print("  re-enabled at a 10% sample rate (previous samples discarded)")


async def part2_feature_usage(cluster) -> None:
    """Count which SDK features the application exercises."""
    print("\n=== 2. Feature usage ===")

    cluster.enable_metrics(MetricsPolicy(usage_enabled=True))
    await _traffic(cluster, 10)

    snapshot = await cluster.metrics()
    for name, count in sorted(snapshot.usage.items()):
        print(f"  {name:32s} {count}")
    if not snapshot.usage:
        print("  (no counters — usage_enabled was not set)")


async def part3_export(cluster) -> None:
    """Push snapshots to an exporter instead of polling for them."""
    print("\n=== 3. Exporting ===")

    class PrintingExporter:
        """Minimal AsyncMetricsExporter: report what arrives, keep nothing."""

        def __init__(self) -> None:
            self.snapshots = 0

        async def on_enable(self, cluster, settings) -> None:
            print("  exporter enabled")

        async def on_snapshot(self, snapshot) -> None:
            self.snapshots += 1
            document = snapshot.to_canonical_dict()
            reads = document["nodes"][0]["namespaces"]
            print(
                f"  snapshot #{self.snapshots}: "
                f"{document['client_type']} {document['client_version']}, "
                f"{len(document['nodes'])} node(s), {len(reads)} namespace(s)"
            )

        async def on_node_close(self, host, snapshot) -> None:
            print(f"  node left the cluster: {host}")

        async def on_disable(self, cluster) -> None:
            print("  exporter disabled")

    exporter = PrintingExporter()
    cluster.metrics_exporter = exporter

    # The export interval comes from configuration; without a config file it
    # is 30s, which is too long to demonstrate, so this reads the snapshot the
    # exporter would have been handed.
    cluster.enable_metrics(MetricsPolicy(latency_unit=LatencyUnit.MICROSECONDS))
    await _traffic(cluster, 5)
    await exporter.on_snapshot(await cluster.metrics())

    cluster.disable_metrics()
    print(f"  metrics_enabled after disable: {cluster.metrics_enabled()}")


async def main() -> None:
    async with await _env.connect().connect() as cluster:
        await part1_collect_and_read(cluster)
        await part2_feature_usage(cluster)
        await part3_export(cluster)


if __name__ == "__main__":
    asyncio.run(main())
