# Client Metrics

The client can record what it observes while executing commands — latency
histograms, byte counts, connection lifecycle, retry and timeout counters —
and hand it back as a structured snapshot. Collection happens inside the
client core with negligible overhead when disabled, and is **off by
default**.

## Enabling and polling

Metrics are cluster-scoped: enable them on the
{class}`~aerospike_sdk.aio.cluster.Cluster` and poll snapshots at your export
interval.

```python
from aerospike_sdk import LatencyType, MetricsPolicy

cluster.enable_metrics()

# ... application traffic ...

snapshot = await cluster.metrics()   # sync: cluster.metrics()
reads = snapshot.latency(LatencyType.READ)
print(f"{reads.count} reads, avg {reads.average:.1f} ms")
```

Snapshot values are **cumulative** since metrics were enabled — they are not
deltas since the last poll. Connection gauges (`open_connections`) are
point-in-time. Snapshotting drains and aggregates per-node state, so poll on
an interval (for example every 30 seconds), not per operation.

## Configuring collection

{class}`~aerospike_sdk.MetricsPolicy` controls the histogram shape and how
much is recorded:

```python
from aerospike_sdk import LatencyUnit, MetricsPolicy, Sampler

# Classic milliseconds view (the default): 7 buckets,
# <=1, >1, >2, >4, >8, >16, >32 ms.
cluster.enable_metrics(MetricsPolicy())

# Sub-millisecond resolution for fast clusters.
cluster.enable_metrics(MetricsPolicy(
    latency_unit=LatencyUnit.MICROSECONDS,
    latency_columns=18,
))

# Record ~10% of calls on a high-throughput deployment.
cluster.enable_metrics(MetricsPolicy(sampler=Sampler.probability(0.1)))
```

- `latency_shift` spaces the bucket boundaries: each boundary after the
  first bucket multiplies by `2**latency_shift`, so `shift=3` gives
  `<=1, >1, >8, >64, ...`.
- A fractional `sampler` decides **once per call** whether the whole call is
  measured — a retried command is either fully recorded (with its retries
  included in the latency) or not at all. Fractional sampling trades p99
  fidelity for lower overhead; prefer full recording unless collection cost
  is measurable.
- Re-enabling with a changed latency unit or histogram shape discards the
  accumulated latency samples. Counters are always retained.

## Reading the snapshot

The canonical detail is per-node and per-command-type. Each node snapshot
(and the `cluster_aggregated` roll-up) carries a latency histogram per
command category, ~20 lifecycle counters, and per-namespace detail:

```python
from aerospike_sdk import CommandType

agg = snapshot.cluster_aggregated
gets = agg.command_histogram(CommandType.GET)
detail = agg.detailed_metric("prod-ns", CommandType.GET)
if detail is not None:
    print(detail.latency.count, detail.bytes_received.count)
```

{meth}`~aerospike_sdk.MetricsSnapshot.latency` derives the classic five-way
grouping (`conn`/`read`/`write`/`batch`/`query`) from those categories, and
{meth}`~aerospike_sdk.MetricsSnapshot.to_dict` renders the whole snapshot
with the cross-client-stable serialized names for logging or shipping to an
external system.

## What the latencies represent

Histograms measure **client-core transaction latency**: node selection,
connection acquisition, serialization, socket I/O, parsing, and retries —
what the caller waited for, per command. On the async surface, event-loop
submission queuing ahead of command start is not included.
