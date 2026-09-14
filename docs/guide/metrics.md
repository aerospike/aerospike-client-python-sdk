# Client Metrics

The client can record what it observes while executing commands — latency
histograms, byte counts, connection lifecycle, retry and timeout counters —
and hand it back as a structured snapshot. Collection happens inside the
client core and is **off by default**.

## What collection costs

Measured on a 3-node cluster, 50/50 read/update over 100K keys with 1KB
records, comparing each configuration against the same build with metrics
disabled:

| Configuration | Throughput vs disabled |
| --- | --- |
| Disabled (default) | baseline |
| `enable_metrics()`, full sampling | within ±2% |
| `enable_metrics()`, `Sampler.probability(0.1)` | within ±2% |
| `usage_enabled=True` | within ±2% |

That held across every client shape measured — blocking calls from 96 to 180
threads, and asyncio at task counts from 32 to 256, single-loop and pooled.
Latency percentiles were unchanged: p50 and p90 matched the disabled build in
nearly every cell. In short, collection is cheap enough that it is not a
reason to leave metrics off, and the sampler exists for tail fidelity and
recording volume rather than for throughput.

## Enabling and polling

Metrics are cluster-scoped: enable them on the
{class}`~aerospike_sdk.aio.cluster.Cluster` and poll snapshots at your export
interval.

```python
from aerospike_sdk import LatencyType

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

# Millisecond view (the default): 7 buckets covering
# <1, >=1, >=2, >=4, >=8, >=16, >=32 ms.
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
  first multiplies by `2**latency_shift`, so `shift=3` gives
  `<1, >=1, >=8, >=64, ...`. Bucket *i* holds samples in the half-open range
  `[2**i, 2**(i+1))` — see [Known limitations](#known-limitations), because
  these boundaries do not line up with the ones server-side latency tools
  report.
- A fractional `sampler` reduces how much is recorded, at the cost of tail
  fidelity. Read [Known limitations](#known-limitations) before choosing a
  rate: the decision is currently taken per network attempt rather than per
  API call, so the effective rate for a retried command is not the rate you
  set.
- Re-enabling with a changed latency unit or histogram shape discards the
  accumulated latency samples. Counters are always retained.

## Reading the snapshot

The canonical detail is per-node and per-command-type. Each node snapshot
(and the `cluster_aggregated` roll-up) carries a latency histogram per
command category, ~20 lifecycle counters, and per-namespace detail:

```python
from aerospike_sdk import CommandType

agg = snapshot.cluster_aggregated

# One command category, cluster-wide.
gets = agg.command_histogram(CommandType.GET)
print(f"{gets.count} reads, {gets.average:.0f} avg")

# The same command, narrowed to one namespace.
detail = agg.detailed_metric("prod-ns", CommandType.GET)
if detail is not None:
    print(detail.latency.count, detail.bytes_received.count)
```

{meth}`~aerospike_sdk.MetricsSnapshot.latency` derives the classic five-way
grouping (`conn`/`read`/`write`/`batch`/`query`) from those categories, and
{meth}`~aerospike_sdk.MetricsSnapshot.to_dict` renders the whole snapshot
with the cross-client-stable serialized names for logging or shipping to an
external system.

(raw-per-command-tier)=
## The raw per-command tier

There are three levels of detail, and the two documented above are not the
deepest. The canonical document is the cross-SDK shape: it merges the
per-command histograms the client core records into the five latency groups
and per-namespace counters. Underneath it sits everything core actually
recorded — roughly ten times as many values.

Reach it through `cluster_aggregated` for the whole cluster, or through any
entry of `nodes` for one node; both expose the same methods:

```python
from aerospike_sdk import CommandType

snapshot = await cluster.metrics()

agg = snapshot.cluster_aggregated
for namespace in agg.detailed_namespaces():
    for command in (CommandType.GET, CommandType.OPERATE, CommandType.BATCH_READ):
        detail = agg.detailed_metric(namespace, command)
        if detail is None:
            continue                      # that command was never issued
        print(namespace, command,
              detail.latency.count,
              detail.latency.average,
              detail.parsing.average,
              detail.connection_aq.average,
              detail.bytes_sent.average)

# The same, for a single node.
node = snapshot.nodes["10.0.0.1:3000"]
node.detailed_metric("prod-ns", CommandType.OPERATE)
```

`detailed_metric` returns `None` for a command the client never issued, so
check before use. Each `CommandMetric` carries five histograms, and every
histogram exposes `count`, `min`, `max`, `sum`, `average`, and `buckets`:

| field | what it measures |
| --- | --- |
| `latency` | round-trip time for that command |
| `parsing` | time spent parsing the response |
| `connection_aq` | time spent acquiring a connection |
| `bytes_sent` | request size |
| `bytes_received` | response size |

Two of those exist nowhere else. **`parsing` and `connection_aq` are absent
from the canonical document and from the log file** — so client-side response
handling and connection-pool acquisition time are only visible at this tier.
If a read looks slow and the server disagrees, this is where to look.

```{note}
Commands are recorded as the client issues them, not as you named them.
`session.upsert(...).put(...)` records under `CommandType.OPERATE`, not
`PUT`, because that is the wire command it sends — so `detailed_metric(ns,
CommandType.PUT)` returns `None` for a workload of upserts.
```

This tier is specific to this SDK and the client core beneath it. The
cross-SDK specification defines the canonical document, not this, so anything
built on `detailed_metric` is not portable to the other Aerospike clients and
may change shape as the core evolves. Prefer
{meth}`~aerospike_sdk.MetricsSnapshot.to_canonical_dict` unless you need
detail it does not carry.

(feature-usage-counters)=
## Feature usage counters

Separately from latency, the SDK can count **which of its features an
application uses** — point versus batch versus query, AEL versus expression
filters, transactions, background jobs. This is adoption insight rather than
operational monitoring, so it is off by default and independent of the
histograms:

```python
cluster.enable_metrics(MetricsPolicy(usage_enabled=True))

# ... application traffic ...

snapshot = await cluster.metrics()
print(snapshot.usage)
# {'feature.api.deferred': 1042, 'feature.shape.point': 900,
#  'feature.shape.batch': 142, 'feature.filter.ael': 37}
```

Or from the configuration file, which is the same key the other Aerospike
clients use:

```yaml
system:
  DEFAULT:
    metrics:
      enabled: true
      extended:
        usage:
          enabled: true
```

Counters are cumulative for the life of the client and are never sampled.
They are recorded in this SDK, so they count calls made through this API —
traffic issued through the underlying client directly does not appear.

The `execution_mode` of a call is carried in the counter name rather than as
a separate dimension: `feature.api.blocking`, `feature.api.deferred`, and
`feature.api.background`.

Three counters defined by the cross-SDK specification have no equivalent here
and are never emitted: `feature.object_mapping.read`,
`feature.object_mapping.write`, and `feature.batch.mixed`. This SDK has no
object-mapping API for them to count, so their absence is by definition
rather than an oversight.

## Exporting snapshots

Rather than polling, register an exporter and the client pushes a snapshot to
it on an interval. A cluster holds exactly one exporter; use a composite to
reach several destinations.

The async and sync clients take different protocols, because an exporter does
I/O and the async client must not block its event loop on it:

```python
from aerospike_sdk.metrics import AsyncMetricsExporter

class JsonExporter:
    async def on_enable(self, cluster, settings): ...
    async def on_snapshot(self, snapshot):
        await self._post(snapshot.to_canonical_dict())
    async def on_node_close(self, host, snapshot): ...
    async def on_disable(self, cluster): ...

cluster.metrics_exporter = JsonExporter()
```

Implement `MetricsExporter` (plain `def`) for the sync client and
`AsyncMetricsExporter` (`async def`) for the async one. `on_node_close` fires
once for a node that has left the cluster, carrying its final snapshot.

{meth}`~aerospike_sdk.MetricsSnapshot.to_canonical_dict` is the payload an
exporter should serialize: a stable `snake_case` document independent of how
the underlying client names its own fields.

Within each namespace object, `errors` is the total of *every* non-OK outcome.
Timeouts and hot keys are part of that total and are also reported on their own
as `timeouts` and `key_busy`, so those two are counted twice by design. An
exporter that wants a breakdown should subtract rather than add:

```python
namespace = document["nodes"][0]["namespaces"][0]
other_errors = namespace["errors"] - namespace["timeouts"] - namespace["key_busy"]
```

### Writing metrics to files

The built-in exporter writes the line-oriented metrics log format, for
existing log shippers:

```yaml
system:
  DEFAULT:
    metrics:
      enabled: true
      export_interval: 30s
      exporter: learn_metrics_file
      report_dir: /var/log/aerospike/metrics
      report_size_limit: 10mb
```

It is active only when `report_dir` is set; with no directory it does
nothing. `exporter: none` installs a no-op. Field names *inside* that file
stay camelCase for compatibility with existing parsers — the one place this
SDK does not use `snake_case`.

This file carries the legacy field list and nothing more: cluster identity,
per-node connections, per-namespace counters, and the latency histograms.
[Feature usage counters](#feature-usage-counters) are **not** written to it,
and neither are the cluster-level `exceeded_max_retries` /
`exceeded_total_timeout` counters. The format is defined outside this SDK and
read by tools such as `asloglatency`, so adding fields to it would make the
file non-interoperable with the other Aerospike clients that read and write
it. Anything outside that field list reaches a consumer through the canonical
snapshot — {meth}`~aerospike_sdk.MetricsSnapshot.to_canonical_dict` carries
all of it — via a custom exporter or a `cluster.metrics()` poll.

```{warning}
The `latency(columns,shift)` pair in this file carries no unit. The format
predates microsecond buckets, so a reader following the legacy convention —
including `asloglatency` — treats those buckets as milliseconds. With
`latency_unit: microseconds` the numbers are still correct but will be read as
1000× larger than they are.

The pair is written regardless of unit, because omitting it would leave the
histogram shape undescribed as well as its unit. Set
`latency_unit: microseconds` for file export only where whatever consumes the
files knows to expect microsecond buckets; the snapshot passed to a custom
exporter carries `latency_unit` explicitly and has no such ambiguity.
```

## Configuring metrics from a file

Everything above can come from the SDK configuration file instead of code, so
collection can be turned on in a deployment without a rebuild:

```yaml
system:
  DEFAULT:
    metrics:
      enabled: true
      latency_unit: microseconds
      latency_columns: 18
      latency_shift: 1
      sampler:
        range: 1000
        threshold: 100
      labels:
        owner: platform-team
      extended:
        usage:
          enabled: true
```

A block that never sets `enabled` leaves collection as it is, so a file that
only tunes the histogram shape does not switch collection on by itself.
Changing `enabled` in the file takes effect on reload, without reconnecting.

`metrics.extended.usage.enabled` switches on the [feature usage
counters](#feature-usage-counters), and defaults to off. It is nested under
`extended` to match the key the other Aerospike clients use.

Its sibling `metrics.extended.operational.*` is **not** recognized. Operational
collection here is part of a single on/off rather than a separately switchable
tier, so accepting that key would promise a split the snapshot cannot deliver;
it is reported as unrecognized and ignored. Usage counters are unaffected
because this SDK records them itself — see
[Known limitations](#known-limitations).

(known-limitations)=
## Known limitations

Client metrics are collected by the underlying client core, and several parts
of the model it implements differ from the cross-SDK specification. These are
current behavior, not bugs in configuration:

| Area | What to expect |
| --- | --- |
| **Bucket boundaries** | Bucket *i* covers `[2**i, 2**(i+1))`. Server-side latency tooling reports the adjacent layout `(2**i, 2**(i+1)]`, so bucket *counts* differ by the samples landing exactly on a boundary. Totals, the unit, and min/max/sum agree. |
| **Sampler granularity** | A fractional sampler decides per network attempt, not per API call. A command that retries gets more than one decision, so its effective sampling rate is higher than configured. |
| **Retried latency** | Each attempt is timed separately, so a retried operation records less than its true end-to-end duration; time spent in backoff between attempts is not represented. |
| **Collection tiers** | Collection is a single on/off. There is no separate always-on tier, and no way to enable error counters without also enabling latency histograms. |
| **TLS handshake counters** | Not collected. The connection counters that are reported do not distinguish TLS or authentication phases. |
| **Connection detail** | Only a single open-connection gauge is available; in-use versus idle-in-pool is not broken out. |
| **Recover queue** | Depth of the timeout-recovery queue is not collected. |
| **Cluster command and retry totals** | Cluster-wide command and retry counts are not collected. Per-node retry counts and the cumulative "exceeded retries/timeout" counters are. |
| **Log file carries the legacy fields only** | Usage counters and the cluster-level `exceeded_max_retries` / `exceeded_total_timeout` have no field in the legacy line format and are not written to it. The format is defined outside this SDK, so it is not extended. All of it is present in the canonical snapshot. |
| **Latency unit in the log file** | The legacy line format has no unit field, so `latency(columns,shift)` is read as milliseconds by `asloglatency` and similar tools. Microsecond buckets are written unchanged and will be misread by those consumers. The canonical snapshot is unaffected — it carries `latency_unit`. |
| **Bytes received** | `bytes_in` is always zero: the underlying histogram records a count but never accumulates the byte total. `bytes_out` is correct. |
| **Usage counter scope** | Usage counters are recorded in this SDK, so they cover calls made through this API only, and do not appear in the underlying client's own snapshot. |

The line-oriented log format additionally defines `inUse`, `inPool`,
`recoverQueueSize`, `commandCount` and `retryCount`. None can be filled from
what is collected, so they are omitted from the output rather than written as
zero, and the file's header line names them.

## What the latencies represent

Histograms measure **client-core transaction latency**: node selection,
connection acquisition, serialization, socket I/O, parsing, and retries —
what the caller waited for, per command. On the async surface, event-loop
submission queuing ahead of command start is not included.
