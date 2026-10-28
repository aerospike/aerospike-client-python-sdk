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
from aerospike_sdk.metrics import LatencyType, MetricsPolicy

cluster.enable_metrics(MetricsPolicy(operational_enabled=True))

# ... application traffic ...

snapshot = cluster.metrics_snapshot()   # plain on both the async and sync Cluster
reads = snapshot.latency(LatencyType.READ)
print(f"{reads.count} reads, avg {reads.average:.1f} ms")
```

Snapshot values are **cumulative** since metrics were enabled — they are not
deltas since the last poll. Connection gauges (`in_use`, `in_pool`) are
point-in-time. Snapshotting drains and aggregates per-node state, so poll on
an interval (for example every 30 seconds), not per operation.

A snapshot can be taken whether or not collection is on; while it is off the
counters are frozen at their last values. `metrics_enabled`,
`operational_metrics_enabled` and `usage_metrics_enabled` on the snapshot (and
in its canonical document) say which tiers were collecting when it was taken.

## Collection tiers

Collection comes in tiers, so the cost of measuring is opt-in:

| Tier | Contents | Switched on by |
|---|---|---|
| Always on | Pool occupancy, connections opened/closed, tend counts, node membership | `enable_metrics()` |
| Operational | Latency and byte histograms, result codes, retry and error counters, the per-call command count | `operational_enabled=True` |
| Usage | Which SDK features the application uses | `usage_enabled=True` |

`enable_metrics()` on its own gives the always-on gauges and nothing more: no
per-command timing is recorded, and the latency histograms read zero. Ask for
the operational tier when you want them.

```python
cluster.enable_metrics(MetricsPolicy())                          # gauges only
cluster.enable_metrics(MetricsPolicy(operational_enabled=True))  # + histograms
```

The two extended tiers are independent — usage counters are recorded by this
SDK rather than by the client core, and ignore the sampler.

## Configuring collection

{class}`~aerospike_sdk.metrics.MetricsPolicy` controls the histogram shape and how
much is recorded. The shape settings configure the operational tier and do
nothing while it is off:

```python
from aerospike_sdk.metrics import LatencyUnit, MetricsPolicy, Sampler

# Millisecond view (the default): 7 buckets covering
# <=1, >1, >2, >4, >8, >16, >32 ms.
cluster.enable_metrics(MetricsPolicy(operational_enabled=True))

# Sub-millisecond resolution for fast clusters.
cluster.enable_metrics(MetricsPolicy(
    operational_enabled=True,
    latency_unit=LatencyUnit.MICROSECONDS,
    latency_columns=18,
))

# Record ~10% of calls on a high-throughput deployment.
cluster.enable_metrics(MetricsPolicy(
    operational_enabled=True,
    sampler=Sampler.probability(0.1),
))
```

- `latency_shift` spaces the bucket boundaries: each boundary after the
  first multiplies by `2**latency_shift`, so `shift=3` gives
  `<=1, >1, >8, >64, ...`. Bucket *i* holds samples in the range
  `(2**i, 2**(i+1)]`, the same layout server-side latency tools report, so
  the two line up bucket for bucket.
- A fractional `sampler` reduces how much is recorded, at the cost of tail
  fidelity. The decision is taken once per API call, before any retry, so a
  retried command is either fully recorded or not at all and the effective
  rate is the rate you set.
- Re-enabling with a changed latency unit or histogram shape discards the
  accumulated latency samples. Counters are always retained.

## Reading the snapshot

The canonical detail is per-node and per-command-type. Each node snapshot
(and the `cluster_aggregated` roll-up) carries a latency histogram per
command category, ~20 lifecycle counters, and per-namespace detail:

```python
from aerospike_sdk.metrics import CommandType

agg = snapshot.cluster_aggregated

# One command category, cluster-wide.
gets = agg.command_histogram(CommandType.GET)
print(f"{gets.count} reads, {gets.average:.0f} avg")

# The same command, narrowed to one namespace.
detail = agg.detailed_metric("prod-ns", CommandType.GET)
if detail is not None:
    print(detail.latency.count, detail.bytes_received.count)
```

{meth}`~aerospike_sdk.metrics.MetricsSnapshot.latency` derives the classic five-way
grouping (`conn`/`read`/`write`/`batch`/`query`) from those categories, and
{meth}`~aerospike_sdk.metrics.MetricsSnapshot.to_canonical_dict` renders the
snapshot as the cross-client document for logging or shipping to an external
system.

### Attributing a snapshot

Every snapshot names the application it came from, so a dashboard can group
client-side latency and errors the same way the server groups its own. The
`app_id` field is whichever of these applies first:

1. the identifier passed to {meth}`~aerospike_sdk.sync.cluster_definition.ClusterDefinition.app_id`,
2. the authenticated user, when the connection has one,
3. `not-set`.

It is never an empty string, so a consumer never needs a missing-value case.
Connection mode is not part of the cross-SDK snapshot identity.

The server's own user-agent record uses the same field, but only the first two
of those steps agree with it: it carries an identifier you set, otherwise
`not-set`. The authenticated-user fallback is metrics-only, so a connection
that authenticates as `reporting_svc` without naming an application appears as
`reporting_svc` in snapshots and `not-set` in the server's user-agent list. Set
`app_id` explicitly to make the two read alike.

Give distinct workloads distinct identifiers: `app_id` is the supported way to
separate them, since snapshots are not broken down by set.

(raw-per-command-tier)=
## The raw per-command tier

There are three levels of detail, and the two documented above are not the
deepest. The canonical document is the cross-SDK shape: it merges the
per-command histograms into whole-call cluster read/write/batch/query,
node connection-acquisition, and namespace RPC read/write/batch/query views.
Underneath it sits everything core actually recorded — roughly ten times as
many values.

Reach it through `cluster_aggregated` for the whole cluster, or through any
entry of `nodes` for one node; both expose the same methods:

```python
from aerospike_sdk.metrics import CommandType

snapshot = cluster.metrics_snapshot()

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
{meth}`~aerospike_sdk.metrics.MetricsSnapshot.to_canonical_dict` unless you need
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

snapshot = cluster.metrics_snapshot()
print(snapshot.usage)
# {'feature.api.deferred': 1042, 'feature.shape.point': 900,
#  'feature.shape.batch': 142, 'feature.filter.ael': 37}
```

Or from the configuration file, which is the same key the other Aerospike
clients use:

```yaml
version: "1.2.0"
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
`feature.api.background`. `feature.transaction` counts transactions opened,
once each however many calls run inside it; the calls themselves are counted
under their own shape and mode.

Three counters defined by the cross-SDK specification have no equivalent here
and are never emitted: `feature.object_mapping.read`,
`feature.object_mapping.write`, and `feature.batch.mixed`. This SDK has no
object-mapping API for them to count, so their absence is by definition
rather than an oversight.

## Exporting snapshots

Rather than polling, let the client push a snapshot on an interval, either to
the built-in [log file writer](#writing-metrics-to-files) or to exporters the
application registers. An exporter implements a single method,
`export(snapshot)`; it never sees the cluster or its lifecycle, and whatever
it writes to — a file, an HTTP client — it opens itself and the application
closes when done. The client never closes an exporter it did not create.

The async and sync clients take different protocols, because an exporter does
I/O and the async client must not block its event loop on it:

```python
from aerospike_sdk.metrics import AsyncMetricsExporter

class RegistryExporter:
    async def export(self, snapshot):
        self.latest = snapshot.to_canonical_dict()

cluster.add_exporter(RegistryExporter())
```

Implement `MetricsExporter` (plain `def export`) for the sync client and
`AsyncMetricsExporter` (`async def export`) for the async one. Exporters are
called in registration order with the same snapshot. It is valid for the
duration of `export`; copy `snapshot.to_canonical_dict()` inside the call if
the exporter retains data. One exporter raising never silences the others:
the error is logged and the rest still run. By default, an exporter
that fails three exports in a row is suspended and retried after 60 seconds.
Configure those values with `metrics.max_consecutive_failures` and
`metrics.suspend_retry_interval`. The retry delay is wall-clock time, not a
number of export intervals. A successful retry restores normal delivery.

Each exporter call is bounded by `metrics.export_timeout` (10 seconds by
default). A timed-out exporter is counted as failed and delivery continues to
the exporters registered after it. The same exporter is not invoked again
while its timed-out call is still unwinding; Python cannot forcibly terminate
arbitrary application code. An async timeout does not cancel the exporter,
because the timeout bounds SDK delivery rather than forcibly terminating
application-owned work.

Async exporter coroutines run on a dedicated metrics-owned daemon thread and
event loop, never on the application loop, tend thread, or command path.
They must not use clients, locks, queues, or other asyncio objects bound to
the application's event loop.

Disabling metrics and closing the cluster each push one last snapshot, so the
activity since the previous interval is not lost. It is taken before
collection stops. Sync shutdown interrupts the SDK metrics thread's wait for a
blocked custom exporter before attempting the final delivery. Python cannot
terminate arbitrary application code, so that exporter's daemon invocation
may remain until the call returns. The async client's `disable_metrics()` is a
plain method that returns straight away, while the closing snapshot is
delivered in the background; `await cluster.close()` waits for that delivery,
and re-enabling holds the next export until it finishes, so an exporter never
sees two exports at once. Changing the policy with `enable_metrics()` while
metrics are on restarts the push without a closing snapshot.

A node that has left the cluster remains under `nodes_departed`, with the same
shape as `nodes` and its final counters, until an export succeeds. On-demand
`cluster.metrics_snapshot()` calls peek at the same pending departures without
consuming them. A node that rejoins and later departs produces another final
sample.

### Choosing who receives snapshots

`metrics.exporter` in the configuration file selects the recipients of each
interval's snapshot. The value is case-insensitive:

| `exporter` | Each interval goes to |
| --- | --- |
| `file` | the built-in log file writer only, into `report_dir` |
| `custom` | the exporters registered with `add_exporter()` only |
| `none` | nobody; snapshots are still available by polling |
| unset | `file` |

The modes do not combine: with `exporter: file` a registered exporter is not
called, and with `exporter: custom` no file is written. Since the default is
`file`, an application that registers its own exporter also selects `custom`,
in the configuration file or in code on the cluster definition:

```python
from datetime import timedelta

from aerospike_sdk.metrics import MetricsExporterType
from aerospike_sdk.policy.system_settings import MetricsSettings, SystemSettings

definition = ClusterDefinition(host).with_system_settings(
    SystemSettings(
        metrics=MetricsSettings(
            exporter=MetricsExporterType.CUSTOM,
            export_timeout=timedelta(seconds=10),
            max_consecutive_failures=3,
            suspend_retry_interval=timedelta(seconds=60),
        )
    )
)
```

{meth}`~aerospike_sdk.metrics.MetricsSnapshot.to_canonical_dict` is the payload an
exporter should serialize: a stable `snake_case` document independent of how
the underlying client names its own fields.

The revised document keeps cluster totals at the top level. `cluster_latencies`
contains whole-call read/write/batch/query histograms; each node has the
four-field `connections` pool block and `conn_latency`; namespace `latencies`
contain RPC read/write/batch/query histograms. `usage` contains only the
approved short names (`blocking`, `deferred`, `background`, shape,
object-mapping, and AEL/EXP filter counters). Deferred connection close
reasons, pool exhaustion, node error rate, and `conn_type` are not part of the
v1 document.

Within each namespace object, each failed command is counted under exactly
one of `server_timeouts`, `key_busy`, `record_too_big`, `device_overload` or
`errors`; `errors` holds whatever has none of the four named causes. The
total failure count is therefore their sum:

```python
namespace = document["nodes"][0]["namespaces"][0]
causes = ("errors", "server_timeouts", "key_busy", "record_too_big", "device_overload")
failures = sum(namespace[k] for k in causes)
```

These are server answers. The native layer cannot yet attribute a client-side
timeout to a namespace, so the shared `client_timeouts` field remains zero
until that native count is available. See [Known limitations](#known-limitations).

(writing-metrics-to-files)=
### Writing metrics to files

The built-in {class}`~aerospike_sdk.metrics.MetricsWriter` writes
the line-oriented metrics log shared by the Aerospike clients, for existing log
shippers and tools such as `asloglatency`. It is the default recipient: set a
directory and the client writes there on every interval.

```yaml
version: "1.2.0"
system:
  DEFAULT:
    metrics:
      enabled: true
      export_interval: 30s
      export_timeout: 10s
      max_consecutive_failures: 3
      suspend_retry_interval: 60s
      exporter: file
      report_dir: /var/log/aerospike/metrics
      report_size_limit: 10mb
```

With no `report_dir` nothing is written. Files are named
`metrics-<yyyyMMddHHmmss>.log`; once one reaches `report_size_limit` the next
line starts a new file. The limit is 0 (never rotate, the default) or at least
1,000,000 bytes. Re-enabling with a different histogram shape also starts a new
file, so that each file's header describes every line beneath it.

Every line begins with a local timestamp (`yyyy-MM-dd HH:mm:ss.SSS`). The
first line of a file is the header declaring the schema, and each later line
is one snapshot, positional rather than named:

```text
header(5) cluster[cluster_name,client_type,client_version,app_id,labels[],cpu,mem,recover_queue_size,nodes_invalid,command_count,command_retries,nodes[]] labels[name,value] nodes[name,address,port,conns_in_use,conns_in_pool,conns_opened,conns_closed,namespaces[]] namespaces[name,errors,timeouts,key_busy,bytes_in,bytes_out,latency[]] latency(MILLISECONDS,7,1)[type[l1,l2,l3...]]
```

- `client_type` is `python-sdk`.
- `cpu` and `mem` are this process's CPU share and resident memory, sampled
  when the snapshot is built. `cpu` is CPU time consumed since the previous
  snapshot as a percentage of the wall time elapsed, so a busy process on
  several cores can read above 100. `mem` is the resident set size in bytes:
  current on Linux, and the process's peak on macOS, which is what the platform
  reports without a third-party dependency.
- `command_count` and `command_retries` are the cluster counters described in
  [Known limitations](#known-limitations). They read 0 unless the operational
  tier is on.
- Usage counters remain on the canonical snapshot and structured exporters;
  they are not added to this positional compatibility file.
- Nodes that left the cluster since the previous line are written immediately
  afterward as separate final `node[...]` lines.
- The legacy file grammar still carries five namespace latency positions.
  Its `conn` position is populated from node `conn_latency`;
  write/read/batch/query retain their namespace positions.

The file can also be written from code: construct the writer and register it
like any exporter, under `custom` mode. Its first `export` opens the file and
writes the header, and the application closes it:

```python
from aerospike_sdk.metrics import MetricsWriter

exporter = MetricsWriter("/var/log/aerospike/metrics")
cluster.add_exporter(exporter)
...
exporter.close()
```

```{warning}
The header names the latency unit, but tools that predate microsecond buckets
read every histogram as milliseconds. With `latency_unit: microseconds` the
numbers are still correct but such a tool reads them as 1000× larger than they
are. Set it for file export only where whatever consumes the files reads the
unit; the canonical snapshot carries `latency_unit` explicitly.
```

## Configuring metrics from a file

Everything above can come from the SDK configuration file instead of code, so
collection can be turned on in a deployment without a rebuild:

```yaml
version: "1.2.0"
system:
  DEFAULT:
    metrics:
      enabled: true
      labels:
        owner: platform-team
      extended:
        operational:
          enabled: true
          latency_unit: microseconds
          latency_columns: 18
          latency_shift: 1
          sampler:
            range: 1000
            threshold: 100
        usage:
          enabled: true
```

The file mirrors the tiers: `metrics.enabled` turns collection on at all, and
each extended tier has its own `enabled` beneath it. The histogram shape and
the sampler live inside `metrics.extended.operational` with the tier they
configure — at the `metrics` root they are reported as unrecognized, since
there is nothing there for them to shape.

A block that never sets `enabled` does not switch collection on by itself. If
collection is already on, its collection and delivery settings are refreshed
without reconnecting. Changing `enabled` also takes effect on reload.

While a loaded file sets `metrics.enabled`, the file owns that switch:
`enable_metrics()` and `disable_metrics()` raise `AerospikeError` with
`PARAMETER_ERROR` rather than make a change the next reload would undo. A
file that leaves `enabled` unset leaves the switch to code.

(known-limitations)=
## Known limitations

Client metrics are collected by the underlying client core, and several parts
of the model it implements differ from the cross-SDK specification. These are
current behavior, not bugs in configuration:

| Area | What to expect |
| --- | --- |
| **Two latency measures** | Per-namespace `latencies` time each network attempt, so a retried command contributes one sample per attempt. Top-level `cluster_latencies` time the whole call, first attempt through the last retry. Node `conn_latency` measures connection acquisition. |
| **Error counters follow the histograms** | The operational tier is one switch: there is no way to record result codes and retry counters without also recording latency histograms. |
| **Command count scope** | The cluster `command_count` is counted by this SDK, one per API call while the operational tier is on — data path, background job registration and admin commands alike. It is exact rather than sampled, and counts calls made through this SDK only. The cluster `command_retries` is the per-node retry counters summed and has no such scope caveat — retries happen inside the client core, so it covers all traffic. |
| **Native fields not yet available** | The Rust layer does not expose delay-queue timeout count, pre-node validation open/TLS/auth failures, namespace-attributed client timeouts, or an authoritative client-wide batch-partial-failure count. The fixed shared snapshot carries these fields as zero until native supplies them. |
| **Latency unit in the log file** | Older consumers of the line format read latency as milliseconds regardless of the unit the header names. Microsecond buckets are written unchanged and will be misread by those tools. The canonical snapshot is unaffected — it carries `latency_unit`. |
| **Usage counter scope** | Usage counters are recorded in this SDK, so they cover calls made through this API only, and do not appear in the underlying client's own snapshot. |

The canonical snapshot carries the log file's `cpu` and `mem` values as
`cpu_percent` and `memory_bytes`.

## What the latencies represent

Histograms measure **client-core transaction latency**: node selection,
connection acquisition, serialization, socket I/O, parsing, and retries —
what the caller waited for, per command. On the async surface, event-loop
submission queuing ahead of command start is not included.
