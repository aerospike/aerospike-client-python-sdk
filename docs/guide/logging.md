# Logging

The SDK produces logs from three layers, each identified by its
Python logger name:

| Logger name | Layer | Description |
|-------------|-------|-------------|
| `aerospike_sdk` | Aerospike Python SDK | Command summaries, queries, lifecycle |
| `aerospike_async` | Python Async Client (PAC) | Client init, connection lifecycle |
| `aerospike_core` | Rust core | Cluster tend, connection pooling, wire protocol |

All three layers use Python's standard `logging` module, so any handler or
formatter you configure will receive messages from all layers. The SDK never
configures handlers, formatters, or levels on import — those are host
application choices.

## Quick Start

The simplest way to enable logging is via environment variables:

```bash
export AEROSPIKE_LOG_LEVEL=DEBUG
export AEROSPIKE_LOG_FILE=/tmp/aerospike.log   # optional; omit for stderr
```

The test suite and example scripts read these variables automatically (see
`conftest.py` and `examples/_env.py`).

## Component loggers

Below the `aerospike_sdk` root, diagnostics are split into stable
operator-tunable areas, so verbosity can be raised for one concern without
enabling everything. The names are published as constants on
{class}`~aerospike_sdk.loggers.SdkLoggers`:

| Logger name | What it logs | Typical levels |
|-------------|--------------|----------------|
| `aerospike_sdk.command` | One summary per point op / key-batch: op type, namespace, set, key count, latency; failures with result code + exception type | DEBUG |
| `aerospike_sdk.behavior` | Named behaviors and SDK config file loading / hot-reload (`AEROSPIKE_SDK_CONFIG_URL`) | INFO load/reload, WARNING file problems |
| `aerospike_sdk.query` | Dataset / secondary-index query execution summaries | DEBUG |
| `aerospike_sdk.lifecycle` | Client connect / close on both surfaces | INFO connect/close, DEBUG detail |
| `aerospike_sdk.pool` | `AsyncPool` start / stop, per-client connect issues | INFO start/stop, WARNING errors |
| `aerospike_sdk.info` | Info-protocol helpers (namespace details, index metadata) | DEBUG (failures) |
| `aerospike_sdk.background` | Background write / UDF task submission | DEBUG |
| `aerospike_sdk.record_stream` | Chunked stream fetches, close diagnostics | DEBUG |

For example, to see per-operation command summaries only:

```python
import logging

logging.getLogger("aerospike_sdk.command").setLevel(logging.DEBUG)
```

A command summary line looks like:

```
DEBUG aerospike_sdk.command: upsert test.users keys=1 latency_ms=0.412
DEBUG aerospike_sdk.command: batch test.users keys=200 latency_ms=3.981
DEBUG aerospike_sdk.command: delete failed rc=2 exc=RecordNotFoundError
```

Command and query loggers sit on the hot path: when their level is off the
cost is a single cached check per operation, but enabling them at high
throughput produces one line per operation — prefer enabling them narrowly
during incidents rather than in steady-state production.

### Rust-side loggers

Cluster tend, connection pools, routing, and the wire protocol are
implemented in the Rust core and log under `aerospike_core.*`. Logger names
mirror the Rust module tree, so areas can be enabled individually:

```python
# Topology / tend diagnostics only — no per-command noise:
logging.getLogger("aerospike_core.cluster").setLevel(logging.DEBUG)
```

Membership events (seeding, node refresh failures, node close), partition
map updates, and rack updates log under `aerospike_core.cluster*` — INFO and
WARNING for milestones and failures, DEBUG for per-tend detail. The exact
set of sub-logger names is owned by the PAC/Rust release and may evolve;
consult PAC release notes.

### Runtime level changes and Rust loggers

Levels on `aerospike_sdk.*` loggers are read live by Python's `logging` and
take effect immediately. Rust-emitted loggers (`aerospike_core.*`,
`aerospike_async`) cache their effective level on first emission for
performance. The SDK re-syncs that cache automatically on every client
connect, so configuration done between import and connect is always honored.
To change Rust-side levels **while connected**, call
{func}`~aerospike_sdk.loggers.refresh_log_levels` after `setLevel()`:

```python
import logging
from aerospike_sdk import refresh_log_levels

logging.getLogger("aerospike_core.cluster").setLevel(logging.DEBUG)
refresh_log_levels()
```

## Programmatic Configuration

For full control, configure the loggers directly:

```python
import logging

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
))

for name in ("aerospike_core", "aerospike_async", "aerospike_sdk"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
```

You can set different levels per layer — for example, `WARNING` for the core
and `DEBUG` for the SDK:

```python
logging.getLogger("aerospike_core").setLevel(logging.WARNING)
logging.getLogger("aerospike_sdk").setLevel(logging.DEBUG)
```

### Production configuration (`dictConfig`)

A production-shaped baseline keeps everything at WARNING and names the
narrow loggers you may raise during incidents:

```python
import logging.config

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s %(levelname)-8s %(name)s: %(message)s"},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "standard"},
    },
    "loggers": {
        "aerospike_core": {"level": "WARNING"},
        "aerospike_async": {"level": "INFO"},
        "aerospike_sdk": {"level": "WARNING"},
        # Raise one of these during an incident, e.g.:
        # "aerospike_sdk.command": {"level": "DEBUG"},
        # "aerospike_core.cluster": {"level": "DEBUG"},
    },
    "root": {"level": "WARNING", "handlers": ["console"]},
})
```

Enable narrow DEBUG loggers (for example `aerospike_sdk.command` only)
during incidents rather than setting DEBUG on all three roots — Rust
command-path DEBUG can flood log pipelines at production throughput.

## Log Output

Using a formatter that names both the layer and the structured cluster field:

```python
logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s [%(aerospike.cluster)s]: %(message)s",
    defaults={"aerospike.cluster": "-"},
)
```

typical log lines look like:

```
2026-07-10 14:22:01,234 INFO     aerospike_sdk.lifecycle [prod-1]: Connected seeds='127.0.0.1:3100'
2026-07-10 14:22:01,289 DEBUG    aerospike_async [-]: connected to 127.0.0.1:3100
2026-07-10 14:22:01,290 DEBUG    aerospike_core.cluster [-]: Tending cluster...
2026-07-10 14:22:01,301 DEBUG    aerospike_sdk.command [prod-1]: upsert test.users keys=1 latency_ms=0.412
```

The `%(name)s` field identifies which layer produced the message, making it
straightforward to filter or route logs. The `[%(aerospike.cluster)s]` field
shows the configured cluster name on the connect and command/query lines that
carry it, and the `defaults` fallback `-` on the `aerospike_async` /
`aerospike_core` lines that don't. If you drop `%(aerospike.cluster)s` from the
formatter (as the config snippets below do), the field is simply not rendered —
the records still carry it. See [Structured cluster field](#structured-cluster-field)
for why only some lines populate it and the `defaults=` requirement.

(structured-cluster-field)=
### Structured cluster field

Client connect lines and per-operation command / query summaries carry the
configured cluster name as a structured `aerospike.cluster` field (via the
stdlib `extra` mechanism), so multi-cluster processes can attribute events to a
cluster in JSON log pipelines. The value is the name set through
`validate_cluster_name_is(...)`, or `None` when cluster-name validation is not
configured. A JSON formatter picks it up directly:

```python
import json
import logging

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"level": record.levelname, "logger": record.name,
                   "message": record.getMessage()}
        cluster = getattr(record, "aerospike.cluster", None)
        if cluster is not None:
            payload["aerospike.cluster"] = cluster
        return json.dumps(payload)
```

With that formatter, connect and command lines render as:

```json
{"level": "INFO", "logger": "aerospike_sdk.lifecycle", "message": "Connected seeds='127.0.0.1:3100'", "aerospike.cluster": "prod-1"}
{"level": "DEBUG", "logger": "aerospike_sdk.command", "message": "upsert test.users keys=1 latency_ms=0.446", "aerospike.cluster": "prod-1"}
{"level": "DEBUG", "logger": "aerospike_sdk.command", "message": "read test.users keys=2 latency_ms=1.943", "aerospike.cluster": "prod-1"}
```

The cluster name is read lazily only when a summary is actually emitted (i.e.
when the `command` / `query` logger is at DEBUG), so tagging adds no cost on the
disabled hot path. The tag reflects the *configured* validation name, not a
server-reported value; without `validate_cluster_name_is(...)` the field is
`None` and the connect line remains the correlation anchor.

To surface the field in a plain-text formatter, reference it by its dotted key
**and** supply a `defaults` fallback (Python 3.10+). Most records — everything
from `aerospike_core`, `aerospike_async`, third-party libraries, and even SDK
lines emitted before connect — do not carry the field, and a bare
`%(aerospike.cluster)s` raises `ValueError: Formatting field not found in
record` on the first such line. The default renders those as `-`:

```python
import logging

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s [%(aerospike.cluster)s]: %(message)s",
    defaults={"aerospike.cluster": "-"},
))
```

That yields, with `validate_cluster_name_is("prod-1")` configured:

```
2026-07-10 14:22:01,234 INFO     aerospike_sdk.lifecycle [prod-1]: Connected seeds='127.0.0.1:3100'
2026-07-10 14:22:01,289 DEBUG    aerospike_async [-]: connected to 127.0.0.1:3100
2026-07-10 14:22:01,301 DEBUG    aerospike_sdk.command [prod-1]: upsert test.users keys=1 latency_ms=0.412
```

## File Logging

To write logs to a file instead of stderr:

```python
handler = logging.FileHandler("/tmp/aerospike.log")
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
))

for name in ("aerospike_core", "aerospike_async", "aerospike_sdk"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
```

Or via environment variables in `aerospike.env`:

```bash
export AEROSPIKE_LOG_LEVEL=DEBUG
export AEROSPIKE_LOG_FILE=/tmp/aerospike.log
```

## What is never logged

The SDK does not write user data to diagnostic logs at any level:

- record keys, bin names, or bin values
- query / scan filter literals or expression text containing values
- batch key lists, UDF arguments, operation payloads
- credentials, tokens, or TLS material

Log lines carry operational metadata only: cluster topology names
(namespace, set, `host:port`), operation types, result codes, counts, and
latencies. Failure lines log the exception *type* and result code rather
than the exception message, since message text can echo user data. This
holds at DEBUG as well — content-level debugging belongs in tests and local
captures, not production logs.

## Recommended Levels

| Level | Use case |
|-------|----------|
| `WARNING` (default) | Production — only unexpected conditions |
| `INFO` | Connection lifecycle, pool start/stop, index creation events |
| `DEBUG` | Full detail: command summaries, cluster tend cycles, expression parsing |
