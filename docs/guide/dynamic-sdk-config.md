# Dynamic SDK Configuration

SDK-level settings can come from a YAML file named by the
`AEROSPIKE_SDK_CONFIG_URL` environment variable (a `file://` URL or a bare
path). The file is read at `connect()` and watched for changes afterward —
edits are picked up on a running client within about a second, so operators
can retune a live deployment without a restart.

:::{note}
This SDK configuration file is unrelated to the cross-client
`AEROSPIKE_CLIENT_CONFIG_URL`, which the underlying client honors
independently. The two use separate environment variables and separate
schemas; they can both be set in the same process without conflict.
:::

## System settings (`system:`)

```yaml
# AEROSPIKE_SDK_CONFIG_URL=file:///etc/aerospike/sdk-config.yaml
version: "1.0.0"
system:
  DEFAULT:
    connections:
      minimumConnectionsPerNode: 10
      maximumConnectionsPerNode: 300
      maximumSocketIdleTime: 55s
    circuitBreaker:
      numTendIntervalsInErrorWindow: 2
      maximumErrorsInErrorWindow: 100
    refresh:
      tendInterval: 1s
    transactions:
      implicitBatchWriteTransactions: true
      sleepBetweenAttempts: 1000ms
      numberOfAttempts: 5
```

The `system:` section holds named profiles. `DEFAULT` applies to every
cluster; a profile whose name matches the cluster name declared via
`validate_cluster_name_is(...)` layers on top of `DEFAULT`. Effective
settings resolve per field, highest layer first:

1. file cluster-name profile (`system.<cluster-name>`)
2. file `DEFAULT` profile
3. programmatic settings (`with_system_settings(...)`)
4. hard defaults

A field absent from one layer falls through to the next, so the file wins
only for the fields it actually provides. Durations use a unit suffix
(`250ms`, `55s`, `5m`, `2h`).

Two kinds of settings live in the file. The `connections`, `circuitBreaker`,
and `refresh` groups configure the connection itself and take effect at
`connect()`; changing them on a running client applies on the next connect.
The `transactions` group is SDK-runtime configuration, read at operation
time — hot-reloaded changes take effect on the next operation.
`implicitBatchWriteTransactions` (default `true`) controls whether
multi-key write batches on strong-consistency namespaces are wrapped in
[implicit transactions](transactions.md);
`numberOfAttempts` (default `5`) and `sleepBetweenAttempts` (default
`1000ms`) drive their retry loop on transient conflicts. See
[`SystemSettings`](../api/system-settings.md) for the programmatic
equivalents.

## Named behaviors (`behaviors:`)

The same file can define named operation-policy profiles that become
registered [`Behavior`](../api/behavior.md) objects at `connect()`:

```yaml
behaviors:
  high-performance:
    allOperations:
      abandonCallAfter: 1s
      maximumNumberOfCallAttempts: 2
      delayBetweenRetries: 25ms
      errorDetailVerbosity: 2    # 0=none, 1=subcode, 2=+message, 3=+expression trace
    retryableWrites:
      useDurableDelete: false
    batchReads:
      maxConcurrentServers: 8
    query:
      recordQueueSize: 5000

  batch-optimized:
    parent: high-performance   # inherits, then overrides per field
    batchReads:
      maxConcurrentServers: 16
```

Selector blocks scope the fields to an operation category: `allOperations`,
`retryableWrites` / `nonRetryableWrites`, `consistencyModeReads` (SC) /
`availabilityModeReads` (AP), `batchReads` / `batchWrites`, and `query`.
`parent:` names another profile (default: `DEFAULT`) whose resolved settings
the profile inherits field by field. A `DEFAULT` entry adjusts
`Behavior.DEFAULT` itself, layered on its built-in settings.
`maximumNumberOfCallAttempts` counts the initial call, so `2` means one
retry. `errorDetailVerbosity` opts operations into extended server error
detail (see [Error Handling](error-handling.md)); it defaults to `0` (off).

Use a file-defined behavior like any other:

```python
from aerospike_sdk.policy import get_behavior

session = cluster.create_session(get_behavior("high-performance"))
```

## Hot-reload

The file's modification time is polled about once a second; a change is
re-read, re-parsed, and applied without reconnecting.

- **`transactions.*`** — read at operation time, so a reload takes effect on
  the next operation.
- **Named behaviors** — reach **already-created sessions**: each reload
  rebuilds the affected behaviors in place and pushes refreshed policies into
  every live session bound to them (including sessions on derived child
  behaviors). Nothing is checked on the operation path; in-flight operations
  complete with whichever policy snapshot they started with.
- **`connections.*` / `refresh.tendInterval`** — applied to the connection at
  `connect()`; a change to these takes effect on the next connect.

## Fail-soft

File handling never breaks a client:

- A missing, unreadable, or malformed file is logged and ignored; the client
  connects with the remaining layers (programmatic settings, then defaults).
- A single bad field value is skipped while the rest of the file still
  applies.
- A file that stops parsing mid-run keeps the last-good settings rather than
  reverting to defaults.

## Complete example

A complete annotated example ships as
[`examples/sdk-config-example.yaml`](https://github.com/aerospike/aerospike-client-python-sdk/blob/main/examples/sdk-config-example.yaml).
