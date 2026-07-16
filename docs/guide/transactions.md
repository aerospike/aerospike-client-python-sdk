# Transactions

Multi-record transactions group several record operations into a single
atomic unit. Writes become visible together at commit, or are rolled back
together on abort.

## Server Requirements

Transactions require **Aerospike Server 8.0+** with the target namespace
configured for **strong consistency**. On an AP namespace the transaction
APIs raise immediately; on an SC namespace the server allocates a
transaction monitor record and tracks the write set.

## Async

Use `Session.transaction()` as a context manager. The returned
[`TransactionalSession`](../api/transactional-session.md) auto-attaches
the transaction to every operation — application code never sees the
`Txn` object itself. On clean exit the transaction commits; on any
exception it aborts.

```python
from aerospike_sdk import ClusterDefinition, Behavior, DataSet

async with await ClusterDefinition("localhost", 3100).connect() as cluster:
    session = cluster.create_session(Behavior.DEFAULT)
    accounts = DataSet.of("test_sc", "accounts")

    async with session.transaction() as tx:
        await tx.upsert(accounts.id("A")).bin("bal").add(-10).execute()
        await tx.upsert(accounts.id("B")).bin("bal").add(10).execute()
```

If anything inside the `with` raises, the transaction is aborted and the
exception propagates.

### Retrying on Transient Conflicts

Strong-consistency transactions can fail with transient conflicts when
concurrent writers touch the same record
(`MRT_BLOCKED`, `MRT_VERSION_MISMATCH`, `TXN_FAILED`). Use
`do_in_transaction` to retry the whole block automatically:

```python
async def transfer(tx):
    await tx.upsert(accounts.id("A")).bin("bal").add(-10).execute()
    await tx.upsert(accounts.id("B")).bin("bal").add(10).execute()
    return "ok"

result = await session.do_in_transaction(
    transfer,
    max_attempts=5,
    sleep_between_retries=0.01,
)
```

Non-transient errors raised inside the callable abort the transaction
and re-raise without retry.

## Sync

The synchronous API mirrors the async surface exactly — just drop the
`async`/`await`:

```python
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.sync import ClusterDefinition

with ClusterDefinition("localhost", 3100).connect() as cluster:
    session = cluster.create_session(Behavior.DEFAULT)
    accounts = DataSet.of("test_sc", "accounts")

    with session.transaction() as tx:
        tx.upsert(accounts.id("A")).bin("bal").add(-10).execute()
        tx.upsert(accounts.id("B")).bin("bal").add(10).execute()
```

`do_in_transaction` is available on
[`SyncSession`](../api/sync/session.md) as well, with a
`time.sleep`-based retry loop.

## Reads Inside a Transaction

Reads issued inside `transaction` participate in the transaction
and see a consistent snapshot of the write set:

```python
async with session.transaction() as tx:
    stream = await tx.query(accounts.id("A")).execute()
    current = (await stream.first_or_raise()).record.bins["bal"]
    if current >= 10:
        await tx.upsert(accounts.id("A")).bin("bal").add(-10).execute()
```

## Errors

| Error | Meaning |
|-------|---------|
| `CommitError` | Commit failed (server-side); the transaction is in an indeterminate state. `in_doubt` flag on the exception indicates whether writes may have reached the server. |
| `MRT_BLOCKED` | Another transaction has one of the records locked. Retry. |
| `MRT_VERSION_MISMATCH` | A non-transactional write raced with the transaction. Retry. |
| `MRT_EXPIRED` | Transaction monitor TTL elapsed before commit. |
| `MRT_TOO_MANY_WRITES` | Exceeded the per-transaction write limit. |

See Also:
- [`TransactionalSession`](../api/transactional-session.md) — async API reference
- [`SyncTransactionalSession`](../api/sync/transactional-session.md) — sync API reference
- [`Session.transaction`](../api/session.md) / [`Session.do_in_transaction`](../api/session.md)
