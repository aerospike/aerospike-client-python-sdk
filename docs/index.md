# Aerospike Python SDK

A high-level, chainable API for the [Aerospike](https://aerospike.com/) database,
built on top of the
[Aerospike Python Async Client](https://github.com/aerospike/aerospike-client-python-async).

## Quick Example

::::{tab-set}

:::{tab-item} Async
```python
import asyncio
from aerospike_sdk import ClusterDefinition, DataSet, Behavior

async def main():
    async with await ClusterDefinition("localhost", 3000).connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")

        # Write
        await (
            session.upsert(users.id(1))
            .put({"name": "Alice", "age": 30})
            .execute()
        )

        # Read
        stream = await session.query(users.id(1)).execute()
        result = await stream.first_or_raise()
        print(result.record.bins)  # {'name': 'Alice', 'age': 30}

        # Query with AEL filter
        stream = await (
            session.query(users)
            .where("$.age > 25")
            .execute()
        )
        async for result in stream:
            print(result.record.bins)
        stream.close()

asyncio.run(main())
```
:::

:::{tab-item} Sync
```python
from aerospike_sdk import DataSet, Behavior
from aerospike_sdk.sync import ClusterDefinition

with ClusterDefinition("localhost", 3000).connect() as cluster:
    session = cluster.create_session(Behavior.DEFAULT)
    users = DataSet.of("test", "users")

    # Write
    session.upsert(users.id(1)).put({"name": "Alice", "age": 30}).execute()

    # Read
    stream = session.query(users.id(1)).execute()
    result = stream.first_or_raise()
    print(result.record.bins)

    # Query with AEL filter
    stream = session.query(users).where("$.age > 25").execute()
    for result in stream:
        print(result.record.bins)
    stream.close()
```
:::

::::

## Installation

```bash
pip install aerospike-sdk
```

Or install from source:

```bash
git clone https://github.com/aerospike/aerospike-client-python-sdk.git
cd aerospike-client-python-sdk
pip install -e ".[dev]"
```

## Key Concepts

**ClusterDefinition / Cluster**
:   Entry point. Configure seeds, auth, and TLS on a [`ClusterDefinition`](api/cluster-definition.md), then `connect()` for a live [`Cluster`](api/cluster.md) that creates sessions and manages the connection lifecycle. (Async at the top level; the sync twins live under `aerospike_sdk.sync`.)

**Session**
:   Scoped to a [`Behavior`](api/behavior.md) (policy defaults for timeouts, consistency, etc.). All reads and writes go through a session.

**DataSet**
:   A namespace + set pair. Use `DataSet.of("ns", "set")` to create one, then `.id(key)` to produce keys.

**Builders**
:   Reads return a [`QueryBuilder`](api/query.md), writes return a [`WriteSegmentBuilder`](api/write-segment.md). Chain methods, then call `.execute()`.

**Aerospike Expression Language (AEL)**
:   Filter records with string expressions: `"$.age > 18 and $.status == 'active'"`. See the [AEL guide](guide/expression-ael.md).

## Requirements

- Python 3.10+
- Aerospike Server 6.0+ (7.0+ for some features)
- [aerospike-client-python-async](https://github.com/aerospike/aerospike-client-python-async)

## Next Steps

- [Connecting to a Cluster](guide/connecting.md)
- [Reading Data](guide/reads.md)
- [Writing Data](guide/writes.md)
- [Transactions](guide/transactions.md)
- [API Reference](api/index.md)

```{toctree}
:hidden:
:maxdepth: 2
:caption: Getting Started

guide/connecting
guide/dynamic-sdk-config
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Working with Data

guide/reads
guide/writes
guide/cdt-operations
guide/string-ops
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Queries & Indexes

guide/expression-ael
guide/indexes
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Transactions & Background

guide/transactions
guide/background-udf
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Operations

guide/error-handling
guide/logging
guide/performance
guide/benchmarking
```

```{toctree}
:hidden:
:maxdepth: 1
:caption: Reference

api/index
```
