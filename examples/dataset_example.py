#!/usr/bin/env python3
"""Example demonstrating DataSet key creation and usage patterns.

Covers: DataSet.of, .id(), .ids(), .id_from_digest(), key types.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet


async def main() -> None:
    users = DataSet.of("test", "users")
    print(f"DataSet: namespace={users.namespace}, set={users.set_name}")

    # Single keys (various types)
    key_str = users.id("user123")
    key_int = users.id(456)
    key_bytes = users.id(b"bytes_key")
    print(f"\nSingle keys:")
    print(f"  String key: {key_str}")
    print(f"  Integer key: {key_int}")
    print(f"  Bytes key: {key_bytes}")

    # Multiple keys
    keys = users.ids("user1", "user2", "user3")
    print(f"\nMultiple keys: {len(keys)} keys")

    # Key from digest
    original = users.id(123)
    digest = original.digest
    from_digest = users.id_from_digest(digest)
    print(f"\nKey from digest:")
    print(f"  Original: {original}")
    print(f"  From digest: {from_digest}")
    print(f"  Equal: {original == from_digest}")

    # Use with live server
    cluster = await _env.connect().connect()
    session = cluster.create_session(Behavior.DEFAULT)

    try:
        key = users.id("example_user")
        await session.upsert(key).put({"name": "John Doe", "age": 30}).execute()

        stream = await session.query(key).execute()
        first = await stream.first_or_raise()
        print(f"\nRetrieved record: {first.record.bins}")

        await session.delete(key).execute()
        print("Cleaned up")
    finally:
        await cluster.close()


if __name__ == "__main__":
    asyncio.run(main())
