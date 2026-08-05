#!/usr/bin/env python3
"""Example demonstrating DataSet key creation and usage patterns.

Covers: DataSet.of, .id(), .ids(), .id_from_digest(), key types.
"""

import _env
from _env import Example
from aerospike_sdk import Behavior


class DatasetExample(Example):
    async def run(self):
        print(f"DataSet: namespace={self.users.namespace}, set={self.users.set_name}")

        # Single keys (various types)
        key_str = self.key
        key_int = self.users.id(456)
        key_bytes = self.users.id(b"bytes_key")
        print("\nSingle keys:")
        print(f"  String key: {key_str}")
        print(f"  Integer key: {key_int}")
        print(f"  Bytes key: {key_bytes}")

        # Multiple keys
        keys = self.users.ids("user1", "user2", "user3")
        print(f"\nMultiple keys: {len(keys)} keys")

        # Key from digest
        original = self.users.id(123)
        digest = original.digest
        from_digest = self.users.id_from_digest(digest)
        print("\nKey from digest:")
        print(f"  Original: {original}")
        print(f"  From digest: {from_digest}")
        print(f"  Equal: {original == from_digest}")

        # Use with live server
        async with await _env.connect().connect() as cluster:
            session = cluster.create_session(Behavior.DEFAULT)

            key = self.users.id("example_user")
            await session.upsert(key).put({"name": "John Doe", "age": 30}).execute()

        stream = await session.query(key).execute()
        first = await stream.first_or_raise()
        print(f"\nRetrieved record: {first.record.bins}")

        await self.session.delete(key).execute()
        print("Cleaned up")
