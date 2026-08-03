#!/usr/bin/env python3
"""Example demonstrating self.session usage with custom Behaviors.

Covers: self.session creation, upsert, query, update, delete, exists, touch,
custom behavior derivation, DataSet self.key patterns.
"""

import asyncio
from datetime import timedelta

from _env import Example
from aerospike_sdk import Behavior, DataSet



class SessionExample(Example):
    async def run(self):
        # Upsert
        await self.session.upsert(self.key).put({"name": "John", "age": 30, "city": "New York"}).execute()
        print("Upserted record")

        # Query (point read)
        stream = await self.session.query(self.key).execute()
        async for rec in stream:
            print(f"Read record: {rec.record.bins}")
        stream.close()

        # Update
        await self.session.update(self.key).bin("age").set_to(31).execute()
        print("Updated age to 31")

        # Touch (refresh TTL)
        await self.session.touch(self.key).execute()
        print("Touched record")

        # Exists
        stream = await self.session.exists(self.key).execute()
        first = await stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")

        # Delete
        await self.session.delete(self.key).execute()
        print("Deleted record")

        # Custom behavior
        fast_behavior = Behavior.DEFAULT.derive_with_changes(
            name="fast",
            total_timeout=timedelta(seconds=5),
            max_retries=1,
        )
        fast_session = self.cluster.create_session(fast_behavior)
        print(f"Created self.session with custom behavior: {fast_session.behavior.name}")

        # Operations with DataSet + key_value
        key2 = self.users.id("user456")
        await fast_session.upsert(key2).put({"name": "Bob", "age": 25}).execute()
        print("Upserted using fast self.session")

        # Query all records in set
        stream = await fast_session.query(self.users).execute()
        count = 0
        async for record in stream:
            count += 1
            print(f"  Query result: {record.record.bins}")
        stream.close()
        print(f"Total records: {count}")

        # Cleanup
        await self.session.delete(key2).execute()
        print("\nAll operations completed successfully!")
