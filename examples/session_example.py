#!/usr/bin/env python3
"""Example demonstrating Session usage with custom Behaviors.

Covers: session creation, upsert, query, update, delete, exists, touch,
custom behavior derivation, DataSet key patterns.
"""

import asyncio
from datetime import timedelta

import _env
from aerospike_sdk import Behavior, DataSet


async def main() -> None:
    cluster = await _env.connect().connect()
    users = DataSet.of("test", "users")

    try:
        # Default session
        session = cluster.create_session(Behavior.DEFAULT)
        key = users.id("user123")

        # Upsert
        await session.upsert(key).put({"name": "John", "age": 30, "city": "New York"}).execute()
        print("Upserted record")

        # Query (point read)
        stream = await session.query(key).execute()
        async for rec in stream:
            print(f"Read record: {rec.record.bins}")
        stream.close()

        # Update
        await session.update(key).bin("age").set_to(31).execute()
        print("Updated age to 31")

        # Touch (refresh TTL)
        await session.touch(key).execute()
        print("Touched record")

        # Exists
        stream = await session.exists(key).execute()
        first = await stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")

        # Delete
        await session.delete(key).execute()
        print("Deleted record")

        # Custom behavior
        fast_behavior = Behavior.DEFAULT.derive_with_changes(
            name="fast",
            total_timeout=timedelta(seconds=5),
            max_retries=1,
        )
        fast_session = cluster.create_session(fast_behavior)
        print(f"Created session with custom behavior: {fast_session.behavior.name}")

        # Operations with DataSet + key_value
        key2 = users.id("user456")
        await fast_session.upsert(key2).put({"name": "Bob", "age": 25}).execute()
        print("Upserted using fast session")

        # Query all records in set
        stream = await fast_session.query(users).execute()
        count = 0
        async for record in stream:
            count += 1
            print(f"  Query result: {record.record.bins}")
        stream.close()
        print(f"Total records: {count}")

        # Cleanup
        await session.delete(key2).execute()
        print("\nAll operations completed successfully!")
    finally:
        await cluster.close()


if __name__ == "__main__":
    asyncio.run(main())
