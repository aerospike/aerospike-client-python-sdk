#!/usr/bin/env python3
"""Example demonstrating Session usage with custom Behaviors.

Covers: session creation, upsert, query, update, delete, exists, touch,
custom behavior derivation, DataSet key patterns.
"""

import asyncio
from datetime import timedelta

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.policy import Settings


async def main() -> None:
    async with _env.connect().connect() as cluster:
        users = DataSet.of("test", "users")

        # --- 1) A session with the default behavior ---
        session = cluster.create_session(Behavior.DEFAULT)
        key = users.id("user123")

        # --- 2) Upsert a record ---
        await session.upsert(key).put({"name": "John", "age": 30, "city": "New York"}).execute()
        print("Upserted record")

        # --- 3) Point read with query ---
        stream = await session.query(key).execute()
        async for rec in stream:
            print(f"Read record: {rec.record.bins}")
        stream.close()

        # --- 4) Update one bin ---
        await session.update(key).bin("age").set_to(31).execute()
        print("Updated age to 31")

        # --- 5) Touch to refresh the TTL ---
        await session.touch(key).execute()
        print("Touched record")

        # --- 6) Check that the record exists ---
        stream = await session.exists(key).execute()
        first = await stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")

        # --- 7) Delete the record ---
        await session.delete(key).execute()
        print("Deleted record")

        # --- 8) A session with a derived behavior ---
        fast_behavior = Behavior.DEFAULT.derive_with_changes(
            name="fast",
            all=Settings(total_timeout=timedelta(seconds=5), max_retries=1),
        )
        fast_session = cluster.create_session(fast_behavior)
        print(f"Created session with custom behavior: {fast_session.behavior.name}")

        # --- 9) Write through the derived session ---
        key2 = users.id("user456")
        await fast_session.upsert(key2).put({"name": "Bob", "age": 25}).execute()
        print("Upserted using fast session")

        # --- 10) Query every record in the set ---
        stream = await fast_session.query(users).execute()
        count = 0
        async for record in stream:
            count += 1
            print(f"  Query result: {record.record.bins}")
        stream.close()
        print(f"Total records: {count}")

        # Cleanup
        await session.delete(key2).execute()


if __name__ == "__main__":
    asyncio.run(main())
