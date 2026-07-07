#!/usr/bin/env python3
"""Basic example demonstrating key-value operations via the SDK API.

Covers: connect, put, get, get with bin selection, exists, delete.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet


async def main() -> None:
    cluster = await _env.connect().connect()
    session = cluster.create_session(Behavior.DEFAULT)
    users = DataSet.of("test", "users")
    key = users.id("user123")

    try:
        # PUT
        await session.upsert(key).put({"name": "John", "age": 30}).execute()
        print("Put record")

        # GET
        stream = await session.query(key).execute()
        first = await stream.first_or_raise()
        print(f"Got record: {first.record.bins}")

        # GET with selected bins
        stream = await session.query(key).bins(["name"]).execute()
        first = await stream.first_or_raise()
        print(f"Got record (name only): {first.record.bins}")

        # EXISTS
        stream = await session.exists(key).execute()
        first = await stream.first()
        print(f"Record exists: {first.as_bool() if first else None}")

        # DELETE
        await session.delete(key).execute()
        print("Deleted record")

        print("\nAll operations completed successfully!")
    finally:
        await cluster.close()


if __name__ == "__main__":
    asyncio.run(main())
