#!/usr/bin/env python3
"""Batch operations example demonstrating chained multi-key operations.

Covers batch insert, batch mixed operations (insert + update + delete
in a single execute).
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet

SET = DataSet.of("test", "set")


async def main() -> None:
    cluster = await _env.connect().connect()
    session = cluster.create_session(Behavior.DEFAULT)

    try:
        await run_examples(session)
    finally:
        await cluster.close()


async def run_examples(session) -> None:
    print("*************")
    print("* Batch tests")
    print("*************")

    await session.truncate(SET)
    await asyncio.sleep(0.2)

    # ------------------------------------------------------------------
    # Batch Insert — 5 keys with same bin values
    # ------------------------------------------------------------------
    print("Batch Insert:")
    for pk in range(1, 6):
        await (
            session.insert(SET.id(pk))
            .bin("name").set_to("Fred")
            .bin("age").set_to(30)
            .bin("value").set_to(10)
            .execute()
        )

    stream = await session.query(SET).execute()
    async for rec in stream:
        print(f"  {rec}")
    stream.close()

    # ------------------------------------------------------------------
    # Batch Modify — insert 3 + update 1 + delete 1 in one execute
    # ------------------------------------------------------------------
    print("\nBatch Modify:")
    results = await (
        session.insert(SET.id(6)).bin("name").set_to("Wilma").bin("age").set_to(33).bin("value").set_to(20)
        .insert(SET.id(7)).bin("name").set_to("Wilma").bin("age").set_to(33).bin("value").set_to(20)
        .insert(SET.id(8)).bin("name").set_to("Wilma").bin("age").set_to(33).bin("value").set_to(20)
        .update(SET.id(2)).bin("value").add(5)
        .delete(SET.id(1))
        .execute()
    )
    async for rr in results:
        print(f"  {rr}")
    results.close()

    print("\nAll records after batch modify:")
    stream = await session.query(SET).execute()
    async for rec in stream:
        print(f"  {rec}")
    stream.close()

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
