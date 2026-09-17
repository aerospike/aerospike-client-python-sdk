#!/usr/bin/env python3
"""Batch operations example demonstrating chained multi-key operations.

Covers batch insert, batch mixed operations (insert + update + delete
in a single execute).
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet

SET = DataSet.of("test", "set")


async def run_examples(session) -> None:
    print("*************")
    print("* Batch tests")
    print("*************")

    await session.truncate(SET)
    await asyncio.sleep(0.2)

    # --- 1) Batch Insert — 5 keys with same bin values ---
    print("Batch Insert:")
    await (
        session.insert(SET.ids(1, 2, 3, 4, 5))
        .bin("name").set_to("Fred")
        .bin("age").set_to(30)
        .bin("value").set_to(10)
        .execute()
    )

    async with await session.query(SET).execute() as stream:
        async for rec in stream:
            print(f"  {rec.record_or_raise().bins}")

    # --- 2) Batch Modify — insert 3 + update 1 + delete 1 in one execute ---
    print("Batch Modify:")
    await (
        session.insert(SET.ids(6, 7, 8))
        .bin("name").set_to("Wilma")
        .bin("age").set_to(33)
        .bin("value").set_to(20)
        .update(SET.id(2)).bin("value").add(5)
        .delete(SET.id(1))
        .execute()
    )

    async with await session.query(SET).execute() as stream:
        async for rec in stream:
            print(f"  {rec.record_or_raise().bins}")


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await run_examples(session)


if __name__ == "__main__":
    asyncio.run(main())
