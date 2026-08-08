#!/usr/bin/env python3
"""Batch operations example demonstrating chained multi-key operations.

Covers batch insert, batch mixed operations (insert + update + delete
in a single execute).
"""

import asyncio

from _env import Example
from aerospike_sdk import DataSet


class BatchExample(Example):
    SET = DataSet.of("test", "set")
    async def __init__(self):
        await super().__init__()
        await self.session.truncate(self.SET)
        await asyncio.sleep(0.2)

    async def cleanup(self):
        if self.stream:
            self.stream.close()


class BatchInsert(BatchExample):
    async def run(self) -> None:
        print("Batch Insert:")
        await (
            self.session.insert(self.SET.ids(1, 2, 3, 4, 5))
            .bin("name").set_to("Fred")
            .bin("age").set_to(30)
            .bin("value").set_to(10)
            .execute()
        )

        self.stream = await self.session.query(self.SET).execute()
        async for rec in self.stream:
            print(f"  {rec.record_or_raise().bins}")


class BatchModify(BatchExample):
    async def run(self) -> None:
        print("Batch Modify:")
        await (
            self.session.insert(self.SET.ids(6, 7, 8))
            .bin("name").set_to("Wilma")
            .bin("age").set_to(33)
            .bin("value").set_to(20)
            .update(self.SET.id(2)).bin("value").add(5)
            .delete(self.SET.id(1))
            .execute()
        )

        self.stream = await self.session.query(self.SET).execute()
        async for rec in self.stream:
            print(f"  {rec.record_or_raise().bins}")
