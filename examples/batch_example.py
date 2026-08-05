#!/usr/bin/env python3
"""Batch operations example demonstrating chained multi-key operations.

Covers batch insert, batch mixed operations (insert + update + delete
in a single execute).
"""

import asyncio

from _env import Example
from aerospike_sdk import DataSet

SET = DataSet.of("test", "set")


class BatchExample(Example):
    async def run(self):
        print("*************")
        print("* Batch tests")
        print("*************")

        await self.session.truncate(SET)
        await asyncio.sleep(0.2)

        # ------------------------------------------------------------------
        # Batch Insert — 5 keys with same bin values
        # ------------------------------------------------------------------
        print("Batch Insert:")
        await (
            self.session.insert(SET.ids(1, 2, 3, 4, 5))
            .bin("name").set_to("Fred")
            .bin("age").set_to(30)
            .bin("value").set_to(10)
            .execute()
        )

        stream = await self.session.query(SET).execute()
        async for rec in stream:
            print(f"  {rec.record_or_raise().bins}")
        stream.close()

        # ------------------------------------------------------------------
        # Batch Modify — insert 3 + update 1 + delete 1 in one execute
        # ------------------------------------------------------------------
        print("Batch Modify:")
        await (
            self.session.insert(SET.ids(6, 7, 8))
            .bin("name").set_to("Wilma")
            .bin("age").set_to(33)
            .bin("value").set_to(20)
            .update(SET.id(2)).bin("value").add(5)
            .delete(SET.id(1))
            .execute()
        )

        stream = await self.session.query(SET).execute()
        async for rec in stream:
            print(f"  {rec.record_or_raise().bins}")
        stream.close()
