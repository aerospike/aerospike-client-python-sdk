#!/usr/bin/env python3
"""Comprehensive example demonstrating all common SDK API operations.

Covers truncate, upsert, query, exists, touch, delete, batch operations,
AEL filters, secondary index queries, background tasks, expression
read/write, and query hints.
"""

import asyncio

from _env import Example
from aerospike_sdk import DataSet
from aerospike_sdk.aio.operations.query import QueryHint
from aerospike_sdk.exceptions import AerospikeError


class CommonExample(Example):
    SET = DataSet.of("test", "set")

    async def __init__(self):
        await super().__init__()
        print("Truncate records")
        await self.session.truncate(self.SET)
        await asyncio.sleep(0.2)

        print("Write 1 record")
        await (
            self.session.upsert(self.SET.id(10))
            .bin("name").set_to("Charlie")
            .bin("age").set_to(11)
            .execute()
        )

        print("Write 3 records")
        await (
            self.session.upsert(self.SET.id(1)).bin("name").set_to("Tim").bin("age").set_to(312)
            .upsert(self.SET.id(2)).bin("name").set_to("Bob").bin("age").set_to(25)
            .upsert(self.SET.id(3)).bin("name").set_to("Jane").bin("age").set_to(46)
            .execute()
        )

        print("Write 10 records")
        wb = self.session
        for pk, name, age in [
            (10, "Tim", 312), (11, "Bob", 25), (12, "Jane", 46),
            (13, "Tim", 200), (14, "User1", 201), (15, "User2", 202),
            (16, "User3", 203), (17, "User4", 204), (18, "User5", 205),
            (19, "User6", 206),
        ]:
            wb = wb.upsert(self.SET.id(pk)).bin("name").set_to(name).bin("age").set_to(age)
        await wb.execute()


class CommonReadOneRecord(CommonExample):
    async def run(self) -> None:
        print("\nRead 1 record")
        stream = await self.session.query(self.SET.id(10)).execute()
        first = await stream.first()
        if first and first.is_ok:
            print(f"  Record = {first.record.bins}")
        else:
            print("  Error: No records returned")
        stream.close()


class CommonReadTwoRecords(CommonExample):
    async def run(self) -> None:
        print("Read 2 records")
        stream = await self.session.query(self.SET.ids(1, 2)).execute()
        async for result in stream:
            rec = result.record_or_raise()
            print(f"  Record = {rec.bins}")
        stream.close()


class CommonExists(CommonExample):
    async def run(self) -> None:
        print("Exists 1 record")
        stream = await self.session.exists(self.SET.id(13)).execute()
        first = await stream.first()
        print(f"  Result: {first.as_bool() if first else None}")


class CommonTouch(CommonExample):
    async def run(self) -> None:
        print("Touch 1 record")
        await self.session.touch(self.SET.id(13)).execute()
        print("  Done")


class CommonDelete(CommonExample):
    async def run(self) -> None:
        print("Delete 1 record")
        await self.session.delete(self.SET.id(18)).execute()
        print("  Done")


class CommonStreamExample(CommonExample):
    async def cleanup(self):
        if hasattr(self, "stream"):
            self.stream.close()
        await super().cleanup()

class CommonBatchExists(CommonStreamExample):
    async def run(self) -> None:
        print("Batch exists")
        self.stream = await (
            self.session.exists(self.SET.id(13), self.SET.id(14), self.SET.id(999))
            .include_missing_keys()
            .execute()
        )
        async for rr in self.stream:
            print(f"  Key: {rr.key} -> {rr.as_bool()}")


class CommonBatchTouch(CommonStreamExample):
    async def run(self) -> None:
        print("Batch touch")
        self.stream = await (
            self.session.touch(self.SET.id(13), self.SET.id(14), self.SET.id(999))
            .include_missing_keys()
            .execute()
        )
        async for rr in self.stream:
            print(f"  Key: {rr.key} -> {rr.as_bool()}")


class CommonBatchDelete(CommonStreamExample):
    async def run(self) -> None:
        print("Batch delete")
        await self.session.upsert(self.SET.id(13)).put({"name": "Tim", "age": 200}).execute()
        await self.session.upsert(self.SET.id(14)).put({"name": "User1", "age": 201}).execute()
        self.stream = await (
            self.session.delete(self.SET.id(13), self.SET.id(14), self.SET.id(999))
            .include_missing_keys()
            .execute()
        )
        async for rr in self.stream:
            print(f"  Key: {rr.key} -> {rr.as_bool()}")

        await self.session.upsert(self.SET.id(13)).put({"name": "Tim", "age": 200}).execute()
        await self.session.upsert(self.SET.id(14)).put({"name": "User1", "age": 201}).execute()


class CommonAelFilterFred(CommonStreamExample):
    async def run(self) -> None:
        self.stream = await (
            self.session.query(self.SET.id(2))
            .where("$.name == 'Fred'")
            .execute()
        )
        first = await self.stream.first()
        if first and first.is_ok:
            print(f"  ERROR: Record for Fred exists, value: {first.record.bins}")
        else:
            print("  Record for Fred does not exist (expected)")


class CommonAelFilterIncludeMissingFred(CommonStreamExample):
    async def run(self) -> None:
        self.stream = await (
            self.session.query(self.SET.id(2))
            .where("$.name == 'Fred'")
            .include_missing_keys()
            .execute()
        )
        first = await self.stream.first()
        if first:
            print(f"  With include_missing_keys — Key: {first.key}, is_ok: {first.is_ok}")
        else:
            print("  ERROR: No result even with include_missing_keys")


class CommonAelFilterBob(CommonStreamExample):
    async def run(self) -> None:
        self.stream = await (
            self.session.query(self.SET.id(2))
            .where("$.name == 'Bob'")
            .execute()
        )
        first = await self.stream.first()
        if first and first.is_ok:
            print(f"  Record for Bob exists, value: {first.record.bins}")
        else:
            print("  ERROR: Record for Bob does not exist")


class CommonAelFilterFredAgain(CommonStreamExample):
    async def run(self) -> None:
        self.stream = await (
            self.session.query(self.SET.id(2))
            .where("$.name == 'Fred'")
            .execute()
        )
        first = await self.stream.first()
        if first and first.is_ok:
            print(f"  ERROR: Record for Fred exists, value: {first.record.bins}")
        else:
            print("  Record for Fred does not exist (expected)")


class CommonAelFilterIncludeMissingFredAgain(CommonStreamExample):
    async def run(self) -> None:
        self.stream = await (
            self.session.query(self.SET.id(2))
            .where("$.name == 'Fred'")
            .include_missing_keys()
            .execute()
        )
        first = await self.stream.first()
        if first:
            print(f"  With include_missing_keys — Key: {first.key}, is_ok: {first.is_ok}")
        else:
            print("  ERROR: No result even with include_missing_keys")


class CommonSecondaryIndexQuery(CommonStreamExample):
    async def run(self) -> None:
        print("Foreground secondary index query")
        self.stream = await self.session.query(self.SET).where("$.age > 200").execute()
        count = 0
        async for result in self.stream:
            print(f"  {result.record.bins}")
            count += 1
        print(f"  Query count: {count}")


class CommonFailOnFilteredOut(CommonStreamExample):
    async def run(self) -> None:
        try:
            self.stream = await (
                self.session.query(self.SET.id(2))
                .where("$.name == 'Fred'")
                .fail_on_filtered_out()
                .execute()
            )
        except AerospikeError as ae:
            print(f"  Exception received as expected: {ae}")


class CommonPrimaryIndexScan(CommonStreamExample):
    async def run(self) -> None:
        print("\nForeground primary index query")
        self.stream = await self.session.query(self.SET).records_per_second(5000).execute()
        count = 0
        async for _ in self.stream:
            count += 1
        print(f"  Query count: {count}")


class StreamIndexExample(CommonStreamExample):
    async def __init__(self):
        await super().__init__()

        print("Create index")
        try:
            await self.session.index(self.SET).on_bin("age").named("ageidx").numeric().create()
        except Exception:
            pass  # Index may already exist
        await asyncio.sleep(0.3)


    async def cleanup(self):
        try:
            await self.session.index(self.SET).named("ageidx").drop()
        except Exception:
            pass

        await super().cleanup()

class CommonCreateIndexAndExpressions(CommonStreamExample):
    async def run(self) -> None:
        self.stream = await (
            self.session.upsert(self.SET.ids(1, 2, 3))
            .bin("name").set_to("Tim")
            .bin("readBin").select_from("$.age + 12")
            .bin("writeBin").upsert_from("$.age + 30")
            .execute()
        )
        async for rr in self.stream:
            print(rr.result_code)
            print(f"  Upsert with expressions: {rr.record.bins}")


class CommonBackgroundQuery(StreamIndexExample):
    async def run(self) -> None:
        print("\nBackground query")
        task = await (
            self.session.background_task()
            .update(self.SET)
            .bin("age").add(1)
            .where("$.name == 'Tim' and $.age > 20")
            .execute()
        )
        await task.wait_till_complete()

        self.stream = await self.session.query(self.SET.ids(10, 13)).execute()
        async for result in self.stream:
            rec = result.record_or_raise()
            print(f"  Record = {rec.bins}")
        self.stream.close()


class CommonQueryHints(StreamIndexExample):
    async def run(self) -> None:
        print("\nQuery with hint")
        self.stream = await (
            self.session.query(self.SET)
            .where("$.age > 200")
            .with_hint(QueryHint(index_name="ageidx"))
            .execute()
        )
        count = 0
        async for _ in self.stream:
            count += 1
        print(f"  Query count with hint: {count}")


class CommonReadExpressions(CommonStreamExample):
    async def run(self) -> None:
        self.stream = await (
            self.session.query(self.SET.id(1))
            .bin("ageIn20Years").select_from("$.age + 20")
            .execute()
        )
        first = await self.stream.first()
        if first and first.is_ok:
            print(f"  Single read expression: {first.record.bins}")

        self.stream = await (
            self.session.query(self.SET.ids(1, 2, 3))
            .bin("ageIn20Years").select_from("$.age + 20")
            .execute()
        )
        async for result in self.stream:
            print(f"  Batch read expression: {result.record.bins}")


class CommonQueryHintsAgain(StreamIndexExample):
    async def run(self) -> None:
        print("\nQuery with hint")
        self.stream = await (
            self.session.query(self.SET)
            .where("$.age > 200")
            .with_hint(QueryHint(index_name="ageidx"))
            .execute()
        )
        count = 0
        async for _ in self.stream:
            count += 1
        print(f"  Query count with hint: {count}")
