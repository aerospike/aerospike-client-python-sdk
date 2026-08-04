#!/usr/bin/env python3
"""Comprehensive example demonstrating all common SDK API operations.

Covers truncate, upsert, query, exists, touch, delete, batch operations,
AEL filters, secondary index queries, background tasks, expression
read/write, and query hints.
"""

import asyncio

from _env import Example
import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.aio.operations.query import QueryHint
from aerospike_sdk.exceptions import AerospikeError

SET = DataSet.of("test", "set")

class CommonExample(Example):
    async def run(self):
        # ------------------------------------------------------------------
        # Truncate
        # ------------------------------------------------------------------
        print("Truncate records")
        await self.session.truncate(SET)
        await asyncio.sleep(0.2)

        # ------------------------------------------------------------------
        # Write a single record
        # ------------------------------------------------------------------
        print("Write 1 record")
        await (
            self.session.upsert(SET.id(10))
            .bin("name").set_to("Charlie")
            .bin("age").set_to(11)
            .execute()
        )

        # ------------------------------------------------------------------
        # Write multiple records — one batched call, not a call per record
        # ------------------------------------------------------------------
        print("Write 3 records")
        await (
            self.session.upsert(SET.id(1)).bin("name").set_to("Tim").bin("age").set_to(312)
            .upsert(SET.id(2)).bin("name").set_to("Bob").bin("age").set_to(25)
            .upsert(SET.id(3)).bin("name").set_to("Jane").bin("age").set_to(46)
            .execute()
        )

        # Ten records is the same one call, built in a loop rather than spelled out.

        print("Write 10 records")
        wb = self.session
        for pk, name, age in [
            (10, "Tim", 312), (11, "Bob", 25), (12, "Jane", 46),
            (13, "Tim", 200), (14, "User1", 201), (15, "User2", 202),
            (16, "User3", 203), (17, "User4", 204), (18, "User5", 205),
            (19, "User6", 206),
        ]:
            wb = wb.upsert(SET.id(pk)).bin("name").set_to(name).bin("age").set_to(age)
        await wb.execute()

        # ------------------------------------------------------------------
        # Read 1 record (point read)
        # ------------------------------------------------------------------
        print("\nRead 1 record")
        stream = await self.session.query(SET.id(10)).execute()
        first = await stream.first()
        if first and first.is_ok:
            print(f"  Record = {first.record.bins}")
        else:
            print("  Error: No records returned")
        stream.close()

        # ------------------------------------------------------------------
        # Read 2 records (batch read)
        # ------------------------------------------------------------------
        print("Read 2 records")
        stream = await self.session.query(SET.ids(1, 2)).execute()
        async for result in stream:
            rec = result.record_or_raise()
            print(f"  Record = {rec.bins}")
        stream.close()

        # ------------------------------------------------------------------
        # Exists
        # ------------------------------------------------------------------
        print("Exists 1 record")
        stream = await self.session.exists(SET.id(13)).execute()
        first = await stream.first()
        print(f"  Result: {first.as_bool() if first else None}")

        # ------------------------------------------------------------------
        # Touch
        # ------------------------------------------------------------------
        print("Touch 1 record")
        await self.session.touch(SET.id(13)).execute()
        print("  Done")

        # ------------------------------------------------------------------
        # Delete
        # ------------------------------------------------------------------
        print("Delete 1 record")
        await self.session.delete(SET.id(18)).execute()
        print("  Done")

        # ------------------------------------------------------------------
        # Batch exists (with include_missing_keys)
        # ------------------------------------------------------------------
        print("Batch exists")
        stream = await self.session.exists(SET.id(13), SET.id(14), SET.id(999)).include_missing_keys().execute()
        async for rr in stream:
            print(f"  Key: {rr.key} -> {rr.as_bool()}")
        stream.close()

        # ------------------------------------------------------------------
        # Batch touch (with include_missing_keys)
        # ------------------------------------------------------------------
        print("Batch touch")
        stream = await self.session.touch(SET.id(13), SET.id(14), SET.id(999)).include_missing_keys().execute()
        async for rr in stream:
            print(f"  Key: {rr.key} -> {rr.as_bool()}")
        stream.close()

        # ------------------------------------------------------------------
        # Batch delete (with include_missing_keys)
        # ------------------------------------------------------------------
        print("Batch delete")
        await self.session.upsert(SET.id(13)).put({"name": "Tim", "age": 200}).execute()
        await self.session.upsert(SET.id(14)).put({"name": "User1", "age": 201}).execute()
        stream = await self.session.delete(SET.id(13), SET.id(14), SET.id(999)).include_missing_keys().execute()
        async for rr in stream:
            print(f"  Key: {rr.key} -> {rr.as_bool()}")
        stream.close()

        # Re-insert deleted records for remaining examples
        await self.session.upsert(SET.id(13)).put({"name": "Tim", "age": 200}).execute()
        await self.session.upsert(SET.id(14)).put({"name": "User1", "age": 201}).execute()

        stream = await (
            self.session.query(SET.id(2))
            .where("$.name == 'Fred'")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"  ERROR: Record for Fred exists, value: {first.record.bins}")
        else:
            print("  Record for Fred does not exist (expected)")
        stream.close()

        # ------------------------------------------------------------------
        # include_missing_keys + AEL filter
        # ------------------------------------------------------------------
        stream = await (
            self.session.query(SET.id(2))
            .where("$.name == 'Fred'")
            .include_missing_keys()
            .execute()
        )
        first = await stream.first()
        if first:
            print(f"  With include_missing_keys — Key: {first.key}, is_ok: {first.is_ok}")
        else:
            print("  ERROR: No result even with include_missing_keys")
        stream.close()

    # ------------------------------------------------------------------
    # fail_on_filtered_out
    # ------------------------------------------------------------------
        stream = await (
            self.session.query(SET.id(2))
            .where("$.name == 'Bob'")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"  Record for Bob exists, value: {first.record.bins}")
        else:
            print("  ERROR: Record for Bob does not exist")
        stream.close()

        stream = await (
            self.session.query(SET.id(2))
            .where("$.name == 'Fred'")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"  ERROR: Record for Fred exists, value: {first.record.bins}")
        else:
            print("  Record for Fred does not exist (expected)")
        stream.close()

        # ------------------------------------------------------------------
        # include_missing_keys + AEL filter
        # ------------------------------------------------------------------
        stream = await (
            self.session.query(SET.id(2))
            .where("$.name == 'Fred'")
            .include_missing_keys()
            .execute()
        )
        first = await stream.first()
        if first:
            print(f"  With include_missing_keys — Key: {first.key}, is_ok: {first.is_ok}")
        else:
            print("  ERROR: No result even with include_missing_keys")
        stream.close()

        # ------------------------------------------------------------------
        # Secondary index query with AEL where
        # ------------------------------------------------------------------
        print("Foreground secondary index query")
        stream = await self.session.query(SET).where("$.age > 200").execute()
        count = 0
        async for result in stream:
            print(f"  {result.record.bins}")
            count += 1
        stream.close()
        print(f"  Query count: {count}")

        try:
            stream = await (
                self.session.query(SET.id(2))
                .where("$.name == 'Fred'")
                .fail_on_filtered_out()
                .execute()
            )
            first = await stream.first()
            if first and first.is_ok:
                print("  ERROR: No exception was thrown, this is unexpected")
            else:
                print(f"  fail_on_filtered_out — result code: {first.result_code if first else 'none'}")
            stream.close()
        except AerospikeError as ae:
            print(f"  Exception received as expected: {ae}")

        # ------------------------------------------------------------------
        # Foreground primary index query (full scan)
        # ------------------------------------------------------------------
        print("\nForeground primary index query")
        stream = await self.session.query(SET).records_per_second(5000).execute()
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        print(f"  Query count: {count}")

        # ------------------------------------------------------------------
        # Create secondary index
        # ------------------------------------------------------------------
        print("Create index")
        try:
            await self.session.index(SET).on_bin("age").named("ageidx").numeric().create()
        except Exception:
            pass  # Index may already exist
        await asyncio.sleep(0.3)

        # Upsert + select_from + upsert_from in one operate
        stream = await (
            self.session.upsert(SET.ids(1, 2, 3))
            .bin("name").set_to("Tim")
            .bin("readBin").select_from("$.age + 12")
            .bin("writeBin").upsert_from("$.age + 30")
            .execute()
        )
        async for rr in stream:
            print(f"  Upsert with expressions: {rr.record.bins}")
        stream.close()

        # ------------------------------------------------------------------
        # Background update with AEL where
        # ------------------------------------------------------------------
        print("\nBackground query")
        task = await (
            self.session.background_task()
            .update(SET)
            .bin("age").add(1)
            .where("$.name == 'Tim' and $.age > 20")
            .execute()
        )
        await task.wait_till_complete()

        stream = await self.session.query(SET.ids(10, 13)).execute()
        async for result in stream:
            rec = result.record_or_raise()
            print(f"  Record = {rec.bins}")
        stream.close()

        # ------------------------------------------------------------------
        # Query hints
        # ------------------------------------------------------------------
        print("\nQuery with hint")
        stream = await (
            self.session.query(SET)
            .where("$.age > 200")
            .with_hint(QueryHint(index_name="ageidx"))
            .execute()
        )
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        print(f"  Query count with hint: {count}")

        # Single read expression: compute $.age + 20
        stream = await (
            self.session.query(SET.id(1))
            .bin("ageIn20Years").select_from("$.age + 20")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"  Single read expression: {first.record.bins}")

        # Batch read expression
        stream = await (
            self.session.query(SET.ids(1, 2, 3))
            .bin("ageIn20Years").select_from("$.age + 20")
            .execute()
        )
        async for result in stream:
            print(f"  Batch read expression: {result.record.bins}")
        stream.close()

        # ------------------------------------------------------------------
        # Query hints
        # ------------------------------------------------------------------
        from aerospike_sdk.aio.operations.query import QueryHint

        print("\nQuery with hint")
        stream = await (
            self.session.query(SET)
            .where("$.age > 200")
            .with_hint(QueryHint(index_name="ageidx"))
            .execute()
        )
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        print(f"  Query count with hint: {count}")

        # ------------------------------------------------------------------
        # Cleanup
        # ------------------------------------------------------------------
        print("\nCleanup: drop index")
        try:
            await self.session.index(SET).named("ageidx").drop()
        except Exception:
            pass

        print("Done!")
