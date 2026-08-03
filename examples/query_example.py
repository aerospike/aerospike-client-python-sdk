#!/usr/bin/env python3
"""Query examples demonstrating the full query API surface.

Covers CRUD, map CDT operations, AEL where filters, batch reads with
includeMissingKeys/failOnFilteredOut, limit, header-only reads, bin
projection, background tasks, records-per-second, client-side sort/page
(Python idiom for NavigatableRecordStream), expression read/write,
query hints, multi-operation batches, generation checks, and TTL.
"""

import asyncio
import time
from _env import Example
from datetime import timedelta

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.aio.operations.query import QueryHint
from aerospike_sdk.exceptions import AerospikeError

SET = DataSet.of("test", "person")


async def _print_stream(stream) -> int:
    count = 0
    async for rr in stream:
        count += 1
        print(f"  {count:5d} - Key: {rr.key}, Value: {rr}")
    return count


class QueryExample(Example):
    def __init__(self):
        custom_behavior = Behavior.DEFAULT.derive_with_changes(
            "newBehavior",
            total_timeout=timedelta(seconds=2),
        )
        super().__init__(custom_behavior)

    async def run(self):
        await self.session.truncate(SET)
        await asyncio.sleep(0.2)

        # ------------------------------------------------------------------
        # Upsert individual records
        # ------------------------------------------------------------------
        await (
            self.session.upsert(SET.id(80))
            .bin("name").set_to("Tim")
            .bin("age").set_to(342)
            .execute()
        )
        print(f"Upserted id(80)")

        for pk in (81, 82):
            await (
                self.session.upsert(SET.id(pk))
                .bin("name").set_to("Tim")
                .bin("age").set_to(343)
                .execute()
            )

        # ------------------------------------------------------------------
        # Upsert id(100) with TTL
        # ------------------------------------------------------------------
        await (
            self.session.upsert(SET.id(100))
            .bin("name").set_to("Tim")
            .bin("age").set_to(312)
            .bin("dob").set_to(int(time.time() * 1000))
            .bin("id2").set_to(100)
            .execute()
        )

        # ------------------------------------------------------------------
        # Insert customers at ids 900-905
        # ------------------------------------------------------------------
        await self.session.delete(SET.id(900), SET.id(901), SET.id(902),
                            SET.id(903), SET.id(904), SET.id(905)).execute()

        customer_rows = [
            (900, "Tim", 312, "brown"), (901, "Jane", 28, "blonde"),
            (902, "Bob", 54, "brown"), (903, "Jordan", 45, "red"),
            (904, "Alex", 67, "blonde"), (905, "Sam", 24, "brown"),
        ]
        for pk, name, age, hair in customer_rows:
            await (
                self.session.upsert(SET.id(pk))
                .bin("name").set_to(name)
                .bin("age").set_to(age)
                .bin("hair").set_to(hair)
                .bin("dob").set_to(int(time.time() * 1000))
                .execute()
            )
        print("Inserted ids 900-905")

        # ------------------------------------------------------------------
        # Loop insert ids 0-14 and 1000-1014
        # ------------------------------------------------------------------
        for i in range(15):
            await (
                self.session.upsert(SET.id(i))
                .bin("name").set_to(f"Tim-{i}")
                .bin("age").set_to(312 + i)
                .bin("hair").set_to("brown")
                .bin("dob").set_to(int(time.time() * 1000))
                .execute()
            )
            await (
                self.session.upsert(SET.id(1000 + i))
                .bin("name").set_to(f"Tim-{i}")
                .bin("age").set_to(312 + i)
                .bin("hair").set_to("brown")
                .bin("dob").set_to(int(time.time() * 1000))
                .execute()
            )

        await self.session.delete(SET.id(1), SET.id(2), SET.id(3), SET.id(5),
                            SET.id(7), SET.id(11), SET.id(13)).execute()

        # ------------------------------------------------------------------
        # Insert/update/delete id(102) + room map CDT operations
        # ------------------------------------------------------------------
        await self.session.delete(SET.id(102)).execute()

        await (
            self.session.insert(SET.id(102))
            .bin("name").set_to("Sue")
            .bin("age").set_to(27)
            .bin("id").set_to(102)
            .bin("dob").set_to(int(time.time() * 1000))
            .execute()
        )

        await (
            self.session.update(SET.id(102))
            .bin("age").set_to(26)
            .execute()
        )

        await self.session.delete(SET.id(102)).execute()

        await (
            self.session.upsert(SET.id(102))
            .bin("name").set_to("Sue")
            .bin("age").set_to(27)
            .bin("id").set_to(102)
            .bin("dob").set_to(int(time.time() * 1000))
            .bin("rooms").set_to({
                "room1": {"occupied": False, "rates": {1: 100, 2: 150, 3: -1}},
                "room2": {"occupied": True, "rates": {1: 90, 2: -1, 3: -1}},
                "room3": {"occupied": False, "rates": {1: 67, 2: 200, 3: 99}},
                "room4": {"occupied": True, "rates": {1: 98, 2: -1, 3: -1}},
                "room5": {"occupied": False, "rates": {1: 98, 2: -1, 3: -1}},
                "room6": {"occupied": True, "rates": {1: 98, 2: -1, 3: -1}},
            })
            .bin("rooms2").set_to({"test": True})
            .execute()
        )

        # CDT read operations on rooms map
        print("\nMap CDT operations on id(102)")
        stream = await (
            self.session.query(SET.id(102))
            .bin("rooms").on_map_index(2).get_values()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"  onMapIndex(2).getValues(): {first.record.bins}")

        stream = await (
            self.session.query(SET.id(102))
            .bin("rooms").on_map_key("room1").get_values()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"  onMapKey('room1').getValues(): {first.record.bins}")

        stream = await (
            self.session.query(SET.id(102))
            .bin("rooms").on_map_key_range("room1", "room3").count()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"  onMapKeyRange('room1','room3').count(): {first.record.bins}")

        stream = await (
            self.session.query(SET.id(102))
            .bin("rooms").on_map_key_range("room1", "room2").count_all_others()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"  onMapKeyRange('room1','room2').countAllOthers(): {first.record.bins}")

        # CDT write operations on rooms map
        await (
            self.session.upsert(SET.id(102))
            .bin("rooms").on_map_key("room2").map_clear()
            .execute()
        )

        await (
            self.session.upsert(SET.id(102))
            .bin("rooms2").map_clear()
            .execute()
        )

        print(await (await self.session.query(SET.id(102)).execute()).first())

        # Append / add on id(102)
        await (
            self.session.update(SET.id(102))
            .bin("name").append("-test")
            .bin("age").add(1)
            .execute()
        )
        print(await (await self.session.query(SET.id(102)).execute()).first())

        # Re-set id(102) for later tests
        await (
            self.session.upsert(SET.id(102))
            .bin("name").set_to("Sue")
            .bin("age").set_to(26)
            .bin("dob").set_to(int(time.time() * 1000))
            .execute()
        )

        # ------------------------------------------------------------------
        # Insert customers at ids 20-46
        # ------------------------------------------------------------------
        customers = [
            (20, "Jordan", 36), (21, "Alex", 27), (22, "Betty", 27),
            (23, "Bob", 33), (24, "Fred", 6), (25, "Alex", 28),
            (26, "Alex", 26), (27, "Jordan", 19), (28, "Gruper", 28),
            (29, "Bree", 24), (30, "Perry", 44), (31, "Alex", 27),
            (32, "Betty", 27), (33, "Wilma", 18), (34, "Joran", 82),
            (35, "Alex", 27), (36, "Fred", 99), (37, "Sydney", 22),
            (38, "Ita", 99), (39, "Rupert", 83), (40, "Dominic", 53),
            (41, "Tim", 27), (42, "Tim", 29), (43, "Tim", 31),
            (44, "Tim", 30), (45, "Tim", 33), (46, "Tim", 35),
        ]
        for pk, name, age in customers:
            await (
                self.session.upsert(SET.id(pk))
                .bin("name").set_to(name)
                .bin("age").set_to(age)
                .execute()
            )

        # ------------------------------------------------------------------
        # Background task
        # ------------------------------------------------------------------
        print(f"\nCustomer 46 age before scan: "
            f"{(await (await self.session.query(SET.id(46)).execute()).first()).record.bins.get('age')}")

        task = await (
            self.session.background_task()
            .update(SET)
            .bin("age").add(1)
            .execute()
        )
        await task.wait_till_complete()

        print(f"Customer 46 age after scan: "
            f"{(await (await self.session.query(SET.id(46)).execute()).first()).record.bins.get('age')}")

        # ------------------------------------------------------------------
        # Batch reads with where filter
        # ------------------------------------------------------------------
        keys = SET.ids(*range(20, 49))

        print("\nFull batch read:")
        stream = await self.session.query(keys).execute()
        await _print_stream(stream)
        stream.close()

        print("\nBatchRead where name = 'Tim':")
        stream = await self.session.query(keys).where("$.name == 'Tim'").execute()
        await _print_stream(stream)
        stream.close()

        print("\nBatchRead where name = 'Tim' (includeMissingKeys):")
        stream = await self.session.query(keys).include_missing_keys().where("$.name == 'Tim'").execute()
        await _print_stream(stream)
        stream.close()

        print("\nBatchRead where name = 'Tim' (includeMissingKeys + failOnFilteredOut):")
        try:
            stream = await (
                self.session.query(keys)
                .where("$.name == 'Tim'")
                .include_missing_keys()
                .fail_on_filtered_out()
                .execute()
            )
            await _print_stream(stream)
            stream.close()
        except AerospikeError as ae:
            print(f"  Exception: {ae}")

        # ------------------------------------------------------------------
        # Read the set, limit 6
        # ------------------------------------------------------------------
        print("\nRead the set, limit 6")
        stream = await self.session.query(SET).limit(6).execute()
        await _print_stream(stream)
        stream.close()

        # ------------------------------------------------------------------
        # Batch update with where filter
        # ------------------------------------------------------------------
        key_list_2 = SET.ids(20, 21, 22, 23, 24, 25, 26, 27)

        print(f"\nUpdate people in list whose age is < 35")
        stream = await (
            self.session.update(key_list_2)
            .bin("age").add(1)
            .where("$.age < 35")
            .execute()
        )
        await _print_stream(stream)
        stream.close()

        stream = await self.session.query(key_list_2).execute()
        await _print_stream(stream)
        stream.close()

        # ------------------------------------------------------------------
        # Read point records, limit
        # ------------------------------------------------------------------
        print("\nRead point records - in the same order as the keys, limit to 3")
        stream = await self.session.query(SET.ids(1, 3, 5, 7)).limit(3).execute()
        await _print_stream(stream)
        stream.close()

        print("\nSingle point record")
        stream = await self.session.query(SET.ids(6)).execute()
        await _print_stream(stream)
        stream.close()

        # ------------------------------------------------------------------
        # Read the set, output as stream, limit of 5
        # ------------------------------------------------------------------
        print("Read the set, output as stream, limit of 5")
        stream = await self.session.query(SET).limit(5).execute()
        async for rr in stream:
            print(f"  Name: {rr.record.bins.get('name') if rr.is_ok else 'N/A'}")
        stream.close()

        # ------------------------------------------------------------------
        # Read header
        # ------------------------------------------------------------------
        print("Read header, point read")
        stream = await self.session.query(SET.id(6)).with_no_bins().execute()
        await _print_stream(stream)
        stream.close()

        print("Read header, batch read")
        stream = await self.session.query(SET.ids(6, 7, 8)).with_no_bins().execute()
        await _print_stream(stream)
        stream.close()

        print("Read header, set read")
        stream = await self.session.query(SET).with_no_bins().execute()
        count = await _print_stream(stream)
        stream.close()

        # ------------------------------------------------------------------
        # Read with select bins
        # ------------------------------------------------------------------
        print("Read with select bins, point read")
        stream = await self.session.query(SET.ids(6)).bins(["name", "age"]).execute()
        await _print_stream(stream)
        stream.close()

        print("Read with select bins, batch read")
        stream = await self.session.query(SET.ids(6, 7, 8)).bins(["name", "age"]).execute()
        await _print_stream(stream)
        stream.close()

        print("Read with select bins, set read")
        stream = await self.session.query(SET).bins(["name", "age"]).execute()
        await _print_stream(stream)
        stream.close()

        # ------------------------------------------------------------------
        # Records-per-second check
        # ------------------------------------------------------------------
        print("Records-per-second check")
        stream = await self.session.query(SET).records_per_second(1).execute()
        count = 0
        async for rr in stream:
            if rr.is_ok:
                print(f"  {rr.record.bins}")
            count += 1
        stream.close()

        # ------------------------------------------------------------------
        # Server-side chunking — process records in chunks of 10
        # ------------------------------------------------------------------
        print("\nServer-side chunking, chunk_size=10")
        stream = await self.session.query(SET).chunk_size(10).execute()
        chunk = 0
        while await stream.has_more_chunks():
            chunk += 1
            print(f"Chunk: {chunk}")
            async for rr in stream:
                if rr.is_ok:
                    print(f"  {rr.record.bins}")
        stream.close()

        # ------------------------------------------------------------------
        # Sorting customers by Name with a where clause
        # (Python idiom for NavigatableRecordStream)
        # ------------------------------------------------------------------
        print("\n\nSorting customers by Name with a where clause using client-side sort")
        stream = await (
            self.session.query(SET)
            .where("$.name == 'Tim' and $.age > 30")
            .limit(1000)
            .execute()
        )
        results = []
        async for rr in stream:
            if rr.is_ok:
                results.append(rr.record)
        stream.close()

        results.sort(key=lambda r: r.bins.get("name", "").lower())
        for rec in results:
            print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
        print("End sorting customers by Name with a where clause\n")

        print("---- End sort ---")

        print("\n\nSorting customers by Age (desc) then name (asc), client-side pagination")
        stream = await self.session.query(SET).limit(13).execute()
        results = []
        async for rr in stream:
            if rr.is_ok:
                results.append(rr.record)
        stream.close()

        results.sort(key=lambda r: (-r.bins.get("age", 0), r.bins.get("name", "").lower()))
        page_size = 5
        page = 0
        for start in range(0, len(results), page_size):
            page += 1
            print(f"---- Page {page} -----")
            for rec in results[start:start + page_size]:
                print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
        print("---- End sort ---")

        # Jump to page 2
        print("--- Setting page to 2 ---")
        page_2 = results[page_size:page_size * 2]
        for rec in page_2:
            print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
        print("--- done with page 2 ---")

        # Re-sort by name only
        print("Re-sorting records by name")
        results.sort(key=lambda r: r.bins.get("name", ""))
        page_num = 0
        for start in range(0, len(results), page_size):
            page_num += 1
            print(f"---- Page {page_num} -----")
            for rec in results[start:start + page_size]:
                print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
        print("---- End sort ---")

        # ------------------------------------------------------------------
        # TTL Test
        # ------------------------------------------------------------------
        print("--- Test TTL ---")
        await self.session.delete(SET.id(1)).execute()

        await (
            self.session.upsert(SET.id(1))
            .bin("binA").set_to(5)
            .expire_record_after_seconds(5)
            .execute()
        )
        print("Initial read, should be there")
        stream = await self.session.query(SET.id(1)).execute()
        print(await stream.first())

        await asyncio.sleep(6)

        print("Read after TTL expires, should not be there")
        stream = await self.session.query(SET.id(1)).execute()
        print(await stream.first())

        # ------------------------------------------------------------------
        # Read and write expressions
        # ------------------------------------------------------------------
        print("--- Expression testing ---")
        await (
            self.session.upsert(SET.id(223))
            .bin("age").set_to(500)
            .bin("value").set_to(123)
            .execute()
        )

        stream = await self.session.query(SET.id(223)).execute()
        print(f"Base record: {await stream.first()}")

        print("Using a read expression")
        stream = await (
            self.session.query(SET.ids(223))
            .bin("bob").select_from("$.age + $.value", ignore_eval_failure=True)
            .execute()
        )
        await _print_stream(stream)
        stream.close()

        print("Using a write expression")
        await (
            self.session.update(SET.id(223))
            .bin("bob").upsert_from("$.age + 2 * $.value")
            .execute()
        )
        stream = await self.session.query(SET.id(223)).execute()
        print(f"Modified record: {await stream.first()}")

        # ------------------------------------------------------------------
        # Query hints
        # ------------------------------------------------------------------
        print("\n--- Query hints ---")
        try:
            await self.session.index(SET).on_bin("age").named("age_idx").numeric().create()
        except Exception:
            pass
        await asyncio.sleep(0.3)

        # Hint with index name: tell the server to use a specific secondary index
        stream = await (
            self.session.query(SET)
            .where("$.age > 30")
            .with_hint(QueryHint(index_name="age_idx"))
            .execute()
        )
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        print(f"  Hint with index name: {count} records")

        # Hint with bin name: prefer the secondary index on a given bin
        stream = await (
            self.session.query(SET)
            .where("$.age > 30")
            .with_hint(QueryHint(bin_name="age"))
            .execute()
        )
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        print(f"  Hint with bin name: {count} records")

        # ------------------------------------------------------------------
        # Background query operations
        # ------------------------------------------------------------------
        await (
            self.session.background_task()
            .update(SET)
            .bin("age").add(1)
            .where("$.name == 'Tim'")
            .execute()
        )

        # ------------------------------------------------------------------
        # Multi operation batches
        # ------------------------------------------------------------------
        print("\n--- Multi operation batches ---")
        stream = await (
            self.session.update(SET.id(1000)).bin("age").add(1)
            .update(SET.id(1001)).bin("age").add(1)
            .delete(SET.id(1003))
            .execute()
        )
        print("Multi operations:")
        await _print_stream(stream)
        stream.close()

        # ------------------------------------------------------------------
        # Generation check
        # ------------------------------------------------------------------
        print("\n--- Generation check test ----")

        stream = await self.session.query(SET.id(999)).execute()
        first = await stream.first()
        if first is None or not first.is_ok:
            await (
                self.session.upsert(SET.id(999))
                .bin("name").set_to("sample")
                .bin("age").set_to(456)
                .execute()
            )
            stream = await self.session.query(SET.id(999)).execute()
            first = await stream.first()

        if first and first.is_ok:
            generation = first.record.generation
            print(f"   Read record with generation of {generation}")
            await (
                self.session.update(SET.id(999))
                .bin("gen").set_to(generation)
                .ensure_generation_is(generation)
                .execute()
            )
            print("   First update was successful")

            try:
                await (
                    self.session.update(SET.id(999))
                    .bin("gen").set_to(generation)
                    .ensure_generation_is(generation)
                    .execute()
                )
                print("   Second update was successful -- this is an error")
            except AerospikeError as ae:
                print("   Second update failed as expected")

        # ------------------------------------------------------------------
        # Cleanup
        # ------------------------------------------------------------------
        try:
            await self.session.index(SET).named("age_idx").drop()
        except Exception:
            pass

        print("\nDone!")
