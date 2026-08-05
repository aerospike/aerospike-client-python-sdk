#!/usr/bin/env python3
"""Broad tour of the query, batch, CDT, expression, and hint surface.

Each logical area is factored into its own ``Demonstrate*`` class sharing a
single :class:`Session` and :class:`DataSet`, mirroring the reference example's
section layout so equivalent Java/Python snippets pair up one-to-one.

Object-mapping sections of the reference (typed data sets, ``toObjectList``,
async object mapping) are omitted here — PSDK reads records as ``dict`` bins,
so those sections have no Python counterpart yet.
"""

import asyncio
from datetime import timedelta

from _env import Example
from aerospike_sdk import Behavior, BitwiseOverflowActions, DataSet, MapOrder
from aerospike_sdk.aio.operations.query import QueryHint
from aerospike_sdk.exceptions import AerospikeError


class QueryExample(Example):
    SET = DataSet.of("test", "person")
    ADDRESS = DataSet.of("test", "address")

    async def __init__(self, host: "QueryExample | None" = None):
        if host is None:
            await super().__init__(
                Behavior.DEFAULT.derive_with_changes(
                    "newBehavior",
                    total_timeout=timedelta(seconds=2),
                )
            )
        else:
            self._behavior = host._behavior
            self._sc = host._sc
            self.cluster = host.cluster
            self.session = host.session
            self.users = host.users
            self.key = host.key

    async def _print_stream(self, stream) -> int:
        count = 0
        async for rr in stream:
            count += 1
            value = rr.record.bins if rr.is_ok and rr.record is not None else rr.result_code
            print(f"  {count:5d} - {value}")
        return count

    async def _first_bins(self, key) -> dict | None:
        rr = await (await self.session.query(key).execute()).first()
        return rr.record.bins if rr and rr.is_ok else None

    async def _first_bins_op(self, key, bin_name, op):
        # Small helper: run one CDT read op on a bin and return the positional result.
        builder = getattr(self.session.query(key).bin(bin_name), op)()
        stream = await builder.execute()
        return (await stream.first()).record.bins[bin_name]


class DemonstrateClusterInfo(QueryExample):
    async def run(self) -> None:
        # Namespaces the cluster serves, plus the secondary indexes it knows about.
        info = self.session.info()
        print(f"Namespaces: {sorted(await info.namespaces())}")
        for sindex in await info.secondary_indexes():
            print(f"Secondary index: {sindex.get('name')} on bin {sindex.get('bin')}")


class DemonstrateBasicWritesAndErrors(QueryExample):
    async def run(self) -> None:
        await self.session.truncate(self.SET)
        await asyncio.sleep(0.2)

        # Update a record that does not exist yet -> fails: update requires the record.
        try:
            await self.session.update(self.SET.id(1)).bin("bob").set_to(5).execute()
        except AerospikeError as ae:
            print(f"Exception caught as expected: {ae}")

        # Insert a record with several typed bins.
        await (
            self.session.insert(self.SET.id(1))
            .bin("Name").set_to("test1")
            .bin("i1").set_to(1)
            .bin("i2").set_to(2)
            .bin("f1").set_to(1.1)
            .bin("f2").set_to(2.2)
            .bin("s1").set_to("hello ")
            .bin("s2").set_to("world")
            .execute()
        )

        # Read back only a projection of bins.
        await (
            self.session.upsert(self.SET.id("bob")).bin("A").set_to(2).bin("B").set_to(2.2).execute()
        )
        stream = await self.session.query(self.SET.id("bob")).bins(["A"]).execute()
        rr = await stream.first()
        print(f"Projected read of id('bob'): {rr.record.bins if rr and rr.is_ok else None}")


class SeedData(QueryExample):
    async def run(self) -> None:
        # Bump a "holdings" counter on a handful of records with a single batch add.
        await self.session.upsert(self.SET.ids(1, 2, 3, 4, 5)).bin("holdings").add(1).execute()

        # Named/aged customers used by the batch, filter, sort, and hint sections.
        customers = [
            (1, "Tim", 312), (2, "Bob", 25), (3, "Jane", 46),
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
                self.session.upsert(self.SET.id(pk))
                .bin("name").set_to(name)
                .bin("age").set_to(age)
                .execute()
            )

        # A second block used by the point-read and multi-operation sections.
        for i in range(15):
            await (
                self.session.upsert(self.SET.id(1000 + i))
                .bin("name").set_to(f"Tim-{i}")
                .bin("age").set_to(312 + i)
                .bin("hair").set_to("brown")
                .execute()
            )
        print("Seeded customer records")


class DemonstrateConditionalUpdates(QueryExample):
    async def run(self) -> None:
        before = (await self._first_bins(self.SET.id(46))).get("age")
        print(f"\nCustomer 46 age before scan: {before}")

        # Background set-wide update: add 1 to every record's age.
        task = await self.session.background_task().update(self.SET).bin("age").add(1).execute()
        await task.wait_till_complete()

        after = (await self._first_bins(self.SET.id(46))).get("age")
        print(f"Customer 46 age after scan: {after}")


class DemonstrateBatchReads(QueryExample):
    async def run(self) -> None:
        keys = self.SET.ids(*range(20, 49))

        print("\nRead only records in partitions 0->2047")
        stream = await self.session.query(keys).on_partition_range(0, 2048).execute()
        await self._print_stream(stream)
        stream.close()

        print("\nFull batch read:")
        stream = await self.session.query(keys).execute()
        await self._print_stream(stream)
        stream.close()

        print("\nBatchRead where name = 'Tim':")
        stream = await self.session.query(keys).where("$.name == 'Tim'").execute()
        await self._print_stream(stream)
        stream.close()

        print("\nBatchRead where name = 'Tim' (include missing keys):")
        stream = await self.session.query(keys).include_missing_keys().where("$.name == 'Tim'").execute()
        await self._print_stream(stream)
        stream.close()

        print("\nBatchRead where name = 'Tim' (include missing keys + fail on filtered out):")
        try:
            stream = await (
                self.session.query(keys)
                .where("$.name == 'Tim'")
                .include_missing_keys()
                .fail_on_filtered_out()
                .execute()
            )
            await self._print_stream(stream)
            stream.close()
        except AerospikeError as ae:
            print(f"  Exception: {ae}")

        print("\nRead the set, limit 6")
        stream = await self.session.query(self.SET).limit(6).execute()
        await self._print_stream(stream)
        stream.close()


class DemonstrateFilteredUpdates(QueryExample):
    async def run(self) -> None:
        key_list = self.SET.ids(20, 21, 22, 23, 24, 25, 26, 27)

        print("\nUpdate people in list whose age is < 35")
        stream = await (
            self.session.update(key_list)
            .bin("age").add(1)
            .where("$.age < 35")
            .execute()
        )
        await self._print_stream(stream)
        stream.close()

        print("Results now that the update has finished:")
        stream = await self.session.query(key_list).execute()
        await self._print_stream(stream)
        stream.close()


class DemonstratePointAndHeaderReads(QueryExample):
    async def run(self) -> None:
        # With a list of ids and no sort clause, records stream back in id order.
        print("\nRead point records - in the same order as the keys, limit to 3")
        stream = await self.session.query(self.SET.ids(1, 3, 5, 7)).limit(3).execute()
        await self._print_stream(stream)
        stream.close()

        print("\nSingle point record")
        stream = await self.session.query(self.SET.ids(6)).execute()
        await self._print_stream(stream)
        stream.close()

        print("Read the set, output as stream, limit of 5")
        stream = await self.session.query(self.SET).limit(5).execute()
        async for rr in stream:
            print(f"  Name: {rr.record.bins.get('name') if rr.is_ok else 'N/A'}")
        stream.close()

        print("Read header, point read")
        stream = await self.session.query(self.SET.id(6)).with_no_bins().execute()
        await self._print_stream(stream)
        stream.close()
        print("Read header, batch read")
        stream = await self.session.query(self.SET.ids(6, 7, 8)).with_no_bins().execute()
        await self._print_stream(stream)
        stream.close()
        print("Read header, set read")
        stream = await self.session.query(self.SET).with_no_bins().execute()
        await self._print_stream(stream)
        stream.close()

        print("Read with select bins, point read")
        stream = await self.session.query(self.SET.ids(6)).bins(["name", "age"]).execute()
        await self._print_stream(stream)
        stream.close()
        print("Read with select bins, batch read")
        stream = await self.session.query(self.SET.ids(6, 7, 8)).bins(["name", "age"]).execute()
        await self._print_stream(stream)
        stream.close()
        print("Read with select bins, set read")
        stream = await self.session.query(self.SET).bins(["name", "age"]).execute()
        await self._print_stream(stream)
        stream.close()


class DemonstrateRecordsPerSecondAndChunking(QueryExample):
    async def run(self) -> None:
        print("\nRecords-per-second check")
        stream = await self.session.query(self.SET).records_per_second(1).execute()
        async for rr in stream:
            if rr.is_ok:
                print(f"  {rr.record.bins}")
        stream.close()

        print("\nServer-side chunking, chunk_size=10")
        stream = await self.session.query(self.SET).chunk_size(10).execute()
        chunk = 0
        while await stream.has_more_chunks():
            chunk += 1
            print(f"Chunk: {chunk}")
            async for rr in stream:
                if rr.is_ok:
                    print(f"  {rr.record.bins}")
        stream.close()


class DemonstrateSortingAndPagination(QueryExample):
    async def run(self) -> None:
        # PSDK returns records as a stream; sort and page client-side with plain Python.
        print("\n\nSorting customers by name with a where clause (client-side sort)")
        stream = await (
            self.session.query(self.SET)
            .where("$.name == 'Tim' and $.age > 30")
            .limit(1000)
            .execute()
        )
        results = [rr.record async for rr in stream if rr.is_ok]
        stream.close()

        results.sort(key=lambda r: r.bins.get("name", "").lower())
        for rec in results:
            print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
        print("---- End sort ---")

        print("\n\nSorting by age (desc) then name (asc), client-side pagination")
        stream = await self.session.query(self.SET).limit(13).execute()
        results = [rr.record async for rr in stream if rr.is_ok]
        stream.close()

        results.sort(key=lambda r: (-r.bins.get("age", 0), r.bins.get("name", "").lower()))
        page_size = 5
        for page, start in enumerate(range(0, len(results), page_size), start=1):
            print(f"---- Page {page} -----")
            for rec in results[start:start + page_size]:
                print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
        print("---- End sort ---")


class DemonstrateReusableFilter(QueryExample):
    async def run(self) -> None:
        # PSDK has no separate prepared-filter type; a reusable AEL string, formatted
        # per call, fills the same role.
        name_and_age_filter = "$.name == '{name}' and $.age > {age}"

        print("\nReusable filter (Tim, age > 30):")
        stream = await self.session.query(self.SET).where(
            name_and_age_filter.format(name="Tim", age=30)
        ).execute()
        await self._print_stream(stream)
        stream.close()

        print("Reusable filter (Alex, age > 21):")
        stream = await self.session.query(self.SET).where(
            name_and_age_filter.format(name="Alex", age=21)
        ).execute()
        await self._print_stream(stream)
        stream.close()


class DemonstrateTtl(QueryExample):
    async def run(self) -> None:
        print("\n--- Test TTL ---")
        await self.session.delete(self.SET.id(1)).execute()

        await (
            self.session.upsert(self.SET.id(1))
            .bin("binA").set_to(5)
            .expire_record_after_seconds(2)
            .execute()
        )
        print("Initial read, should be there")
        print(await self._first_bins(self.SET.id(1)))

        await asyncio.sleep(3)

        print("Read after TTL expires, should not be there")
        print(await self._first_bins(self.SET.id(1)))


class DemonstrateReadWriteExpressions(QueryExample):
    async def run(self) -> None:
        print("\n--- Expression testing ---")
        await (
            self.session.upsert(self.SET.id(223))
            .bin("age").set_to(500)
            .bin("value").set_to(123)
            .execute()
        )
        print(f"Base record: {await self._first_bins(self.SET.id(223))}")

        print("Using a read expression")
        stream = await (
            self.session.query(self.SET.ids(223))
            .bin("bob").select_from("$.age + $.value", ignore_eval_failure=True)
            .execute()
        )
        await self._print_stream(stream)
        stream.close()

        print("Using a write expression")
        await (
            self.session.update(self.SET.id(223))
            .bin("bob").upsert_from("$.age + 2 * $.value")
            .execute()
        )
        print(f"Modified record: {await self._first_bins(self.SET.id(223))}")


class DemonstrateQueryHints(QueryExample):
    async def run(self) -> None:
        print("\n--- Query hints ---")
        try:
            await self.session.index(self.SET).on_bin("age").named("age_idx").numeric().create()
        except Exception:
            pass
        await asyncio.sleep(0.3)

        # Hint with index name: tell the server to use a specific secondary index.
        stream = await (
            self.session.query(self.SET).where("$.age > 30").with_hint(QueryHint(index_name="age_idx")).execute()
        )
        count = sum([1 async for _ in stream])
        stream.close()
        print(f"  Hint with index name: {count} records")

        # Hint with bin name: prefer the secondary index on a given bin.
        stream = await (
            self.session.query(self.SET).where("$.age > 30").with_hint(QueryHint(bin_name="age")).execute()
        )
        count = sum([1 async for _ in stream])
        stream.close()
        print(f"  Hint with bin name: {count} records")


class DemonstrateBackgroundQuery(QueryExample):
    async def run(self) -> None:
        # Background set-wide update restricted by a where clause.
        task = await (
            self.session.background_task()
            .update(self.SET)
            .bin("age").add(1)
            .where("$.name == 'Tim'")
            .execute()
        )
        await task.wait_till_complete()


class DemonstrateMultiOperationBatches(QueryExample):
    async def run(self) -> None:
        print("\n--- Multi operation batches ---")
        stream = await (
            self.session.update(self.SET.id(1000)).bin("age").add(1)
            .update(self.SET.id(1001)).bin("age").add(1)
            .delete(self.SET.id(1003))
            .execute()
        )
        await self._print_stream(stream)
        stream.close()


class DemonstrateGenerationCheck(QueryExample):
    async def run(self) -> None:
        print("\n--- Generation check test ----")

        first = await (await self.session.query(self.SET.id(999)).execute()).first()
        if first is None or not first.is_ok:
            await (
                self.session.upsert(self.SET.id(999))
                .bin("name").set_to("sample")
                .bin("age").set_to(456)
                .execute()
            )
            first = await (await self.session.query(self.SET.id(999)).execute()).first()

        generation = first.record.generation
        print(f"   Read record with generation of {generation}")
        await (
            self.session.update(self.SET.id(999))
            .bin("gen").set_to(generation)
            .ensure_generation_is(generation)
            .execute()
        )
        print("   First update was successful")

        try:
            # The second update reuses the now-stale generation and must fail.
            await (
                self.session.update(self.SET.id(999))
                .bin("gen").set_to(generation)
                .ensure_generation_is(generation)
                .execute()
            )
            print("   Second update was successful -- this is an error")
        except AerospikeError:
            print("   Second update failed as expected")


class DemonstrateComplexCdt(QueryExample):
    async def run(self) -> None:
        print("\n--- Complex CDT operations ---")
        cdt = self.SET.id(500)
        await self.session.delete(cdt).execute()
        await (
            self.session.upsert(cdt)
            .bin("scores").set_to([95, 82, 73, 88, 91])
            .bin("tags").set_to(["python", "rust", "go"])
            .bin("inventory").set_to({"apples": 10, "bananas": 5, "cherries": 20})
            .bin("nested").set_to({
                "team1": {"members": ["Alice", "Bob", "Charlie"]},
                "team2": {"members": ["Dave", "Eve"]},
            })
            .execute()
        )

        # --- Read-only operations via query path (top-level) ---
        print(f"List size of 'scores': {(await self._first_bins_op(cdt, 'scores', 'list_size'))}")
        print(f"Map size of 'inventory': {(await self._first_bins_op(cdt, 'inventory', 'map_size'))}")
        stream = await self.session.query(cdt).bin("scores").list_get(0).execute()
        print(f"First score: {(await stream.first()).record.bins['scores']}")
        stream = await self.session.query(cdt).bin("scores").list_get_range(1, 3).execute()
        print(f"Scores [1..3]: {(await stream.first()).record.bins['scores']}")

        # --- Read-only operations via query path with CDT navigation ---
        stream = await (
            self.session.query(cdt).bin("nested").on_map_key("team1").on_map_key("members").list_size().execute()
        )
        print(f"Team1 member count: {(await stream.first()).record.bins['nested']}")

        # --- Write operations: list mutations ---
        await self.session.update(cdt).bin("scores").list_append_items([77, 65, 99]).execute()
        print(f"After list_append_items: {(await self._first_bins(cdt))['scores']}")

        await self.session.update(cdt).bin("tags").list_insert(1, "kotlin").execute()
        print(f"After list_insert(1, 'kotlin'): {(await self._first_bins(cdt))['tags']}")

        await self.session.update(cdt).bin("scores").list_sort().execute()
        print(f"After list_sort: {(await self._first_bins(cdt))['scores']}")

        await self.session.update(cdt).bin("scores").list_remove(0).execute()
        print(f"After list_remove(0): {(await self._first_bins(cdt))['scores']}")

        # --- Write operations: map mutations ---
        await (
            self.session.update(cdt)
            .bin("inventory").map_upsert_items({"dates": 15, "elderberries": 8})
            .execute()
        )
        print(f"After map_upsert_items: {(await self._first_bins(cdt))['inventory']}")

        await self.session.update(cdt).bin("inventory").map_set_policy(MapOrder.KEY_ORDERED).execute()
        print(f"After map_set_policy(KEY_ORDERED): {(await self._first_bins(cdt))['inventory']}")

        # --- Write operations: CDT navigation ---
        await (
            self.session.update(cdt)
            .bin("nested").on_map_key("team1").on_map_key("members").list_append_items(["Diana"])
            .execute()
        )
        stream = await (
            self.session.query(cdt).bin("nested").on_map_key("team1").on_map_key("members").list_size().execute()
        )
        print(f"Team1 size after nested list_append('Diana'): {(await stream.first()).record.bins['nested']}")

        # --- Combined multi-bin CDT operations in one call ---
        stream = await (
            self.session.update(cdt)
            .bin("scores").list_append_items([50, 60, 70])
            .bin("inventory").map_upsert_items({"figs": 12})
            .bin("nested").on_map_key("team2").on_map_key("members").list_append_items(["Quinn"])
            .execute()
        )
        await self._print_stream(stream)
        stream.close()
        print(f"Final state: {await self._first_bins(cdt)}")
        print("--- End Complex CDT operations ---")


class DemonstrateBitOperations(QueryExample):
    async def run(self) -> None:
        print("\n--- Bit (BLOB) operations ---")
        bit_key = self.SET.id(501)
        await self.session.delete(bit_key).execute()
        await self.session.upsert(bit_key).bin("flags").set_to(b"\x01\x42").execute()

        await (
            self.session.update(bit_key)
            .bin("flags").bit_resize(4)
            .bin("flags").bit_set(8, 8, b"\xff")
            .bin("flags").bit_or(0, 16, b"\x0f\xf0")
            .execute()
        )

        stream = await (
            self.session.query(bit_key)
            .bin("flags").bit_get(0, 8)
            .bin("flags").bit_count(0, 32)
            .execute()
        )
        print(f"First byte + set-bit count: {(await stream.first()).record.bins['flags']}")

        stream = await self.session.query(bit_key).bin("flags").bit_get_int(0, 16, False).execute()
        print(f"UInt16 at bit 0: {(await stream.first()).record.bins['flags']}")

        await (
            self.session.update(bit_key)
            .bin("flags").bit_set_int(16, 16, 100)
            .bin("flags").bit_add(16, 16, 1, False, BitwiseOverflowActions.WRAP)
            .execute()
        )
        stream = await self.session.query(bit_key).bin("flags").bit_get_int(16, 16, False).execute()
        print(f"After bit_set_int/bit_add: {(await stream.first()).record.bins['flags']}")

        await (
            self.session.update(bit_key)
            .bin("flags").bit_insert(1, b"\x11\x22")
            .bin("flags").bit_remove(3, 1)
            .execute()
        )
        print(f"Final flags blob: {(await self._first_bins(bit_key))['flags']}")
        print("--- End Bit (BLOB) operations ---")


class DemonstrateHeterogeneousBatch(QueryExample):
    async def run(self) -> None:
        # One batch spanning two different sets (person + address).
        await (
            self.session.upsert(self.ADDRESS.id(1))
            .bin("line1").set_to("123 Main St")
            .bin("city").set_to("Denver")
            .bin("state").set_to("CO")
            .execute()
        )
        print("\n--- Heterogeneous batch example ---")
        stream = await (
            self.session.query(self.SET.ids(21, 22, 23))
            .query(self.ADDRESS.id(1))
            .execute()
        )
        await self._print_stream(stream)
        stream.close()
