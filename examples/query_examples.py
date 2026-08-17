#!/usr/bin/env python3
"""Broad tour of the query, batch, CDT, expression, and hint surface.

Each logical area is factored into its own ``demonstrate_*`` coroutine sharing a
single :class:`Session` and :class:`DataSet`, mirroring the reference example's
section layout so equivalent Java/Python snippets pair up one-to-one.

Object-mapping sections of the reference (typed data sets, ``toObjectList``,
async object mapping) are omitted here — PSDK reads records as ``dict`` bins,
so those sections have no Python counterpart yet.
"""

import asyncio
from datetime import timedelta

import _env
from aerospike_sdk import Behavior, BitwiseOverflowActions, DataSet, MapOrder
from aerospike_sdk.aio.operations.query import QueryHint
from aerospike_sdk.exceptions import AerospikeError

SET = DataSet.of("test", "person")
ADDRESS = DataSet.of("test", "address")


async def _print_stream(stream) -> int:
    count = 0
    async for rr in stream:
        count += 1
        value = rr.record.bins if rr.is_ok and rr.record is not None else rr.result_code
        print(f"  {count:5d} - {value}")
    return count


async def _first_bins(session, key) -> dict | None:
    rr = await (await session.query(key).execute()).first()
    return rr.record.bins if rr and rr.is_ok else None


async def main() -> None:
    async with await _env.connect().connect() as cluster:
        # A behavior deriving a longer query timeout from the default.
        custom_behavior = Behavior.DEFAULT.derive_with_changes(
            "newBehavior",
            total_timeout=timedelta(seconds=2),
        )
        session = cluster.create_session(custom_behavior)

        await demonstrate_cluster_info(session)
        await demonstrate_basic_writes_and_errors(session)
        await seed_data(session)
        await demonstrate_conditional_updates(session)
        await demonstrate_batch_reads(session)
        await demonstrate_filtered_updates(session)
        await demonstrate_point_and_header_reads(session)
        await demonstrate_records_per_second_and_chunking(session)
        await demonstrate_sorting_and_pagination(session)
        await demonstrate_reusable_filter(session)
        await demonstrate_ttl(session)
        await demonstrate_read_write_expressions(session)
        await demonstrate_query_hints(session)
        await demonstrate_background_query(session)
        await demonstrate_multi_operation_batches(session)
        await demonstrate_generation_check(session)
        await demonstrate_complex_cdt(session)
        await demonstrate_bit_operations(session)
        await demonstrate_heterogeneous_batch(session)

        print("\nDone!")


async def demonstrate_cluster_info(session) -> None:
    # Namespaces the cluster serves, plus the secondary indexes it knows about.
    info = session.info()
    print(f"Namespaces: {sorted(await info.namespaces())}")
    for sindex in await info.secondary_indexes():
        print(f"Secondary index: {sindex.get('name')} on bin {sindex.get('bin')}")


async def demonstrate_basic_writes_and_errors(session) -> None:
    await session.truncate(SET)
    await asyncio.sleep(0.2)

    # Update a record that does not exist yet -> fails: update requires the record.
    try:
        await session.update(SET.id(1)).bin("bob").set_to(5).execute()
    except AerospikeError as ae:
        print(f"Exception caught as expected: {ae}")

    # Insert a record with several typed bins.
    await (
        session.insert(SET.id(1))
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
        session.upsert(SET.id("bob")).bin("A").set_to(2).bin("B").set_to(2.2).execute()
    )
    stream = await session.query(SET.id("bob")).bins(["A"]).execute()
    rr = await stream.first()
    print(f"Projected read of id('bob'): {rr.record.bins if rr and rr.is_ok else None}")


async def seed_data(session) -> None:
    # Bump a "holdings" counter on a handful of records with a single batch add.
    await session.upsert(SET.ids(1, 2, 3, 4, 5)).bin("holdings").add(1).execute()

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
            session.upsert(SET.id(pk))
            .bin("name").set_to(name)
            .bin("age").set_to(age)
            .execute()
        )

    # A second block used by the point-read and multi-operation sections.
    for i in range(15):
        await (
            session.upsert(SET.id(1000 + i))
            .bin("name").set_to(f"Tim-{i}")
            .bin("age").set_to(312 + i)
            .bin("hair").set_to("brown")
            .execute()
        )
    print("Seeded customer records")


async def demonstrate_conditional_updates(session) -> None:
    before = (await _first_bins(session, SET.id(46))).get("age")
    print(f"\nCustomer 46 age before scan: {before}")

    # Background set-wide update: add 1 to every record's age.
    task = await session.background_task().update(SET).bin("age").add(1).execute()
    await task.wait_till_complete()

    after = (await _first_bins(session, SET.id(46))).get("age")
    print(f"Customer 46 age after scan: {after}")


async def demonstrate_batch_reads(session) -> None:
    keys = SET.ids(*range(20, 49))

    print("\nRead only records in partitions 0->2047")
    stream = await session.query(keys).on_partition_range(0, 2048).execute()
    await _print_stream(stream)
    stream.close()

    print("\nFull batch read:")
    stream = await session.query(keys).execute()
    await _print_stream(stream)
    stream.close()

    print("\nBatchRead where name = 'Tim':")
    stream = await session.query(keys).where("$.name == 'Tim'").execute()
    await _print_stream(stream)
    stream.close()

    print("\nBatchRead where name = 'Tim' (include missing keys):")
    stream = await session.query(keys).include_missing_keys().where("$.name == 'Tim'").execute()
    await _print_stream(stream)
    stream.close()

    print("\nBatchRead where name = 'Tim' (include missing keys + fail on filtered out):")
    try:
        stream = await (
            session.query(keys)
            .where("$.name == 'Tim'")
            .include_missing_keys()
            .fail_on_filtered_out()
            .execute()
        )
        await _print_stream(stream)
        stream.close()
    except AerospikeError as ae:
        print(f"  Exception: {ae}")

    print("\nRead the set, limit 6")
    stream = await session.query(SET).limit(6).execute()
    await _print_stream(stream)
    stream.close()


async def demonstrate_filtered_updates(session) -> None:
    key_list = SET.ids(20, 21, 22, 23, 24, 25, 26, 27)

    print("\nUpdate people in list whose age is < 35")
    stream = await (
        session.update(key_list)
        .bin("age").add(1)
        .where("$.age < 35")
        .execute()
    )
    await _print_stream(stream)
    stream.close()

    print("Results now that the update has finished:")
    stream = await session.query(key_list).execute()
    await _print_stream(stream)
    stream.close()


async def demonstrate_point_and_header_reads(session) -> None:
    # With a list of ids and no sort clause, records stream back in id order.
    print("\nRead point records - in the same order as the keys, limit to 3")
    stream = await session.query(SET.ids(1, 3, 5, 7)).limit(3).execute()
    await _print_stream(stream)
    stream.close()

    print("\nSingle point record")
    stream = await session.query(SET.ids(6)).execute()
    await _print_stream(stream)
    stream.close()

    print("Read the set, output as stream, limit of 5")
    stream = await session.query(SET).limit(5).execute()
    async for rr in stream:
        print(f"  Name: {rr.record.bins.get('name') if rr.is_ok else 'N/A'}")
    stream.close()

    print("Read header, point read")
    stream = await session.query(SET.id(6)).with_no_bins().execute()
    await _print_stream(stream)
    stream.close()
    print("Read header, batch read")
    stream = await session.query(SET.ids(6, 7, 8)).with_no_bins().execute()
    await _print_stream(stream)
    stream.close()
    print("Read header, set read")
    stream = await session.query(SET).with_no_bins().execute()
    await _print_stream(stream)
    stream.close()

    print("Read with select bins, point read")
    stream = await session.query(SET.ids(6)).bins(["name", "age"]).execute()
    await _print_stream(stream)
    stream.close()
    print("Read with select bins, batch read")
    stream = await session.query(SET.ids(6, 7, 8)).bins(["name", "age"]).execute()
    await _print_stream(stream)
    stream.close()
    print("Read with select bins, set read")
    stream = await session.query(SET).bins(["name", "age"]).execute()
    await _print_stream(stream)
    stream.close()


async def demonstrate_records_per_second_and_chunking(session) -> None:
    print("\nRecords-per-second check")
    stream = await session.query(SET).records_per_second(1).execute()
    async for rr in stream:
        if rr.is_ok:
            print(f"  {rr.record.bins}")
    stream.close()

    print("\nServer-side chunking, chunk_size=10")
    stream = await session.query(SET).chunk_size(10).execute()
    chunk = 0
    while await stream.has_more_chunks():
        chunk += 1
        print(f"Chunk: {chunk}")
        async for rr in stream:
            if rr.is_ok:
                print(f"  {rr.record.bins}")
    stream.close()


async def demonstrate_sorting_and_pagination(session) -> None:
    # PSDK returns records as a stream; sort and page client-side with plain Python.
    print("\n\nSorting customers by name with a where clause (client-side sort)")
    stream = await (
        session.query(SET)
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
    stream = await session.query(SET).limit(13).execute()
    results = [rr.record async for rr in stream if rr.is_ok]
    stream.close()

    results.sort(key=lambda r: (-r.bins.get("age", 0), r.bins.get("name", "").lower()))
    page_size = 5
    for page, start in enumerate(range(0, len(results), page_size), start=1):
        print(f"---- Page {page} -----")
        for rec in results[start:start + page_size]:
            print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
    print("---- End sort ---")


async def demonstrate_reusable_filter(session) -> None:
    # PSDK has no separate prepared-filter type; a reusable AEL string, formatted
    # per call, fills the same role.
    name_and_age_filter = "$.name == '{name}' and $.age > {age}"

    print("\nReusable filter (Tim, age > 30):")
    stream = await session.query(SET).where(
        name_and_age_filter.format(name="Tim", age=30)
    ).execute()
    await _print_stream(stream)
    stream.close()

    print("Reusable filter (Alex, age > 21):")
    stream = await session.query(SET).where(
        name_and_age_filter.format(name="Alex", age=21)
    ).execute()
    await _print_stream(stream)
    stream.close()


async def demonstrate_ttl(session) -> None:
    print("\n--- Test TTL ---")
    await session.delete(SET.id(1)).execute()

    await (
        session.upsert(SET.id(1))
        .bin("binA").set_to(5)
        .expire_record_after_seconds(2)
        .execute()
    )
    print("Initial read, should be there")
    print(await _first_bins(session, SET.id(1)))

    await asyncio.sleep(3)

    print("Read after TTL expires, should not be there")
    print(await _first_bins(session, SET.id(1)))


async def demonstrate_read_write_expressions(session) -> None:
    print("\n--- Expression testing ---")
    await (
        session.upsert(SET.id(223))
        .bin("age").set_to(500)
        .bin("value").set_to(123)
        .execute()
    )
    print(f"Base record: {await _first_bins(session, SET.id(223))}")

    print("Using a read expression")
    stream = await (
        session.query(SET.ids(223))
        .bin("bob").select_from("$.age + $.value", ignore_eval_failure=True)
        .execute()
    )
    await _print_stream(stream)
    stream.close()

    print("Using a write expression")
    await (
        session.update(SET.id(223))
        .bin("bob").upsert_from("$.age + 2 * $.value")
        .execute()
    )
    print(f"Modified record: {await _first_bins(session, SET.id(223))}")


async def demonstrate_query_hints(session) -> None:
    print("\n--- Query hints ---")
    try:
        await session.index(SET).on_bin("age").named("age_idx").numeric().create()
    except Exception:
        pass
    await asyncio.sleep(0.3)

    # Hint with index name: tell the server to use a specific secondary index.
    stream = await (
        session.query(SET).where("$.age > 30").with_hint(QueryHint(index_name="age_idx")).execute()
    )
    count = sum([1 async for _ in stream])
    stream.close()
    print(f"  Hint with index name: {count} records")

    # Hint with bin name: prefer the secondary index on a given bin.
    stream = await (
        session.query(SET).where("$.age > 30").with_hint(QueryHint(bin_name="age")).execute()
    )
    count = sum([1 async for _ in stream])
    stream.close()
    print(f"  Hint with bin name: {count} records")


async def demonstrate_background_query(session) -> None:
    # Background set-wide update restricted by a where clause.
    task = await (
        session.background_task()
        .update(SET)
        .bin("age").add(1)
        .where("$.name == 'Tim'")
        .execute()
    )
    await task.wait_till_complete()


async def demonstrate_multi_operation_batches(session) -> None:
    print("\n--- Multi operation batches ---")
    stream = await (
        session.update(SET.id(1000)).bin("age").add(1)
        .update(SET.id(1001)).bin("age").add(1)
        .delete(SET.id(1003))
        .execute()
    )
    await _print_stream(stream)
    stream.close()


async def demonstrate_generation_check(session) -> None:
    print("\n--- Generation check test ----")

    first = await (await session.query(SET.id(999)).execute()).first()
    if first is None or not first.is_ok:
        await (
            session.upsert(SET.id(999))
            .bin("name").set_to("sample")
            .bin("age").set_to(456)
            .execute()
        )
        first = await (await session.query(SET.id(999)).execute()).first()

    generation = first.record.generation
    print(f"   Read record with generation of {generation}")
    await (
        session.update(SET.id(999))
        .bin("gen").set_to(generation)
        .ensure_generation_is(generation)
        .execute()
    )
    print("   First update was successful")

    try:
        # The second update reuses the now-stale generation and must fail.
        await (
            session.update(SET.id(999))
            .bin("gen").set_to(generation)
            .ensure_generation_is(generation)
            .execute()
        )
        print("   Second update was successful -- this is an error")
    except AerospikeError:
        print("   Second update failed as expected")


async def demonstrate_complex_cdt(session) -> None:
    print("\n--- Complex CDT operations ---")
    cdt = SET.id(500)
    await session.delete(cdt).execute()
    await (
        session.upsert(cdt)
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
    print(f"List size of 'scores': {(await _first_bins_op(session, cdt, 'scores', 'list_size'))}")
    print(f"Map size of 'inventory': {(await _first_bins_op(session, cdt, 'inventory', 'map_size'))}")
    stream = await session.query(cdt).bin("scores").list_get(0).execute()
    print(f"First score: {(await stream.first()).record.bins['scores']}")
    stream = await session.query(cdt).bin("scores").list_get_range(1, 3).execute()
    print(f"Scores [1..3]: {(await stream.first()).record.bins['scores']}")

    # --- Read-only operations via query path with CDT navigation ---
    stream = await (
        session.query(cdt).bin("nested").on_map_key("team1").on_map_key("members").list_size().execute()
    )
    print(f"Team1 member count: {(await stream.first()).record.bins['nested']}")

    # --- Write operations: list mutations ---
    await session.update(cdt).bin("scores").list_append_items([77, 65, 99]).execute()
    print(f"After list_append_items: {(await _first_bins(session, cdt))['scores']}")

    await session.update(cdt).bin("tags").list_insert(1, "kotlin").execute()
    print(f"After list_insert(1, 'kotlin'): {(await _first_bins(session, cdt))['tags']}")

    await session.update(cdt).bin("scores").list_sort().execute()
    print(f"After list_sort: {(await _first_bins(session, cdt))['scores']}")

    await session.update(cdt).bin("scores").list_remove(0).execute()
    print(f"After list_remove(0): {(await _first_bins(session, cdt))['scores']}")

    # --- Write operations: map mutations ---
    await (
        session.update(cdt)
        .bin("inventory").map_upsert_items({"dates": 15, "elderberries": 8})
        .execute()
    )
    print(f"After map_upsert_items: {(await _first_bins(session, cdt))['inventory']}")

    await session.update(cdt).bin("inventory").map_set_policy(MapOrder.KEY_ORDERED).execute()
    print(f"After map_set_policy(KEY_ORDERED): {(await _first_bins(session, cdt))['inventory']}")

    # --- Write operations: CDT navigation ---
    await (
        session.update(cdt)
        .bin("nested").on_map_key("team1").on_map_key("members").list_append_items(["Diana"])
        .execute()
    )
    stream = await (
        session.query(cdt).bin("nested").on_map_key("team1").on_map_key("members").list_size().execute()
    )
    print(f"Team1 size after nested list_append('Diana'): {(await stream.first()).record.bins['nested']}")

    # --- Combined multi-bin CDT operations in one call ---
    stream = await (
        session.update(cdt)
        .bin("scores").list_append_items([50, 60, 70])
        .bin("inventory").map_upsert_items({"figs": 12})
        .bin("nested").on_map_key("team2").on_map_key("members").list_append_items(["Quinn"])
        .execute()
    )
    await _print_stream(stream)
    stream.close()
    print(f"Final state: {await _first_bins(session, cdt)}")
    print("--- End Complex CDT operations ---")


async def _first_bins_op(session, key, bin_name, op):
    # Small helper: run one CDT read op on a bin and return the positional result.
    builder = getattr(session.query(key).bin(bin_name), op)()
    stream = await builder.execute()
    return (await stream.first()).record.bins[bin_name]


async def demonstrate_bit_operations(session) -> None:
    print("\n--- Bit (BLOB) operations ---")
    bit_key = SET.id(501)
    await session.delete(bit_key).execute()
    await session.upsert(bit_key).bin("flags").set_to(b"\x01\x42").execute()

    await (
        session.update(bit_key)
        .bin("flags").bit_resize(4)
        .bin("flags").bit_set(8, 8, b"\xff")
        .bin("flags").bit_or(0, 16, b"\x0f\xf0")
        .execute()
    )

    stream = await (
        session.query(bit_key)
        .bin("flags").bit_get(0, 8)
        .bin("flags").bit_count(0, 32)
        .execute()
    )
    print(f"First byte + set-bit count: {(await stream.first()).record.bins['flags']}")

    stream = await session.query(bit_key).bin("flags").bit_get_int(0, 16, False).execute()
    print(f"UInt16 at bit 0: {(await stream.first()).record.bins['flags']}")

    await (
        session.update(bit_key)
        .bin("flags").bit_set_int(16, 16, 100)
        .bin("flags").bit_add(16, 16, 1, False, BitwiseOverflowActions.WRAP)
        .execute()
    )
    stream = await session.query(bit_key).bin("flags").bit_get_int(16, 16, False).execute()
    print(f"After bit_set_int/bit_add: {(await stream.first()).record.bins['flags']}")

    await (
        session.update(bit_key)
        .bin("flags").bit_insert(1, b"\x11\x22")
        .bin("flags").bit_remove(3, 1)
        .execute()
    )
    print(f"Final flags blob: {(await _first_bins(session, bit_key))['flags']}")
    print("--- End Bit (BLOB) operations ---")


async def demonstrate_heterogeneous_batch(session) -> None:
    # One batch spanning two different sets (person + address).
    await (
        session.upsert(ADDRESS.id(1))
        .bin("line1").set_to("123 Main St")
        .bin("city").set_to("Denver")
        .bin("state").set_to("CO")
        .execute()
    )
    print("\n--- Heterogeneous batch example ---")
    stream = await (
        session.query(SET.ids(21, 22, 23))
        .query(ADDRESS.id(1))
        .execute()
    )
    await _print_stream(stream)
    stream.close()


if __name__ == "__main__":
    asyncio.run(main())
