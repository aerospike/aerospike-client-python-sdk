#!/usr/bin/env python3
"""Broad tour of the query, batch, CDT, expression, and hint surface.

Each logical area is factored into its own ``demonstrate_*`` coroutine sharing a
single :class:`Session` and :class:`DataSet`, so each section can be read and
lifted on its own.

Records are read as ``dict`` bins throughout; there is no object-mapping layer
to demonstrate.
"""

import asyncio
from datetime import datetime, timedelta

import _env
from aerospike_sdk import (
    Behavior,
    BitwiseOverflowActions,
    DataSet,
    Exp,
    ListOrderType,
    MapOrder,
    QueryDuration,
    QueryHint,
    ResultCode,
)
from aerospike_sdk.exceptions import AerospikeError, IndexAlreadyExistsError
from aerospike_sdk.policy import Settings

SET = DataSet.of("test", "person")
ADDRESS = DataSet.of("test", "address")
USERS = DataSet.of("test", "users")


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


async def demonstrate_cluster_info(session) -> None:
    # Namespaces the cluster serves, plus the secondary indexes it knows about.
    info = session.info()
    for ns in sorted(await info.namespaces()):
        detail = await info.namespace_details(ns)
        if detail is not None:
            print(detail)
    for sindex in await info.secondary_indexes():
        print(f"Secondary index: {sindex.name} on bin {sindex.bin_name}")
        print(f"   {await info.secondary_index_details(sindex.namespace, sindex.name)}")


async def demonstrate_basic_writes_and_errors(session) -> None:
    await session.truncate(SET)
    await asyncio.sleep(0.2)

    # An update with no operations fails before reaching the server.
    try:
        await session.update(SET.id(1)).execute()
    except AerospikeError as ae:
        print(f"Exception caught as expected: {ae} ({type(ae).__name__})")

    # Update a record that does not exist yet -> fails: update requires the record.
    try:
        await session.update(SET.id(1)).bin("bob").set_to(5).execute()
    except AerospikeError as ae:
        print(
            f"Exception caught as expected on node {ae.node}: {ae} ({type(ae).__name__})"
        )

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
    stream = await session.query(SET.id("bob")).bins("A").execute()
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

    # Probe for a record's existence, then delete it.
    exists_row = await (await session.exists(SET.ids(2)).execute()).first_or_raise()
    print(f"id(2) exists: {exists_row.as_bool()}")
    await session.delete(SET.ids(2)).execute()

    # Absolute-time TTL: the record expires at a fixed calendar date.
    await (
        session.upsert(SET.id(100))
        .bin("name").set_to("Tim")
        .bin("age").set_to(312)
        .bin("id2").set_to(100)
        .expire_record_at(datetime(2030, 1, 1))
        .execute()
    )

    # One batched insert with a per-record TTL on one row. The "state" bin
    # feeds the background-query section later.
    await session.delete(SET.ids(900, 901, 902, 903, 904, 905)).execute()
    stream = await (
        session.insert(SET.id(900))
        .bin("name").set_to("Tim").bin("age").set_to(312).bin("hair").set_to("brown")
        .insert(SET.id(901))
        .bin("name").set_to("Jane").bin("age").set_to(28).bin("hair").set_to("blonde")
        .insert(SET.id(902))
        .bin("name").set_to("Bob").bin("age").set_to(54).bin("hair").set_to("brown")
        .expire_record_after(timedelta(days=5))
        .insert(SET.id(903))
        .bin("name").set_to("Jordan").bin("age").set_to(45).bin("hair").set_to("red")
        .bin("state").set_to("nsw")
        .insert(SET.id(904))
        .bin("name").set_to("Alex").bin("age").set_to(67).bin("hair").set_to("blonde")
        .bin("state").set_to("nsw")
        .insert(SET.id(905))
        .bin("name").set_to("Sam").bin("age").set_to(24).bin("hair").set_to("brown")
        .bin("state").set_to("qld")
        .execute()
    )
    count = 0
    async for _ in stream:
        count += 1
    stream.close()
    print(f"Batched insert of ids 900-905 returned {count} rows (902 expires in 5 days)")

    # A second block used by the point-read and multi-operation sections.
    for i in range(15):
        await (
            session.upsert(SET.id(1000 + i))
            .bin("name").set_to(f"Tim-{i}")
            .bin("age").set_to(312 + i)
            .bin("hair").set_to("brown")
            .expire_record_after(timedelta(days=30))
            .execute()
        )

    # A record with nested map bins, then one call mixing reads and writes on it.
    await session.delete(SET.id(102)).execute()
    await (
        session.upsert(SET.id(102))
        .bin("name").set_to("Sue")
        .bin("age").set_to(27)
        .bin("id").set_to(102)
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

    stream = await (
        session.upsert(SET.id(102))
        .bin("name").set_to("Bob")
        .bin("age").set_to(30)
        .bin("id").get()
        .bin("rooms").on_map_index(2).get_values()
        .bin("rooms").on_map_key_range("room1", "room2").count_all_others()
        .bin("rooms").on_map_key("room1").get_values()
        .bin("rooms").on_map_key_range("room1", "room3").count()
        .bin("rooms").on_map_key("room1").on_map_key("rates").on_map_key(1).set_to(110)
        .bin("rooms").on_map_key("room2").map_clear()
        .bin("rooms").on_map_key_range("room4", "room9").remove()
        .bin("rooms").on_map_key("room1").on_map_key("rates").on_map_key(1).add(5)
        .bin("rooms").on_map_key_relative_index_range("bob", -1, 1).get_keys_and_values()
        .bin("rooms2").map_clear()
        .bin("rooms2").on_map_key("child", create_type=MapOrder.KEY_ORDERED)
        .on_map_key("sub_child").set_to(5)
        .execute()
    )
    print(f"Mixed read/write results: {(await stream.first_or_raise()).record.bins}")
    print(f"Record 102: {await _first_bins(session, SET.id(102))}")

    # In-place string append and numeric add on existing bins.
    await (
        session.update(SET.id(102))
        .bin("name").append("-test")
        .bin("age").add(1)
        .execute()
    )
    print(f"Record 102 after append/add: {await _first_bins(session, SET.id(102))}")

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

    print("\nBatch read where name = 'Tim':")
    stream = await session.query(keys).where("$.name == 'Tim'").execute()
    await _print_stream(stream)
    stream.close()

    print("\nBatch read where name = 'Tim' (include missing keys):")
    stream = await session.query(keys).include_missing_keys().where("$.name == 'Tim'").execute()
    await _print_stream(stream)
    stream.close()

    print("\nBatch read where name = 'Tim' (include missing keys + fail on filtered out):")
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

    print("\nUnfiltered update: add 1 to every age in the list")
    stream = await session.update(key_list).bin("age").add(1).execute()
    await _print_stream(stream)
    stream.close()
    print("Results now that the update has finished:")
    stream = await session.query(key_list).execute()
    await _print_stream(stream)
    stream.close()

    print("\nUpdate people in list whose age is < 35")
    stream = await (
        session.update(key_list)
        .bin("age").add(1)
        .where("$.age < 35")
        .execute()
    )
    await _print_stream(stream)
    stream.close()

    # fail_on_filtered_out reports the rows the filter excluded as errors
    # instead of silently skipping them.
    print("Same update with fail_on_filtered_out:")
    stream = await (
        session.update(key_list)
        .bin("age").add(1)
        .where("$.age < 35")
        .fail_on_filtered_out()
        .execute()
    )
    await _print_stream(stream)
    stream.close()

    print("Results after the filtered updates:")
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
    stream = await session.query(SET.ids(6)).bins("name", "age").execute()
    await _print_stream(stream)
    stream.close()
    print("Read with select bins, batch read")
    stream = await session.query(SET.ids(6, 7, 8)).bins("name", "age").execute()
    await _print_stream(stream)
    stream.close()
    print("Read with select bins, set read")
    stream = await session.query(SET).bins("name", "age").execute()
    await _print_stream(stream)
    stream.close()

    # Requesting specific bins and no bins at once is contradictory and raises.
    try:
        session.query(SET.ids(6, 7, 8)).bins("name", "age").with_no_bins()
    except ValueError as ve:
        print(f"Exception caught as expected: {ve}")


async def demonstrate_records_per_second_and_chunking(session) -> None:
    print("\nRecords-per-second check")
    stream = await session.query(SET).records_per_second(1).execute()
    async for rr in stream:
        if rr.is_ok:
            print(f"  {rr.record.bins}")
    stream.close()

    print("\nServer-side chunking, chunk_size=20 (collect all)")
    stream = await session.query(SET).chunk_size(20).execute()
    names = sorted([
        rr.record.bins["name"]
        async for rr in stream
        if rr.is_ok and "name" in rr.record.bins
    ])
    stream.close()
    print(f"  {len(names)} named records: {names}")

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
    # Records come back as a stream; sort and page client-side with plain Python.
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

    # The same predicate built programmatically instead of as an AEL string.
    print("\nSame filter built as an expression")
    stream = await (
        session.query(SET)
        .where(Exp.and_([
            Exp.eq(Exp.string_bin("name"), Exp.string_val("Tim")),
            Exp.gt(Exp.int_bin("age"), Exp.int_val(30)),
        ]))
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
    pages = [results[i:i + page_size] for i in range(0, len(results), page_size)]
    for page_num, page in enumerate(pages, start=1):
        print(f"---- Page {page_num} -----")
        for rec in page:
            print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
    print("---- End sort ---")

    # Jump straight back to a specific page.
    if len(pages) > 1:
        print("--- Setting page to 2 ---")
        for rec in pages[1]:
            print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
        print("--- done with page 2 ---")

    # Now re-sort the same records by name only.
    print("Re-sorting records by name")
    results.sort(key=lambda r: r.bins.get("name", "").lower())
    for page_num, start in enumerate(range(0, len(results), page_size), start=1):
        print(f"---- Page {page_num} -----")
        for rec in results[start:start + page_size]:
            print(f"  name={rec.bins.get('name')}, age={rec.bins.get('age')}")
    print("---- End sort ---")


async def demonstrate_reusable_filter(session) -> None:
    # A reusable AEL template; where() binds the printf-style parameters per call.
    name_and_age_filter = "$.name == '%s' and $.age > %d"

    print("\nReusable filter (Tim, age > 30):")
    stream = await session.query(SET).where(name_and_age_filter, "Tim", 30).execute()
    await _print_stream(stream)
    stream.close()

    print("Reusable filter (Jane, age > 21):")
    stream = await session.query(SET).where(name_and_age_filter, "Jane", 21).execute()
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
    # replace writes the record fresh: only these bins remain afterward.
    await (
        session.replace(SET.id(223))
        .bin("age").set_to(500)
        .bin("value").set_to(123)
        .execute()
    )
    print(f"Base record: {await _first_bins(session, SET.id(223))}")

    # A plain single-bin projection read.
    stream = await session.query(SET.id(223)).bin("age").get().execute()
    await _print_stream(stream)
    stream.close()

    print("Using a read expression")
    # A bin path carries no type of its own. With a literal operand the server
    # infers one (see "$.age + 2 * $.value" below), but adding two bins leaves
    # both untyped, so ":INT" pins one side and the operator carries it across.
    stream = await (
        session.query(SET.ids(223))
        .bin("bob").select_from("$.age:INT + $.value", ignore_eval_failure=True)
        .execute()
    )
    await _print_stream(stream)
    stream.close()

    print("Using a write expression")
    # Expression write flags: ignore a failed op; delete the bin on a null result.
    await (
        session.upsert(SET.id(1))
        .bin("age").set_to(50)
        .bin("value").set_to(10)
        .bin("c2").upsert_from(
            "$.age + 2 * $.value", ignore_op_failure=True, delete_if_null=True,
        )
        .execute()
    )
    await (
        session.update(SET.id(223))
        .bin("bob").upsert_from("$.age + 2 * $.value")
        .execute()
    )
    print(f"Modified record: {await _first_bins(session, SET.id(223))}")

    # A bins projection over the whole set; the rows are not printed here.
    stream = await session.query(SET).bins("name", "age").execute()
    async for _ in stream:
        pass
    stream.close()


async def demonstrate_query_hints(session) -> None:
    print("\n--- Query hints ---")
    # A hint can only name an index that exists, so this section owns one.
    try:
        task = await session.index(SET).on_bin("age").named("age_idx").integer().create()
        await task.wait_till_complete()
    except IndexAlreadyExistsError:
        pass  # An earlier run already created it with the same definition.

    # Every hint below runs the same predicate; only the plan advice changes.
    for label, hint in (
        # Name a specific secondary index for the server to use.
        ("index_name", QueryHint(index_name="age_idx")),
        # Prefer the secondary index on a given bin.
        ("bin_name", QueryHint(bin_name="age")),
        # Declare how long the query is expected to run, which steers how the
        # server sizes its resources for it.
        ("query_duration", QueryHint(query_duration=QueryDuration.SHORT)),
        # Hints combine.
        (
            "index_name + duration",
            QueryHint(index_name="age_idx", query_duration=QueryDuration.SHORT),
        ),
        # Order doesn't matter: duration first, then the bin.
        (
            "query_duration + bin_name",
            QueryHint(query_duration=QueryDuration.SHORT, bin_name="age"),
        ),
    ):
        await count_hinted_query(
            label, session.query(SET).where("$.age > 30").with_hint(hint)
        )

    task = await session.index(SET).named("age_idx").drop()
    await task.wait_till_complete()


async def count_hinted_query(label: str, query) -> None:
    """Run a hinted query and report how many records it matched.

    A hint only changes how the server reaches the records, never which ones
    come back, so every variant should report the same count.
    """
    stream = await query.execute()
    count = 0
    async for _ in stream:
        count += 1
    stream.close()
    print(f"  {label:<25} matched {count} records")


async def demonstrate_background_query(session) -> None:
    # Background set-wide update restricted by a where clause.
    task = await (
        session.background_task()
        .update(SET)
        .bin("age").add(1)
        .where("$.state == 'nsw'")
        .execute()
    )
    await task.wait_till_complete()


async def demonstrate_multi_operation_batches(session) -> None:
    print("\n--- Multi operation batches ---")

    # Reads, writes, existence checks, and deletes mix freely in one round trip.
    # Chain-level defaults (like a default TTL) are set where the chain starts.
    stream = await (
        session.query(SET.ids(10, 12))
        .default_expire_record_after(timedelta(minutes=20))
        .update(SET.ids(1000, 1001))
        .bin("age").add(1)
        .expire_record_after(timedelta(minutes=5))
        .exists(SET.ids(1000, 1001))
        .delete(SET.id(1003))
        .with_txn(None)
        .execute()
    )
    print("Multi operations:")
    await _print_stream(stream)
    stream.close()

    # Read segments can carry their own bin and CDT projections.
    stream = await (
        session.query(SET.ids(1, 2, 3))
        .bin("name").get()
        .bin("map").on_map_key_range(5, 10).get_keys_and_values()
        .update(SET.ids(1))
        .bin("age").add(1)
        .execute()
    )
    async for _ in stream:
        pass
    stream.close()

    # default_where applies to every segment that has no filter of its own.
    stream = await (
        session.query(SET.ids(5, 6, 7))
        .default_where("$.updated == false")
        .update(SET.ids(1, 2, 3))
        .bin("age").add(1)
        .bin("updated").set_to(True)
        .where("$.age < 21")
        .delete(SET.ids(11, 12, 13, 14, 15))
        .update(SET.ids(5, 6, 7))
        .bin("lucky_winner").set_to("true")
        .execute()
    )
    async for _ in stream:
        pass
    stream.close()

    # Per-segment limits and TTLs.
    stream = await (
        session.query(SET.ids(1, 2, 3))
        .limit(2)
        .update(SET.ids(4, 5, 6))
        .bin("name").set_to("bob")
        .expire_record_after_seconds(500)
        .query(SET.id(7))
        .execute()
    )
    async for _ in stream:
        pass
    stream.close()

    # Iterate a filtered query row by row, checking each result.
    stream = await (
        session.query(USERS)
        .where("$.status == 'active' and $.age >= 21")
        .execute()
    )
    async for res in stream:
        if res.is_ok:
            print(f"  active user: {res.record.bins}")
    stream.close()

    # A dataset query cannot also carry bin operations; the combination raises.
    try:
        await (
            session.query(SET)
            .where("$.name == 'Tim'")
            .bin("fred").get()
            .execute()
        )
        print("Expected query with bin operations to raise")
    except AerospikeError as ae:
        print(f"Query with bin operations raised as expected: {ae}")


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
    except AerospikeError as ae:
        print("   Second update failed as expected")
        print(f"   {ae.result_code == ResultCode.GENERATION_ERROR}")


async def demonstrate_complex_cdt(session) -> None:
    print("\n--- Complex CDT operations ---")
    cdt = SET.id(500)
    await session.delete(cdt).execute()
    await (
        session.upsert(cdt)
        .bin("scores").set_to([95, 82, 73, 88, 91])
        .bin("tags").set_to(["python", "rust", "c"])
        .bin("inventory").set_to({"apples": 10, "bananas": 5, "cherries": 20})
        .bin("nested").set_to({
            "team1": {"members": ["Alice", "Bob", "Charlie"]},
            "team2": {"members": ["Dave", "Eve"]},
        })
        .execute()
    )

    # --- Read-only operations via query path (top-level) ---
    stream = await session.query(cdt).bin("scores").list_size().execute()
    print(f"List size of 'scores': {(await stream.first()).record.bins['scores']}")
    stream = await session.query(cdt).bin("inventory").map_size().execute()
    print(f"Map size of 'inventory': {(await stream.first()).record.bins['inventory']}")
    stream = await session.query(cdt).bin("scores").list_get(0).execute()
    print(f"First score: {(await stream.first()).record.bins['scores']}")
    stream = await session.query(cdt).bin("scores").list_get_range(1, 3).execute()
    print(f"Scores [1..3]: {(await stream.first()).record.bins['scores']}")
    # Omitting the count reads to the end of the list.
    stream = await session.query(cdt).bin("scores").list_get_range(3).execute()
    print(f"Scores from index 3 onward: {(await stream.first()).record.bins['scores']}")

    # --- Read-only operations via query path with CDT navigation ---
    stream = await (
        session.query(cdt).bin("nested").on_map_key("team1").on_map_key("members").list_size().execute()
    )
    print(f"Team1 member count: {(await stream.first()).record.bins['nested']}")
    stream = await (
        session.query(cdt).bin("nested").on_map_key("team1").on_map_key("members").list_get(1).execute()
    )
    print(f"Team1 second member: {(await stream.first()).record.bins['nested']}")
    stream = await (
        session.query(cdt)
        .bin("nested").on_map_key("team2").on_map_key("members").list_get_range(0, 2)
        .execute()
    )
    print(f"Team2 members [0..1]: {(await stream.first()).record.bins['nested']}")

    # --- Write operations: list mutations ---
    await session.update(cdt).bin("scores").list_append_items([77, 65, 99]).execute()
    print(f"After list_append_items: {(await _first_bins(session, cdt))['scores']}")

    await session.update(cdt).bin("tags").list_insert(1, "go").execute()
    print(f"After list_insert(1, 'go'): {(await _first_bins(session, cdt))['tags']}")

    # list_set overwrites in place; list_insert shifts the tail right.
    await session.update(cdt).bin("tags").list_set(0, "kotlin").execute()
    print(f"After list_set(0, 'kotlin'): {(await _first_bins(session, cdt))['tags']}")

    await session.update(cdt).bin("scores").list_insert_items(2, [100, 200]).execute()
    print(f"After list_insert_items(2, [100, 200]): {(await _first_bins(session, cdt))['scores']}")

    await session.update(cdt).bin("scores").list_increment(0, 5).execute()
    print(f"After list_increment(0, 5): {(await _first_bins(session, cdt))['scores']}")

    await session.update(cdt).bin("scores").list_sort().execute()
    print(f"After list_sort: {(await _first_bins(session, cdt))['scores']}")

    await session.update(cdt).bin("scores").list_remove(0).execute()
    print(f"After list_remove(0): {(await _first_bins(session, cdt))['scores']}")

    await session.update(cdt).bin("scores").list_remove_range(4, 2).execute()
    print(f"After list_remove_range(4, 2): {(await _first_bins(session, cdt))['scores']}")

    # list_pop removes the element and hands it back.
    stream = await session.update(cdt).bin("scores").list_pop(0).execute()
    print(f"Popped element: {(await stream.first()).record.bins['scores']}")

    await session.update(cdt).bin("scores").list_trim(0, 3).execute()
    print(f"After list_trim(0, 3): {(await _first_bins(session, cdt))['scores']}")

    await session.update(cdt).bin("tags").list_clear().execute()
    print(f"After list_clear on tags: {(await _first_bins(session, cdt))['tags']}")

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
    print(f"Team1 size after nested list_append_items(['Diana']): {(await stream.first()).record.bins['nested']}")

    await (
        session.update(cdt)
        .bin("nested").on_map_key("team2").on_map_key("members").list_insert(0, "Zara")
        .execute()
    )
    stream = await (
        session.query(cdt)
        .bin("nested").on_map_key("team2").on_map_key("members").list_get_range(0)
        .execute()
    )
    print(f"After nested list_insert to team2: {(await stream.first()).record.bins['nested']}")

    await (
        session.update(cdt)
        .bin("nested").on_map_key("team1").on_map_key("members").list_sort()
        .execute()
    )
    stream = await (
        session.query(cdt)
        .bin("nested").on_map_key("team1").on_map_key("members").list_get_range(0)
        .execute()
    )
    print(f"After sorting team1 members: {(await stream.first()).record.bins['nested']}")

    # Create a new ordered list sub-element via CDT navigation.
    await (
        session.update(cdt)
        .bin("nested").on_map_key("team3", create_type=MapOrder.KEY_ORDERED)
        .on_map_key("members").list_create(ListOrderType.ORDERED)
        .execute()
    )
    await (
        session.update(cdt)
        .bin("nested").on_map_key("team3").on_map_key("members")
        .list_add_items(["Ivy", "Frank", "Grace"])
        .execute()
    )
    stream = await (
        session.query(cdt)
        .bin("nested").on_map_key("team3").on_map_key("members").list_get_range(0)
        .execute()
    )
    print(f"Team3 members (ordered): {(await stream.first()).record.bins['nested']}")

    # --- Combined multi-bin CDT operations in one call ---
    stream = await (
        session.update(cdt)
        .bin("scores").list_append_items([50, 60, 70])
        .bin("inventory").map_upsert_items({"figs": 12})
        .bin("nested").on_map_key("team2").on_map_key("members").list_append_items(["Quinn"])
        .bin("nested").on_map_key("team1").on_map_key("members").list_size()
        .execute()
    )
    print(f"Combined CDT result: {(await stream.first_or_raise()).record.bins}")
    print(f"Final state: {await _first_bins(session, cdt)}")
    print("--- End Complex CDT operations ---")


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

    # Scan for the first set/unset bit in a range: lscan works left-to-right,
    # rscan right-to-left, and both report -1 when the range holds no match.
    result = await (
        await session.query(bit_key)
        .bin("flags").bit_lscan(0, 32, True)
        .bin("flags").bit_rscan(0, 32, True)
        .execute()
    ).first_or_raise()
    print(f"Scan result: first set bit={result.operation_result(0)}, "
          f"last set bit={result.operation_result(1)}")

    await (
        session.update(bit_key)
        .bin("flags").bit_set_int(16, 16, 100)
        .bin("flags").bit_add(16, 16, 1, False, BitwiseOverflowActions.WRAP)
        .execute()
    )
    stream = await session.query(bit_key).bin("flags").bit_get_int(16, 16, False).execute()
    print(f"After bit_set_int/bit_add: {(await stream.first()).record.bins['flags']}")

    # Shift the first byte left one bit, then invert the second byte.
    await (
        session.update(bit_key)
        .bin("flags").bit_lshift(0, 8, 1)
        .bin("flags").bit_not(8, 8)
        .execute()
    )

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
        .bin("country").set_to("USA")
        .bin("zip").set_to("80000")
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


async def main() -> None:
    async with _env.connect().connect() as cluster:
        # A behavior deriving a longer query timeout from the default.
        # Set-wide queries below filter on bins with no secondary index. That is
        # rejected by default so a full scan is never entered by accident, so
        # this behavior opts into the scan fallback for query reads.
        custom_behavior = Behavior.DEFAULT.derive_with_changes(
            "custom-behavior",
            total_timeout=timedelta(seconds=2),
            reads_query=Settings(allow_scans_with_where=True),
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


if __name__ == "__main__":
    asyncio.run(main())
