#!/usr/bin/env python3
"""Comprehensive example demonstrating all common SDK API operations.

Covers truncate, upsert, query, exists, touch, delete, batch operations,
AEL filters, secondary index queries, background tasks, expression
read/write, and query hints.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.aio.operations.query import QueryHint
from aerospike_sdk.exceptions import AerospikeError

SET = DataSet.of("test", "set")


async def main() -> None:
    async with await _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await run_examples(session)


async def run_examples(session) -> None:
    # ------------------------------------------------------------------
    # Truncate
    # ------------------------------------------------------------------
    print("Truncate records")
    await session.truncate(SET)
    await asyncio.sleep(0.2)

    # ------------------------------------------------------------------
    # Write a single record
    # ------------------------------------------------------------------
    print("Write 1 record")
    await (
        session.upsert(SET.id(10))
        .bin("name").set_to("Charlie")
        .bin("age").set_to(11)
        .execute()
    )

    # ------------------------------------------------------------------
    # Write multiple records — one batched call, not a call per record
    # ------------------------------------------------------------------
    print("Write 3 records")
    await (
        session.upsert(SET.id(1)).bin("name").set_to("Tim").bin("age").set_to(312)
        .upsert(SET.id(2)).bin("name").set_to("Bob").bin("age").set_to(25)
        .upsert(SET.id(3)).bin("name").set_to("Jane").bin("age").set_to(46)
        .execute()
    )

    # Ten records is the same one call, built in a loop rather than spelled out.
    print("Write 10 records")
    batch = session
    for pk, name, age in [
        (10, "Tim", 312), (11, "Bob", 25), (12, "Jane", 46),
        (13, "Tim", 200), (14, "User1", 201), (15, "User2", 202),
        (16, "User3", 203), (17, "User4", 204), (18, "User5", 205),
        (19, "User6", 206),
    ]:
        batch = batch.upsert(SET.id(pk)).bin("name").set_to(name).bin("age").set_to(age)
    await batch.execute()

    # ------------------------------------------------------------------
    # Read 1 record (point read)
    # ------------------------------------------------------------------
    print("\nRead 1 record")
    stream = await session.query(SET.id(10)).execute()
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
    stream = await session.query(SET.ids(1, 2)).execute()
    async for result in stream:
        rec = result.record_or_raise()
        print(f"  Record = {rec.bins}")
    stream.close()

    # ------------------------------------------------------------------
    # Exists
    # ------------------------------------------------------------------
    print("Exists 1 record")
    stream = await session.exists(SET.id(13)).execute()
    first = await stream.first()
    print(f"  Result: {first.as_bool() if first else None}")

    # ------------------------------------------------------------------
    # Touch
    # ------------------------------------------------------------------
    print("Touch 1 record")
    await session.touch(SET.id(13)).execute()
    print("  Done")

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------
    print("Delete 1 record")
    await session.delete(SET.id(18)).execute()
    print("  Done")

    # ------------------------------------------------------------------
    # Batch exists (with include_missing_keys)
    # ------------------------------------------------------------------
    print("Batch exists")
    stream = await session.exists(SET.id(13), SET.id(14), SET.id(999)).include_missing_keys().execute()
    async for rr in stream:
        print(f"  Key: {rr.key} -> {rr.as_bool()}")
    stream.close()

    # ------------------------------------------------------------------
    # Batch touch (with include_missing_keys)
    # ------------------------------------------------------------------
    print("Batch touch")
    stream = await session.touch(SET.id(13), SET.id(14), SET.id(999)).include_missing_keys().execute()
    async for rr in stream:
        print(f"  Key: {rr.key} -> {rr.as_bool()}")
    stream.close()

    # ------------------------------------------------------------------
    # Batch delete (with include_missing_keys)
    # ------------------------------------------------------------------
    print("Batch delete")
    await session.upsert(SET.id(13)).put({"name": "Tim", "age": 200}).execute()
    await session.upsert(SET.id(14)).put({"name": "User1", "age": 201}).execute()
    stream = await session.delete(SET.id(13), SET.id(14), SET.id(999)).include_missing_keys().execute()
    async for rr in stream:
        print(f"  Key: {rr.key} -> {rr.as_bool()}")
    stream.close()

    # Re-insert deleted records for remaining examples
    await session.upsert(SET.id(13)).put({"name": "Tim", "age": 200}).execute()
    await session.upsert(SET.id(14)).put({"name": "User1", "age": 201}).execute()

    # ------------------------------------------------------------------
    # Query with AEL where (filter expression)
    # ------------------------------------------------------------------
    print("\nTest filtering out")
    stream = await (
        session.query(SET.id(2))
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
        session.query(SET.id(2))
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
        session.query(SET.id(2))
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
    try:
        stream = await (
            session.query(SET.id(2))
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
    stream = await session.query(SET).records_per_second(5000).execute()
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
        await session.index(SET).on_bin("age").named("ageidx").numeric().create()
    except Exception:
        pass  # Index may already exist
    await asyncio.sleep(0.3)

    # ------------------------------------------------------------------
    # Secondary index query with AEL where
    # ------------------------------------------------------------------
    print("Foreground secondary index query")
    stream = await session.query(SET).where("$.age > 200").execute()
    count = 0
    async for result in stream:
        print(f"  {result.record.bins}")
        count += 1
    stream.close()
    print(f"  Query count: {count}")

    # ------------------------------------------------------------------
    # Batch read after changes
    # ------------------------------------------------------------------
    stream = await session.query(SET.ids(10, 11)).execute()
    async for result in stream:
        rec = result.record_or_raise()
        print(f"  Record = {rec.bins}")
    stream.close()

    # ------------------------------------------------------------------
    # Background update with AEL where
    # ------------------------------------------------------------------
    print("\nBackground query")
    task = await (
        session.background_task()
        .update(SET)
        .bin("age").add(1)
        .where("$.name == 'Tim' and $.age > 20")
        .execute()
    )
    await task.wait_till_complete()

    stream = await session.query(SET.ids(10, 13)).execute()
    async for result in stream:
        rec = result.record_or_raise()
        print(f"  Record = {rec.bins}")
    stream.close()

    # ------------------------------------------------------------------
    # Expression read and write operations
    # ------------------------------------------------------------------
    print("\nRead and write operation example")

    # Upsert + select_from + upsert_from in one operate
    stream = await (
        session.upsert(SET.ids(1, 2, 3))
        .bin("name").set_to("Tim")
        .bin("readBin").select_from("$.age + 12")
        .bin("writeBin").upsert_from("$.age + 30")
        .execute()
    )
    async for rr in stream:
        print(f"  Upsert with expressions: {rr.record.bins}")
    stream.close()

    # Single read expression: compute $.age + 20
    stream = await (
        session.query(SET.id(1))
        .bin("ageIn20Years").select_from("$.age + 20")
        .execute()
    )
    first = await stream.first()
    if first and first.is_ok:
        print(f"  Single read expression: {first.record.bins}")

    # Batch read expression
    stream = await (
        session.query(SET.ids(1, 2, 3))
        .bin("ageIn20Years").select_from("$.age + 20")
        .execute()
    )
    async for result in stream:
        print(f"  Batch read expression: {result.record.bins}")
    stream.close()

    # ------------------------------------------------------------------
    # Query hints
    # ------------------------------------------------------------------
    print("\nQuery with hint")
    stream = await (
        session.query(SET)
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
        await session.index(SET).named("ageidx").drop()
    except Exception:
        pass

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
