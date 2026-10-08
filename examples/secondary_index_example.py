#!/usr/bin/env python3
"""Demonstrates secondary indexes.

Covers the scalar index types (numeric, string) and the queries they serve,
a LIST collection index with a membership query, a MAP_VALUES collection
index, an index on a nested element via a CTX path, the error raised by a
conflicting redefinition, and dropping an index.

Index creation is asynchronous on the server. ``create_index()`` returns a task and
``wait_till_complete()`` waits until every node has finished building, which is
what makes the query immediately afterwards deterministic.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, CollectionIndexType, CTX, DataSet, IndexType
from aerospike_sdk.exceptions import AerospikeError, SecondaryIndexError

SET = DataSet.of("test", "sindex-demo")

AGE_INDEX = "sidx_age"
CITY_INDEX = "sidx_city"
TAGS_INDEX = "sidx_tags"
SCORE_INDEX = "sidx_scores"
NESTED_INDEX = "sidx_addr_zip"

INDEX_NAMES = (AGE_INDEX, CITY_INDEX, TAGS_INDEX, SCORE_INDEX, NESTED_INDEX)

PEOPLE = (
    (1, "Alice", 34, "Berlin", ["admin", "ops"], {"math": 91, "art": 70},
     {"zip": "10115", "street": "Torstr"}),
    (2, "Bob", 28, "Berlin", ["dev"], {"math": 64, "art": 88},
     {"zip": "10247", "street": "Boxhagener"}),
    (3, "Carol", 45, "Lisbon", ["admin", "dev"], {"math": 78, "art": 95},
     {"zip": "1100", "street": "Rua Augusta"}),
    (4, "Dan", 22, "Lisbon", ["intern"], {"math": 55, "art": 61},
     {"zip": "1200", "street": "Rua do Ouro"}),
)


async def run_examples(session) -> None:
    # --- 1) Scalar indexes: numeric and string ---
    print("--- 1) Scalar indexes: numeric and string ---")
    task = await session.create_index(SET, AGE_INDEX, "age", IndexType.INTEGER)
    await task.wait_till_complete()
    task = await session.create_index(SET, CITY_INDEX, "city", IndexType.STRING)
    await task.wait_till_complete()

    over_30 = await count(session, "$.age > 30")
    in_berlin = await count(session, "$.city == 'Berlin'")
    print(f"people over 30: {over_30}")
    print(f"people in Berlin: {in_berlin}")

    # --- 2) Collection index: LIST indexes each element of a list bin ---
    print("--- 2) Collection index: LIST indexes each element of a list bin ---")
    task = await session.create_index(
        SET, TAGS_INDEX, "tags", IndexType.STRING, CollectionIndexType.LIST,
    )
    await task.wait_till_complete()
    tagged_admin = await count(session, '"admin" in $.tags')
    print(f"people tagged 'admin': {tagged_admin}")

    # --- 3) Collection index: MAP_VALUES indexes each value of a map bin ---
    print("--- 3) Collection index: MAP_VALUES indexes each value of a map bin ---")
    task = await session.create_index(
        SET, SCORE_INDEX, "scores", IndexType.INTEGER, CollectionIndexType.MAP_VALUES,
    )
    await task.wait_till_complete()
    print(f"created {SCORE_INDEX} over the values of the 'scores' map")

    # --- 4) Index a nested element with a CTX path ---
    print("--- 4) Index a nested element with a CTX path ---")
    task = await session.create_index(
        SET, NESTED_INDEX, "address", IndexType.STRING, ctx=[CTX.map_key("zip")],
    )
    await task.wait_till_complete()
    print(f"created {NESTED_INDEX} over address.zip")

    # --- 5) Reusing an index name for a different bin raises SecondaryIndexError ---
    print("--- 5) Reusing an index name for a different bin raises SecondaryIndexError ---")
    # Re-creating an index with an identical definition is accepted as a no-op,
    # which makes startup code that unconditionally creates its indexes safe.
    # Reusing the name for a different definition is the case that fails.
    try:
        task = await session.create_index(SET, AGE_INDEX, "city", IndexType.STRING)
        await task.wait_till_complete()
        raise AssertionError("expected SecondaryIndexError was not raised")
    except SecondaryIndexError as exc:
        print(f"  caught {type(exc).__name__} (code {exc.result_code}): {exc.base_message}")

    # --- 6) Drop an index ---
    print("--- 6) Drop an index ---")
    task = await session.drop_index(SET, SCORE_INDEX)
    await task.wait_till_complete()
    print(f"dropped {SCORE_INDEX}")


async def count(session, ael: str) -> int:
    """Number of records in the set matching an AEL filter."""
    found = 0
    async with await session.query(SET).where(ael).execute() as stream:
        async for row in stream:
            row.record_or_raise()
            found += 1
    return found


async def seed(session) -> None:
    for pk, name, age, city, tags, scores, address in PEOPLE:
        await (
            session.upsert(SET.id(pk))
            .bin("name").set_to(name)
            .bin("age").set_to(age)
            .bin("city").set_to(city)
            .bin("tags").set_to(tags)
            .bin("scores").set_to(scores)
            .bin("address").set_to(address)
            .execute()
        )


async def drop_all(session) -> None:
    """Drop every index this example creates, ignoring ones that aren't there."""
    for name in INDEX_NAMES:
        try:
            task = await session.drop_index(SET, name)
            await task.wait_till_complete()
        except AerospikeError:
            pass


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await session.truncate(SET)
        await asyncio.sleep(0.2)
        # A previous run's indexes would make section 5's conflict check pass
        # for the wrong reason, so start from a known-clean state.
        await drop_all(session)
        await seed(session)

        try:
            await run_examples(session)
        finally:
            await drop_all(session)


if __name__ == "__main__":
    asyncio.run(main())
