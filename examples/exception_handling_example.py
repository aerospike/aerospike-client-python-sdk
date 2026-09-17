#!/usr/bin/env python3
"""Demonstrates the exception hierarchy.

Provokes the common failures on purpose, catches them at a specific type, and
catches a whole category at a parent type.

Every SDK failure derives from :class:`AerospikeError`, so the value of the
hierarchy is that you catch at exactly the granularity you need:
``BinTypeError`` for one condition, ``BinError`` for every bin-level problem.
See ``docs/api/exceptions.md`` for the full tree and the result-code table.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.exceptions import (
    BinError,
    FilteredOutError,
    GenerationError,
    RecordExistsError,
    RecordNotFoundError,
)

SET = DataSet.of("test", "exception-demo")


async def run_examples(session) -> None:
    await session.truncate(SET)
    await asyncio.sleep(0.2)

    alice = SET.id("alice")
    missing = SET.id("does-not-exist")

    await session.insert(alice).bin("name").set_to("Alice").bin("visits").set_to(3).execute()

    # --- 1) insert on an existing record -> RecordExistsError ---
    print("--- 1) insert on an existing record -> RecordExistsError ---")
    try:
        await session.insert(alice).bin("name").set_to("Alice again").execute()
        raise AssertionError("expected RecordExistsError was not raised")
    except RecordExistsError as exc:
        describe(exc)

    # --- 2) update on a missing record -> RecordNotFoundError ---
    print("--- 2) update on a missing record -> RecordNotFoundError ---")
    try:
        await session.update(missing).bin("name").set_to("Nobody").execute()
        raise AssertionError("expected RecordNotFoundError was not raised")
    except RecordNotFoundError as exc:
        describe(exc)

    # --- 3) arithmetic on a string bin -> BinTypeError, caught as BinError ---
    print("--- 3) arithmetic on a string bin -> BinTypeError, caught as BinError ---")
    try:
        await session.update(alice).bin("name").add(1).execute()
        raise AssertionError("expected BinError was not raised")
    except BinError as exc:
        # Catching the parent handles BinExistsError, BinNotFoundError,
        # BinTypeError, and BinOpInvalidError alike.
        print(f"  caught as BinError; actual type is {type(exc).__name__}")
        describe(exc)

    # --- 4) generation mismatch -> GenerationError ---
    print("--- 4) generation mismatch -> GenerationError ---")
    current = await (await session.query(alice).execute()).first_or_raise()
    stale_generation = current.record.generation + 99

    try:
        await (
            session.update(alice)
            .ensure_generation_is(stale_generation)
            .bin("visits").add(1)
            .execute()
        )
        raise AssertionError("expected GenerationError was not raised")
    except GenerationError as exc:
        describe(exc)

    # --- 5) filter expression excludes the record: silent by default ---
    print("--- 5) filter expression excludes the record: silent by default ---")
    # A filter that doesn't match is not an error by default. The write is
    # skipped and nothing is raised, so a single-key call cannot tell
    # "filtered" from "applied" here.
    await (
        session.update(alice)
        .where("$.visits > 1000000")
        .bin("visits").add(1)
        .execute()
    )
    print("  no exception; the write was silently skipped")

    # --- 6) ...and opt in with fail_on_filtered_out() -> FilteredOutError ---
    print("--- 6) ...and opt in with fail_on_filtered_out() -> FilteredOutError ---")
    try:
        await (
            session.update(alice)
            .fail_on_filtered_out()
            .where("$.visits > 1000000")
            .bin("visits").add(1)
            .execute()
        )
        raise AssertionError("expected FilteredOutError was not raised")
    except FilteredOutError as exc:
        # Rejected by the filter, not by a failure. Usually means
        # "precondition not met".
        describe(exc)

    # --- 7) Mapping a result code to its exception type ---
    print("--- 7) Mapping a result code to its exception type ---")
    # A batch does not raise for one bad row; each row carries its own
    # result code, and record_or_raise() turns that code into the very
    # exception type sections 1-6 caught directly.
    async with await (
        session.query([alice, missing]).include_missing_keys().execute()
    ) as stream:
        async for row in stream:
            status = "no error" if row.is_ok else "failed"
            print(f"  {row.key.value} -> code {row.result_code} ({status})")
            try:
                row.record_or_raise()
            except RecordNotFoundError as exc:
                print(f"  code {row.result_code} maps to {type(exc).__name__}")

    print("Overall: SUCCESS")


def describe(exc: Exception) -> None:
    """Print the parts of a failure that a caller usually branches on."""
    print(f"  {type(exc).__name__} (code {exc.result_code}, "
          f"in_doubt={exc.in_doubt}): {exc.base_message}")


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await run_examples(session)


if __name__ == "__main__":
    asyncio.run(main())
