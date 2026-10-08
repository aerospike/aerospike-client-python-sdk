#!/usr/bin/env python3
"""What the remove-by-key-range read expression returns for each return type.

``Exp.map_remove_by_key_range`` evaluated through ``select_from`` is a *read*:
the server computes the map as it would be after the removal and returns it in
the projection bin, leaving the stored record untouched. The expression always
yields the resulting map, so the return type decides only which side of the
range goes: ``INVERTED`` keeps the range and removes everything outside it,
and every other return type removes the range itself.

To remove entries from the stored record and get a count, keys or values back,
use the write form instead:
``session.update(key).bin("m").on_map_key_range("b", "e").remove(return_type=...)``.
"""

import asyncio

import _env
from aerospike_sdk import DataSet, Exp, MapReturnType

SET = DataSet.of("test", "map_remove_test")
KEY = SET.id(1)
SOURCE_MAP = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}


async def run_examples(session) -> None:
    await session.upsert(KEY).bin("m").set_to(SOURCE_MAP).execute()
    print(f"Source map: {SOURCE_MAP}")
    # The range is "b" (inclusive) to "e" (exclusive): keys b, c and d.
    try:
        # --- 1) Return type NONE: the map with keys b..e removed ---
        print("\n--- 1) Return type NONE: the map with keys b..e removed ---")
        print(f"result: {await _remove_range(session, MapReturnType.NONE)}")

        # --- 2) Return type INVERTED: the map with everything outside b..e removed ---
        print("\n--- 2) Return type INVERTED: the map with everything outside b..e removed ---")
        print(f"result: {await _remove_range(session, MapReturnType.INVERTED)}")

        # --- 3) Return type COUNT: still the resulting map, not a count ---
        print("\n--- 3) Return type COUNT: still the resulting map, not a count ---")
        print(f"result: {await _remove_range(session, MapReturnType.COUNT)}")

        # --- 4) Return type KEY: still the resulting map, not the removed keys ---
        print("\n--- 4) Return type KEY: still the resulting map, not the removed keys ---")
        print(f"result: {await _remove_range(session, MapReturnType.KEY)}")

        # --- 5) Return type VALUE: still the resulting map, not the removed values ---
        print("\n--- 5) Return type VALUE: still the resulting map, not the removed values ---")
        print(f"result: {await _remove_range(session, MapReturnType.VALUE)}")

        # --- 6) Return type KEY_VALUE: still the resulting map, not the removed pairs ---
        print("\n--- 6) Return type KEY_VALUE: still the resulting map, not the removed pairs ---")
        print(f"result: {await _remove_range(session, MapReturnType.KEY_VALUE)}")

        # --- 7) The stored map is unchanged: a read expression never writes ---
        print("\n--- 7) The stored map is unchanged: a read expression never writes ---")
        record = (await (await session.query(KEY).execute()).first_or_raise()).record
        print(f"stored map: {record.bins['m']}")

    finally:
        await session.delete(KEY).execute()


async def _remove_range(session, return_type: MapReturnType) -> dict:
    """Evaluate the removal over keys b..e as a read into the ``result`` bin."""
    removal = Exp.map_remove_by_key_range(
        return_type, Exp.val("b"), Exp.val("e"), Exp.map_bin("m"), [],
    )
    stream = await session.query(KEY).bin("result").select_from(removal).execute()
    return (await stream.first_or_raise()).record.bins["result"]


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session()

        await run_examples(session)


if __name__ == "__main__":
    asyncio.run(main())
