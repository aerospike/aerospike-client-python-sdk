#!/usr/bin/env python3
"""Removing and reading map entries by key range, and what each return type gives back.

A map operation reports back through its ``return_type``: the same
remove-by-key-range can yield nothing, a count, the keys, the values, or the
key/value pairs it acted on. This walks those return types over one known map,
then covers the surrounding surface — reading a key through AEL, reading by
index, counting a range and its complement, and clearing the map.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet, MapReturnType

SET = DataSet.of("test", "map_remove_test")


async def main() -> None:
    async with await _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await run_examples(session)


async def run_examples(session) -> None:
    await session.truncate(SET)
    await asyncio.sleep(0.2)

    source_map = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}

    await (
        session.upsert(SET.id(1))
        .bin("m").set_to(source_map)
        .execute()
    )
    print(f"Source map: {source_map}\n")

    # Tests 1-6: one remove-by-key-range, six return types.
    #
    # on_map_key_range("b", "e") selects keys b, c, d (begin inclusive, end
    # exclusive). The removal itself is identical every time; only what the
    # server reports back changes. The map is restored between each so every
    # return type sees the same starting point.
    for position, (label, return_type, expected) in enumerate(
        (
            ("NONE", MapReturnType.NONE, "nothing reported"),
            ("VALUE (inverted)", MapReturnType.VALUE, "[1, 5] — a and e, the keys NOT in range"),
            ("COUNT", MapReturnType.COUNT, "3 (b, c, d were removed)"),
            ("KEY", MapReturnType.KEY, "['b', 'c', 'd']"),
            ("VALUE", MapReturnType.VALUE, "[2, 3, 4]"),
            ("KEY_VALUE", MapReturnType.KEY_VALUE, "the removed pairs"),
        ),
        start=1,
    ):
        print(f"=== Test {position}: remove by key range 'b'..'e', "
              f"return_type={label} ===")
        print(f"Expected: {expected}")
        try:
            selection = session.upsert(SET.id(1)).bin("m").on_map_key_range("b", "e")
            # Inversion is its own terminal rather than a return-type flag: it
            # removes everything the range did *not* select.
            removal = (
                selection.remove_all_others(return_type=return_type)
                if "inverted" in label
                else selection.remove(return_type=return_type)
            )
            stream = await removal.execute()
            first = await stream.first()
            reported = first.record.bins.get("m") if first and first.is_ok else None
            print(f"Actual:   {reported!r}")
            print(f"Type:     {type(reported).__name__}")
        except Exception as e:
            print(f"ERROR:    {type(e).__name__}: {e}")

        # Restore the map so the next return type starts from the same state.
        await session.upsert(SET.id(1)).bin("m").set_to(source_map).execute()
        print()

    # ==================================================================
    # Test 7: Read map key by AEL
    # ==================================================================
    print("=== Test 7: Read map key 'c' via AEL ===")
    print("Expected: 3")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("result").select_from("$.m.c:INT")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"Actual:   {first.record.bins.get('result')}")
        else:
            print("Actual:   no result")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # ==================================================================
    # Test 8: Read map key range via chainable CDT builder
    # ==================================================================
    print("=== Test 8: Read map key 'b' values via chainable builder ===")
    print("Expected: value for key 'b' = 2")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("m").on_map_key("b").get_values()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"Actual:   {first.record.bins}")
        else:
            print("Actual:   no result")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # ==================================================================
    # Test 9: Count map elements
    # ==================================================================
    print("=== Test 9: Count map elements ===")
    print("Expected: 5")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("m").map_size()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"Actual:   {first.record.bins}")
        else:
            print("Actual:   no result")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # ==================================================================
    # Test 10: Read map index 0
    # ==================================================================
    print("=== Test 10: Read map index 0 values ===")
    print("Expected: value at index 0 of key-ordered map")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("m").on_map_index(0).get_values()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"Actual:   {first.record.bins}")
        else:
            print("Actual:   no result")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # ==================================================================
    # Test 11: Remove map key via chainable CDT write builder
    # ==================================================================
    print("=== Test 11: Remove map key 'c' via chainable write builder ===")
    print("Expected: map becomes {a: 1, b: 2, d: 4, e: 5}")
    try:
        await (
            session.upsert(SET.id(1))
            .bin("m").on_map_key("c").remove()
            .execute()
        )
        stream = await session.query(SET.id(1)).execute()
        first = await stream.first()
        if first and first.is_ok:
            print(f"Actual:   {first.record.bins.get('m')}")
        else:
            print("Actual:   no result")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # Restore original map
    await (
        session.upsert(SET.id(1))
        .bin("m").set_to(source_map)
        .execute()
    )

    # ==================================================================
    # Test 12: Map key range read via chainable CDT
    # ==================================================================
    print("=== Test 12: Map key range 'b'..'d' count ===")
    print("Expected: count of keys in range [b, d) = 2 (b, c)")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("m").on_map_key_range("b", "d").count()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"Actual:   {first.record.bins}")
        else:
            print("Actual:   no result")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # ==================================================================
    # Test 13: Map key range count all others
    # ==================================================================
    print("=== Test 13: Map key range 'b'..'d' count all others ===")
    print("Expected: count of keys NOT in range [b, d) = 3 (a, d, e)")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("m").on_map_key_range("b", "d").count_all_others()
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            print(f"Actual:   {first.record.bins}")
        else:
            print("Actual:   no result")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # ==================================================================
    # Test 14: Map clear via chainable CDT write
    # ==================================================================
    print("=== Test 14: Map clear ===")
    print("Expected: map becomes empty {}")
    # Use a copy so we don't destroy the original for the verification
    await (
        session.upsert(SET.id(2))
        .bin("m").set_to(dict(source_map))
        .execute()
    )
    try:
        await (
            session.upsert(SET.id(2))
            .bin("m").map_clear()
            .execute()
        )
        stream = await session.query(SET.id(2)).execute()
        first = await stream.first()
        if first and first.is_ok:
            print(f"Actual:   {first.record.bins.get('m')}")
        else:
            print("Actual:   no result")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # ==================================================================
    # Test 15: AEL comparison on map value
    # ==================================================================
    print("=== Test 15: AEL filter on map key value ===")
    print("Filter: $.m.c:INT > 2")
    print("Expected: record passes filter (m.c = 3 > 2)")
    try:
        stream = await (
            session.query(SET.id(1))
            .where("$.m.c:INT > 2")
            .execute()
        )
        first = await stream.first()
        found = first is not None and first.is_ok
        print(f"Actual:   {'record returned (filter passed)' if found else 'filtered out'}")
    except Exception as e:
        print(f"ERROR:    {type(e).__name__}: {e}")
    print()

    # ==================================================================
    # Verify original map is unchanged
    # ==================================================================
    print("=== Verify original map (record 1) is unchanged ===")
    stream = await session.query(SET.id(1)).execute()
    first = await stream.first()
    if first and first.is_ok:
        print(f"Original map after all tests: {first.record.bins.get('m')}")


if __name__ == "__main__":
    asyncio.run(main())
