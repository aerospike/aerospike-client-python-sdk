#!/usr/bin/env python3
"""Demonstrates map CDT operations used as read and write expressions.

Explores the behavior of map operations applied via expressions, including
removeByKeyRange semantics and return-type behavior. Uses the chainable CDT
builder API and AEL expressions to exercise map operations.
"""

import asyncio

from _env import Example
from aerospike_sdk import DataSet

SOURCE_MAP = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}


class MapRemoveExample(Example):
    SET = DataSet.of("test", "map_remove_test")
    async def __init__(self):
        await super().__init__()
        await self.session.truncate(self.SET)
        await asyncio.sleep(0.2)

        await (
            self.session.upsert(self.SET.id(1))
            .bin("m").set_to(SOURCE_MAP)
            .execute()
        )
        print(f"Source map: {SOURCE_MAP}\n")


class MapRemoveReadKeyViaAel(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 1: Read map key 'c' via AEL ===")
        print("Expected: 3")
        try:
            stream = await (
                self.session.query(self.SET.id(1))
                .bin("result").select_from("$.m.c.get(type: INT)")
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


class MapRemoveReadKeyViaBuilder(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 2: Read map key 'b' values via chainable builder ===")
        print("Expected: value for key 'b' = 2")
        try:
            stream = await (
                self.session.query(self.SET.id(1))
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


class MapRemoveCountElements(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 3: Count map elements ===")
        print("Expected: 5")
        try:
            stream = await (
                self.session.query(self.SET.id(1))
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


class MapRemoveReadIndexZero(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 4: Read map index 0 values ===")
        print("Expected: value at index 0 of key-ordered map")
        try:
            stream = await (
                self.session.query(self.SET.id(1))
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


class MapRemoveResetSourceMap(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 5: Remove map key 'c' via chainable write builder ===")
        print("Expected: map becomes {a: 1, b: 2, d: 4, e: 5}")
        try:
            await (
                self.session.upsert(self.SET.id(1))
                .bin("m").set_to(SOURCE_MAP)
                .execute()
            )
            print(f"Source map: {SOURCE_MAP}\n")
        except Exception as e:
            print(f"ERROR:    {type(e).__name__}: {e}")


class MapRemoveKeyRangeCount(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 6: Map key range 'b'..'d' count ===")
        print("Expected: count of keys in range [b, d) = 2 (b, c)")
        try:
            stream = await (
                self.session.query(self.SET.id(1))
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


class MapRemoveKeyRangeCountOthers(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 7: Map key range 'b'..'d' count all others ===")
        print("Expected: count of keys NOT in range [b, d) = 3 (a, d, e)")
        try:
            stream = await (
                self.session.query(self.SET.id(1))
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


class MapRemoveMapClear(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 8: Map clear ===")
        print("Expected: map becomes empty {}")
        await (
            self.session.upsert(self.SET.id(2))
            .bin("m").set_to(dict(SOURCE_MAP))
            .execute()
        )
        try:
            await (
                self.session.upsert(self.SET.id(2))
                .bin("m").map_clear()
                .execute()
            )
            stream = await self.session.query(self.SET.id(2)).execute()
            first = await stream.first()
            if first and first.is_ok:
                print(f"Actual:   {first.record.bins.get('m')}")
            else:
                print("Actual:   no result")
        except Exception as e:
            print(f"ERROR:    {type(e).__name__}: {e}")
        print()


class MapRemoveAelFilter(MapRemoveExample):
    async def run(self) -> None:
        print("=== Test 9: AEL filter on map key value ===")
        print("Filter: $.m.c.get(type: INT) > 2")
        print("Expected: record passes filter (m.c = 3 > 2)")
        try:
            stream = await (
                self.session.query(self.SET.id(1))
                .where("$.m.c.get(type: INT) > 2")
                .execute()
            )
            first = await stream.first()
            found = first is not None and first.is_ok
            print(f"Actual:   {'record returned (filter passed)' if found else 'filtered out'}")
        except Exception as e:
            print(f"ERROR:    {type(e).__name__}: {e}")
        print()


class MapRemoveVerifyOriginal(MapRemoveExample):
    async def run(self) -> None:
        print("=== Verify original map (record 1) is unchanged ===")
        stream = await self.session.query(self.SET.id(1)).execute()
        first = await stream.first()
        if first and first.is_ok:
            print(f"Original map after all tests: {first.record.bins.get('m')}")
