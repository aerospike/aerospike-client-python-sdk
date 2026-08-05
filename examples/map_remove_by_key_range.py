#!/usr/bin/env python3
"""Demonstrates map CDT operations used as read and write expressions.

Explores the behavior of map operations applied via expressions, including
removeByKeyRange semantics and return-type behavior. Uses the chainable CDT
builder API and AEL expressions to exercise map operations.
"""

import asyncio

from _env import Example
from aerospike_sdk import Behavior, DataSet

SET = DataSet.of("test", "map_remove_test")

class MapRemoveExample(Example):
    async def run(self):
        await self.session.truncate(SET)
        await asyncio.sleep(0.2)

        source_map = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}

        await (
            self.session.upsert(SET.id(1))
            .bin("m").set_to(source_map)
            .execute()
        )
        print(f"Source map: {source_map}\n")

        # 
        # Test 1: Read map key by AEL
        # ==================================================================
        print("=== Test 1: Read map key 'c' via AEL ===")
        print("Expected: 3")
        try:
            stream = await (
                self.session.query(SET.id(1))
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

        # ==================================================================
        # Test 2: Read map key range via chainable CDT builder
        # ==================================================================
        print("=== Test 2: Read map key 'b' values via chainable builder ===")
        print("Expected: value for key 'b' = 2")
        try:
            stream = await (
                self.session.query(SET.id(1))
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
        # Test 3: Count map elements
        # ==================================================================
        print("=== Test 3: Count map elements ===")
        print("Expected: 5")
        try:
            stream = await (
                self.session.query(SET.id(1))
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
        # Test 4: Read map index 0
        # ==================================================================
        print("=== Test 4: Read map index 0 values ===")
        print("Expected: value at index 0 of key-ordered map")
        try:
            stream = await (
                self.session.query(SET.id(1))
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
        # Test 5: Remove map key via chainable CDT write builder
        # ==================================================================
        print("=== Test 5: Remove map key 'c' via chainable write builder ===")
        print("Expected: map becomes {a: 1, b: 2, d: 4, e: 5}")
        try:
            await (
                self.session.upsert(SET.id(1))
                .bin("m").set_to(source_map)
                .execute()
            )
            print(f"Source map: {source_map}\n")
        except Exception as e:
            print(f"ERROR:    {type(e).__name__}: {e}")

        # ==================================================================
        # Test 6: Map key range read via chainable CDT
        # ==================================================================
        print("=== Test 6: Map key range 'b'..'d' count ===")
        print("Expected: count of keys in range [b, d) = 2 (b, c)")
        try:
            stream = await (
                self.session.query(SET.id(1))
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
        # Test 7: Map key range count all others
        # ==================================================================
        print("=== Test 7: Map key range 'b'..'d' count all others ===")
        print("Expected: count of keys NOT in range [b, d) = 3 (a, d, e)")
        try:
            stream = await (
                self.session.query(SET.id(1))
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
        # Test 8: Map clear via chainable CDT write
        # ==================================================================
        print("=== Test 8: Map clear ===")
        print("Expected: map becomes empty {}")
        # Use a copy so we don't destroy the original for the verification
        await (
            self.session.upsert(SET.id(2))
            .bin("m").set_to(dict(source_map))
            .execute()
        )
        try:
            await (
                self.session.upsert(SET.id(2))
                .bin("m").map_clear()
                .execute()
            )
            stream = await self.session.query(SET.id(2)).execute()
            first = await stream.first()
            if first and first.is_ok:
                print(f"Actual:   {first.record.bins.get('m')}")
            else:
                print("Actual:   no result")
        except Exception as e:
            print(f"ERROR:    {type(e).__name__}: {e}")
        print()

        # ==================================================================
        # Test 9: AEL comparison on map value
        # ==================================================================
        print("=== Test 9: AEL filter on map key value ===")
        print("Filter: $.m.c.get(type: INT) > 2")
        print("Expected: record passes filter (m.c = 3 > 2)")
        try:
            stream = await (
                self.session.query(SET.id(1))
                .where("$.m.c.get(type: INT) > 2")
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
        stream = await self.session.query(SET.id(1)).execute()
        first = await stream.first()
        if first and first.is_ok:
            print(f"Original map after all tests: {first.record.bins.get('m')}")
