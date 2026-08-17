#!/usr/bin/env python3
"""Demonstrates known AEL spec-vs-implementation incongruities.

Each test targets a specific issue identified in the spec review:
  2a: let...then keyword alignment (PSDK uses let...then — aligned)
  2b: NAME_IDENTIFIER accepts digit-starting tokens → integer map key confusion
  2c: >> operator performs logical right shift instead of arithmetic
  2d: exists() path function behavior
  2e: Mutation path functions (sort, remove, clear) not implemented
  Casting: asInt()/asFloat() for mixed-type arithmetic
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet

SET = DataSet.of("test", "ael_diff_test")
SEPARATOR = "=" * 70
PASS = "PASS"
FAIL = "** FAIL **"

total_tests = 0
failed_tests = 0


def check(test_id: str, passed: bool, description: str) -> None:
    global total_tests, failed_tests
    total_tests += 1
    status = PASS if passed else FAIL
    if not passed:
        failed_tests += 1
    print(f"      [{status}] {test_id} - {description}")


async def main() -> None:
    async with await _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await session.truncate(SET)
        await asyncio.sleep(0.2)
        await setup_test_data(session)

        await test_2a_let_then(session)
        await test_2b_name_identifier_too_permissive(session)
        await test_2c_right_shift_reversed(session)
        await test_2d_exists_silently_ignored(session)
        await test_2e_mutation_operations_ignored(session)
        await test_casting_as_int_as_float(session)

        print(SEPARATOR)
        print(f"SUMMARY: {failed_tests}/{total_tests} tests show spec/implementation differences")
        print(SEPARATOR)


async def setup_test_data(session) -> None:
    # Record 1: integer bins for shift operator tests
    await (
        session.upsert(SET.id(1))
        .bin("intBin").set_to(-8)
        .bin("posInt").set_to(16)
        .execute()
    )

    # Record 2: map with integer keys (stored as Long on server)
    await (
        session.upsert(SET.id(2))
        .bin("m").set_to({1: "val_from_int_key_1", 2: "val_from_int_key_2", 3: "val_from_int_key_3"})
        .execute()
    )

    # Record 3: map with both integer key 1 and string key "1"
    await (
        session.upsert(SET.id(3))
        .bin("m").set_to({1: "INTEGER_KEY_1", "1": "STRING_KEY_1", "name": "hello"})
        .execute()
    )

    # Record 4: bins for exists() test — has binA but NOT binB
    await (
        session.upsert(SET.id(4))
        .bin("binA").set_to(42)
        .bin("flag").set_to(True)
        .execute()
    )

    # Record 5: list bin for mutation operation tests
    await (
        session.upsert(SET.id(5))
        .bin("listBin").set_to([50, 10, 40, 20, 30])
        .execute()
    )

    # Record 6: bins for let/then test
    await (
        session.upsert(SET.id(6))
        .bin("x").set_to(10)
        .bin("y").set_to(20)
        .execute()
    )

    # Record 7: int and float bins for asInt()/asFloat() casting tests
    await (
        session.upsert(SET.id(7))
        .bin("intBin").set_to(10)
        .bin("floatBin").set_to(3.5)
        .execute()
    )


# =========================================================================
# 2a: let...then keyword alignment
# =========================================================================
async def test_2a_let_then(session) -> None:
    print(SEPARATOR)
    print("TEST 2a: let...then keyword alignment")
    print(SEPARATOR)
    print()
    print("  Grammar uses: let (x = expr) then (body)")
    print("  Variables are unquoted.")
    print()

    print("  [A] Testing let...then:")
    try:
        stream = await (
            session.query(SET.id(6))
            .bin("result").select_from("let (x = $.x, y = $.y) then (${x} + ${y})")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expression:  let (x = $.x, y = $.y) then (${x} + ${y})")
            print("      Expected:    30")
            print(f"      Actual:      {result}")
            check("2a-let-then", result == 30, "let...then produces correct result")
        else:
            check("2a-let-then", False, "no result returned")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2a-let-then", False, "let...then should work")
    print()


# =========================================================================
# 2b: NAME_IDENTIFIER treats bare integers as string map keys
# =========================================================================
async def test_2b_name_identifier_too_permissive(session) -> None:
    print(SEPARATOR)
    print("TEST 2b: NAME_IDENTIFIER treats bare integers as string map keys")
    print(SEPARATOR)
    print()
    print("  Grammar defines: NAME_IDENTIFIER: [a-zA-Z0-9_]+")
    print("  Impact: $.m.1 looks up string key '1' instead of integer key 1")
    print()

    print("  [A] Map with only integer keys: {1: 'val_from_int_key_1', ...}")
    print("      $.m.1 per spec should access integer key 1")
    try:
        stream = await (
            session.query(SET.id(2))
            .bin("result").select_from("$.m.1.get(type: STRING)", ignore_eval_failure=True)
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    val_from_int_key_1 (integer key 1)")
            print(f"      Actual:      {result or 'null (key not found)'}")
            check("2b-int-key-lookup", result == "val_from_int_key_1",
                  "AEL looks up string key '1' instead of integer key 1")
        else:
            print("      Actual:      record filtered/missing (eval failure)")
            check("2b-int-key-lookup", False, "integer key lookup returned nothing")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2b-int-key-lookup", False, "failed to access integer key")

    print()

    print("  [B] Map with both integer key 1 and string key '1':")
    try:
        stream = await (
            session.query(SET.id(3))
            .bin("result").select_from("$.m.1.get(type: STRING)", ignore_eval_failure=True)
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      $.m.1 per spec should return: INTEGER_KEY_1")
            print(f"      $.m.1 actually returns:       {result}")
            check("2b-ambiguous-key", result == "INTEGER_KEY_1",
                  "AEL resolves to string key instead of integer key")
        else:
            check("2b-ambiguous-key", False, "no result returned")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2b-ambiguous-key", False, "failed to access key")
    print()


# =========================================================================
# 2c: >> operator semantics
# =========================================================================
async def test_2c_right_shift_reversed(session) -> None:
    print(SEPARATOR)
    print("TEST 2c: >> operator semantics")
    print(SEPARATOR)
    print()
    print("  Convention:")
    print("    >>  = arithmetic right shift (sign-preserving)")
    print("    >>> = logical right shift (zero-fill)")
    print()

    expected_arithmetic = -8 >> 1  # -4
    expected_logical = (-8 % (1 << 64)) >> 1  # unsigned interpretation

    print("  [A] Negative number: -8 >> 1")
    print(f"      Python -8 >> 1   (arithmetic): {expected_arithmetic}")
    print(f"      Python unsigned  (logical):    {expected_logical}")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("result").select_from("$.intBin >> 1")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print(f"      Expected (spec):     {expected_arithmetic} (arithmetic, sign preserved)")
            print(f"      Actual AEL >>:       {result}")
            is_correct = result == expected_arithmetic
            if not is_correct:
                print("      Note: >> may be wired to logical right shift instead of arithmetic")
            check("2c-rshift-negative", is_correct,
                  ">> performs logical right shift instead of arithmetic")
        else:
            check("2c-rshift-negative", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2c-rshift-negative", False, "right shift evaluation failed")

    print()

    print("  [B] Positive number (sanity check): 16 >> 1")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("result").select_from("$.posInt >> 1")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    8")
            print(f"      Actual:      {result}")
            check("2c-rshift-positive", result == 8, "positive shift works")
        else:
            check("2c-rshift-positive", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2c-rshift-positive", False, "positive right shift failed")

    print()

    print("  [C] Testing >>> (logical right shift):")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("result").select_from("$.intBin >>> 1")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print(f"      Actual AEL >>>:      {result}")
            check("2c-logical-rshift", True, ">>> is supported in PSDK")
        else:
            check("2c-logical-rshift", False, "no result for >>>")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2c-logical-rshift", False, ">>> not supported")
    print()


# =========================================================================
# 2d: exists() path function
# =========================================================================
async def test_2d_exists_silently_ignored(session) -> None:
    print(SEPARATOR)
    print("TEST 2d: exists() path function")
    print(SEPARATOR)
    print()
    print("  Record 4 has: binA=42, flag=true (binB does NOT exist)")
    print()

    print("  [A] Filter: $.binA.exists() and $.flag")
    try:
        stream = await (
            session.query(SET.id(4))
            .where("$.binA.exists() and $.flag")
            .execute()
        )
        first = await stream.first()
        found = first is not None and first.is_ok
        print(f"      Filter passed: {found}")
        if found:
            print("      Note: may pass for wrong reason if exists() is silently dropped")
        check("2d-exists-present", found, "exists() on present bin")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2d-exists-present", False, "exists on present bin errored")

    print()

    print("  [B] Filter: $.binB.exists()")
    print("      Per spec: should evaluate to false (binB missing)")
    try:
        stream = await (
            session.query(SET.id(4))
            .where("$.binB.exists()")
            .execute()
        )
        first = await stream.first()
        found = first is not None and first.is_ok
        print(f"      Filter passed: {found}")
        if not found:
            print("      Record filtered out — but was it exists()==false or missing-bin error?")
        check("2d-exists-missing", not found, "exists() on missing bin filters correctly")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2d-exists-missing", False, "exists() on missing bin causes error")

    print()

    print("  [C] Read expression: $.binA.exists()")
    print("      Per spec: should return true (boolean)")
    try:
        stream = await (
            session.query(SET.id(4))
            .bin("result").select_from("$.binA.exists()", ignore_eval_failure=True)
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    True (boolean)")
            print(f"      Actual:      {result} (type: {type(result).__name__})")
            check("2d-exists-read-expr", isinstance(result, bool) and result is True,
                  "should return boolean existence check")
        else:
            check("2d-exists-read-expr", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2d-exists-read-expr", False, "exists() as read expression failed")
    print()


# =========================================================================
# 2e: Mutation path functions (sort, remove, clear) not implemented
# =========================================================================
async def test_2e_mutation_operations_ignored(session) -> None:
    print(SEPARATOR)
    print("TEST 2e: Mutation path functions (sort, remove, clear)")
    print(SEPARATOR)
    print()
    print("  Record 5 has: listBin = [50, 10, 40, 20, 30]")
    print("  Path functions sort(), remove(), clear() are in the grammar but")
    print("  may not have visitor implementations.")
    print()

    stream = await session.query(SET.id(5)).execute()
    first = await stream.first()
    if first and first.is_ok:
        print(f"  Original listBin: {first.record.bins.get('listBin')}")
    print()

    print("  [A] Write expression: $.listBin.[].sort()")
    try:
        await (
            session.upsert(SET.id(5))
            .bin("sortedList").upsert_from("$.listBin.[].sort()")
            .execute()
        )
        stream = await session.query(SET.id(5)).execute()
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("sortedList")
            original = first.record.bins.get("listBin")
            print(f"      Original:    {original}")
            print("      Expected:    [10, 20, 30, 40, 50] (sorted list)")
            print(f"      Actual:      {result}")
            is_sorted = isinstance(result, list) and len(result) == 5 and result[0] == 10
            check("2e-sort", is_sorted, "sort() implementation")
        else:
            check("2e-sort", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2e-sort", False, "sort() not implemented")

    print()

    print("  [B] Write expression: $.listBin.[=30].remove()")
    try:
        await (
            session.upsert(SET.id(5))
            .bin("removedList").upsert_from("$.listBin.[=30].remove()")
            .execute()
        )
        stream = await session.query(SET.id(5)).execute()
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("removedList")
            print("      Expected:    [50, 10, 40, 20] (list without 30)")
            print(f"      Actual:      {result}")
            check("2e-remove", isinstance(result, list) and len(result) == 4,
                  "remove() implementation")
        else:
            check("2e-remove", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2e-remove", False, "remove() not implemented")

    print()

    print("  [D] Verify original listBin is unchanged:")
    stream = await session.query(SET.id(5)).execute()
    first = await stream.first()
    if first and first.is_ok:
        list_bin = first.record.bins.get("listBin")
        print(f"      listBin now:  {list_bin}")
        print("      Original was: [50, 10, 40, 20, 30]")
        if isinstance(list_bin, list) and len(list_bin) == 5:
            print("      Original data is unchanged")
    print()


# =========================================================================
# Casting: asInt() and asFloat()
# =========================================================================
async def test_casting_as_int_as_float(session) -> None:
    print(SEPARATOR)
    print("TEST: asInt() / asFloat() type casting for arithmetic")
    print(SEPARATOR)
    print()
    print("  Record 7 has: intBin=10 (INT), floatBin=3.5 (FLOAT)")
    print()

    print("  [A] Mixed types without casting: $.intBin + $.floatBin")
    try:
        stream = await (
            session.query(SET.id(7))
            .bin("result").select_from("$.intBin + $.floatBin")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    error (type mismatch)")
            print(f"      Actual:      {result} (type: {type(result).__name__ if result else 'null'})")
            check("cast-mixed-no-cast", False, "mixed-type arithmetic should require explicit cast")
        else:
            print("      Expression failed/filtered (expected for mixed types)")
            check("cast-mixed-no-cast", True, "mixed types correctly rejected without cast")
    except Exception as e:
        print(f"      Actual:      {type(e).__name__}: {e}")
        check("cast-mixed-no-cast", True, "mixed types correctly rejected without cast")

    print()

    print("  [B] Float-to-int cast: $.intBin + $.floatBin.asInt()")
    print("      floatBin (3.5) cast to int -> 3 (truncated), 10 + 3 = 13")
    try:
        stream = await (
            session.query(SET.id(7))
            .bin("result").select_from("$.intBin + $.floatBin.asInt()")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    13")
            print(f"      Actual:      {result}")
            check("cast-asInt", result == 13, "asInt() casts float to int for arithmetic")
        else:
            check("cast-asInt", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("cast-asInt", False, "asInt() cast failed")

    print()

    print("  [C] Int-to-float cast: $.intBin.asFloat() + $.floatBin")
    print("      intBin (10) cast to float -> 10.0, 10.0 + 3.5 = 13.5")
    try:
        stream = await (
            session.query(SET.id(7))
            .bin("result").select_from("$.intBin.asFloat() + $.floatBin")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    13.5")
            print(f"      Actual:      {result}")
            check("cast-asFloat", isinstance(result, float) and abs(result - 13.5) < 0.001,
                  "asFloat() casts int to float for arithmetic")
        else:
            check("cast-asFloat", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("cast-asFloat", False, "asFloat() cast failed")

    print()

    print("  [D] No-op cast: $.intBin.asInt()")
    try:
        stream = await (
            session.query(SET.id(7))
            .bin("result").select_from("$.intBin.asInt()")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    10 (unchanged)")
            print(f"      Actual:      {result}")
            check("cast-noop-int", result == 10, "asInt() on int is a no-op")
        else:
            check("cast-noop-int", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("cast-noop-int", False, "asInt() on int failed")

    print()

    print("  [E] No-op cast: $.floatBin.asFloat()")
    try:
        stream = await (
            session.query(SET.id(7))
            .bin("result").select_from("$.floatBin.asFloat()")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    3.5 (unchanged)")
            print(f"      Actual:      {result}")
            check("cast-noop-float", isinstance(result, float) and abs(result - 3.5) < 0.001,
                  "asFloat() on float is a no-op")
        else:
            check("cast-noop-float", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("cast-noop-float", False, "asFloat() on float failed")
    print()


if __name__ == "__main__":
    asyncio.run(main())
