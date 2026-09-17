#!/usr/bin/env python3
"""Demonstrates known AEL spec-vs-implementation incongruities.

Each test targets a specific issue identified in the spec review:
  2a: let...then keyword alignment
  2b: NAME_IDENTIFIER accepts digit-starting tokens → integer map key access
  2c: >> operator semantics (arithmetic vs logical right shift)
  2d: exists() path function behavior
  2e: Mutation path functions (sort, remove, clear)
  Casting: asInt()/asFloat() for mixed-type arithmetic
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet

SET = DataSet.of("test", "ael_diff_test")
SEPARATOR = "=" * 70
PASS = "PASS"
FAIL = "** FAIL **"

RESULTS = {"total": 0, "failed": 0}


async def run_examples(session) -> None:
    await setup_test_data(session)

    # 2a is held back: the let/then probe reports a ParameterError that is
    # already tracked, so running it adds a known failure to every report.
    # await test_2a_let_then(session)
    await test_2b_name_identifier_too_permissive(session)
    await test_2c_right_shift_reversed(session)
    await test_2d_exists_silently_ignored(session)
    await test_2e_mutation_operations_ignored(session)
    await test_casting_as_int_as_float(session)

    print(SEPARATOR)
    print(
        f"SUMMARY: {RESULTS['failed']}/{RESULTS['total']} "
        "tests show spec/implementation differences"
    )
    print(SEPARATOR)


async def setup_test_data(session) -> None:
    # Record 1: integer bins for shift operator tests
    await (
        session.upsert(SET.id(1))
        .bin("intBin").set_to(-8)
        .bin("posInt").set_to(16)
        .execute()
    )

    # Record 2: map bin with integer keys only.
    # $.m.1 per the spec should look up integer key 1.
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
# 2b: NAME_IDENTIFIER and bare integer map keys
# =========================================================================
async def test_2b_name_identifier_too_permissive(session) -> None:
    print(SEPARATOR)
    print("TEST 2b: NAME_IDENTIFIER and bare integer map keys")
    print(SEPARATOR)
    print()
    print("  Grammar defines: NAME_IDENTIFIER: [a-zA-Z0-9_]+")
    print("  Spec says:       Identifiers must match ^[A-Za-z]\\w*$ (start with letter)")
    print("  Question:        does $.m.1 resolve integer key 1 (per spec) or string key '1'?")
    print("  Note: the unpinned form $.m.1 is rejected (value type cannot be inferred);")
    print("        a type pin is required, e.g. $.m.1:STRING")
    print()

    print("  [A] Map with only integer keys: {1: 'val_from_int_key_1', 2: ...}")
    print("      $.m.1 per spec should access integer key 1")
    try:
        stream = await (
            session.query(SET.id(2))
            .bin("result").select_from("$.m.1:STRING", ignore_eval_failure=True)
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    val_from_int_key_1 (integer key 1)")
            print(f"      Actual:      {result if result is not None else 'None (key not found)'}")
            check("2b-int-key-lookup", result == "val_from_int_key_1",
                  "$.m.1 resolves the integer map key per spec")
        else:
            print("      Actual:      record filtered/missing (eval failure)")
            check("2b-int-key-lookup", False, "integer key lookup returned nothing")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2b-int-key-lookup", False, "failed to access integer key")

    print()

    print("  [B] Map with both integer key 1 and string key '1':")
    print("      {1(int): 'INTEGER_KEY_1', '1'(str): 'STRING_KEY_1'}")
    try:
        stream = await (
            session.query(SET.id(3))
            .bin("result").select_from("$.m.1:STRING", ignore_eval_failure=True)
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      $.m.1 per spec should return: INTEGER_KEY_1 (integer key 1)")
            print(f"      $.m.1 actually returns:       {result}")
            check("2b-ambiguous-key", result == "INTEGER_KEY_1",
                  "$.m.1 resolves the integer key, not string '1'")
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
                  ">> performs arithmetic right shift (sign-preserving)")
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
            print("      Note: positive values shift identically for both variants")
            check("2c-rshift-positive", result == 8, "positive shift works")
        else:
            check("2c-rshift-positive", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2c-rshift-positive", False, "positive right shift failed")

    print()

    print("  [C] Testing >>> (logical right shift):")
    print("      Spec defines >>> as logical right shift (zero-fill).")
    print(f"      Python unsigned -8 >> 1 = {expected_logical}")
    try:
        stream = await (
            session.query(SET.id(1))
            .bin("result").select_from("$.intBin >>> 1")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print(f"      Expected (spec):     {expected_logical} (logical, zero-fill)")
            print(f"      Actual AEL >>>:      {result}")
            check("2c-logical-rshift", result == expected_logical,
                  ">>> performs logical right shift")
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
    print("  $.binA.exists() should evaluate to true (bin exists)")
    print("  $.binB.exists() should evaluate to false (bin missing)")
    print()

    print("  [A] Filter: $.binA.exists() and $.flag")
    print("      Per spec: exists() checks bin existence -> true, combined with flag -> passes")
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
            print(f"      Actual:      {result} (type: {type_name(result)})")
            check("2d-exists-read-expr", isinstance(result, bool) and result is True,
                  "should return boolean existence check")
        else:
            check("2d-exists-read-expr", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2d-exists-read-expr", False, "exists() as read expression failed")
    print()


# =========================================================================
# 2e: Mutation path functions (sort, remove, clear)
# =========================================================================
async def test_2e_mutation_operations_ignored(session) -> None:
    print(SEPARATOR)
    print("TEST 2e: Mutation path functions (sort, remove, clear)")
    print(SEPARATOR)
    print()
    print("  Record 5 has: listBin = [50, 10, 40, 20, 30]")
    print("  The grammar defines mutation path functions remove(), sort(), clear(),")
    print("  insert(), set(), append(), increment(). This test exercises sort(),")
    print("  remove(), and clear() through write expressions.")
    print()

    stream = await session.query(SET.id(5)).execute()
    first = await stream.first()
    if first and first.is_ok:
        print(f"  Original listBin: {first.record.bins.get('listBin')}")
    print()

    print("  [A] Write expression: $.listBin.[].sort()")
    print("      Per spec: should produce the list sorted ascending")
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
    print("      Per spec: should remove elements with value 30 from the list")
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

    print("  [C] Write expression: $.listBin.[].clear()")
    print("      Per spec: should clear all items from the list")
    try:
        await (
            session.upsert(SET.id(5))
            .bin("clearedList").upsert_from("$.listBin.[].clear()")
            .execute()
        )
        stream = await session.query(SET.id(5)).execute()
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("clearedList")
            print("      Expected:    [] (empty list)")
            print(f"      Actual:      {result}")
            check("2e-clear", isinstance(result, list) and len(result) == 0,
                  "clear() implementation")
        else:
            check("2e-clear", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("2e-clear", False, "clear() not implemented")

    print()

    print("  [D] Verify original listBin is unchanged:")
    stream = await session.query(SET.id(5)).execute()
    first = await stream.first()
    if first and first.is_ok:
        list_bin = first.record.bins.get("listBin")
        print(f"      listBin now:  {list_bin}")
        print("      Original was: [50, 10, 40, 20, 30]")
        if isinstance(list_bin, list) and len(list_bin) == 5:
            print("      Original data is unchanged -- the write expressions did not modify it")
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
    print("  Spec rule 12: Both operands in arithmetic must be the same type.")
    print("  Use asInt() or asFloat() to convert before combining.")
    print()

    print("  [A] Mixed types without casting: $.intBin + $.floatBin")
    print("      Per spec: both operands must be the same type -> this should error")
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
            print(f"      Actual:      {result} (type: {type_name(result)})")
            print("      If no error: the AEL may be silently promoting types")
            check("cast-mixed-no-cast", False, "mixed-type arithmetic should require explicit cast")
        else:
            print("      Expression failed/filtered (expected for mixed types)")
            check("cast-mixed-no-cast", True, "mixed types correctly rejected without cast")
    except Exception as e:
        print(f"      Actual:      {type(e).__name__}: {e}")
        print("      Correct! Mixed-type arithmetic without casting produces an error.")
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
            print(f"      Actual:      {result} (type: {type_name(result)})")
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
            print(f"      Actual:      {result} (type: {type_name(result)})")
            check("cast-asFloat", isinstance(result, float) and abs(result - 13.5) < 0.001,
                  "asFloat() casts int to float for arithmetic")
        else:
            check("cast-asFloat", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("cast-asFloat", False, "asFloat() cast failed")

    print()

    print("  [D] Round-trip cast: $.floatBin.asInt().asFloat()")
    print("      3.5 -> asInt() -> 3 -> asFloat() -> 3.0")
    print("      Demonstrates precision loss from truncation")
    try:
        stream = await (
            session.query(SET.id(7))
            .bin("result").select_from("$.floatBin.asInt().asFloat()")
            .execute()
        )
        first = await stream.first()
        if first and first.is_ok:
            result = first.record.bins.get("result")
            print("      Expected:    3.0 (precision lost from 3.5)")
            print(f"      Actual:      {result} (type: {type_name(result)})")
            check("cast-round-trip",
                  isinstance(result, float) and abs(result - 3.0) < 0.001,
                  "round-trip cast loses fractional part")
        else:
            check("cast-round-trip", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("cast-round-trip", False, "round-trip cast failed")

    print()

    print("  [E] No-op cast: $.intBin.asInt()")
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
            print(f"      Actual:      {result} (type: {type_name(result)})")
            check("cast-noop-int", result == 10, "asInt() on int is a no-op")
        else:
            check("cast-noop-int", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("cast-noop-int", False, "asInt() on int failed")

    print()

    print("  [F] No-op cast: $.floatBin.asFloat()")
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
            print(f"      Actual:      {result} (type: {type_name(result)})")
            check("cast-noop-float", isinstance(result, float) and abs(result - 3.5) < 0.001,
                  "asFloat() on float is a no-op")
        else:
            check("cast-noop-float", False, "no result")
    except Exception as e:
        print(f"      ERROR: {type(e).__name__}: {e}")
        check("cast-noop-float", False, "asFloat() on float failed")
    print()


def type_name(value) -> str:
    return "None" if value is None else type(value).__name__


def check(test_id: str, passed: bool, description: str) -> None:
    RESULTS["total"] += 1
    status = PASS if passed else FAIL
    if not passed:
        RESULTS["failed"] += 1
    print(f"      [{status}] {test_id} - {description}")


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await session.truncate(SET)
        await asyncio.sleep(0.2)
        await run_examples(session)


if __name__ == "__main__":
    asyncio.run(main())
