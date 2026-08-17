#!/usr/bin/env python3
"""AEL expression examples demonstrating the full breadth of the expression AEL.

Sets up test data and evaluates AEL expressions across multiple categories:
scalar bin access, type casting, map/list access, arithmetic, bitwise,
comparison, logical operators, control structures, and metadata.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet

SET = DataSet.of("test", "ael_spec")

SEP = "=" * 70
total_tests = 0
passed_tests = 0
failed_tests = 0
error_tests = 0


def section(name: str) -> None:
    print(f"\n{SEP}")
    print(f"  {name}")
    print(SEP)


async def read_check(
    test_id: str, session, pk: int, ael: str, expected, *, tolerance: float = 0.001
) -> None:
    """Evaluate an AEL expression via select_from and compare to expected."""
    global total_tests, passed_tests, failed_tests, error_tests
    total_tests += 1
    print(f"  [{test_id}] {ael}")
    try:
        stream = await (
            session.query(SET.id(pk))
            .bin("r").select_from(ael)
            .execute()
        )
        first = await stream.first()
        if first is None or not first.is_ok:
            failed_tests += 1
            print("      ** FAIL ** — No record returned")
            return

        actual = first.record.bins.get("r")
        if isinstance(expected, float) and isinstance(actual, (int, float)):
            ok = abs(expected - float(actual)) < tolerance
        elif isinstance(expected, bool) and isinstance(actual, bool):
            ok = expected == actual
        else:
            ok = (expected is None and actual is None) or (expected == actual)

        if ok:
            passed_tests += 1
            print(f"      PASS — {actual}")
        else:
            failed_tests += 1
            etype = type(expected).__name__
            atype = type(actual).__name__
            print(f"      ** FAIL ** — Expected: {expected} ({etype}), Actual: {actual} ({atype})")
    except Exception as e:
        error_tests += 1
        print(f"      ** ERROR ** — {type(e).__name__}: {e}")


async def read_print(test_id: str, session, pk: int, ael: str, description: str) -> None:
    """Evaluate an AEL expression and print the result (no assertion)."""
    global total_tests, passed_tests, error_tests
    total_tests += 1
    print(f"  [{test_id}] {ael}")
    print(f"      ({description})")
    try:
        stream = await (
            session.query(SET.id(pk))
            .bin("r").select_from(ael)
            .execute()
        )
        first = await stream.first()
        actual = first.record.bins.get("r") if first and first.is_ok else None
        passed_tests += 1
        print(f"      => {actual}")
    except Exception as e:
        error_tests += 1
        print(f"      ** ERROR ** — {type(e).__name__}: {e}")


async def read_expect_error(test_id: str, session, pk: int, ael: str, description: str) -> None:
    """Expect the AEL expression to raise an error."""
    global total_tests, passed_tests, failed_tests, error_tests
    total_tests += 1
    print(f"  [{test_id}] {ael}")
    print(f"      ({description})")
    try:
        stream = await (
            session.query(SET.id(pk))
            .bin("r").select_from(ael)
            .execute()
        )
        first = await stream.first()
        failed_tests += 1
        actual = first.record.bins.get("r") if first and first.is_ok else "N/A"
        print(f"      ** FAIL ** — Expected error, got: {actual}")
    except Exception as e:
        passed_tests += 1
        print(f"      PASS (error) — {type(e).__name__}: {e}")


async def filter_check(test_id: str, session, pk: int, ael: str, expect_found: bool) -> None:
    """Use AEL as a where() filter and check if the record matches."""
    global total_tests, passed_tests, failed_tests, error_tests
    total_tests += 1
    print(f"  [{test_id}] where: {ael}")
    try:
        stream = await session.query(SET.id(pk)).where(ael).execute()
        first = await stream.first()
        found = first is not None and first.is_ok
        if found == expect_found:
            passed_tests += 1
            print(f"      PASS — found={found}")
        else:
            failed_tests += 1
            print(f"      ** FAIL ** — Expected found={expect_found}, got found={found}")
    except Exception as e:
        error_tests += 1
        print(f"      ** ERROR ** — {type(e).__name__}: {e}")


async def filter_print(test_id: str, session, pk: int, ael: str, description: str) -> None:
    """Use AEL as a where() filter and print the result."""
    global total_tests, passed_tests, error_tests
    total_tests += 1
    print(f"  [{test_id}] where: {ael}")
    print(f"      ({description})")
    try:
        stream = await session.query(SET.id(pk)).where(ael).execute()
        first = await stream.first()
        found = first is not None and first.is_ok
        passed_tests += 1
        print(f"      => found={found}")
    except Exception as e:
        error_tests += 1
        print(f"      ** ERROR ** — {type(e).__name__}: {e}")


def print_summary() -> None:
    print(f"\n{SEP}")
    print(f"  SUMMARY: {total_tests} tests, {passed_tests} passed, "
          f"{failed_tests} failed, {error_tests} errors")
    print(SEP)


# ======================================================================
# Test data setup
# ======================================================================

async def setup_test_data(session) -> None:
    """Create test records for AEL expression evaluation."""
    # Record 1: Scalar bins
    await session.upsert(SET.id(1)).put({
        "intBin": 42, "floatBin": 3.14, "strBin": "hello",
        "boolBin": True, "negInt": -8, "zeroBin": 0,
    }).execute()

    # Record 2: Map + list
    await session.upsert(SET.id(2)).put({
        "m": {"alpha": 10, "beta": 20, "gamma": 30, "delta": 40, "epsilon": 50},
        "l": [50, 10, 40, 20, 30, 60, 5],
    }).execute()

    # Record 3: Nested CDT (profile with address and scores)
    await session.upsert(SET.id(3)).put({
        "profile": {
            "name": "Alice",
            "address": {"city": "Austin", "state": "TX", "zip": "73301"},
            "scores": [95, 87, 72, 100, 63],
            "tags": ["vip", "early_adopter"],
        },
    }).execute()

    # Record 4: Deeply nested CDT (users with addresses)
    await session.upsert(SET.id(4)).put({
        "data": {
            "users": [
                {"name": "Bob", "addresses": [
                    {"city": "NYC", "zip": "10001"},
                    {"city": "LA", "zip": "90001"},
                ]},
                {"name": "Eve", "addresses": [
                    {"city": "SF", "zip": "94101"},
                ]},
            ],
        },
    }).execute()

    # Record 5: Integer-key map
    await session.upsert(SET.id(5)).put({
        "m": {1: "one", 2: "two", 3: "three", 10: "ten", 20: "twenty"},
    }).execute()

    # Record 6: Empty collections
    await session.upsert(SET.id(6)).put({
        "emptyList": [], "emptyMap": {}, "intBin": 0, "strBin": "",
    }).execute()

    # Record 7: Transaction/business scenario
    await session.upsert(SET.id(7)).put({
        "price": 100, "qty": 5, "discount": 10.0, "tier": 2,
        "status": "active", "flag1": True, "flag2": False,
        "items": ["gold", "silver", "bronze"],
        "allowed": ["gold", "platinum"],
        "name": "gold",
    }).execute()

    # Record 8: Transaction time series
    await session.upsert(SET.id(8)).put({
        "txns": {
            "1672531200000,txn01": [150, "Coffee subscription"],
            "1675209600000,txn02": [10000, "Laptop"],
            "1677628800000,txn03": [250, "Groceries"],
            "1680307200000,txn04": [500, "Flight ticket"],
            "1682899200000,txn05": [75, "Books"],
            "1685577600000,txn06": [8000, "Phone"],
            "1688169600000,txn07": [3500, "Conference ticket"],
            "1690848000000,txn08": [45, "Snacks"],
            "1693526400000,txn09": [12000, "Vacation"],
            "1696118400000,txn10": [600, "Concert"],
            "1698796800000,txn11": [2750, "Furniture"],
            "1701388800000,txn12": [9500, "Holiday gifts"],
        },
    }).execute()

    # Record 9: Scores map (string keys, int values)
    await session.upsert(SET.id(9)).put({
        "scores": {"math": 85, "science": 92, "english": 78, "art": 95, "history": 88},
    }).execute()

    # Record 10: Numeric bins for arithmetic / type derivation
    await session.upsert(SET.id(10)).put({
        "a": 10, "b": 20, "c": 30.5, "d": 40.7, "name": "gold",
    }).execute()

    # Record 11: Type derivation (mixed types)
    await session.upsert(SET.id(11)).put({
        "a": 10, "b": 10, "c": True, "d": 11, "e": 3.14,
        "f": "hello", "g": False,
    }).execute()


# ======================================================================
# 1. Scalar Bin Access
# ======================================================================

async def test_scalar_bin_access(session) -> None:
    section("1. SCALAR BIN ACCESS")
    await read_check("S01", session, 1, "$.intBin", 42)
    await read_check("S02", session, 1, "$.floatBin.get(type: FLOAT)", 3.14)
    await read_check("S03", session, 1, "$.strBin.get(type: STRING)", "hello")
    await read_check("S04", session, 1, "$.boolBin.get(type: BOOL)", True)
    await read_check("S05", session, 1, "$.negInt", -8)
    await read_check("S06", session, 6, "$.intBin", 0)
    await filter_check("S07", session, 1, "$.intBin > 40", True)
    await filter_check("S08", session, 1, "$.intBin > 50", False)
    await filter_check("S09", session, 1, "$.strBin == 'hello'", True)
    await filter_check("S10", session, 1, "40 < $.intBin", True)


# ======================================================================
# 2. Type Inference and Casting
# ======================================================================

async def test_type_casting(session) -> None:
    section("2. TYPE INFERENCE AND CASTING")
    await read_check("T01", session, 1, "$.intBin.asFloat()", 42.0)
    await read_check("T02", session, 1, "$.floatBin.asInt()", 3)
    await filter_check("T03", session, 10, "$.a.get(type: INT) > $.b.get(type: INT)", False)
    await filter_check("T04", session, 10, "$.a.get(type: INT) == $.b.get(type: INT)", False)
    await read_print("T05", session, 10, "$.a + $.c",
                     "Mixed-type arithmetic (INT + FLOAT)")
    await read_check("T06", session, 10, "$.a + $.c.asInt()", 40)
    await read_check("T07", session, 10, "$.a.asFloat() + $.c", 40.5)
    await read_print("T08", session, 1, "$.floatBin.asInt().asFloat()",
                     "Chained casts: asInt then asFloat")
    await read_print("T09", session, 1, "$.intBin.asInt()",
                     "Typed read of an integer bin")
    await read_print("T10", session, 1, "$.floatBin.asFloat()",
                     "Typed read of a float bin")


# ======================================================================
# 2b. Type Derivation
# ======================================================================

async def test_type_derivation(session) -> None:
    section("2b. TYPE DERIVATION")
    await filter_check("TD01", session, 11, "$.a > 5", True)
    await filter_check("TD02", session, 11, "$.f == 'hello'", True)
    await filter_check("TD03", session, 11, "$.e > 3.0", True)
    await filter_check("TD04", session, 11, "$.c == true", True)
    await filter_check("TD05", session, 11, "$.c and not($.g)", True)
    await filter_check("TD06", session, 11, "$.c or $.g", True)
    await filter_check("TD07", session, 11, "not($.g)", True)
    await read_check("TD08", session, 11, "$.a + 1", 11)
    await read_check("TD09", session, 11, "$.e + 1.0", 4.14)
    await read_print("TD10", session, 11, "$.a + $.b",
                     "Both bins in arithmetic, no literal — can parser derive INT?")
    await filter_check("TD11", session, 11, "$.a + 1 == $.d", True)
    await filter_check("TD12", session, 11, "$.a + 1 > $.b", True)
    await filter_print("TD13", session, 11,
                       "$.a == $.b and $.c and $.a + 1 == $.d",
                       "Cross-expression: a,b,d derived INT; c derived BOOL. Expect true")
    await filter_print("TD14", session, 11,
                       "$.f == 'hello' and $.a + 1 == $.d and $.c",
                       "Mixed types in chain: f=STRING, a/d=INT, c=BOOL. Expect true")
    await filter_check("TD15", session, 11, "($.a * $.b) > 50", True)
    await filter_print("TD16", session, 11,
                       "($.a + $.b) == ($.d + $.b)",
                       "No literals anywhere — can parser derive types? Expect false")
    await filter_check("TD17", session, 11, "$.a > 0 and $.a + $.b > 15", True)
    await read_expect_error("TD18", session, 11, "$.a + $.f",
                            "INT + STRING should fail")
    await filter_print("TD19", session, 11, "$.a > 'hello'",
                       "INT bin compared to STRING literal — should error or return false")
    await read_check("TD20", session, 11, "$.a.asFloat() + $.e", 13.14)
    await read_print("TD21", session, 11, "$.a + $.e", "INT + FLOAT without cast — should fail")


# ======================================================================
# 3. Map Access
# ======================================================================

async def test_map_access(session) -> None:
    section("3. MAP ACCESS")
    await read_check("M01", session, 2, "$.m.alpha.get(type: INT)", 10)
    await read_check("M02", session, 2, "$.m.'alpha'.get(type: INT)", 10)
    await read_print("M03", session, 5, "$.m.1.get(type: STRING)",
                     "Integer key lookup")
    await read_print("M04", session, 2, "$.m.{0}.get(type: INT)",
                     "Map by index 0 (first by key order)")
    await read_check("M09", session, 2, "$.m.{}.count()", 5)
    await read_check("M10", session, 3, "$.profile.address.city.get(type: STRING)", "Austin")
    await read_print("M11", session, 2, "$.m.{alpha-delta}", "Key range [alpha, delta)")
    await read_print("M12", session, 2, "$.m.{delta-}", "Key range from delta onwards")
    await read_print("M13", session, 2, "$.m.{alpha,gamma}", "Key list")
    await read_print("M14", session, 2, "$.m.{!alpha-delta}", "Inverted key range")
    await read_print("M15", session, 2, "$.m.{!alpha,gamma}", "Inverted key list")
    await read_print("M16", session, 2, "$.m.{0:3}", "Index range 0:3")
    await read_print("M17", session, 2, "$.m.{-2:}", "Index range last 2")
    await read_print("M18", session, 2, "$.m.{=15:35}", "Value range [15,35)")
    await read_print("M19", session, 2, "$.m.{=10,30,50}", "Value list")
    await read_print("M20", session, 9, "$.scores.{#0:3}", "Rank range bottom 3")
    await read_print("M21", session, 9, "$.scores.{#-2:}", "Rank range top 2")
    await read_print("M22", session, 9, "$.scores.{!#0:2}", "Inverted rank range")
    await read_print("M23", session, 9, "$.scores.{#-1:2~88}",
                     "Relative rank range (relative to value 88)")
    await read_print("M24", session, 2, "$.m.{0:2~beta}",
                     "Key-relative index range")
    await read_print("M25", session, 2, "$.m.{alpha-delta}.get(return: COUNT)",
                     "Count on key range")


# ======================================================================
# 4. List Access
# ======================================================================

async def test_list_access(session) -> None:
    section("4. LIST ACCESS")
    await read_check("L01", session, 2, "$.l.[0].get(type: INT)", 50)
    await read_check("L02", session, 2, "$.l.[3].get(type: INT)", 20)
    await read_check("L03", session, 2, "$.l.[-1].get(type: INT)", 5)
    await read_check("L04", session, 2, "$.l.[-3].get(type: INT)", 30)
    await read_check("L08", session, 2, "$.l.[].count()", 7)
    await read_print("L09", session, 2, "$.l.[0:3]", "Index range [0,3)")
    await read_print("L10", session, 2, "$.l.[-3:]", "Last 3 elements")
    await read_print("L11", session, 2, "$.l.[!0:2]", "Inverted index range")
    await read_print("L12", session, 2, "$.l.[=10,30,50]", "Value list")
    await read_print("L13", session, 2, "$.l.[!=10,30,50]", "Inverted value list")
    await read_print("L14", session, 2, "$.l.[=20:50]", "Value range [20,50)")
    await read_print("L15", session, 2, "$.l.[#0:3]", "Rank range bottom 3")
    await read_print("L16", session, 2, "$.l.[#-3:]", "Rank range top 3")
    await read_print("L17", session, 2, "$.l.[!#0:2]", "Inverted rank range")
    await read_print("L18", session, 2, "$.l.[#-1:2~30]", "Relative rank range")
    await read_check("L19", session, 2, "$.l.[=10].count()", 1)
    await read_print("L20", session, 2, "$.l.[=20:50].get(return: COUNT)", "Count on value range")


# ======================================================================
# 5. Nested CDT Navigation
# ======================================================================

async def test_nested_cdt(session) -> None:
    section("5. NESTED CDT NAVIGATION")
    await read_check("N01", session, 3, "$.profile.address.city.get(type: STRING)", "Austin")
    await read_check("N02", session, 3, "$.profile.address.zip.get(type: STRING)", "73301")
    await read_check("N03", session, 3, "$.profile.scores.[0].get(type: INT)", 95)
    await read_check("N04", session, 3, "$.profile.scores.[-1].get(type: INT)", 63)
    await read_check("N05", session, 3, "$.profile.scores.[].count()", 5)
    await read_check("N06", session, 3, "$.profile.scores.[#-1].get(type: INT)", 100)
    await read_check("N07", session, 4, "$.data.users.[0].name.get(type: STRING)", "Bob")
    await read_check("N08", session, 4,
                     "$.data.users.[0].addresses.[1].city.get(type: STRING)", "LA")
    await read_check("N09", session, 4, "$.data.users.[1].name.get(type: STRING)", "Eve")
    await read_check("N10", session, 4,
                     "$.data.users.[1].addresses.[0].city.get(type: STRING)", "SF")
    await read_print("N11", session, 3, "$.profile.scores.[0:3]",
                     "Nested CDT list index range")
    await read_check("N13", session, 3, "$.profile.address.{}.count()", 3)


# ======================================================================
# 6. Arithmetic
# ======================================================================

async def test_arithmetic(session) -> None:
    section("6. ARITHMETIC")
    await read_check("A01", session, 7, "$.price + $.qty", 105)
    await read_check("A02", session, 7, "$.price - $.qty", 95)
    await read_check("A03", session, 7, "$.price * $.qty", 500)
    await read_check("A04", session, 7, "$.price / $.qty", 20)
    await read_check("A05", session, 7, "$.price % $.qty", 0)
    await read_check("A06", session, 7, "$.price + 50", 150)
    await read_check("A07", session, 7, "($.price * $.qty) - 100", 400)
    await read_check("A08", session, 7, "(($.price + $.qty) * 2) - 10", 200)
    await filter_check("A09", session, 7, "($.price * $.qty) > 400", True)
    await read_print("A10", session, 10, "$.c + $.d",
                     "Sum of two untyped float bins")
    await read_check("A11", session, 1, "$.intBin / 5", 8)
    await read_check("A12", session, 1, "$.intBin % 5", 2)


# ======================================================================
# 7. Bitwise Operations
# ======================================================================

async def test_bitwise(session) -> None:
    section("7. BITWISE OPERATIONS")
    await read_check("B01", session, 1, "$.intBin & 15", 10)
    await read_check("B02", session, 1, "$.intBin | 15", 47)
    await read_check("B03", session, 1, "$.intBin ^ 15", 37)
    await read_check("B04", session, 1, "~$.intBin", -43)
    await read_check("B05", session, 1, "$.intBin << 2", 168)
    await read_check("B06", session, 1, "$.intBin >> 1", 21)
    await read_print("B07", session, 1, "$.negInt >> 1",
                     ">> is logical, not arithmetic. Expect -4 but may get large positive")
    await read_print("B08", session, 1, "$.negInt >>> 1",
                     "Unsigned right shift (>>>)")
    await filter_check("B09", session, 1, "($.intBin & 1) == 0", True)
    await filter_check("B10", session, 1, "(($.intBin >> 3) & 1) == 1", True)


# ======================================================================
# 8. Comparison Operators
# ======================================================================

async def test_comparison(session) -> None:
    section("8. COMPARISON OPERATORS")
    await filter_check("C01", session, 1, "$.intBin == 42", True)
    await filter_check("C02", session, 1, "$.intBin != 42", False)
    await filter_check("C03", session, 1, "$.intBin > 41", True)
    await filter_check("C04", session, 1, "$.intBin >= 42", True)
    await filter_check("C05", session, 1, "$.intBin < 43", True)
    await filter_check("C06", session, 1, "$.intBin <= 42", True)
    await filter_check("C07", session, 1, "$.strBin == 'hello'", True)
    await filter_check("C08", session, 1, "$.strBin != 'world'", True)
    await filter_check("C09", session, 1, "$.floatBin > 3.0", True)
    await filter_check("C10", session, 1, "$.boolBin == true", True)
    await filter_check("C11", session, 7, '"gold" in $.items', True)
    await filter_check("C12", session, 7, '"platinum" in $.items', False)
    await filter_check("C13", session, 7, '$.status in ["active", "pending"]', True)
    await filter_check("C15", session, 1, "$.intBin >= 42 and $.intBin <= 42", True)
    await filter_check("C16", session, 7, "100 == $.price", True)


# ======================================================================
# 9. Logical Operators
# ======================================================================

async def test_logical(session) -> None:
    section("9. LOGICAL OPERATORS")
    await filter_check("LG01", session, 1, "$.intBin > 40 and $.strBin == 'hello'", True)
    await filter_check("LG02", session, 1, "$.intBin > 50 and $.strBin == 'hello'", False)
    await filter_check("LG03", session, 1, "$.intBin > 50 or $.strBin == 'hello'", True)
    await filter_check("LG04", session, 1, "$.intBin > 50 or $.strBin == 'world'", False)
    await filter_check("LG05", session, 1, "not($.intBin > 50)", True)
    await filter_check("LG06", session, 1, "not(not($.intBin > 40))", True)
    await filter_check("LG07", session, 7, "exclusive($.flag1, $.flag2)", True)
    await filter_check("LG09", session, 7, "$.flag1 or $.flag2 and $.flag2", True)
    await filter_check("LG10", session, 7, "($.flag1 or $.flag2) and $.flag2", False)
    await filter_check("LG11", session, 7, "$.flag1 and not($.flag2)", True)
    await filter_check("LG12", session, 1,
                       "$.intBin > 0 and $.intBin < 100 and $.strBin == 'hello'", True)
    await filter_check("LG13", session, 1,
                       "$.intBin == 0 or $.intBin == 42 or $.intBin == 99", True)


# ======================================================================
# 10. Control Structures
# ======================================================================

async def test_control_structures(session) -> None:
    section("10. CONTROL STRUCTURES")
    # PSDK uses unquoted variable names: let (x = ...) not let ('x' = ...)
    await read_check("CS01", session, 7, "let (x = $.price) then (${x} + 1)", 101)
    await read_check("CS02", session, 7,
                     "let (x = $.price, y = $.qty) then (${x} * ${y})", 500)
    await read_check("CS03", session, 7,
                     "let (x = $.price, y = ${x} * 2) then (${y} + ${x})", 300)
    await read_check("CS04", session, 7,
                     "let (total = $.price * $.qty, tax = ${total} / 10) "
                     "then (${total} + ${tax})", 550)
    await read_print("CS05", session, 7,
                     "let (x = $.price) do (with (y = ${x} * 2) then (${y} + ${x}))",
                     "Nested with...do, expect 300")
    await filter_check("CS06", session, 7,
                       "let (total = $.price * $.qty) then (${total} > 400)", True)

    await read_check("CS07", session, 7,
                     'when ($.tier == 1 => "gold", $.tier == 2 => "silver", '
                     'default => "bronze")',
                     "silver")
    await read_check("CS08", session, 7,
                     'when ($.tier == 5 => "diamond", default => "standard")',
                     "standard")
    await read_check("CS09", session, 7,
                     "when ($.tier == 1 => 100, $.tier == 2 => 200, "
                     "$.tier == 3 => 300, default => 0)",
                     200)
    await read_check("CS10", session, 7,
                     'when ($.price > 200 => "expensive", $.price > 50 => "moderate", '
                     'default => "cheap")',
                     "moderate")
    await read_print("CS11", session, 7,
                     'when ($.tier > 0 => when ($.tier == 1 => "tier1", '
                     'default => "tierN"), default => "none")',
                     "Nested when, expect 'tierN'")
    await filter_print("CS12", session, 7,
                       '$.status == (when ($.tier == 2 => "active", '
                       'default => "inactive"))',
                       "String comparison against a when() result")
    await read_print("CS13", session, 7,
                     'let (t = $.tier) then (when (${t} == 1 => "gold", '
                     '${t} == 2 => "silver", default => "bronze"))',
                     "when inside let, expect 'silver'")
    await read_print("CS14", session, 7,
                     "when ($.tier == 2 => let (p = $.price) then (${p} * 2), "
                     "default => 0)",
                     "let inside when branch, expect 200")
    await read_print("CS15", session, 7,
                     "let (t = $.tier) then (when (${t} == 2 => "
                     "let (p = $.price) then (${p} + ${t}), default => 0))",
                     "Deeply nested: let -> when -> let, expect 102")


# ======================================================================
# 11. Metadata
# ======================================================================

async def test_metadata(session) -> None:
    section("11. METADATA")
    await read_print("MD01", session, 1, "$.ttl()", "TTL in seconds")
    await read_print("MD02", session, 1, "$.recordSize()", "Record size in bytes (> 0)")
    await filter_print("MD03", session, 1, "$.keyExists()", "Key exists check")
    await filter_check("MD04", session, 1, "$.setName() == 'ael_spec'", True)
    await filter_check("MD05", session, 1, "$.ttl() > 0 or $.ttl() == -1", True)
    await filter_check("MD06", session, 1, "$.sinceUpdate() >= 0", True)
    await read_print("MD07", session, 1, "$.voidTime()", "Void time")
    await read_print("MD08", session, 1, "$.digestModulo(3)", "Digest modulo 3 (0, 1, or 2)")
    await filter_check("MD09", session, 1, "not($.isTombstone())", True)
    await filter_check("MD10", session, 1, "$.recordSize() > 0 and $.ttl() != 0", True)


# ======================================================================
# 12. Path Functions
# ======================================================================

async def test_path_functions(session) -> None:
    section("12. PATH FUNCTIONS")
    await read_check("PF01", session, 2, "$.m.alpha.get(type: INT)", 10)
    await read_print("PF02", session, 2, "$.m.{alpha-delta}.get(return: COUNT)",
                     "Count on key range")
    await read_print("PF03", session, 9, "$.scores.{#-1}.get(return: KEY)",
                     "Key at highest rank")
    await read_print("PF04", session, 2, "$.m.alpha.get(return: INDEX)",
                     "Index of key alpha")
    await read_print("PF05", session, 9, "$.scores.math.get(return: RANK)",
                     "Rank of key math")
    await read_print("PF06", session, 2, "$.m.{alpha,beta}.get(return: ORDERED_MAP)",
                     "Ordered map of key list")
    await read_print("PF07", session, 2, "$.m.alpha.get(return: EXISTS)",
                     "Existing key -> true")
    await read_print("PF08", session, 2, "$.m.zzz.get(return: EXISTS)",
                     "Missing key -> false")
    await read_check("PF09", session, 2, "$.l.[].count()", 7)
    await read_check("PF10", session, 2, "$.l.[=50].count()", 1)
    await read_print("PF11", session, 1, "$.intBin.exists()",
                     "exists() path function on a scalar bin")
    await read_print("PF12", session, 6, "$.emptyList.exists()",
                     "exists() path function on an empty list")


# ======================================================================
# 13. Transaction Scenario
# ======================================================================

async def test_transaction_scenario(session) -> None:
    section("13. TRANSACTION SCENARIO")
    await read_check("TX01", session, 8, "$.txns.{}.count()", 12)
    await read_print("TX02", session, 8, "$.txns.{0}.get(return: KEY_VALUE)",
                     "First transaction by key order")
    await read_print("TX03", session, 8, "$.txns.{-1}.get(return: KEY_VALUE)",
                     "Last transaction by key order")
    await read_print("TX04", session, 8, '$.txns.{"1688169600000"-"1696118400000"}',
                     "Transactions in Q3 2023 (Jul-Sep) -- expect 3 entries")
    await read_print("TX05", session, 8,
                     '$.txns.{"1688169600000"-"1696118400000"}.get(return: COUNT)',
                     "Count in Q3 2023 -- expect 3")
    await read_print("TX06", session, 8, '$.txns.{"1685577600000"-}',
                     "All from Jun 2023 onwards -- expect 7 entries")
    await read_print("TX07", session, 8, '$.txns.{-"1680307200000"}',
                     "All before Apr 2023 -- expect 3 entries")
    await read_print("TX08", session, 8, '$.txns.{"9999999999999"-}',
                     "Empty range -- expect empty map")
    await read_print("TX09", session, 8,
                     '$.txns.{"1685577600000"-"1701388800000"}.get(return: COUNT)',
                     "Count Jun-Nov -- expect 6")
    await read_print("TX10", session, 8, "$.txns.{#-1}.get(return: KEY_VALUE)",
                     "Highest value transaction (rank -1)")
    await read_print("TX11", session, 8, "$.txns.{#0}.get(return: KEY_VALUE)",
                     "Lowest value transaction (rank 0)")
    await read_print("TX12", session, 8, "$.txns.{#-3:}", "Top 3 by value")
    await read_print("TX13", session, 8, "$.txns.{#0:3}", "Bottom 3 by value")
    await read_print("TX14", session, 8, "$.txns.{#-5:}", "Top 5 by value")
    await read_print("TX15", session, 8,
                     "let ('filtered' = $.txns.{\"1685577600000\"-\"1701388800000\"}"
                     ".get(return: COUNT)) then (${filtered})",
                     "Chained: time range -> count via let...then -- expect 6")
    await filter_check("TX18", session, 8, "$.txns.{}.count() > 10", True)
    await filter_check("TX19", session, 8,
                       '$.txns.{"1688169600000"-"1696118400000"}.get(return: COUNT) > 0',
                       True)
    await read_print("TX20", session, 8, "$.txns.{#-1}.[0].get(type: INT)",
                     "Amount of highest value transaction")


# ======================================================================
# 14. Rank-Based Access (Record 9)
# ======================================================================

async def test_rank_based(session) -> None:
    section("14. RANK-BASED ACCESS")
    print("  scores rank order: english(78) < math(85) < history(88) "
          "< science(92) < art(95)")
    print()
    await read_print("R01", session, 9, "$.scores.{#0}.get(return: KEY)",
                     "Key at rank 0 -- expect 'english'")
    await read_print("R02", session, 9, "$.scores.{#0}.get(type: INT)",
                     "Value at rank 0 -- expect 78")
    await read_print("R03", session, 9, "$.scores.{#-1}.get(return: KEY)",
                     "Key at rank -1 (highest) -- expect 'art'")
    await read_print("R04", session, 9, "$.scores.{#-1}.get(type: INT)",
                     "Value at rank -1 -- expect 95")
    await read_print("R05", session, 9, "$.scores.{#-2:}",
                     "Top 2 by rank -- expect science(92), art(95)")
    await read_print("R06", session, 9, "$.scores.{#0:2}",
                     "Bottom 2 by rank -- expect english(78), math(85)")
    await read_print("R07", session, 9, "$.scores.{!#-2:}", "All except top 2")
    await read_print("R08", session, 9, "$.scores.math.get(return: RANK)",
                     "Rank of math -- expect 1 (second lowest)")
    await read_print("R09", session, 9, "$.scores.{#-1}.get(return: KEY_VALUE)",
                     "Key-value of highest -- expect {art: 95}")


# ======================================================================
# 15. Return Type Variations
# ======================================================================

async def test_return_types(session) -> None:
    section("15. RETURN TYPE VARIATIONS")
    print("  Using $.m.{alpha,beta,gamma} on Record 2")
    print()
    await read_print("RT01", session, 2, "$.m.{alpha,beta,gamma}",
                     "Default (ORDERED_MAP)")
    await read_print("RT02", session, 2, "$.m.{alpha,beta,gamma}.get(return: COUNT)",
                     "COUNT -- expect 3")
    await read_print("RT03", session, 2, "$.m.{alpha,beta,gamma}.get(return: KEY)",
                     "KEY -- expect list of keys")
    await read_print("RT04", session, 2, "$.m.{alpha,beta,gamma}.get(return: VALUE)",
                     "VALUE -- expect list of values")
    await read_print("RT05", session, 2, "$.m.{alpha,beta,gamma}.get(return: KEY_VALUE)",
                     "KEY_VALUE")
    await read_print("RT06", session, 2, "$.m.alpha.get(return: INDEX)",
                     "INDEX -- expect 0")
    await read_print("RT07", session, 2, "$.m.alpha.get(return: RANK)",
                     "RANK -- expect 0 (value 10 is lowest)")
    await read_print("RT08", session, 2, "$.m.alpha.get(return: EXISTS)",
                     "EXISTS -- expect true")
    await read_print("RT09", session, 2, "$.m.zzz.get(return: EXISTS)",
                     "EXISTS (absent) -- expect false")
    await read_print("RT10", session, 2, "$.m.alpha.get(return: NONE)",
                     "NONE -- expect null")


# ======================================================================
# 16. Edge Cases
# ======================================================================

async def test_edge_cases(session) -> None:
    section("16. EDGE CASES")
    await read_check("E01", session, 2, "$.l.[0].get(type: INT)", 50)
    await read_check("E02", session, 2, "$.l.[-1].get(type: INT)", 5)
    await read_check("E03", session, 6, "$.emptyList.[].count()", 0)
    await read_check("E04", session, 6, "$.emptyMap.{}.count()", 0)
    await read_check("E05", session, 6, "$.intBin + 1", 1)
    await filter_check("E06", session, 6, "$.strBin == ''", True)
    await read_check("E07", session, 1, "$.negInt + $.negInt", -16)
    await read_print("E08", session, 2, "$.l.[100].get(type: INT)",
                     "Out of range index — expect error or null")
    await read_print("E09", session, 2, "$.l.[-100].get(type: INT)",
                     "Negative index beyond list — expect error or null")
    await read_print("E10", session, 2, "$.m.zzz.get(type: INT)",
                     "Non-existent map key — expect error or null")
    await read_expect_error("E12", session, 1, "$.intBin / 0", "Division by zero")
    await read_expect_error("E13", session, 1, "$.intBin % 0", "Modulus by zero")
    await read_check("E17", session, 3, "$.profile.'name'.get(type: STRING)", "Alice")
    await read_check("E18", session, 3, '$.profile."name".get(type: STRING)', "Alice")


# ======================================================================
# Main
# ======================================================================

async def main() -> None:
    async with await _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await session.truncate(SET)
        await asyncio.sleep(0.2)
        await setup_test_data(session)

        await test_scalar_bin_access(session)
        await test_type_casting(session)
        await test_type_derivation(session)
        await test_map_access(session)
        await test_list_access(session)
        await test_nested_cdt(session)
        await test_arithmetic(session)
        await test_bitwise(session)
        await test_comparison(session)
        await test_logical(session)
        await test_control_structures(session)
        await test_metadata(session)
        await test_path_functions(session)
        await test_transaction_scenario(session)
        await test_rank_based(session)
        await test_return_types(session)
        await test_edge_cases(session)

        print_summary()


if __name__ == "__main__":
    asyncio.run(main())
