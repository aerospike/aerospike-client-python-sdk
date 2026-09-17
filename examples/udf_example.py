#!/usr/bin/env python3
"""Demonstrates user defined functions (UDFs).

Registers Lua on the cluster, executes a record UDF against a single key,
executes the same UDF across a batch of keys, chains a UDF with ordinary write
operations, and surfaces a UDF failure as ``UdfError``.

A record UDF runs on the node that owns the record, so read-modify-write logic
that would otherwise need two round-trips (and a race between them) executes
atomically in one.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.exceptions import UdfError

SET = DataSet.of("test", "udf-demo")

# Server-side module name, referenced by function(PACKAGE, ...).
PACKAGE = "example_bonus"

# Path the module is registered under, relative to the server's UDF directory.
SERVER_PATH = f"{PACKAGE}.lua"

# Registered from a string so the example is self-contained; production code
# more often ships a .lua file and calls register_udf_from_file().
LUA_SOURCE = b"""
local function balance_of(rec)
    return rec['balance'] or 0
end

-- Read-modify-write in one server-side step, returning the new balance.
function apply_bonus(rec, amount)
    if not aerospike:exists(rec) then
        return nil
    end
    rec['balance'] = balance_of(rec) + amount
    rec['bonuses'] = (rec['bonuses'] or 0) + 1
    aerospike:update(rec)
    return rec['balance']
end

-- Read-only: derives a value without writing the record.
function tier(rec)
    local balance = balance_of(rec)
    if balance >= 1000 then
        return 'GOLD'
    elseif balance >= 500 then
        return 'SILVER'
    end
    return 'BRONZE'
end

-- Always fails, to show how a Lua error reaches the client.
function always_fails(rec)
    error('deliberate failure from Lua')
end
"""


async def run_examples(session) -> None:
    # --- 1) Register the Lua module ---
    print("--- 1) Register the Lua module ---")
    task = await session.register_udf(LUA_SOURCE, SERVER_PATH)
    await task.wait_till_complete()
    print(f"registered {SERVER_PATH}")

    await session.upsert(SET.id("acct-1")).bin("balance").set_to(100).execute()
    await session.upsert(SET.id("acct-2")).bin("balance").set_to(600).execute()
    await session.upsert(SET.id("acct-3")).bin("balance").set_to(1500).execute()

    # --- 2) Single-key UDF: apply_bonus(50) on acct-1 ---
    print("--- 2) Single-key UDF: apply_bonus(50) on acct-1 ---")
    async with await (
        session.execute_udf(SET.id("acct-1"))
        .function(PACKAGE, "apply_bonus")
        .passing(50)
        .execute()
    ) as stream:
        new_balance = await stream.first_udf_result()
    if new_balance is None:
        raise AssertionError("apply_bonus returned no result")
    print(f"apply_bonus returned new balance: {new_balance}")

    # --- 3) Batch UDF: apply_bonus(10) on three keys in one call ---
    print("--- 3) Batch UDF: apply_bonus(10) on three keys in one call ---")
    keys = SET.ids("acct-1", "acct-2", "acct-3")
    async with await (
        session.execute_udf(*keys)
        .function(PACKAGE, "apply_bonus")
        .passing(10)
        .execute()
    ) as stream:
        async for row in stream:
            # In a batch the Lua return value arrives in the record's "SUCCESS" bin.
            # The single-key first_udf_result() accessor is not populated here.
            print(f"  {row.key.value} -> {row.record_or_raise().bins['SUCCESS']}")

    # --- 4) Read-only UDF: tier() derives a value without writing ---
    print("--- 4) Read-only UDF: tier() derives a value without writing ---")
    for key in keys:
        async with await (
            session.execute_udf(key)
            .function(PACKAGE, "tier")
            .execute()
        ) as stream:
            tier_name = await stream.first_udf_result()
        if tier_name is None:
            raise AssertionError(f"tier() returned no result for {key.value}")
        print(f"  {key.value} -> {tier_name}")

    # --- 5) Chain a UDF with an ordinary write in one round-trip ---
    print("--- 5) Chain a UDF with an ordinary write in one round-trip ---")
    await (
        session.execute_udf(SET.id("acct-2"))
        .function(PACKAGE, "apply_bonus")
        .passing(25)
        .upsert(SET.id("acct-3"))
        .bin("note").set_to("audited")
        .execute()
    )
    acct3 = await (await session.query(SET.id("acct-3")).execute()).first_or_raise()
    print(f"acct-3 after chained write: {acct3.record.bins}")

    # --- 6) A Lua error surfaces as UdfError ---
    print("--- 6) A Lua error surfaces as UdfError ---")
    try:
        async with await (
            session.execute_udf(SET.id("acct-1"))
            .function(PACKAGE, "always_fails")
            .execute()
        ) as stream:
            await stream.first_or_raise()
        raise AssertionError("expected UdfError was not raised")
    except UdfError as exc:
        print(f"caught {type(exc).__name__} (code {exc.result_code}): {exc.base_message}")

    print("Overall: SUCCESS")


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await session.truncate(SET)
        await asyncio.sleep(0.2)

        try:
            await run_examples(session)
        finally:
            await session.remove_udf(SERVER_PATH)


if __name__ == "__main__":
    asyncio.run(main())
