#!/usr/bin/env python3
"""Multi-record transactions (MRT).

``session.transaction()`` opens a transaction context: every operation run on
the returned session auto-participates, no policy juggling. The block commits
atomically on clean exit and aborts if an exception propagates out — so a
partial transfer can never be left behind. ``abort()`` rolls back deliberately
when a precondition fails.

``session.do_in_transaction()`` is the alternative entry point: it owns the
lifecycle, retries the block on a transient conflict, and returns the
callable's value.

The classic motivating case is a funds transfer: two records must both change
or neither may.

Requires an Aerospike Enterprise cluster with strong consistency (MRT) support.
On an AP namespace this example is skipped.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode


async def run_transfers(session, accounts) -> None:
    alice = accounts.id("alice")
    bob = accounts.id("bob")

    await report(session, accounts, "opening balances")

    # --- 1) Commit: transfer 250 from alice to bob ---
    print("--- 1) Commit: transfer 250 from alice to bob ---")
    async with session.transaction() as tx:
        await tx.update(alice).bin("balance").add(-250).execute()
        await tx.update(bob).bin("balance").add(250).execute()
    await report(session, accounts, "after committed transfer")

    # --- 2) Read a value out of the transaction ---
    print("--- 2) Read a value out of the transaction ---")
    # do_in_transaction() is the other entry point: it owns the lifecycle and
    # retries the whole block on a transient conflict, returning whatever the
    # callable returns. Reads inside see the transaction's own uncommitted
    # writes, so the balance below reflects the debit that has not committed yet.
    async def debit_and_read(tx):
        await tx.update(alice).bin("balance").add(-100).execute()
        return await balance(tx, accounts, "alice")

    observed = await session.do_in_transaction(debit_and_read)
    print(f"balance observed inside the transaction: {observed}")
    await report(session, accounts, "after second transfer")

    # --- 3) abort(): roll back deliberately when a precondition fails ---
    print("--- 3) abort(): roll back deliberately when a precondition fails ---")
    async with session.transaction() as tx:
        await tx.update(alice).bin("balance").add(-5000).execute()

        if await balance(tx, accounts, "alice") < 0:
            # Nothing written in this block survives; abort() unwinds it all.
            await tx.abort()
    await report(session, accounts, "after aborted overdraft (unchanged)")

    # --- 4) An exception escaping the block also rolls back ---
    print("--- 4) An exception escaping the block also rolls back ---")
    try:
        async with session.transaction() as tx:
            await tx.update(alice).bin("balance").add(-1).execute()
            raise RuntimeError("simulated downstream failure")
        raise AssertionError("expected the exception to propagate")
    except RuntimeError as exc:
        print(f"caught {exc}; transaction was rolled back")
    await report(session, accounts, "after failed transaction (unchanged)")

    print("Overall: SUCCESS")


async def balance(session, accounts, who: str) -> int:
    result = await (
        await session.query(accounts.id(who)).bins("balance").execute()
    ).first_or_raise()
    return result.record.bins["balance"]


async def report(session, accounts, label: str) -> None:
    print(f"{label}: alice={await balance(session, accounts, 'alice')} "
          f"bob={await balance(session, accounts, 'bob')}")


async def main() -> None:
    # MRT requires a strong-consistency namespace; connect_sc() uses the
    # AEROSPIKE_HOST_SC seed (+ auth) when configured, else the default seed.
    async with _env.connect_sc().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        accounts = DataSet.of(_env.sc_namespace(), "txn-demo")

        try:
            await session.upsert(accounts.id("alice")).bin("balance").set_to(1000).execute()
            await session.upsert(accounts.id("bob")).bin("balance").set_to(1000).execute()
            try:
                await run_transfers(session, accounts)
            except AerospikeError as exc:
                if exc.result_code == ResultCode.UNSUPPORTED_FEATURE:
                    print("Skipped: multi-record transactions require a "
                          "strong-consistency namespace (this namespace is AP).")
                else:
                    raise
        finally:
            await session.delete(accounts.id("alice")).execute()
            await session.delete(accounts.id("bob")).execute()


if __name__ == "__main__":
    asyncio.run(main())
