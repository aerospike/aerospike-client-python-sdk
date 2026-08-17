#!/usr/bin/env python3
"""Multi-record transactions (MRT).

``session.transaction()`` opens a transaction context: every operation run on
the returned session auto-participates, no policy juggling. The block commits
atomically on clean exit and aborts if an exception propagates out — so a
partial transfer can never be left behind.

Requires an Aerospike Enterprise cluster with strong consistency (MRT) support.
"""

import asyncio

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode


async def balance(session, accounts, who: int) -> int:
    result = await (
        await session.query(accounts.id(who)).bins(["balance"]).execute()
    ).first_or_raise()
    return result.record.bins["balance"]


async def run_transfers(session, accounts) -> None:
    print(f"Start:  A={await balance(session, accounts, 1)}  "
          f"B={await balance(session, accounts, 2)}")

    # -- Successful transfer: both writes commit together ---------------------
    async with session.transaction() as tx:
        await tx.upsert(accounts.id(1)).bin("balance").add(-30).execute()
        await tx.upsert(accounts.id(2)).bin("balance").add(30).execute()
    print(f"Commit: A={await balance(session, accounts, 1)}  "
          f"B={await balance(session, accounts, 2)}  (transferred 30)")

    # -- Aborted transfer: an error mid-block rolls back both writes -----------
    try:
        async with session.transaction() as tx:
            await tx.upsert(accounts.id(1)).bin("balance").add(-50).execute()
            raise RuntimeError("fraud check failed")  # forces abort
    except RuntimeError as exc:
        print(f"Abort:  {exc} — rolled back")
    print(f"After:  A={await balance(session, accounts, 1)}  "
          f"B={await balance(session, accounts, 2)}  (unchanged)")


async def main() -> None:
    # MRT requires a strong-consistency namespace; connect_sc() uses the
    # AEROSPIKE_HOST_SC seed (+ auth) when configured, else the default seed.
    async with await _env.connect_sc().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        accounts = DataSet.of(_env.sc_namespace(), "accounts")

        try:
            await session.upsert(accounts.id(1)).bin("balance").set_to(100).execute()
            await session.upsert(accounts.id(2)).bin("balance").set_to(0).execute()
            try:
                await run_transfers(session, accounts)
            except AerospikeError as exc:
                if exc.result_code == ResultCode.UNSUPPORTED_FEATURE:
                    print("Skipped: multi-record transactions require a "
                          "strong-consistency namespace (this namespace is AP).")
                else:
                    raise
        finally:
            await session.delete(accounts.id(1)).execute()
            await session.delete(accounts.id(2)).execute()


if __name__ == "__main__":
    asyncio.run(main())
