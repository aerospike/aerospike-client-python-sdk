#!/usr/bin/env python3
"""Multi-record transactions (MRT).

``session.transaction()`` opens a transaction context: every operation run on
the returned session auto-participates, no policy juggling. The block commits
atomically on clean exit and aborts if an exception propagates out — so a
partial transfer can never be left behind.

Requires an Aerospike Enterprise cluster with strong consistency (MRT) support.
"""

import _env
from _env import Example
from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError, ResultCode


class MultiRecordTransactionExample(Example):
    accounts = DataSet.of(_env.sc_namespace(), "accounts")

    async def __init__(self):
        await super().__init__(sc=True)

    async def _balance(self, who: int) -> int:
        result = await (
            await self.session.query(self.accounts.id(who)).bins(["balance"]).execute()
        ).first_or_raise()
        return result.record.bins["balance"]

    async def _run_transfers(self) -> None:
        print(f"Start:  A={await self._balance(1)}  "
              f"B={await self._balance(2)}")

        # -- Successful transfer: both writes commit together ---------------------
        async with self.session.transaction() as tx:
            await tx.upsert(self.accounts.id(1)).bin("balance").add(-30).execute()
            await tx.upsert(self.accounts.id(2)).bin("balance").add(30).execute()
        print(f"Commit: A={await self._balance(1)}  "
              f"B={await self._balance(2)}  (transferred 30)")

        # -- Aborted transfer: an error mid-block rolls back both writes -----------
        try:
            async with self.session.transaction() as tx:
                await tx.upsert(self.accounts.id(1)).bin("balance").add(-50).execute()
                raise RuntimeError("fraud check failed")  # forces abort
        except RuntimeError as exc:
            print(f"Abort:  {exc} — rolled back")
        print(f"After:  A={await self._balance(1)}  "
              f"B={await self._balance(2)}  (unchanged)")

    async def run(self) -> None:
        try:
            await self.session.upsert(self.accounts.id(1)).bin("balance").set_to(100).execute()
            await self.session.upsert(self.accounts.id(2)).bin("balance").set_to(0).execute()
            try:
                await self._run_transfers()
            except AerospikeError as exc:
                if exc.result_code == ResultCode.UNSUPPORTED_FEATURE:
                    print("Skipped: multi-record transactions require a "
                          "strong-consistency namespace (this namespace is AP).")
                else:
                    raise
        finally:
            await self.session.delete(self.accounts.id(1)).execute()
            await self.session.delete(self.accounts.id(2)).execute()
