#!/usr/bin/env python3
"""Server-side string operations (requires Aerospike server 8.1.3+).

Strings can be measured, sliced, searched, and transformed entirely on the
server — the value never round-trips to the client. Three ways to reach them:

1. the fluent ``bin(...).str_*`` builder, chained in a single call;
2. the low-level ``StringOperation`` factories via ``add_operation``;
3. a query projection with ``Exp.string_*`` computed into result bins.

If the cluster is older than 8.1.3, this example prints a skip message and exits.
"""

import _env
from _env import Example
from aerospike_sdk import DataSet, Exp, StringOperation


class StringOperationsExample(Example):
    docs = DataSet.of("test", "string_ops_demo")
    key = docs.id("row1")

    async def run(self) -> None:

        if not await _env.server_at_least(self.session, (8, 1, 3)):
            print("Skipped: server-side string operations require Aerospike 8.1.3+.")
            return

        # --- 1) Fluent bin builder: strlen, substr, find, upper, get in one call ---
        # Each op contributes one positional result slot in request order,
        # read back by index with operation_result(i).
        await self.session.upsert(self.key).bin("message").set_to("hello").execute()

        result = await (
            await self.session.upsert(self.key)
            .bin("message").str_strlen()
            .bin("message").str_substr(1, 4)
            .bin("message").str_substr(3)
            .bin("message").str_find("ll")
            .bin("message").str_upper()
            .bin("message").get()
            .execute()
        ).first_or_raise()
        print(f"  strlen           -> {result.operation_result(0)}")
        print(f"  substr(1, 4)     -> {result.operation_result(1)!r}")
        print(f"  substr(3) suffix -> {result.operation_result(2)!r}")
        print(f"  find('ll')       -> {result.operation_result(3)}")
        # A modify op yields nil positionally; the trailing get shows the new value.
        print(f"  upper (modify)   -> {result.operation_result(4)!r}")
        print(f"  get after upper  -> {result.operation_result(5)!r}")

        # --- 2) Low-level StringOperation factories: same reads on a fresh value ---
        await self.session.upsert(self.key).bin("message").set_to("hello").execute()

        result = await (
            await self.session.upsert(self.key)
            .add_operation(StringOperation.strlen("message"))
            .add_operation(StringOperation.substr("message", 1, 4))
            .add_operation(StringOperation.find("message", "ll"))
            .execute()
        ).first_or_raise()
        print(
            f"  strlen / substr / find via factories -> "
            f"{result.operation_result(0)}, "
            f"{result.operation_result(1)!r}, "
            f"{result.operation_result(2)}"
        )

        # --- 3) Query: select_from(Exp.string_*) projection into result bins ---
        await self.session.upsert(self.key).bin("message").set_to("hello").execute()

        result = await (
            await self.session.query(self.key)
            .bin("slen").select_from(Exp.string_strlen(Exp.string_bin("message")))
            .bin("stail").select_from(
                Exp.string_substr(Exp.val(3), Exp.string_bin("message"))
            )
            .bin("atLl").select_from(
                Exp.string_find(Exp.val("ll"), Exp.string_bin("message"))
            )
            .execute()
        ).first_or_raise()
        record = result.record_or_raise()
        print(
            f"  slen={record.bins['slen']}, "
            f"stail={record.bins['stail']!r}, "
            f"find(ll)={record.bins['atLl']}"
        )

    async def cleanup(self):
        await self.session.delete(self.key).execute()
        super().cleanup()
