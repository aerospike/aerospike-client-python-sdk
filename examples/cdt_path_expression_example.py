#!/usr/bin/env python3
"""CDT path select / modify over nested collections (server 8.1.1+).

Reads and rewrites deep inside lists and maps in a single server operation. The
path is a list of ``CTX`` steps — ``CTX.all_children()`` walks every element at a
level, ``CTX.all_children_with_filter(pred)`` keeps only the matches, and a
``pred`` is an :class:`~aerospike_sdk.Exp` over the current element's *loop
variable*. The path is handed to a ``CdtOperation.select_by_path`` /
``modify_by_path`` / ``remove`` factory and added to an ordinary query or update.

These are the low-level factories; PSDK does not yet expose a fluent path builder
(``.on_each_child().modify_by(...)``), so the ``CTX`` list is spelled out.

If the cluster is older than 8.1.1, this example prints a skip message and exits.
"""

import _env
from _env import Example
from aerospike_sdk import (
    CTX,
    CdtOperation,
    DataSet,
    Exp,
    ExpType,
    LoopVarPart,
    MapReturnType,
    ModifyFlags,
    SelectFlags,
)

CATALOG = {
    "book": [
        {"title": "Sayings of the Century", "price": 8.95},
        {"title": "Sword of Honour", "price": 12.99},
        {"title": "Moby Dick", "price": 8.99},
        {"title": "The Lord of the Rings", "price": 22.99},
    ]
}


def _price_at_most(limit: float) -> Exp:
    """A predicate: the current book's ``price`` is <= ``limit``.

    The book is the map loop variable at this path level; pull its ``price``
    value out and compare.
    """
    price = Exp.map_get_by_key(
        MapReturnType.VALUE, ExpType.FLOAT,
        Exp.val("price"), Exp.map_loop_var(LoopVarPart.VALUE), [],
    )
    return Exp.le(price, Exp.val(limit))


class CdtPathExpressionExample(Example):
    demo = DataSet.of("test", "cdt_path_demo")

    async def __init__(self):
        await super().__init__()
        if not await _env.server_at_least(self.session, (8, 1, 1)):
            print("Skipped: CDT path operations require Aerospike 8.1.1+.")
            self._skipped = True

    async def _bins(self, key) -> dict:
        return (await (await self.session.query(key).execute()).first_or_raise()).record.bins


class CdtPathModifyElements(CdtPathExpressionExample):
    async def run(self) -> None:
        # --- 1) Bin-root list: modify every element (add 10 to each) ---
        k1 = self.demo.id(1)
        await self.session.delete(k1).execute()
        await self.session.upsert(k1).bin("nums").set_to([1, 2, 3]).execute()
        add_10 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10)])
        await self.session.update(k1).add_operation(
            CdtOperation.modify_by_path("nums", ModifyFlags.DEFAULT, add_10, [CTX.all_children()])
        ).execute()
        print(f"1) +10 to each element -> {(await self._bins(k1))['nums']}")


class CdtPathRemoveElements(CdtPathExpressionExample):
    async def run(self) -> None:
        # --- 2) Bin-root list: remove elements matching a predicate (value > 5) ---
        k2 = self.demo.id(2)
        await self.session.delete(k2).execute()
        await self.session.upsert(k2).bin("nums").set_to([3, 7, 2, 9]).execute()
        over_5 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(5))
        await self.session.update(k2).add_operation(
            CdtOperation.remove("nums", [CTX.all_children_with_filter(over_5)])
        ).execute()
        print(f"2) remove values > 5 -> {(await self._bins(k2))['nums']}")


class CdtPathCheapTitles(CdtPathExpressionExample):
    async def run(self) -> None:
        # --- 3) Nested map/list: collect titles of books priced <= 10 ---
        k3 = self.demo.id(3)
        await self.session.delete(k3).execute()
        await self.session.upsert(k3).bin("catalog").set_to(CATALOG).execute()
        cheap_titles = CdtOperation.select_by_path(
            "catalog", SelectFlags.VALUE,
            [CTX.map_key("book"), CTX.all_children_with_filter(_price_at_most(10.0)),
             CTX.map_key("title")],
        )
        result = await (
            await self.session.query(k3).add_operation(cheap_titles).execute()
        ).first_or_raise()
        print(f"3) titles priced <= 10 -> {result.record.bins['catalog']}")


class CdtPathBumpPrices(CdtPathExpressionExample):
    async def run(self) -> None:
        k3 = self.demo.id(3)
        await self.session.delete(k3).execute()
        await self.session.upsert(k3).bin("catalog").set_to(CATALOG).execute()

        # --- 4) Nested: multiply every book price by 1.10 (modify each) ---
        bump = Exp.num_mul([Exp.float_loop_var(LoopVarPart.VALUE), Exp.val(1.10)])
        await self.session.update(k3).add_operation(
            CdtOperation.modify_by_path(
                "catalog", ModifyFlags.DEFAULT, bump,
                [CTX.map_key("book"), CTX.all_children(), CTX.map_key("price")],
            )
        ).execute()
        prices = [round(b["price"], 2) for b in (await self._bins(k3))["catalog"]["book"]]
        print(f"4) prices after 10% bump -> {prices}")


class CdtPathAllTitles(CdtPathExpressionExample):
    async def run(self) -> None:
        k3 = self.demo.id(3)
        await self.session.delete(k3).execute()
        await self.session.upsert(k3).bin("catalog").set_to(CATALOG).execute()

        # --- 5) Expression read: project all titles into a result bin ---
        all_titles = CdtOperation.select_by_path(
            "catalog", SelectFlags.VALUE,
            [CTX.map_key("book"), CTX.all_children(), CTX.map_key("title")],
        )
        result = await (
            await self.session.query(k3).add_operation(all_titles).execute()
        ).first_or_raise()
        print(f"5) all titles -> {result.record.bins['catalog']}")
