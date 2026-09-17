#!/usr/bin/env python3
"""CDT path select / modify over nested collections.

Reads and rewrites deep inside lists and maps in a single server operation. The
path is a list of ``CTX`` steps — ``CTX.all_children()`` walks every element at a
level, ``CTX.all_children_with_filter(pred)`` keeps only the matches, and a
``pred`` is an :class:`~aerospike_sdk.Exp` over the current element's *loop
variable*. The path is handed to a ``CdtOperation.select_by_path`` /
``modify_by_path`` / ``remove`` factory and added to an ordinary query or update,
or to ``Exp.exp_select_by_path`` for the expression-read form.

These are the low-level factories; PSDK does not yet expose a fluent path builder
(``.on_each_child().modify_by(...)``), so the ``CTX`` list is spelled out.

If the cluster does not support CDT path operations, this example prints a
skip message and exits.
"""

import asyncio

import _env
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

DEMO = DataSet.of("test", "cdt_path_demo")

CATALOG = {
    "book": [
        {"title": "Sayings of the Century", "price": 8.95},
        {"title": "Sword of Honour", "price": 12.99},
        {"title": "Moby Dick", "price": 8.99},
        {"title": "The Lord of the Rings", "price": 22.99},
    ]
}


async def run_examples(session) -> None:
    all_ok = True
    try:
        if not await _env.server_at_least(session, (8, 1, 1)):
            print("Skipped: this cluster does not support CDT path operations.")
            return

        # --- 1) Bin-root list: modify every element (add 10 to each) ---
        print("--- 1) Bin-root list: modify every element (add 10 to each) ---")
        k1 = DEMO.id(1)
        await session.delete(k1).execute()
        nums = [1, 2, 3]
        await session.upsert(k1).bin("nums").set_to(nums).execute()
        print(f"initial nums (before +10 to each): {nums}")
        add_10 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10)])
        await session.update(k1).add_operation(
            CdtOperation.modify_by_path("nums", ModifyFlags.DEFAULT, add_10, [CTX.all_children()])
        ).execute()
        nums = (await _bins(session, k1))["nums"]
        print(f"nums after +10 to each: {nums}")
        all_ok &= _report_check(1, nums == [11, 12, 13])

        # --- 2) Bin-root list: remove elements matching a predicate (value > 5) ---
        print("\n--- 2) Bin-root list: remove elements matching a predicate (value > 5) ---")
        k2 = DEMO.id(2)
        await session.delete(k2).execute()
        nums = [3, 7, 2, 9]
        await session.upsert(k2).bin("nums").set_to(nums).execute()
        print(f"initial nums (before removing values > 5): {nums}")
        over_5 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(5))
        await session.update(k2).add_operation(
            CdtOperation.remove("nums", [CTX.all_children_with_filter(over_5)])
        ).execute()
        nums = (await _bins(session, k2))["nums"]
        print(f"nums after removing values > 5: {nums}")
        all_ok &= _report_check(2, nums == [3, 2])

        # --- 3) Nested map/list: collect titles of books priced <= 10 ---
        print("\n--- 3) Nested map/list: collect titles of books priced <= 10 ---")
        k3 = DEMO.id(3)
        await session.delete(k3).execute()
        await session.upsert(k3).bin("catalog").set_to(CATALOG).execute()
        print(f"initial catalog (before collecting cheap-book titles): {CATALOG}")
        cheap_titles = CdtOperation.select_by_path(
            "catalog", SelectFlags.VALUE,
            [CTX.map_key("book"), CTX.all_children_with_filter(_price_at_most(10.0)),
             CTX.map_key("title")],
        )
        result = await (await session.query(k3).add_operation(cheap_titles).execute()).first_or_raise()
        titles = result.record.bins["catalog"]
        print(f"titles priced <= 10 (projection bin 'catalog'): {titles}")
        all_ok &= _report_check(3, set(titles) == {"Sayings of the Century", "Moby Dick"})

        # --- 4) Nested: multiply every book price by 1.10 (modify each) ---
        print("\n--- 4) Nested: multiply every book price by 1.10 (modify each) ---")
        k4 = DEMO.id(4)
        await session.delete(k4).execute()
        await session.upsert(k4).bin("catalog").set_to(CATALOG).execute()
        print(f"initial catalog (before 1.10x on each price): {CATALOG}")
        bump = Exp.num_mul([Exp.float_loop_var(LoopVarPart.VALUE), Exp.val(1.10)])
        await session.update(k4).add_operation(
            CdtOperation.modify_by_path(
                "catalog", ModifyFlags.DEFAULT, bump,
                [CTX.map_key("book"), CTX.all_children(), CTX.map_key("price")],
            )
        ).execute()
        catalog = (await _bins(session, k4))["catalog"]
        print(f"catalog after 10% price bump: {_rendered_books(catalog)}")
        original_prices = [8.95, 12.99, 8.99, 22.99]
        all_ok &= _report_check(4, _prices_match(catalog, original_prices, 1.10, 0.02))

        # --- 5) Expression read: project all titles into a result bin ---
        print("\n--- 5) Expression read: project all titles into a result bin ---")
        k5 = DEMO.id(5)
        await session.delete(k5).execute()
        await session.upsert(k5).bin("catalog").set_to(CATALOG).execute()
        print(f"initial catalog (before expression read of all titles): {CATALOG}")
        # Unlike section 3's select_by_path *operation*, this is a read
        # *expression* over the bin, evaluated by select_from.
        all_titles = Exp.exp_select_by_path(
            ExpType.LIST, SelectFlags.VALUE, Exp.map_bin("catalog"),
            [CTX.map_key("book"), CTX.all_children(), CTX.map_key("title")],
        )
        result = await (
            await session.query(k5).bin("catalog").select_from(all_titles).execute()
        ).first_or_raise()
        titles = result.record.bins["catalog"]
        print(f"expression read of all titles (projection bin 'catalog'): {titles}")
        all_ok &= _report_check(5, set(titles) == {
            "Sayings of the Century", "Sword of Honour", "Moby Dick", "The Lord of the Rings",
        })

        if not all_ok:
            raise AssertionError("One or more CDT path expression checks failed")
        print("\nOverall: SUCCESS")

    finally:
        for pk in range(1, 6):
            await session.delete(DEMO.id(pk)).execute()


def _report_check(step: int, ok: bool) -> bool:
    print(f"Step {step}: {'SUCCESS' if ok else '*** FAILURE ***'}")
    return ok


def _prices_match(catalog: dict, original_prices: list[float],
                  factor: float, epsilon: float) -> bool:
    """Every book's price is its original price times ``factor``, within ``epsilon``."""
    books = catalog.get("book")
    if not isinstance(books, list) or len(books) != len(original_prices):
        return False
    return all(
        abs(book["price"] - original * factor) <= epsilon
        for book, original in zip(books, original_prices)
    )


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


async def _bins(session, key) -> dict:
    return (await (await session.query(key).execute()).first_or_raise()).record.bins


def _rendered_books(catalog: dict) -> list[dict]:
    """Books with prices rounded, so float noise stays out of the output."""
    return [{"title": b["title"], "price": round(b["price"], 2)} for b in catalog["book"]]


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session()

        await run_examples(session)


if __name__ == "__main__":
    asyncio.run(main())
