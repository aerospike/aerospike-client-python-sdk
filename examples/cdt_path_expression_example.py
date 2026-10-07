#!/usr/bin/env python3
"""CDT path select / modify over nested collections.

Reads and rewrites deep inside lists and maps in a single server operation. A
path starts at a bin and steps down a level at a time: ``on_map_key`` picks one
entry, ``on_each_child()`` walks every element at a level, and
``on_each_child_where(pred)`` keeps only the elements a predicate accepts. A
``pred`` is an :class:`~aerospike_sdk.Exp` over the current element's *loop
variable*. The path ends in a terminal: ``modify_by(expr)`` rewrites each
selected element, ``remove_matches()`` drops them, and ``collect_values()``
reads them back.

Section 5 reads a path as an *expression* with
``collect_values_as_expression_read``, and
section 6 shows the explicit ``CTX`` list the path builder emits. See
``docs/guide/cdt-operations.md``.
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
    try:
        # --- 1) Bin-root list: on_each_child + modify_by (add 10 to each element) ---
        print("--- 1) Bin-root list: on_each_child + modify_by (add 10 to each element) ---")
        k1 = DEMO.id(1)
        await session.delete(k1).execute()
        nums = [1, 2, 3]
        await session.upsert(k1).bin("nums").set_to(nums).execute()
        print(f"initial nums (before +10 to each): {nums}")
        add_10 = Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10)])
        await session.update(k1).bin("nums").on_each_child().modify_by(add_10).execute()
        print(f"nums after +10 to each: {(await _bins(session, k1))['nums']}")

        # --- 2) Bin-root list: on_each_child_where + remove_matches (drop values > 5) ---
        print("\n--- 2) Bin-root list: on_each_child_where + remove_matches (drop values > 5) ---")
        k2 = DEMO.id(2)
        await session.delete(k2).execute()
        nums = [3, 7, 2, 9]
        await session.upsert(k2).bin("nums").set_to(nums).execute()
        print(f"initial nums (before removing values > 5): {nums}")
        over_5 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(5))
        await session.update(k2).bin("nums").on_each_child_where(over_5).remove_matches().execute()
        print(f"nums after removing values > 5: {(await _bins(session, k2))['nums']}")

        # --- 3) Nested map/list: collect_values for titles of books priced <= 10 ---
        print("\n--- 3) Nested map/list: collect_values for titles of books priced <= 10 ---")
        k3 = DEMO.id(3)
        await session.delete(k3).execute()
        await session.upsert(k3).bin("catalog").set_to(CATALOG).execute()
        print(f"initial catalog (before collecting cheap-book titles): {CATALOG}")
        result = await (
            await session.query(k3)
            .bin("catalog")
            .on_map_key("book")
            .on_each_child_where(_price_at_most(10.0))
            .on_map_key("title")
            .collect_values()
            .execute()
        ).first_or_raise()
        print(f"titles priced <= 10 (projection bin 'catalog'): {result.record.bins['catalog']}")

        # --- 4) Nested map/list: modify_by to multiply every book price by 1.10 ---
        print("\n--- 4) Nested map/list: modify_by to multiply every book price by 1.10 ---")
        k4 = DEMO.id(4)
        await session.delete(k4).execute()
        await session.upsert(k4).bin("catalog").set_to(CATALOG).execute()
        print(f"initial catalog (before 1.10x on each price): {CATALOG}")
        bump = Exp.num_mul([Exp.float_loop_var(LoopVarPart.VALUE), Exp.val(1.10)])
        await (
            session.update(k4)
            .bin("catalog")
            .on_map_key("book")
            .on_each_child()
            .on_map_key("price")
            .modify_by(bump)
            .execute()
        )
        catalog = (await _bins(session, k4))["catalog"]
        print(f"catalog after 10% price bump: {_rendered_books(catalog)}")

        # --- 5) Expression read: project all titles into a result bin ---
        print("\n--- 5) Expression read: project all titles into a result bin ---")
        k5 = DEMO.id(5)
        await session.delete(k5).execute()
        await session.upsert(k5).bin("catalog").set_to(CATALOG).execute()
        print(f"initial catalog (before expression read of all titles): {CATALOG}")
        # Unlike section 3's collect_values() *operation*, this is a read
        # *expression* over the bin; ExpType.MAP is the type of "catalog".
        result = await (
            await session.query(k5)
            .bin("catalog")
            .on_map_key("book")
            .on_each_child()
            .on_map_key("title")
            .collect_values_as_expression_read(ExpType.MAP)
            .execute()
        ).first_or_raise()
        print(f"expression read of all titles (projection bin 'catalog'): "
              f"{result.record.bins['catalog']}")

        # --- 6) Keys-in with a filter: the explicit CTX form and the path builder ---
        print("\n--- 6) Keys-in with a filter: the explicit CTX form and the path builder ---")
        k6 = DEMO.id(6)
        await session.delete(k6).execute()
        stock = {"apples": 5, "pears": 15, "plums": 25, "figs": 35}
        await session.upsert(k6).bin("stock").set_to(stock).execute()
        print(f"initial stock: {stock}")
        over_10 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10))
        # CTX.map_keys_in picks the entries; CTX.and_filter keeps those over 10.
        # The filter refines the step before it and cannot follow an
        # all-children step or another filter.
        chosen = CdtOperation.select_by_path(
            "stock", SelectFlags.MAP_KEY_VALUE,
            [CTX.map_keys_in(["apples", "pears", "plums"]), CTX.and_filter(over_10)],
        )
        result = await (await session.query(k6).add_operation(chosen).execute()).first_or_raise()
        print(f"explicit CTX form (projection bin 'stock'): {_entries(result.record.bins['stock'])}")
        # The path builder emits the same operation.
        result = await (
            await session.query(k6)
            .bin("stock").on_map_keys_in(["apples", "pears", "plums"]).and_filter(over_10)
            .collect_map_entries()
            .execute()
        ).first_or_raise()
        print(f"path builder form (projection bin 'stock'): {_entries(result.record.bins['stock'])}")

    finally:
        for pk in range(1, 7):
            await session.delete(DEMO.id(pk)).execute()


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


def _entries(flat: list) -> dict:
    """Map entries come back as a flat key, value, key, value list."""
    return dict(zip(flat[::2], flat[1::2]))


def _rendered_books(catalog: dict) -> list[dict]:
    """Books with prices rounded, so float noise stays out of the output."""
    return [{"title": b["title"], "price": round(b["price"], 2)} for b in catalog["book"]]


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session()

        await run_examples(session)


if __name__ == "__main__":
    asyncio.run(main())
