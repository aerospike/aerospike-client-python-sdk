#!/usr/bin/env python3
"""Vector bins + Top-K ("ORDER BY <bin> LIMIT k") hybrid search PREVIEW.

Two parts:

1. Writing/reading ``Vector`` bins -- FULLY WORKING. A Vector round-trips
   through put/get like any other bin value (see ``write_products``).

2. Vector *similarity search* -- NOT YET SUPPORTED SERVER-SIDE. Projecting a
   distance expression (``Exp.cosine_similarity`` etc.) and ranking with
   ``.order_by(...)`` / ``.top_k(...)`` is a preview of the intended fluent
   API. Scalar Top-K works on the current dev server, but the vector case does
   not because evaluating **any** expression over a VECTOR bin is currently a
   server crash:

     * ``rt_bin_translate`` has no ``AS_PARTICLE_TYPE_VECTOR`` case, so
       ``Exp.vector_bin`` (and consequently a distance expression, a filter,
       or ``bin_exists``) falls through to ``cf_crash`` and aborts ``asd``.

   The search functions below are shown for API illustration and are
   deliberately **not executed** by ``main()``. They remain ordinary TODO/WIP
   examples until a server build fixes VECTOR expression evaluation.
"""

import asyncio

import _env
from aerospike_async import ExpOperation, ExpReadFlags, Vector
from aerospike_sdk import Behavior, DataSet, Order, OrderByType
from aerospike_sdk.exp import Exp


def embed(_text: str) -> list[float]:
    """Stand-in for a real embedding model call."""
    return [0.10, 0.95, 0.40, 0.02]


async def write_products(session, products: DataSet) -> None:
    """Milestone 1: a Vector bin round-trips through put/get like any other
    bin value — no new write/read methods needed."""
    await session.upsert(products.id("sku-1")).put({
        "name": "wireless mouse",
        "category": "electronics",
        "embedding": Vector([0.12, 0.98, 0.44, 0.05]),
    }).execute()
    await session.upsert(products.id("sku-2")).put({
        "name": "running shoes",
        "category": "footwear",
        "embedding": Vector([0.55, 0.20, 0.90, 0.60]),
    }).execute()
    print("Wrote product records with vector embeddings")


async def vector_topk_example(session, products: DataSet) -> None:
    """PREVIEW (not yet supported server-side). Rank every record in the set by
    cosine similarity to a query vector, keeping the top 10.

    `Exp.cosine_similarity(query, bin)` projects a distance value into the
    "similarity" bin via `.with_op_projection(...)` — the query op-projection
    mechanism the server applies per matching record. `.order_by(...)` names
    that projected bin as the Top-K order key; larger cosine similarity means
    "more similar", hence `Order.DESC`.
    """
    query_vector = Vector(embed("running shoes for marathons"))

    stream = await (
        session.query(products)
        .with_op_projection(
            ExpOperation.read(
                "similarity",
                Exp.cosine_similarity(query_vector, Exp.vector_bin("embedding")),
                ExpReadFlags.DEFAULT,
            ))
        .order_by("similarity", OrderByType.DOUBLE, Order.DESC)
        .top_k(10)
        .execute()
    )
    async for row in stream:
        print(row.record.bins)  # would include the projected "similarity" bin
    stream.close()


async def hybrid_search_example(session, products: DataSet) -> None:
    """PREVIEW (not yet supported server-side). Filter to a category first, then
    Top-K-rank only the records that pass. Top-K would compose with
    `.where(...)` the same way non-vector queries do."""
    query_vector = Vector(embed("running shoes for marathons"))

    stream = await (
        session.query(products)
        .where("$.category == 'footwear'")
        .with_op_projection(
            ExpOperation.read(
                "similarity",
                Exp.cosine_similarity(query_vector, Exp.vector_bin("embedding")),
                ExpReadFlags.DEFAULT,
            ))
        .order_by("similarity", OrderByType.DOUBLE, Order.DESC)
        .top_k(10)
        .execute()
    )
    async for row in stream:
        print(row.record.bins)
    stream.close()


async def main() -> None:
    cluster = await _env.connect().connect()
    session = cluster.create_session(Behavior.DEFAULT)
    products = DataSet.of("test", "products")

    try:
        # Part 1: writing/reading Vector bins — fully working.
        await write_products(session, products)

        # TODO(vector-expression-support): Re-enable these normal examples
        # once the server handles AS_PARTICLE_TYPE_VECTOR in rt_bin_translate.
        # They are intentionally commented out: executing either currently
        # crashes the server, rather than returning a regular query error.
        #
        # await vector_topk_example(session, products)
        # await hybrid_search_example(session, products)
        print("\nVector search examples are TODO/WIP: VECTOR expressions currently crash the server.")
    finally:
        await cluster.close()


if __name__ == "__main__":
    asyncio.run(main())
