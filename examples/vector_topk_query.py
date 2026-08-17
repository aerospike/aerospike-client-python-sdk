#!/usr/bin/env python3
"""Vector bins + Top-K ("ORDER BY <bin> LIMIT k") hybrid search example,
using cosine similarity as the distance metric.

Covers: writing a Vector bin like any other bin value, projecting a
vector-distance expression into a named bin via `.select_from(...)`, and
ranking/limiting the result with `.order_by(...)` / `.top_k(...)`.

WORK IN PROGRESS: Top-K's wire encode is capability-gated in the underlying
native client and has no assigned minimum server version yet, so any query
below that sets `.order_by(...)`/`.top_k(...)` currently fails fast
client-side with a ValueError, regardless of the server it targets. Kept
here so the fluent API surface (which is fully implemented) is documented
and easy to try again once the server-side capability lands.
"""

import asyncio

import _env
from aerospike_async import Vector
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
    """Rank every record in the set by cosine similarity to a query vector,
    keeping the top 10.

    `Exp.cosine_similarity(query, bin)` projects a distance value into the
    "similarity" bin via `.bin(name).select_from(expr)` — the same
    projection mechanism used for any other computed read. `.order_by(...)`
    names that projected bin as the Top-K order key; larger cosine
    similarity means "more similar", hence `Order.DESC`.
    """
    query_vector = Vector(embed("running shoes for marathons"))

    stream = await (
        session.query(products)
        .bin("similarity").select_from(
            Exp.cosine_similarity(query_vector, Exp.vector_bin("embedding")))
        .order_by("similarity", OrderByType.DOUBLE, Order.DESC)
        .top_k(10)
        .execute()
    )
    async for row in stream:
        print(row.record.bins)  # includes the projected "similarity" bin
    stream.close()


async def hybrid_search_example(session, products: DataSet) -> None:
    """Filter to a category first, then Top-K-rank only the records that
    pass. Top-K composes with `.where(...)` the same way non-vector queries
    do — nothing extra needed here."""
    query_vector = Vector(embed("running shoes for marathons"))

    stream = await (
        session.query(products)
        .where("$.category == 'footwear'")
        .bin("similarity").select_from(
            Exp.cosine_similarity(query_vector, Exp.vector_bin("embedding")))
        .bins(["name", "category", "similarity"])
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
        await write_products(session, products)

        print("--- Top-K by cosine_similarity ---")
        try:
            await vector_topk_example(session, products)
        except Exception as e:
            print(f"Query failed (expected until the server-side Top-K capability gate is set): {e}")

        print("\n--- Hybrid search (category filter + Top-K by cosine_similarity) ---")
        try:
            await hybrid_search_example(session, products)
        except Exception as e:
            print(f"Query failed (expected until the server-side Top-K capability gate is set): {e}")
    finally:
        await cluster.close()


if __name__ == "__main__":
    asyncio.run(main())
