#!/usr/bin/env python3
"""Vector bins and Top-K similarity search (Server 8.1.3+)."""

import asyncio

import _env
from aerospike_async import ExpOperation, ExpReadFlags

from aerospike_sdk import Behavior, DataSet, Order, OrderByType, QueryHint, Vector
from aerospike_sdk.exp import Exp


def embed(_text: str) -> list[float]:
    """Stand-in for a real embedding model call."""
    return [0.10, 0.95, 0.40, 0.02]


async def write_products(session, products: DataSet) -> None:
    """Write product records with vector embeddings."""
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
    """Rank products by cosine similarity to a query vector."""
    query_vector = Vector(embed("running shoes for marathons"))

    async with await (
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
    ) as stream:
        async for row in stream:
            print(row.record.bins)


async def hybrid_search_example(session, products: DataSet) -> None:
    """Filter to a category first, then rank its products by similarity."""
    query_vector = Vector(embed("running shoes for marathons"))

    async with await (
        session.query(products)
        .where("$.category == 'footwear'")
        # Permit a primary-index scan because this example defines no index.
        .with_hint(QueryHint(allow_scans_with_where=True))
        .with_op_projection(
            ExpOperation.read(
                "similarity",
                Exp.cosine_similarity(query_vector, Exp.vector_bin("embedding")),
                ExpReadFlags.DEFAULT,
            ))
        .order_by("similarity", OrderByType.DOUBLE, Order.DESC)
        .top_k(10)
        .execute()
    ) as stream:
        async for row in stream:
            print(row.record.bins)


async def main() -> None:
    cluster = await _env.connect().connect()
    session = cluster.create_session(Behavior.DEFAULT)
    products = DataSet.of("test", "products")

    try:
        await write_products(session, products)
        await vector_topk_example(session, products)
        await hybrid_search_example(session, products)
    finally:
        await cluster.close()


if __name__ == "__main__":
    asyncio.run(main())
