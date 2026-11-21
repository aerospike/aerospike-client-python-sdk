#!/usr/bin/env python3
"""Demonstrates vector bins and Top-K ranked queries.

Seeds a small product catalog with embeddings, then runs a hybrid query: an
AEL filter on ordinary bins, the cosine similarity to a probe vector projected
into a ``similarity`` bin, and the ten most similar matches, best first.
Requires every node on Aerospike Server 8.2.1 or later.
"""

import asyncio

import _env
from aerospike_sdk import BinDataType, DataSet, Exp, Order, Vector, VectorDistanceMetric

PRODUCTS = DataSet.of("test", "vector-demo")
CATALOG = {
    "sku-1": ("shoes", True, [0.10, 0.90, 0.40]),
    "sku-2": ("shoes", True, [0.80, 0.10, 0.30]),
    "sku-3": ("shoes", False, [0.12, 0.88, 0.47]),
    "sku-4": ("hats", True, [0.11, 0.89, 0.45]),
    "sku-5": ("shoes", True, [0.20, 0.70, 0.60]),
}


async def run_examples(session) -> None:
    # --- 1) Store embeddings as vector bins ---
    print("--- 1) Store embeddings as vector bins ---")
    for sku, (category, in_stock, embedding) in CATALOG.items():
        await session.upsert(PRODUCTS.id(sku)).put({
            "sku": sku,
            "category": category,
            "in_stock": in_stock,
            "embedding": Vector(embedding),
        }).execute()
    print(f"stored {len(CATALOG)} products")

    # --- 2) Hybrid Top-K: filter, project similarity, keep the best ten ---
    print("--- 2) Hybrid Top-K: filter, project similarity, keep the best ten ---")
    probe = Vector([0.12, 0.88, 0.47])
    similarity = Exp.vector_distance(
        VectorDistanceMetric.COSINE_SIMILARITY, probe, Exp.vector_bin("embedding"),
    )
    stream = await (
        session.query(PRODUCTS)
        .where("$.category == 'shoes' and $.in_stock == true")
        .bin("sku").get()
        .bin("similarity").select_from(similarity, ignore_eval_failure=True)
        .order_by("similarity", BinDataType.DOUBLE, Order.DESC)
        .top_k(10)
        .execute()
    )
    async for row in stream:
        bins = row.record_or_raise().bins
        print(f"{bins['sku']}: {bins['similarity']:.4f}")


async def main() -> None:
    async with _env.connect().connect() as cluster:
        if not (cluster.supports_vector() and cluster.supports_query_top_k()):
            print("Skipping: vectors and Top-K need Aerospike Server 8.2.1 or later")
            return
        session = cluster.create_session()
        try:
            await run_examples(session)
        finally:
            for sku in CATALOG:
                await session.delete(PRODUCTS.id(sku)).execute()


if __name__ == "__main__":
    asyncio.run(main())
