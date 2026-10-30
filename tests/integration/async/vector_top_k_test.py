# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Integration tests for vector bins and Top-K ranked dataset queries."""

import pytest
import pytest_asyncio

from aerospike_sdk import (
    BinDataType,
    DataSet,
    Exp,
    OperationResult,
    Order,
    ResultCode,
    Vector,
    VectorDistanceMetric,
    VectorElementType,
)
from aerospike_sdk.exceptions import AerospikeError
from tests.integration.namespace import general_namespace
from tests.pnc_compat import requires_query_selection, requires_query_top_k, requires_vector

_DS = DataSet.of(general_namespace(), "vector_top_k")
_PROBE = Vector([1.0, 0.0])
# Record i points at angle i/10 from the probe and costs 10 * (12 - i), so the
# most similar record is also the most expensive one. Four records share each
# tier, so ranking on tier alone ties.
_PRODUCTS = {
    i: {
        "embedding": Vector([1.0, i / 10]),
        "category": "shoes" if i % 2 else "hats",
        "price": 10.0 * (12 - i),
        "tier": i // 4,
    }
    for i in range(12)
}


def _similarity() -> object:
    return Exp.vector_distance(
        VectorDistanceMetric.COSINE_SIMILARITY, _PROBE, Exp.vector_bin("embedding"),
    )


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def session(aerospike_host, make_cluster_definition):
    async with make_cluster_definition(aerospike_host).connect() as cluster:
        s = cluster.create_session()
        for i, bins in _PRODUCTS.items():
            await s.upsert(_DS.id(i)).put(bins).execute()
        yield s
        for i in _PRODUCTS:
            await s.delete(_DS.id(i)).execute()


async def _bins(stream, name: str) -> list:
    return [row.record_or_raise().bins[name] async for row in stream]


@requires_vector
class TestVectorBins:

    @pytest.mark.parametrize(
        "element_type, values",
        [
            (VectorElementType.FLOAT16, [0.5, -2.0, 1024.0]),
            (VectorElementType.FLOAT32, [0.5, -1.25, 3.0]),
            (VectorElementType.FLOAT64, [0.1, -2.000001, 1e300]),
            (VectorElementType.INT32, [7, -3, 2147483647]),
        ],
        ids=["float16", "float32", "float64", "int32"],
    )
    async def test_round_trip_keeps_values_and_element_type(self, session, element_type, values):
        key = _DS.id(f"round_trip_{element_type}")
        written = Vector(values, element_type)
        await session.upsert(key).bin("embedding").set_to(written).execute()
        try:
            stream = await session.query(key).execute()
            read = OperationResult(
                (await stream.first_or_raise()).record_or_raise().bins["embedding"],
            ).get_vector()
            assert read == written
            assert read.element_type == element_type
        finally:
            await session.delete(key).execute()

    async def test_distance_projection_on_a_point_read(self, session):
        stream = await (
            session.query(_DS.id(0))
            .bin("similarity").select_from(_similarity())
            .execute()
        )
        record = (await stream.first_or_raise()).record_or_raise()
        assert record.bins == {"similarity": pytest.approx(1.0)}

    async def test_distance_in_a_filter(self, session):
        stream = await (
            session.query(_DS)
            .where(Exp.ge(_similarity(), Exp.float_val(0.99)))
            .execute()
        )
        assert sorted(await _bins(stream, "price")) == [110.0, 120.0]


@requires_query_top_k
class TestTopK:

    async def test_plain_query_returns_the_k_cheapest_in_order(self, session):
        stream = await (
            session.query(_DS)
            .order_by("price", BinDataType.DOUBLE, Order.ASC)
            .top_k(3)
            .execute()
        )
        assert await _bins(stream, "price") == [10.0, 20.0, 30.0]

    async def test_second_key_breaks_ties(self, session):
        stream = await (
            session.query(_DS)
            .order_by("tier", BinDataType.INTEGER, Order.DESC)
            .then_order_by("price", BinDataType.DOUBLE, Order.ASC)
            .top_k(3)
            .execute()
        )
        rows = [row.record_or_raise().bins async for row in stream]
        assert [(r["tier"], r["price"]) for r in rows] == [(2, 10.0), (2, 20.0), (2, 30.0)]

    @pytest.mark.parametrize("order", [Order.ASC, Order.DESC], ids=["asc", "desc"])
    async def test_record_missing_the_key_ranks_last(self, session, order):
        key = _DS.id("no_price")
        await session.upsert(key).put({"category": "hats"}).execute()
        try:
            stream = await (
                session.query(_DS)
                .order_by("price", BinDataType.DOUBLE, order)
                .top_k(len(_PRODUCTS) + 1)
                .execute()
            )
            rows = [row.record_or_raise().bins async for row in stream]
            assert "price" not in rows[-1]
            assert all("price" in r for r in rows[:-1])
        finally:
            await session.delete(key).execute()

    @requires_query_selection
    async def test_string_where_ranks_only_matching_records(self, session):
        stream = await (
            session.query(_DS)
            .where("$.category == 'hats'")
            .order_by("price", BinDataType.DOUBLE, Order.DESC)
            .top_k(2)
            .execute()
        )
        assert await _bins(stream, "price") == [120.0, 100.0]

    @requires_vector
    async def test_ranks_by_projected_similarity(self, session):
        stream = await (
            session.query(_DS)
            .bin("similarity").select_from(_similarity(), ignore_eval_failure=True)
            .order_by("similarity", BinDataType.DOUBLE, Order.DESC)
            .top_k(3)
            .execute()
        )
        similarities = await _bins(stream, "similarity")
        assert similarities == sorted(similarities, reverse=True)
        assert similarities[0] == pytest.approx(1.0)
        assert len(similarities) == 3

    async def test_two_keys_on_one_bin_are_refused(self, session):
        with pytest.raises(AerospikeError) as exc_info:
            await (
                session.query(_DS)
                .order_by("price", BinDataType.DOUBLE, Order.ASC)
                .then_order_by("price", BinDataType.DOUBLE, Order.DESC)
                .top_k(3)
                .execute()
            )
        assert exc_info.value.result_code == ResultCode.PARAMETER_ERROR
        assert "different bins" in str(exc_info.value)
