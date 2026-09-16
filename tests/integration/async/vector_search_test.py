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

"""VECTOR distance-expression and Top-K integration tests."""

import pytest
import pytest_asyncio
from aerospike_async import ExpOperation, ExpReadFlags, Operation

from aerospike_sdk import (
    DataSet,
    Exp,
    Order,
    OrderByFlags,
    OrderByType,
    Vector,
    VectorElementType,
)
from aerospike_sdk.exceptions import AerospikeError

NAMESPACE = "test"
SET = "vector_search_psdk"


@pytest_asyncio.fixture(autouse=True)
async def _skip_without_vector_support(supports_vector_bins):
    if not supports_vector_bins:
        pytest.skip("cluster does not support VECTOR and Top-K (requires Server 8.1.3+)")


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as c:
        yield c


@pytest_asyncio.fixture
async def search_session(cluster):
    """Session and clean-key factory."""
    session = cluster.create_session()
    dataset = DataSet.of(NAMESPACE, SET)
    used: list = []

    async def seed(name: str, bins: dict):
        key = dataset.id(name)
        used.append(key)
        await session.delete(key).execute()
        await session.upsert(key).put(bins).execute()
        return key

    yield session, dataset, seed

    for key in used:
        await session.delete(key).execute()


async def _records(stream):
    return [row.record_or_raise() async for row in stream]


class TestScalarTopK:
    async def test_requires_paired_valid_order_by_and_top_k(self, search_session):
        session, dataset, seed = search_session
        await seed("validation", {"id": 1, "score": 1})

        invalid_queries = (
            session.query(dataset).bins(["id"]).top_k(1),
            session.query(dataset)
            .bins(["score"])
            .order_by("score", OrderByType.INTEGER, Order.ASC),
            session.query(dataset)
            .bins(["score"])
            .order_by("score", OrderByType.INTEGER, Order.ASC)
            .top_k(0),
            session.query(dataset)
            .bins(["score"])
            .order_by("score", OrderByType.INTEGER, Order.ASC)
            .top_k(1001),
        )
        for query in invalid_queries:
            with pytest.raises(AerospikeError):
                await query.execute()

    async def test_order_by_bin_absent_from_projection_is_rejected(self, search_session):
        """The order-by bin must be projected."""
        session, dataset, seed = search_session
        await seed("projection", {"id": 1, "score": 1})

        with pytest.raises(AerospikeError):
            await (
                session.query(dataset)
                .bins(["id"])
                .order_by("score", OrderByType.INTEGER, Order.ASC)
                .top_k(1)
                .execute()
            )

    async def test_integer_order_and_k_boundaries(self, search_session):
        session, dataset, seed = search_session
        for value in range(25):
            await seed(f"integer-{value}", {"id": value, "score": value})

        for direction, expected_start in ((Order.ASC, 0), (Order.DESC, 24)):
            for k in (1, 5, 25):
                rows = await _records(await (
                    session.query(dataset)
                    .bins(["id", "score"])
                    .order_by("score", OrderByType.INTEGER, direction)
                    .top_k(k)
                    .execute()
                ))
                expected = (
                    list(range(expected_start, expected_start + k))
                    if direction is Order.ASC
                    else list(range(expected_start, expected_start - k, -1))
                )
                assert [row.bins["id"] for row in rows] == expected

    async def test_k_maximum_is_accepted(self, search_session):
        session, dataset, seed = search_session
        for value in range(3):
            await seed(f"k-max-{value}", {"id": value, "score": value})

        rows = await _records(await (
            session.query(dataset)
            .bins(["id", "score"])
            .order_by("score", OrderByType.INTEGER, Order.ASC)
            .top_k(1000)
            .execute()
        ))
        assert [row.bins["id"] for row in rows] == [0, 1, 2]

    async def test_nan_and_nil_rank_after_finite_values(self, search_session):
        session, dataset, seed = search_session
        values = [1.0, 2.0, 3.0, float("nan"), None]
        for value_id, value in enumerate(values):
            bins = {"id": value_id}
            if value is not None:
                bins["score"] = value
            await seed(f"nan-nil-{value_id}", bins)

        for direction, expected in (
            (Order.ASC, [0, 1, 2, 3, 4]),
            (Order.DESC, [3, 2, 1, 0, 4]),
        ):
            rows = await _records(await (
                session.query(dataset)
                .bins(["id", "score"])
                .order_by("score", OrderByType.DOUBLE, direction)
                .top_k(5)
                .execute()
            ))
            assert [row.bins["id"] for row in rows] == expected

    async def test_bytes_and_ascii_case_insensitive_string_ordering(self, search_session):
        session, dataset, seed = search_session
        for value_id, value in enumerate((b"a", b"ab", b"b")):
            await seed(f"bytes-{value_id}", {"id": value_id, "key": value})
        rows = await _records(await (
            session.query(dataset)
            .bins(["id", "key"])
            .order_by("key", OrderByType.BYTES, Order.ASC)
            .top_k(3)
            .execute()
        ))
        assert [row.bins["id"] for row in rows] == [0, 1, 2]

        for value_id, value in enumerate(("a", "B", "c")):
            await seed(f"string-{value_id}", {"id": value_id, "key": value})
        rows = await _records(await (
            session.query(dataset)
            .bins(["id", "key"])
            .order_by(
                "key",
                OrderByType.STRING,
                Order.ASC,
                OrderByFlags.CASE_INSENSITIVE,
            )
            .top_k(3)
            .execute()
        ))
        assert [row.bins["id"] for row in rows] == [0, 1, 2]

    async def test_wrong_type_and_collections_rank_as_nil(self, search_session):
        session, dataset, seed = search_session
        await seed("nil-integer-1", {"id": "integer-1", "key": 1})
        await seed("nil-integer-2", {"id": "integer-2", "key": 2})
        await seed("nil-string", {"id": "string", "key": "wrong"})
        await seed("nil-list", {"id": "list", "key": [3]})
        await seed("nil-map", {"id": "map", "key": {"n": 4}})
        await seed("nil-missing", {"id": "missing"})

        rows = await _records(await (
            session.query(dataset)
            .bins(["id", "key"])
            .order_by("key", OrderByType.INTEGER, Order.ASC)
            .top_k(6)
            .execute()
        ))
        identifiers = [row.bins["id"] for row in rows]
        assert identifiers[:2] == ["integer-1", "integer-2"]
        assert set(identifiers[2:]) == {"string", "list", "map", "missing"}

    async def test_equal_order_keys_are_deterministic(self, search_session):
        session, dataset, seed = search_session
        for value_id in range(5):
            await seed(f"ties-{value_id}", {"id": value_id, "score": 1})

        async def query_ids():
            rows = await _records(await (
                session.query(dataset)
                .bins(["id", "score"])
                .order_by("score", OrderByType.INTEGER, Order.ASC)
                .top_k(5)
                .execute()
            ))
            return [row.bins["id"] for row in rows]

        assert await query_ids() == await query_ids()


class TestVectorDistanceExpressions:
    @pytest.mark.parametrize(
        "metric, expected",
        [
            ("euclidean_squared_distance", 0.0),
            ("dot_product", 0.30),
            ("cosine_similarity", 1.0),
        ],
    )
    async def test_distance_to_self(self, search_session, metric, expected):
        session, dataset, seed = search_session
        await seed(
            f"self-{metric}",
            {"embedding": Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)},
        )
        distance = getattr(Exp, metric)(
            Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32),
            Exp.vector_bin("embedding"),
        )

        rows = await _records(await (
            session.query(dataset)
            .with_op_projection(ExpOperation.read("distance", distance))
            .execute()
        ))
        assert rows[0].bins["distance"] == pytest.approx(expected, abs=1e-3)

    async def test_metric_filters_keep_the_expected_records(self, search_session):
        session, dataset, seed = search_session
        await seed("near", {"id": "near", "embedding": Vector([1.0, 0.0])})
        await seed("far", {"id": "far", "embedding": Vector([0.0, 1.0])})

        query = Vector([1.0, 0.0])
        predicates = (
            Exp.lt(Exp.euclidean_squared_distance(query, Exp.vector_bin("embedding")), Exp.float_val(1.0)),
            Exp.gt(Exp.dot_product(query, Exp.vector_bin("embedding")), Exp.float_val(0.5)),
            Exp.gt(Exp.cosine_similarity(query, Exp.vector_bin("embedding")), Exp.float_val(0.5)),
        )
        for predicate in predicates:
            rows = await _records(await (
                session.query(dataset).bins(["id"]).filter_expression(predicate).execute()
            ))
            assert [row.bins["id"] for row in rows] == ["near"]

    async def test_incomparable_vectors_return_no_result_with_no_fail(self, search_session):
        session, dataset, seed = search_session
        await seed("wrong-particle", {"id": "wrong-particle", "embedding": 1})
        await seed(
            "wrong-type",
            {"id": "wrong-type", "embedding": Vector([0.1, 0.2], VectorElementType.FLOAT64)},
        )
        await seed(
            "wrong-dimensions",
            {"id": "wrong-dimensions", "embedding": Vector([0.1], VectorElementType.FLOAT32)},
        )
        distance = Exp.euclidean_squared_distance(
            Vector([0.1, 0.2], VectorElementType.FLOAT32),
            Exp.vector_bin("embedding"),
        )

        rows = await _records(await (
            session.query(dataset)
            .with_op_projection(
                Operation.get_bin("id"),
                ExpOperation.read("distance", distance, ExpReadFlags.EVAL_NO_FAIL),
            )
            .execute()
        ))
        assert {row.bins["id"] for row in rows} == {
            "wrong-particle",
            "wrong-type",
            "wrong-dimensions",
        }
        assert all("distance" not in row.bins for row in rows)


class TestVectorTopK:
    async def test_knn_by_projected_squared_euclidean_distance(self, search_session):
        session, dataset, seed = search_session
        for value_id in range(6):
            await seed(
                f"knn-{value_id}",
                {
                    "id": value_id,
                    "embedding": Vector([float(value_id), 0.0], VectorElementType.FLOAT32),
                },
            )

        distance = Exp.euclidean_squared_distance(
            Vector([0.0, 0.0], VectorElementType.FLOAT32),
            Exp.vector_bin("embedding"),
        )
        rows = await _records(await (
            session.query(dataset)
            .with_op_projection(
                Operation.get_bin("id"),
                ExpOperation.read("distance", distance),
            )
            .order_by("distance", OrderByType.DOUBLE, Order.ASC)
            .top_k(3)
            .execute()
        ))
        assert [(row.bins["id"], row.bins["distance"]) for row in rows] == [
            (0, pytest.approx(0.0)),
            (1, pytest.approx(1.0)),
            (2, pytest.approx(4.0)),
        ]
