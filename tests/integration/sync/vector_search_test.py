# Copyright 2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
# You may not use this file except in compliance with the Apache License,
# Version 2.0. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

"""Synchronous-facade VECTOR distance-expression and Top-K integration tests."""

import pytest
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
SET = "vector_search_psdk_sync"


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as connected:
        version = connected.server_version()
        if version is None or (version.major, version.minor, version.patch) < (8, 1, 3):
            pytest.skip("cluster does not support VECTOR and Top-K (requires Server 8.1.3+)")
        yield connected


@pytest.fixture
def search_session(cluster):
    session = cluster.create_session()
    dataset = DataSet.of(NAMESPACE, SET)
    used: list = []

    def seed(name: str, bins: dict):
        key = dataset.id(name)
        used.append(key)
        session.delete(key).execute()
        session.upsert(key).put(bins).execute()
        return key

    yield session, dataset, seed

    for key in used:
        session.delete(key).execute()


def _records(stream):
    return [row.record_or_raise() for row in stream]


class TestSyncScalarTopK:
    def test_invalid_top_k_queries_are_rejected(self, search_session):
        session, dataset, seed = search_session
        seed("validation", {"id": 1, "score": 1})

        invalid_queries = (
            # top_k without order_by
            session.query(dataset).bins(["id"]).top_k(1),
            # order_by without top_k
            session.query(dataset)
            .bins(["score"])
            .order_by("score", OrderByType.INTEGER, Order.ASC),
            # k below range
            session.query(dataset)
            .bins(["score"])
            .order_by("score", OrderByType.INTEGER, Order.ASC)
            .top_k(0),
            # k above range
            session.query(dataset)
            .bins(["score"])
            .order_by("score", OrderByType.INTEGER, Order.ASC)
            .top_k(1001),
            # order-by bin absent from projection
            session.query(dataset)
            .bins(["id"])
            .order_by("score", OrderByType.INTEGER, Order.ASC)
            .top_k(1),
        )
        for query in invalid_queries:
            with pytest.raises(AerospikeError):
                query.execute()

    def test_integer_order_and_k_boundaries(self, search_session):
        session, dataset, seed = search_session
        for value in range(25):
            seed(f"integer-{value}", {"id": value, "score": value})

        for direction, expected_start in ((Order.ASC, 0), (Order.DESC, 24)):
            for k in (1, 5, 25):
                rows = _records(
                    session.query(dataset)
                    .bins(["id", "score"])
                    .order_by("score", OrderByType.INTEGER, direction)
                    .top_k(k)
                    .execute()
                )
                expected = (
                    list(range(expected_start, expected_start + k))
                    if direction is Order.ASC
                    else list(range(expected_start, expected_start - k, -1))
                )
                assert [row.bins["id"] for row in rows] == expected

    def test_nan_nil_bytes_and_string_ordering(self, search_session):
        session, dataset, seed = search_session
        for value_id, value in enumerate((1.0, 2.0, 3.0, float("nan"), None)):
            bins = {"id": value_id}
            if value is not None:
                bins["score"] = value
            seed(f"nan-nil-{value_id}", bins)

        for direction, expected in (
            (Order.ASC, [0, 1, 2, 3, 4]),
            (Order.DESC, [3, 2, 1, 0, 4]),
        ):
            rows = _records(
                session.query(dataset)
                .bins(["id", "score"])
                .order_by("score", OrderByType.DOUBLE, direction)
                .top_k(5)
                .execute()
            )
            assert [row.bins["id"] for row in rows] == expected

        for value_id, value in enumerate((b"a", b"ab", b"b")):
            seed(f"bytes-{value_id}", {"id": value_id, "key": value})
        rows = _records(
            session.query(dataset)
            .bins(["id", "key"])
            .order_by("key", OrderByType.BYTES, Order.ASC)
            .top_k(3)
            .execute()
        )
        assert [row.bins["id"] for row in rows] == [0, 1, 2]

        for value_id, value in enumerate(("a", "B", "c")):
            seed(f"string-{value_id}", {"id": value_id, "key": value})
        rows = _records(
            session.query(dataset)
            .bins(["id", "key"])
            .order_by(
                "key", OrderByType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE,
            )
            .top_k(3)
            .execute()
        )
        assert [row.bins["id"] for row in rows] == [0, 1, 2]

    def test_nil_and_equal_key_rules(self, search_session):
        session, dataset, seed = search_session
        seed("nil-integer-1", {"id": "integer-1", "key": 1})
        seed("nil-integer-2", {"id": "integer-2", "key": 2})
        seed("nil-string", {"id": "string", "key": "wrong"})
        seed("nil-list", {"id": "list", "key": [3]})
        seed("nil-map", {"id": "map", "key": {"n": 4}})
        seed("nil-missing", {"id": "missing"})
        rows = _records(
            session.query(dataset)
            .bins(["id", "key"])
            .order_by("key", OrderByType.INTEGER, Order.ASC)
            .top_k(6)
            .execute()
        )
        identifiers = [row.bins["id"] for row in rows]
        assert identifiers[:2] == ["integer-1", "integer-2"]
        assert set(identifiers[2:]) == {"string", "list", "map", "missing"}

        for value_id in range(5):
            seed(f"tie-{value_id}", {"id": value_id, "score": 1})

        def query_ids():
            return [
                row.bins["id"]
                for row in _records(
                    session.query(dataset)
                    .bins(["id", "score"])
                    .order_by("score", OrderByType.INTEGER, Order.ASC)
                    .top_k(5)
                    .execute()
                )
            ]

        assert query_ids() == query_ids()


class TestSyncVectorDistanceExpressions:
    @pytest.mark.parametrize(
        "metric, expected",
        [
            ("euclidean_squared_distance", 0.0),
            ("dot_product", 0.30),
            ("cosine_similarity", 1.0),
        ],
    )
    def test_distance_to_self(self, search_session, metric, expected):
        session, dataset, seed = search_session
        seed(
            f"self-{metric}",
            {"embedding": Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)},
        )
        distance = getattr(Exp, metric)(
            Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32),
            Exp.vector_bin("embedding"),
        )
        rows = _records(
            session.query(dataset)
            .with_op_projection(ExpOperation.read("distance", distance))
            .execute()
        )
        assert rows[0].bins["distance"] == pytest.approx(expected, abs=1e-3)

    def test_metric_filters_keep_the_expected_records(self, search_session):
        session, dataset, seed = search_session
        seed("near", {"id": "near", "embedding": Vector([1.0, 0.0])})
        seed("far", {"id": "far", "embedding": Vector([0.0, 1.0])})

        query = Vector([1.0, 0.0])
        predicates = (
            Exp.lt(Exp.euclidean_squared_distance(query, Exp.vector_bin("embedding")), Exp.float_val(1.0)),
            Exp.gt(Exp.dot_product(query, Exp.vector_bin("embedding")), Exp.float_val(0.5)),
            Exp.gt(Exp.cosine_similarity(query, Exp.vector_bin("embedding")), Exp.float_val(0.5)),
        )
        for predicate in predicates:
            rows = _records(
                session.query(dataset).bins(["id"]).filter_expression(predicate).execute()
            )
            assert [row.bins["id"] for row in rows] == ["near"]

    def test_incomparable_vectors_return_no_result_with_no_fail(self, search_session):
        session, dataset, seed = search_session
        seed("wrong-particle", {"id": "wrong-particle", "embedding": 1})
        seed(
            "wrong-type",
            {"id": "wrong-type", "embedding": Vector([0.1, 0.2], VectorElementType.FLOAT64)},
        )
        seed(
            "wrong-dimensions",
            {"id": "wrong-dimensions", "embedding": Vector([0.1], VectorElementType.FLOAT32)},
        )
        distance = Exp.euclidean_squared_distance(
            Vector([0.1, 0.2], VectorElementType.FLOAT32),
            Exp.vector_bin("embedding"),
        )
        rows = _records(
            session.query(dataset)
            .with_op_projection(
                Operation.get_bin("id"),
                ExpOperation.read("distance", distance, ExpReadFlags.EVAL_NO_FAIL),
            )
            .execute()
        )
        assert {row.bins["id"] for row in rows} == {
            "wrong-particle",
            "wrong-type",
            "wrong-dimensions",
        }
        assert all("distance" not in row.bins for row in rows)

    def test_knn_by_projected_squared_euclidean_distance(self, search_session):
        session, dataset, seed = search_session
        for value_id in range(6):
            seed(
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
        rows = _records(
            session.query(dataset)
            .with_op_projection(
                Operation.get_bin("id"),
                ExpOperation.read("distance", distance),
            )
            .order_by("distance", OrderByType.DOUBLE, Order.ASC)
            .top_k(3)
            .execute()
        )
        assert [(row.bins["id"], row.bins["distance"]) for row in rows] == [
            (0, pytest.approx(0.0)),
            (1, pytest.approx(1.0)),
            (2, pytest.approx(4.0)),
        ]
