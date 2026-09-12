# Copyright 2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
# You may not use this file except in compliance with the Apache License,
# Version 2.0. You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Unit tests for the fluent VECTOR distance-expression and Top-K API."""

import pytest

from aerospike_sdk import Order, OrderByFlags, OrderByType, Vector, query_shared
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.exp import Exp


def _qb() -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="vector_unit")


class TestVectorDistanceExpressions:
    """Validate VECTOR expression composition."""

    def test_vector_bin_expression_builds(self):
        assert type(Exp.vector_bin("embedding")).__name__ == "FilterExpression"

    @pytest.mark.parametrize(
        "factory",
        ["cosine_similarity", "euclidean_squared_distance", "dot_product"],
    )
    def test_distance_expression_builds(self, factory):
        expr = getattr(Exp, factory)(Vector([0.1, 0.2, 0.3]), Exp.vector_bin("embedding"))
        assert type(expr).__name__ == "FilterExpression"

    def test_distance_composes_with_comparators(self):
        expr = Exp.gt(
            Exp.cosine_similarity(Vector([0.1, 0.2, 0.3]), Exp.vector_bin("embedding")),
            Exp.float_val(0.8),
        )
        assert type(expr).__name__ == "FilterExpression"

    def test_distance_rejects_non_vector_query(self):
        with pytest.raises(TypeError):
            Exp.cosine_similarity([0.1, 0.2, 0.3], Exp.vector_bin("embedding"))

    def test_distance_rejects_non_expression_bin(self):
        with pytest.raises(TypeError):
            Exp.cosine_similarity(Vector([0.1, 0.2, 0.3]), "embedding")


class TestTopKBuilder:
    """Validate Top-K builder state and Statement forwarding."""

    def test_order_by_and_top_k_chain(self):
        qb = _qb().order_by("similarity", OrderByType.DOUBLE, Order.DESC).top_k(5)
        assert qb._order_by == ("similarity", OrderByType.DOUBLE, Order.DESC, None)
        assert qb._top_k == 5

    @pytest.mark.parametrize(
        "order_type",
        [OrderByType.INTEGER, OrderByType.DOUBLE, OrderByType.STRING, OrderByType.BYTES],
    )
    @pytest.mark.parametrize("direction", [Order.ASC, Order.DESC])
    def test_order_by_all_types_and_directions(self, order_type, direction):
        qb = _qb().order_by("rank", order_type, direction).top_k(1)
        assert qb._order_by == ("rank", order_type, direction, None)

    def test_order_by_with_case_insensitive_flag(self):
        qb = _qb().order_by(
            "name", OrderByType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE,
        ).top_k(10)
        assert qb._order_by == (
            "name", OrderByType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE,
        )

    def test_build_statement_forwards_projection_and_top_k(self, monkeypatch):
        class FakeStatement:
            def __init__(self, namespace, set_name, bins):
                self.namespace = namespace
                self.set_name = set_name
                self.bins = bins
                self.operations = None
                self.order_by = None
                self.top_k = None

            def set_operations(self, operations):
                self.operations = operations

            def set_order_by(self, bin_name, order_type, direction, flags=None):
                self.order_by = (bin_name, order_type, direction, flags)

            def set_top_k(self, k):
                self.top_k = k

        monkeypatch.setattr(query_shared, "Statement", FakeStatement)
        distance = Exp.euclidean_squared_distance(Vector([0.0, 0.0]), Exp.vector_bin("v"))
        projection = object()
        qb = (
            _qb()
            .with_op_projection(projection, distance)
            .order_by("distance", OrderByType.DOUBLE, Order.ASC)
            .top_k(3)
        )

        statement = qb._build_statement()

        assert statement.operations == [projection, distance]
        assert statement.order_by == ("distance", OrderByType.DOUBLE, Order.ASC, None)
        assert statement.top_k == 3
