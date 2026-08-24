# Copyright 2026 Aerospike, Inc.
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

"""Client-side vector-SEARCH API *build* surface (no server involved).

Vector similarity search has two developer-facing pieces:

1. **Vector-distance expressions** -- ``cosine_similarity`` /
   ``euclidean_squared_distance`` / ``dot_product`` and the ``vector_bin``
   operand they read.
2. **Top-K** -- ``QueryBuilder.order_by`` / ``top_k``
   ("``ORDER BY <bin> LIMIT k``").

This module unit-tests only that the fluent surface **exists and builds**
entirely client-side -- it never touches a server. The end-to-end behaviour is
covered (and quarantined) in ``tests/integration/async/vector_search_test.py``,
whose findings are:

- *Scalar* Top-K works against the current dev server.
- Evaluating **any** expression over a VECTOR bin (a plain read, ``bin_exists``,
  a filter, or a distance metric) crashes ``asd``: the server's
  ``rt_bin_translate`` (``exp/exp_rt.c``) has no ``AS_PARTICLE_TYPE_VECTOR``
  case and falls through to ``cf_crash``. So the distance builders below pack
  correctly, but sending them to an unfixed server downs the node.

Mirrors the dependency repos' build-surface unit tests --
``aerospike-client-python-async``'s ``TestFilterExprVector`` (in
``filter_expr_test.py``) and ``TestStatementTopK`` (in ``query_test.py``).
"""

import pytest

from aerospike_sdk import Order, OrderByFlags, OrderByType, Vector
from aerospike_sdk.exp import Exp
from aerospike_sdk.aio.operations.query import QueryBuilder


def _qb() -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="wip")


class TestVectorDistanceExpressionsBuild:
    """Distance-expression builders pack correctly without a server. Mirrors
    ``aerospike-client-python-async``'s ``TestFilterExprVector``."""

    def test_vector_bin_expression_builds(self):
        expr = Exp.vector_bin("embedding")
        assert expr is not None
        assert type(expr).__name__ == "FilterExpression"

    @pytest.mark.parametrize(
        "factory", ["cosine_similarity", "euclidean_squared_distance", "dot_product"],
    )
    def test_distance_expression_builds(self, factory):
        query = Vector([0.1, 0.2, 0.3])
        expr = getattr(Exp, factory)(query, Exp.vector_bin("embedding"))
        assert type(expr).__name__ == "FilterExpression"

    def test_distance_composes_with_comparators(self):
        """A distance metric feeds a scalar comparator to form a filter."""
        query = Vector([0.1, 0.2, 0.3])
        expr = Exp.gt(
            Exp.cosine_similarity(query, Exp.vector_bin("embedding")),
            Exp.float_val(0.8),
        )
        assert type(expr).__name__ == "FilterExpression"

    def test_distance_rejects_non_vector_query(self):
        """The query operand must be a ``Vector``, not a raw list."""
        with pytest.raises(TypeError):
            Exp.cosine_similarity([0.1, 0.2, 0.3], Exp.vector_bin("embedding"))

    def test_distance_rejects_non_expression_bin(self):
        """The bin operand must be an expression (``vector_bin(...)``), not a
        bare bin-name string."""
        query = Vector([0.1, 0.2, 0.3])
        with pytest.raises(TypeError):
            Exp.cosine_similarity(query, "embedding")


class TestTopKBuilderSurface:
    """``order_by`` / ``top_k`` set builder state client-side (no execute).
    The request-time validation (bad bin name, ``k`` out of ``[1, 1000]``,
    order-by bin missing from the projection, etc.) lives in the native
    ``Statement`` and only fires when the query is actually executed, so it is
    not observable here. Mirrors ``TestStatementTopK`` in the async client."""

    def test_order_by_sets_state(self):
        qb = _qb().order_by("similarity", OrderByType.DOUBLE, Order.DESC)
        assert qb._order_by == ("similarity", OrderByType.DOUBLE, Order.DESC, None)

    def test_order_by_with_flags(self):
        qb = _qb().order_by(
            "name", OrderByType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE,
        )
        assert qb._order_by == (
            "name", OrderByType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE,
        )

    @pytest.mark.parametrize(
        "order_type",
        [OrderByType.INTEGER, OrderByType.DOUBLE, OrderByType.STRING, OrderByType.BYTES],
    )
    @pytest.mark.parametrize("direction", [Order.ASC, Order.DESC])
    def test_order_by_all_types_and_directions(self, order_type, direction):
        qb = _qb().order_by("bin", order_type, direction)
        assert qb._order_by == ("bin", order_type, direction, None)

    def test_top_k_sets_state(self):
        qb = _qb().top_k(10)
        assert qb._top_k == 10

    def test_order_by_and_top_k_chain(self):
        qb = _qb().order_by("similarity", OrderByType.DOUBLE, Order.DESC).top_k(5)
        assert qb._order_by[0] == "similarity"
        assert qb._top_k == 5
