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

"""Vector SEARCH -- Top-K ordering and vector-distance expressions.

Mirrors ``aerospike-client-python-async``'s ``vector_search_test.py`` and the
rust core's TODO/ignored vector-distance tests, adapted to the SDK's fluent
builder.

The client-side *build* surface for all of this (distance expressions,
``order_by`` / ``top_k``) is unit-tested separately -- see
``tests/unit/vector_search_wip_test.py``.

What actually runs against the current dev server:

* **Scalar Top-K** ("``ORDER BY <scalar bin> LIMIT k``", no vector expression
  involved) works today: the query returns the correctly ordered and limited
  result set and the node stays healthy. It is kept as a normal, un-skipped
  test -- but note its status is *unconfirmed* by the server team; treat the
  passing result as informative rather than a guaranteed contract. See
  ``test_scalar_topk_orders_and_limits`` below.

Everything else here -- evaluating **any** expression over a VECTOR bin (a
plain read, ``bin_exists``, a filter, or a distance metric) -- reaches a real
server-side defect: ``rt_bin_translate``
(``aerospike-server/as/src/exp/exp_rt.c``) switches on the stored particle
type and has no ``case AS_PARTICLE_TYPE_VECTOR``, so it falls into
``default: cf_crash(AS_EXP, "unexpected")`` and aborts ``asd``. Every
expression path that loads a vector bin -- a filter, ``bin_exists``,
``bin_type``, or an ``ExpOperation.read`` -- routes through
``rt_load_bin -> rt_bin_translate``, so a plain read and a distance metric hit
the exact same crash; distance is just one more caller on top of it. The
sibling ``rt_value_translate`` already degrades unknown particle types
gracefully (``AS_EXP_UNK``) instead of crashing, so this looks like a one-line
server fix (give VECTOR the same treatment as BLOB) rather than a fundamental
limitation.

Because running any of those against an unfixed server takes the whole node
down (not just fails the one request), the affected test classes are
permanently marked ``@pytest.mark.skip`` with a TODO. There is deliberately no
runtime opt-in flag: these are normal regression tests that are currently
unsupported, not tests users should attempt against an arbitrary dev build.
Each asserts the *correct, non-crashing* outcome so it can be re-enabled once
the server carries the ``rt_bin_translate`` fix.
"""

import pytest
import pytest_asyncio

from aerospike_async import ExpOperation, ExpReadFlags
from aerospike_sdk import DataSet, Exp, Order, OrderByType, Vector, VectorElementType
from aerospike_sdk.exceptions import FilteredOutError


NAMESPACE = "test"
SET = "vector_search_psdk"

# TODO(vector-expression-support): Re-enable once the server's
# ``rt_bin_translate`` handles ``AS_PARTICLE_TYPE_VECTOR`` rather than calling
# ``cf_crash``. Keep all VECTOR-expression paths under this permanent skip: a
# plain ExpOperation.read, bin_exists, filter, and distance expression share
# the same crash path.
_vector_expression_unsupported = pytest.mark.skip(
    reason=(
        "TODO(vector-expression-support): evaluating an expression over a "
        "VECTOR bin crashes asd because rt_bin_translate lacks an "
        "AS_PARTICLE_TYPE_VECTOR case"
    ),
)


@pytest_asyncio.fixture(autouse=True)
async def _skip_without_vector_support(supports_vector_bins):
    if not supports_vector_bins:
        pytest.skip("cluster does not support VECTOR bins (requires a dev server build)")


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as c:
        yield c


@pytest_asyncio.fixture
async def search_session(cluster):
    """A session plus a clean, dedicated set (all seeded keys auto-deleted)."""
    session = cluster.create_session()
    ds = DataSet.of(NAMESPACE, SET)
    used: list = []

    async def _seed(name: str, bins: dict):
        k = ds.id(name)
        used.append(k)
        try:
            await session.delete(k).execute()
        except Exception:
            pass
        await session.upsert(k).put(bins).execute()
        return k

    yield session, ds, _seed

    for k in used:
        try:
            await session.delete(k).execute()
        except Exception:
            pass


async def _drain(stream):
    out = []
    async for result in stream:
        out.append(result.record_or_raise())
    return out


# ---------------------------------------------------------------------------
# Scalar Top-K -- works today (not on the crash path). Kept un-skipped.
# ---------------------------------------------------------------------------

class TestScalarTopK:

    async def test_scalar_topk_orders_and_limits(self, search_session):
        """Scalar-bin Top-K ("``ORDER BY <scalar bin> LIMIT k``", no vector
        expression involved) is not on the crash path and passes today: the
        query returns the correctly ordered (desc) and limited (k=3) result set
        and the node stays healthy.

        Status caveat: this is empirically working but *unconfirmed* by the
        server team. Whether Top-K is intended-supported on this build, its
        minimum version / capability gate, and whether the docs are stale are
        all open questions -- don't treat this as a stable guarantee yet.
        """
        session, ds, _seed = search_session
        for i in range(5):
            await _seed(f"topk-{i}", {"score": i * 10})

        stream = await (
            session.query(ds)
            .bins(["score"])
            .order_by("score", OrderByType.INTEGER, Order.DESC)
            .top_k(3)
            .execute()
        )
        records = await _drain(stream)
        scores = [r.bins["score"] for r in records]

        assert scores == [40, 30, 20]


# ---------------------------------------------------------------------------
# Any expression over a vector bin crashes asd -- TODO/WIP, not runnable.
# General repros first (a plain read / existence check), then the same defect
# via distance metrics.
# ---------------------------------------------------------------------------

@_vector_expression_unsupported
class TestVectorBinExpressionReadUnsupported:
    """Any expression that evaluates *over* a vector bin -- a plain read, an
    existence check, or a comparison -- crashes an unfixed node. These are the
    minimal repros (no distance math); see ``TestVectorDistanceExpressions``
    for the same defect reached via distance metrics."""

    async def test_expression_read_of_vector_bin_returns_bin(self, search_session):
        """Reading the vector bin back through an ``ExpOperation.read``
        projection exercises ``rt_load_bin -> rt_bin_translate`` from the read
        side. On a fixed server the expression evaluates and the result bin
        comes back."""
        session, ds, _seed = search_session
        await _seed("exp-read", {"v": Vector([0.5, -1.5, 2.0], VectorElementType.FLOAT32)})

        stream = await (
            session.query(ds)
            .with_op_projection(
                ExpOperation.read("out", Exp.vector_bin("v"), ExpReadFlags.DEFAULT),
            )
            .execute()
        )
        records = await _drain(stream)
        assert any("out" in r.bins for r in records)

    async def test_vector_bin_filter_evaluates_to_filtered_out(self, search_session):
        """``eq(vector_bin("v"), blob_val(...))`` against a stored vector must
        evaluate to unknown (FILTERED_OUT) on a fixed server, not abort the
        node. The blob comparand can never equal a VECTOR particle, so the
        correct, non-crashing outcome is FILTERED_OUT."""
        session, ds, _seed = search_session
        k = await _seed(
            "exp-filter", {"v": Vector([0.1, -2.5, 3.375], VectorElementType.FLOAT32), "scalar": 1},
        )

        with pytest.raises(FilteredOutError):
            stream = await (
                session.query(k)
                .bins(["scalar"])
                .filter_expression(Exp.eq(Exp.vector_bin("v"), Exp.blob_val([0])))
                .fail_on_filtered_out()
                .execute()
            )
            await _drain(stream)

        # The node must still be reachable afterwards -- a genuine crash would
        # make this follow-up read fail too.
        rec = await (await session.query(k).bins(["scalar"]).execute()).first_or_raise()
        assert rec.record_or_raise().bins["scalar"] == 1

    async def test_bin_exists_filter_returns_record(self, search_session):
        """``bin_exists("v")`` is a different entry point into the same defect:
        it compiles to ``bin_type(...) != NULL``, evaluated via
        ``rt_load_bin -> rt_bin_translate``. On a fixed server the bin exists,
        so the predicate is true and the record comes back normally."""
        session, ds, _seed = search_session
        k = await _seed(
            "exp-exists", {"v": Vector([0.1, -2.5, 3.375], VectorElementType.FLOAT32), "scalar": 3},
        )

        stream = await (
            session.query(k)
            .bins(["scalar"])
            .filter_expression(Exp.bin_exists("v"))
            .execute()
        )
        rec = await stream.first_or_raise()
        assert rec.record_or_raise().bins["scalar"] == 3


@_vector_expression_unsupported
class TestVectorDistanceExpressionsUnsupported:
    """Distance expressions used in a query projection. These hit the same
    ``rt_bin_translate`` crash as a plain vector-bin read (see module
    docstring); the distance kernel is just one more caller on top of it."""

    # Distance of the stored vector to *itself* has a known closed form per
    # metric (mirrors the rust core's distance-to-self tests):
    #   euclidean_squared_distance -> 0
    #   dot_product                -> sum of squares (0.1^2+..+0.4^2 = 0.30)
    #   cosine_similarity          -> 1
    @pytest.mark.parametrize(
        "metric, expected",
        [
            ("euclidean_squared_distance", 0.0),
            ("dot_product", 0.30),
            ("cosine_similarity", 1.0),
        ],
    )
    async def test_distance_to_self_has_known_value(self, search_session, metric, expected):
        session, ds, _seed = search_session
        await _seed(
            "dist-self",
            {"embedding": Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)},
        )

        query = Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)
        distance = getattr(Exp, metric)(query, Exp.vector_bin("embedding"))

        stream = await (
            session.query(ds)
            .with_op_projection(
                ExpOperation.read("distance", distance, ExpReadFlags.DEFAULT),
            )
            .execute()
        )
        records = await _drain(stream)
        projected = [r for r in records if "distance" in r.bins]
        assert projected
        assert projected[0].bins["distance"] == pytest.approx(expected, abs=1e-3)

    async def test_euclidean_squared_distance_is_sum_of_squared_diffs(self, search_session):
        """Squared L2 between [0, 0] and [3, 4] is 3^2 + 4^2 = 25 (mirrors the
        rust core's ``euclidean_squared_distance_is_sum_of_squared_differences``)."""
        session, ds, _seed = search_session
        await _seed("dist-l2", {"embedding": Vector([0.0, 0.0], VectorElementType.FLOAT32)})

        query = Vector([3.0, 4.0], VectorElementType.FLOAT32)
        distance = Exp.euclidean_squared_distance(query, Exp.vector_bin("embedding"))

        stream = await (
            session.query(ds)
            .with_op_projection(
                ExpOperation.read("distance", distance, ExpReadFlags.DEFAULT),
            )
            .execute()
        )
        records = await _drain(stream)
        projected = [r for r in records if "distance" in r.bins]
        assert projected
        assert projected[0].bins["distance"] == pytest.approx(25.0, abs=1e-3)

    async def test_distance_filter_expression_on_read(self, search_session):
        session, ds, _seed = search_session
        k = await _seed(
            "dist-filter",
            {"embedding": Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)},
        )

        query = Vector([0.1, 0.2, 0.3, 0.4], VectorElementType.FLOAT32)
        similarity = Exp.cosine_similarity(query, Exp.vector_bin("embedding"))

        stream = await (
            session.query(k)
            .filter_expression(Exp.gt(similarity, Exp.float_val(0.5)))
            .execute()
        )
        rec = await stream.first_or_raise()
        assert rec.record_or_raise() is not None


@_vector_expression_unsupported
class TestVectorTopKWithDistanceUnsupported:
    """The full hybrid flow: project a distance into a bin, then Top-K by it.
    Requires both server-side vector-distance evaluation (the crash above) and
    Top-K ordering by the projected bin."""

    async def test_topk_by_cosine_similarity(self, search_session):
        session, ds, _seed = search_session
        await _seed(
            "hybrid",
            {"embedding": Vector([0.12, 0.98, 0.44, 0.05], VectorElementType.FLOAT32)},
        )

        query = Vector([0.10, 0.95, 0.40, 0.02], VectorElementType.FLOAT32)
        similarity = Exp.cosine_similarity(query, Exp.vector_bin("embedding"))

        stream = await (
            session.query(ds)
            .with_op_projection(
                ExpOperation.read("similarity", similarity, ExpReadFlags.DEFAULT),
            )
            .order_by("similarity", OrderByType.DOUBLE, Order.DESC)
            .top_k(10)
            .execute()
        )
        records = await _drain(stream)
        assert len(records) >= 1
