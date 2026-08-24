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

"""``Vector`` bin round-trip integration tests (sync facade).

Parity check for the sync builder path: the sync API wraps the async one, so
this mirrors a representative subset of ``tests/integration/async/vector_test.py``
rather than the full matrix. VECTOR bins are a dev-server-only feature; this
skips cleanly unless the target build reports >= 8.1.3 (the interim vector
gate -- see ``supports_vector_bins`` in the top-level ``conftest.py``).

Vector *search* over a vector bin (distance expressions, and any filter/read
expression that loads a VECTOR bin) currently crashes ``asd`` server-side and
is intentionally out of scope here; see
``tests/integration/async/vector_search_test.py`` for the full, quarantined
coverage and root cause. *Scalar* Top-K, which does not touch a vector bin,
does work today and is exercised below.
"""

import pytest

from aerospike_sdk import DataSet, Order, OrderByType, Vector, VectorElementType


NS = "test"
SET = "vector_psdk_sync"
DS = DataSet.of(NS, SET)

_KEYS = ("v1", "v2", "v3", "v4", "cdt", "distinct", "life") + tuple(
    f"topk-{i}" for i in range(5)
)


@pytest.fixture(scope="module")
def shared_cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        v = c.server_version()
        if v is None or (v.major, v.minor, v.patch) < (8, 1, 3):
            pytest.skip("cluster does not support VECTOR bins (requires a dev server build)")
        yield c


@pytest.fixture
def session(shared_cluster):
    s = shared_cluster.create_session()
    for k in _KEYS:
        try:
            s.delete(DS.id(k)).execute()
        except Exception:
            pass
    yield s
    for k in _KEYS:
        try:
            s.delete(DS.id(k)).execute()
        except Exception:
            pass


def _read_bin(session, key, bin_name):
    return session.query(key).execute().first_or_raise().record_or_raise().bins[bin_name]


class TestSyncVectorRoundTrip:

    def test_set_to_vector_default_float32(self, session):
        k = DS.id("v1")
        session.upsert(k).bin("embedding").set_to_vector([0.1, 0.2, 0.3, 0.4]).execute()

        vector = _read_bin(session, k, "embedding")
        assert isinstance(vector, Vector)
        assert vector.element_type == VectorElementType.FLOAT32
        assert vector.dimensions == 4
        assert list(vector.value) == pytest.approx([0.1, 0.2, 0.3, 0.4])

    def test_put_vector_int32(self, session):
        k = DS.id("v2")
        session.upsert(k).put(
            {"embedding": Vector([-5, 0, 7], VectorElementType.INT32)},
        ).execute()

        vector = _read_bin(session, k, "embedding")
        assert vector.element_type == VectorElementType.INT32
        assert list(vector.value) == [-5, 0, 7]

    def test_float16_via_numpy(self, session):
        np = pytest.importorskip("numpy")
        k = DS.id("v3")
        session.upsert(k).put(
            {"embedding": Vector(np.array([1.5, -2.5, 3.5], dtype=np.float16))},
        ).execute()

        vector = _read_bin(session, k, "embedding")
        assert vector.element_type == VectorElementType.FLOAT16
        assert vector.numpy_value.tolist() == [1.5, -2.5, 3.5]

    def test_large_vector_round_trips(self, session):
        k = DS.id("v4")
        v = Vector([i * 0.5 for i in range(4096)], VectorElementType.FLOAT32)
        session.upsert(k).put({"embedding": v}).execute()

        assert _read_bin(session, k, "embedding") == v

    def test_vector_in_list_and_map(self, session):
        k = DS.id("cdt")
        session.upsert(k) \
            .bin("embeddings").list_append(Vector([0.1, 0.2])) \
            .bin("emb_by_model").map_upsert_items({"m1": Vector([0.3, 0.4])}) \
            .execute()

        bins = session.query(k).execute().first_or_raise().record_or_raise().bins
        assert isinstance(bins["embeddings"][0], Vector)
        assert list(bins["embeddings"][0].value) == pytest.approx([0.1, 0.2])
        assert list(bins["emb_by_model"]["m1"].value) == pytest.approx([0.3, 0.4])

    def test_same_value_preserves_element_type_distinctly(self, session):
        """Same number, four element types, kept distinct through the server."""
        np = pytest.importorskip("numpy")
        k = DS.id("distinct")
        session.upsert(k).put({
            "f16": Vector(np.array([1.0], dtype=np.float16)),
            "i32": Vector([1], VectorElementType.INT32),
            "f32": Vector([1.0], VectorElementType.FLOAT32),
            "f64": Vector([1.0], VectorElementType.FLOAT64),
        }).execute()

        bins = session.query(k).execute().first_or_raise().record_or_raise().bins
        assert bins["f16"].element_type == VectorElementType.FLOAT16
        assert bins["i32"].element_type == VectorElementType.INT32
        assert bins["f32"].element_type == VectorElementType.FLOAT32
        assert bins["f64"].element_type == VectorElementType.FLOAT64
        assert bins["f32"] != bins["f64"]

    def test_exists_touch_delete(self, session):
        k = DS.id("life")
        v = Vector([1.0, 2.0], VectorElementType.FLOAT32)
        session.upsert(k).put({"v": v}).execute()

        assert session.exists(k).execute().first().as_bool() is True
        session.touch(k).execute()
        assert _read_bin(session, k, "v") == v

        session.delete(k).execute()
        after = session.exists(k).include_missing_keys().execute().first()
        assert after.as_bool() is False


class TestSyncScalarTopK:
    """Scalar Top-K ("``ORDER BY <scalar bin> LIMIT k``") works today; it does
    not touch a vector bin so it is not on the server crash path. Status is
    unconfirmed by the server team -- see the async search suite's docstring."""

    def test_scalar_topk_orders_and_limits(self, session):
        for i in range(5):
            session.upsert(DS.id(f"topk-{i}")).put({"score": i * 10}).execute()

        stream = (
            session.query(DS)
            .bins(["score"])
            .order_by("score", OrderByType.INTEGER, Order.DESC)
            .top_k(3)
            .execute()
        )
        scores = [r.record_or_raise().bins["score"] for r in stream]
        assert scores == [40, 30, 20]
