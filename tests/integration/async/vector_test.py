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

"""Vector data type round-trip integration tests.

Coverage:
- Plain bin put/get for FLOAT32 (default), FLOAT64, and INT32 element types,
  via both ``set_to_vector(...)`` and ``put({"bin": Vector(...)})``. Each is
  read back through both ``.value`` and ``.numpy_value``, so a value written
  as a list can be read as a numpy array and vice versa.
- Embedding a ``Vector`` inside a CDT list and a CDT map.
- Writing from a numpy array (including FLOAT16, which requires numpy) and
  reading back via ``.numpy_value``.

Construction, defaults, and error cases are in ``tests/unit/vector_builder_test.py``.
"""

import pytest

from aerospike_sdk import Vector, VectorElementType
from aerospike_sdk.dataset import DataSet


NAMESPACE = "test"
SET = "vector_psdk"

_KEYS = ("v1", "v2", "v3", "v4", "v5", "cdt")


@pytest.fixture
async def cluster(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as c:
        session = c.create_session()
        ds = DataSet.of(NAMESPACE, SET)
        for k in _KEYS:
            try:
                await session.delete(ds.id(k)).execute()
            except Exception:
                pass
        yield c
        for k in _KEYS:
            try:
                await session.delete(ds.id(k)).execute()
            except Exception:
                pass


class TestVectorPutGet:
    """Written as a plain list; read back both as a list and as a numpy array."""

    async def test_set_to_vector_default_float32(self, cluster):
        session = cluster.create_session()
        k = DataSet.of(NAMESPACE, SET).id("v1")

        await (
            session.upsert(k)
            .bin("embedding").set_to_vector([0.1, 0.2, 0.3, 0.4])
            .execute()
        )

        result = await session.query(k).execute()
        first = await result.first_or_raise()
        record = first.record_or_raise()
        vector = record.bins["embedding"]

        assert isinstance(vector, Vector)
        assert vector.element_type == VectorElementType.FLOAT32
        assert vector.dimensions == 4
        assert list(vector.value) == pytest.approx([0.1, 0.2, 0.3, 0.4])
        np = pytest.importorskip("numpy")
        assert vector.numpy_value.tolist() == pytest.approx([0.1, 0.2, 0.3, 0.4])
        assert vector.numpy_value.dtype == np.float32

    async def test_put_vector_float64(self, cluster):
        session = cluster.create_session()
        k = DataSet.of(NAMESPACE, SET).id("v2")

        await session.upsert(k).put(
            {"embedding": Vector([1.5, 2.5, 3.5], VectorElementType.FLOAT64)},
        ).execute()

        result = await session.query(k).execute()
        first = await result.first_or_raise()
        vector = first.record_or_raise().bins["embedding"]

        assert vector.element_type == VectorElementType.FLOAT64
        assert list(vector.value) == pytest.approx([1.5, 2.5, 3.5])
        np = pytest.importorskip("numpy")
        assert vector.numpy_value.tolist() == pytest.approx([1.5, 2.5, 3.5])
        assert vector.numpy_value.dtype == np.float64

    async def test_set_to_vector_int32(self, cluster):
        session = cluster.create_session()
        k = DataSet.of(NAMESPACE, SET).id("v3")

        await (
            session.upsert(k)
            .bin("embedding").set_to_vector([1, 2, 3], VectorElementType.INT32)
            .execute()
        )

        result = await session.query(k).execute()
        first = await result.first_or_raise()
        vector = first.record_or_raise().bins["embedding"]

        assert vector.element_type == VectorElementType.INT32
        assert list(vector.value) == [1, 2, 3]
        np = pytest.importorskip("numpy")
        assert vector.numpy_value.tolist() == [1, 2, 3]
        assert vector.numpy_value.dtype == np.int32


class TestVectorNumpyPutGet:
    """Written as a numpy array; read back both as a numpy array and as a list."""

    @pytest.mark.parametrize(
        "np_dtype, element_type, values",
        [
            ("float32", VectorElementType.FLOAT32, [0.1, 0.2, 0.3]),
            ("float64", VectorElementType.FLOAT64, [0.1, 0.2, 0.3]),
            ("int32", VectorElementType.INT32, [1, 2, 3]),
        ],
    )
    async def test_set_to_vector_from_numpy_array(self, cluster, np_dtype, element_type, values):
        np = pytest.importorskip("numpy")
        session = cluster.create_session()
        k = DataSet.of(NAMESPACE, SET).id("v4")

        await (
            session.upsert(k)
            .bin("embedding").set_to_vector(np.array(values, dtype=np_dtype))
            .execute()
        )

        result = await session.query(k).execute()
        first = await result.first_or_raise()
        vector = first.record_or_raise().bins["embedding"]

        assert vector.element_type == element_type
        assert vector.numpy_value.tolist() == pytest.approx(values)
        assert list(vector.value) == pytest.approx(values)

    async def test_put_vector_float16_via_numpy(self, cluster):
        np = pytest.importorskip("numpy")
        session = cluster.create_session()
        k = DataSet.of(NAMESPACE, SET).id("v5")

        await session.upsert(k).put(
            {"embedding": Vector(np.array([1.5, -2.5, 3.5], dtype=np.float16))},
        ).execute()

        result = await session.query(k).execute()
        first = await result.first_or_raise()
        vector = first.record_or_raise().bins["embedding"]

        assert vector.element_type == VectorElementType.FLOAT16
        # FLOAT16 is only readable via .numpy_value.
        with pytest.raises(TypeError):
            _ = vector.value
        out = vector.numpy_value
        assert out.dtype == np.float16
        assert out.tolist() == [1.5, -2.5, 3.5]


class TestVectorInCdt:

    async def test_vector_in_list_and_map(self, cluster):
        session = cluster.create_session()
        k = DataSet.of(NAMESPACE, SET).id("cdt")

        await (
            session.upsert(k)
            .bin("embeddings").list_append(Vector([0.1, 0.2]))
            .bin("embeddings_by_model").map_upsert_items({"v1": Vector([0.3, 0.4])})
            .execute()
        )

        result = await session.query(k).execute()
        first = await result.first_or_raise()
        bins = first.record_or_raise().bins

        list_vec = bins["embeddings"][0]
        assert isinstance(list_vec, Vector)
        assert list(list_vec.value) == pytest.approx([0.1, 0.2])

        map_vec = bins["embeddings_by_model"]["v1"]
        assert isinstance(map_vec, Vector)
        assert list(map_vec.value) == pytest.approx([0.3, 0.4])
