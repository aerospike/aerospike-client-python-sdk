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

"""``Vector`` bin (``VECTOR`` particle) round-trip integration tests.

Scope: storing and retrieving :class:`~aerospike_sdk.Vector` bins through the
SDK's fluent API -- construction, put/get, every element type, numpy input,
special float values (non-finite, signed zero, bit-exact), large vectors
(including the 16-bit msgpack length boundary, top-level and nested),
multi-bin records, absence, overwrite/type-churn, bin selection,
exists/touch/delete + generation, batch, element-type fidelity, and nesting
inside CDT list/map bins (including map-in-list and list-in-map). Read back
through both ``.value`` and ``.numpy_value`` so a value written as a list can
be read as a numpy array and vice versa. This mirrors the hardened suites in
the two dependency repos -- ``aerospike-client-rust``'s ``tests/src/vector.rs``
and ``aerospike-client-python-async``'s ``tests/integration/vector_test.py``.

VECTOR bins are an unreleased, dev-server-only feature. Support is gated via
the ``supports_vector_bins`` fixture (see ``conftest.py``): point
``AEROSPIKE_HOST`` at such a dev build to run these; they skip cleanly
otherwise.

Out of scope -- vector SEARCH (Top-K ``ORDER BY <bin> LIMIT k`` and
vector-distance expressions), which lives in ``vector_search_test.py``. The
key finding, confirmed by the hardened dependency suites: *scalar* Top-K
works today, but evaluating **any** expression over a VECTOR bin (a plain
read, ``bin_exists``, a filter, or a distance metric) crashes ``asd``. The
root cause is server-side -- ``rt_bin_translate``
(``aerospike-server/as/src/exp/exp_rt.c``) has no ``AS_PARTICLE_TYPE_VECTOR``
case and falls through to ``cf_crash``. Because those paths take the whole
node down, they are marked TODO/WIP with a permanent ``pytest.skip`` in
``vector_search_test.py`` (mirroring the rust core's ``#[ignore]``). Nothing
here routes a vector bin through an expression, so this file is always safe to
run.

Construction/defaults/error cases are in ``tests/unit/vector_builder_test.py``;
the client-side search *build* surface is in
``tests/unit/vector_search_wip_test.py``.
"""

import pytest
import pytest_asyncio

from aerospike_async.exceptions import ValueError as PacValueError
from aerospike_sdk import Vector, VectorElementType
from aerospike_sdk.dataset import DataSet


NAMESPACE = "test"
SET = "vector_psdk"


@pytest_asyncio.fixture(autouse=True)
async def _skip_without_vector_support(supports_vector_bins):
    if not supports_vector_bins:
        pytest.skip("cluster does not support VECTOR bins (requires a dev server build)")


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    """One connected cluster for the whole module (connect is ~1s)."""
    async with await make_cluster_definition(aerospike_host).connect() as c:
        yield c


@pytest_asyncio.fixture
async def session_and_key(cluster):
    """A fresh session plus a factory of clean, auto-deleted keys.

    Each returned key is deleted before the test (so a crashed prior run
    can't leak state) and again after (teardown), keeping tests isolated
    without a fixed shared key list.
    """
    session = cluster.create_session()
    ds = DataSet.of(NAMESPACE, SET)
    used: list = []

    async def _key(name: str):
        k = ds.id(name)
        used.append(k)
        try:
            await session.delete(k).execute()
        except Exception:
            pass
        return k

    yield session, _key

    for k in used:
        try:
            await session.delete(k).execute()
        except Exception:
            pass


async def _read_bin(session, key, bin_name):
    """Query a single key and return one bin's value."""
    result = await session.query(key).execute()
    first = await result.first_or_raise()
    return first.record_or_raise().bins[bin_name]


async def _read_bins(session, key):
    result = await session.query(key).execute()
    first = await result.first_or_raise()
    return first.record_or_raise().bins


async def _read_record(session, key):
    """Query a single key and return the whole Record (bins + generation)."""
    result = await session.query(key).execute()
    first = await result.first_or_raise()
    return first.record_or_raise()


# ---------------------------------------------------------------------------
# Plain put/get, per element type, list-constructed
# ---------------------------------------------------------------------------

class TestVectorPutGet:
    """Written as a plain list; read back both as a list and as a numpy array."""

    async def test_set_to_vector_default_float32(self, session_and_key):
        session, key = session_and_key
        k = await key("v_f32_default")

        await session.upsert(k).bin("embedding").set_to_vector([0.1, 0.2, 0.3, 0.4]).execute()

        vector = await _read_bin(session, k, "embedding")
        assert isinstance(vector, Vector)
        assert vector.element_type == VectorElementType.FLOAT32
        assert vector.dimensions == 4
        assert list(vector.value) == pytest.approx([0.1, 0.2, 0.3, 0.4])
        np = pytest.importorskip("numpy")
        assert vector.numpy_value.tolist() == pytest.approx([0.1, 0.2, 0.3, 0.4])
        assert vector.numpy_value.dtype == np.float32

    async def test_put_vector_float64(self, session_and_key):
        session, key = session_and_key
        k = await key("v_f64")

        await session.upsert(k).put(
            {"embedding": Vector([1.5, 2.5, 3.5, 1e300], VectorElementType.FLOAT64)},
        ).execute()

        vector = await _read_bin(session, k, "embedding")
        assert vector.element_type == VectorElementType.FLOAT64
        assert list(vector.value) == pytest.approx([1.5, 2.5, 3.5, 1e300])
        np = pytest.importorskip("numpy")
        assert vector.numpy_value.dtype == np.float64

    async def test_set_to_vector_int32(self, session_and_key):
        session, key = session_and_key
        k = await key("v_i32")

        await (
            session.upsert(k)
            .bin("embedding").set_to_vector(
                [-5, 0, 7, -2147483648, 2147483647], VectorElementType.INT32,
            )
            .execute()
        )

        vector = await _read_bin(session, k, "embedding")
        assert vector.element_type == VectorElementType.INT32
        assert list(vector.value) == [-5, 0, 7, -2147483648, 2147483647]
        np = pytest.importorskip("numpy")
        assert vector.numpy_value.tolist() == [-5, 0, 7, -2147483648, 2147483647]
        assert vector.numpy_value.dtype == np.int32

    async def test_single_element_vector(self, session_and_key):
        """The one-dimension floor: a 1-element vector is the smallest legal one."""
        session, key = session_and_key
        k = await key("v_single")

        await session.upsert(k).bin("embedding").set_to_vector([42.0]).execute()

        vector = await _read_bin(session, k, "embedding")
        assert vector.dimensions == 1
        assert list(vector.value) == pytest.approx([42.0])

    async def test_roundtrip_equals_written_value(self, session_and_key):
        """The read-back Vector is == the exact object written."""
        session, key = session_and_key
        k = await key("v_eq")
        written = Vector([0.12, 0.98, -0.34])

        await session.upsert(k).put({"embedding": written}).execute()

        assert await _read_bin(session, k, "embedding") == written


# ---------------------------------------------------------------------------
# numpy-constructed
# ---------------------------------------------------------------------------

class TestVectorNumpyPutGet:
    """Written as a numpy array; read back both as a numpy array and as a list."""

    @pytest.mark.parametrize(
        "np_dtype, element_type, values",
        [
            ("float32", VectorElementType.FLOAT32, [0.5, -1.5, 2.0]),
            ("float64", VectorElementType.FLOAT64, [0.25, -0.5, 1.0]),
            ("int32", VectorElementType.INT32, [1, -2, 3]),
        ],
    )
    async def test_set_to_vector_from_numpy_array(
        self, session_and_key, np_dtype, element_type, values,
    ):
        np = pytest.importorskip("numpy")
        session, key = session_and_key
        k = await key(f"v_np_{np_dtype}")

        await (
            session.upsert(k)
            .bin("embedding").set_to_vector(np.array(values, dtype=np_dtype))
            .execute()
        )

        vector = await _read_bin(session, k, "embedding")
        assert vector.element_type == element_type
        assert vector.numpy_value.dtype == np.dtype(np_dtype)
        assert vector.numpy_value.tolist() == pytest.approx(values)
        assert list(vector.value) == pytest.approx(values)

    async def test_put_vector_float16_via_numpy(self, session_and_key):
        """FLOAT16 can only be built from (and read back via) numpy."""
        np = pytest.importorskip("numpy")
        session, key = session_and_key
        k = await key("v_f16")

        await session.upsert(k).put(
            {"embedding": Vector(np.array([1.5, -2.5, 3.5], dtype=np.float16))},
        ).execute()

        vector = await _read_bin(session, k, "embedding")
        assert vector.element_type == VectorElementType.FLOAT16
        # FLOAT16 is only readable via .numpy_value.
        with pytest.raises(TypeError):
            _ = vector.value
        out = vector.numpy_value
        assert out.dtype == np.float16
        assert out.tolist() == [1.5, -2.5, 3.5]


# ---------------------------------------------------------------------------
# Special / non-finite float values
# ---------------------------------------------------------------------------

class TestVectorSpecialValues:
    """Non-finite elements and signed zero survive a round trip bit-for-bit."""

    @pytest.mark.parametrize("np_dtype", ["float16", "float32", "float64"])
    async def test_non_finite_values_round_trip_bit_exact(self, session_and_key, np_dtype):
        np = pytest.importorskip("numpy")
        session, key = session_and_key
        k = await key(f"v_nonfinite_{np_dtype}")
        arr = np.array([np.nan, np.inf, -np.inf, 0.0, -0.0, 1.5], dtype=np_dtype)

        await session.upsert(k).put({"embedding": Vector(arr)}).execute()

        got = (await _read_bin(session, k, "embedding")).numpy_value
        assert got.tobytes() == arr.tobytes()


# ---------------------------------------------------------------------------
# Multiple vector bins, absence, and overwrite
# ---------------------------------------------------------------------------

class TestVectorMultiBinAndAbsence:

    async def test_multiple_vector_bins_in_one_record(self, session_and_key):
        session, key = session_and_key
        k = await key("v_multi")
        v1 = Vector([1.0, 2.0], VectorElementType.FLOAT32)
        v2 = Vector([1, 2, 3], VectorElementType.INT32)

        await session.upsert(k).put({"a": v1, "b": v2, "scalar": 42}).execute()

        bins = await _read_bins(session, k)
        assert bins["a"] == v1
        assert bins["b"] == v2
        assert bins["scalar"] == 42

    async def test_absent_vector_bin_is_not_materialized(self, session_and_key):
        """A record with no vector bin must not surface one."""
        session, key = session_and_key
        k = await key("v_absent")

        await session.upsert(k).put({"scalar": 1}).execute()

        bins = await _read_bins(session, k)
        assert "embedding" not in bins
        assert bins["scalar"] == 1

    async def test_overwrite_replaces_vector_including_element_type(self, session_and_key):
        """Re-putting a bin replaces the vector wholesale, element type and all."""
        session, key = session_and_key
        k = await key("v_overwrite")

        await session.upsert(k).put(
            {"embedding": Vector([1, 2, 3], VectorElementType.INT32)},
        ).execute()
        assert (await _read_bin(session, k, "embedding")).element_type == \
            VectorElementType.INT32

        await session.upsert(k).put(
            {"embedding": Vector([9.0, 8.0], VectorElementType.FLOAT64)},
        ).execute()

        vector = await _read_bin(session, k, "embedding")
        assert vector.element_type == VectorElementType.FLOAT64
        assert vector.dimensions == 2
        assert list(vector.value) == pytest.approx([9.0, 8.0])


# ---------------------------------------------------------------------------
# Nesting inside CDT list/map bins
# ---------------------------------------------------------------------------

class TestVectorInCdt:

    async def test_vector_in_list_and_map(self, session_and_key):
        session, key = session_and_key
        k = await key("v_cdt")

        await (
            session.upsert(k)
            .bin("embeddings").list_append(Vector([0.1, 0.2]))
            # Bin names cap at 15 chars server-side; keep well under it.
            .bin("emb_by_model").map_upsert_items({"m1": Vector([0.3, 0.4])})
            .execute()
        )

        bins = await _read_bins(session, k)

        list_vec = bins["embeddings"][0]
        assert isinstance(list_vec, Vector)
        assert list(list_vec.value) == pytest.approx([0.1, 0.2])

        map_vec = bins["emb_by_model"]["m1"]
        assert isinstance(map_vec, Vector)
        assert list(map_vec.value) == pytest.approx([0.3, 0.4])

    async def test_vector_in_map_value_via_put(self, session_and_key):
        session, key = session_and_key
        k = await key("v_map_put")
        v = Vector([1.5, -2.5], VectorElementType.FLOAT64)

        await session.upsert(k).put({"m": {"embedding": v, "count": 3}}).execute()

        got = await _read_bin(session, k, "m")
        assert got["embedding"] == v
        assert got["count"] == 3


# ---------------------------------------------------------------------------
# Size / wire-length boundaries
# ---------------------------------------------------------------------------

class TestVectorSize:

    async def test_large_vector_round_trips(self, session_and_key):
        session, key = session_and_key
        k = await key("v_large")
        v = Vector([i * 0.5 for i in range(4096)], VectorElementType.FLOAT32)

        await session.upsert(k).put({"embedding": v}).execute()

        assert await _read_bin(session, k, "embedding") == v

    async def test_vector_crossing_16bit_msgpack_boundary_in_list(self, session_and_key):
        """A vector whose wire size exceeds 65535 bytes, nested in a list bin,
        exercises the extended msgpack length header."""
        session, key = session_and_key
        k = await key("v_16bit")
        v = Vector([i * 0.25 for i in range(16_384)], VectorElementType.FLOAT64)
        assert v.dimensions * 8 + 8 > 65_535

        await session.upsert(k).bin("l").list_append(v).execute()

        assert (await _read_bin(session, k, "l"))[0] == v


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestVectorEdgeCases:

    async def test_empty_vector_rejected_before_write(self, session_and_key):
        """Empty vectors can't exist; the error is raised client-side at
        construction, so nothing is ever sent to the server."""
        session, key = session_and_key
        await key("v_empty")  # reserve/clean the key even though we never write
        with pytest.raises(PacValueError, match="at least 1 dimension"):
            Vector([])

    async def test_int32_min_max_boundaries(self, session_and_key):
        session, key = session_and_key
        k = await key("v_i32_bounds")
        v = Vector([-2147483648, 2147483647, 0], VectorElementType.INT32)

        await session.upsert(k).put({"embedding": v}).execute()

        assert list((await _read_bin(session, k, "embedding")).value) == \
            [-2147483648, 2147483647, 0]

    async def test_list_value_written_readable_as_numpy_and_back(self, session_and_key):
        """A value written from a plain list reads back identically whether
        accessed as ``.value`` (list) or ``.numpy_value`` (array)."""
        pytest.importorskip("numpy")
        session, key = session_and_key
        k = await key("v_cross_read")

        await session.upsert(k).bin("e").set_to_vector([0.5, 1.5, 2.5]).execute()

        vector = await _read_bin(session, k, "e")
        assert list(vector.value) == pytest.approx([0.5, 1.5, 2.5])
        assert vector.numpy_value.tolist() == pytest.approx([0.5, 1.5, 2.5])
        # Reconstructing from the numpy readback equals the stored vector.
        assert Vector(vector.numpy_value) == vector


# ---------------------------------------------------------------------------
# Type churn: overwrite a bin across element types and value kinds
# ---------------------------------------------------------------------------

class TestVectorTypeChurn:
    """A vector bin is not pinned to VECTOR once written; the last write wins.
    Mirrors the rust core's ``a_vector_bin_can_be_replaced_by_a_scalar_and_back``.
    """

    async def test_overwrite_same_type_new_dimensions(self, session_and_key):
        session, key = session_and_key
        k = await key("v_churn_dims")

        await session.upsert(k).put(
            {"embedding": Vector([1.0, 2.0], VectorElementType.FLOAT32)},
        ).execute()
        await session.upsert(k).put(
            {"embedding": Vector([9.0, 8.0, 7.0, 6.0], VectorElementType.FLOAT32)},
        ).execute()

        vector = await _read_bin(session, k, "embedding")
        assert vector.dimensions == 4
        assert list(vector.value) == pytest.approx([9.0, 8.0, 7.0, 6.0])

    async def test_scalar_then_vector_then_string(self, session_and_key):
        session, key = session_and_key
        k = await key("v_churn_kind")
        v = Vector([1.0, 2.0], VectorElementType.FLOAT32)

        await session.upsert(k).put({"b": 42}).execute()
        assert (await _read_bin(session, k, "b")) == 42

        await session.upsert(k).put({"b": v}).execute()
        assert (await _read_bin(session, k, "b")) == v

        await session.upsert(k).put({"b": "text"}).execute()
        assert (await _read_bin(session, k, "b")) == "text"


# ---------------------------------------------------------------------------
# Bin projection on reads includes/excludes vector bins as requested
# ---------------------------------------------------------------------------

class TestVectorBinSelection:

    async def test_read_only_the_vector_bin(self, session_and_key):
        session, key = session_and_key
        k = await key("v_select_only")
        v = Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)
        await session.upsert(k).put({"v": v, "scalar": 1}).execute()

        result = await session.query(k).bins(["v"]).execute()
        bins = (await result.first_or_raise()).record_or_raise().bins

        assert bins["v"] == v
        assert "scalar" not in bins

    async def test_read_excluding_the_vector_bin(self, session_and_key):
        session, key = session_and_key
        k = await key("v_select_excl")
        v = Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)
        await session.upsert(k).put({"v": v, "scalar": 1}).execute()

        result = await session.query(k).bins(["scalar"]).execute()
        bins = (await result.first_or_raise()).record_or_raise().bins

        assert bins["scalar"] == 1
        assert "v" not in bins


# ---------------------------------------------------------------------------
# Record lifecycle: exists / touch / delete / generation
# ---------------------------------------------------------------------------

class TestVectorRecordLifecycle:
    """Record-lifecycle ops behave normally for records carrying vector bins."""

    async def test_exists_touch_delete(self, session_and_key):
        session, key = session_and_key
        k = await key("v_lifecycle")
        v = Vector([1.0, 2.0], VectorElementType.FLOAT32)

        await session.upsert(k).put({"v": v}).execute()

        exists = await (await session.exists(k).execute()).first()
        assert exists is not None and exists.as_bool() is True

        await session.touch(k).execute()
        assert (await _read_bin(session, k, "v")) == v

        await session.delete(k).execute()
        after = await (await session.exists(k).include_missing_keys().execute()).first()
        assert after is not None and after.as_bool() is False

    async def test_generation_increments_on_update(self, session_and_key):
        session, key = session_and_key
        k = await key("v_generation")

        await session.upsert(k).put(
            {"v": Vector([1.0], VectorElementType.FLOAT32)},
        ).execute()
        gen1 = (await _read_record(session, k)).generation

        await session.upsert(k).put(
            {"v": Vector([2.0, 3.0], VectorElementType.FLOAT32)},
        ).execute()
        rec2 = await _read_record(session, k)

        assert rec2.generation > gen1
        assert rec2.bins["v"] == Vector([2.0, 3.0], VectorElementType.FLOAT32)


# ---------------------------------------------------------------------------
# Batch: vector bins round-trip across multiple keys
# ---------------------------------------------------------------------------

class TestVectorBatch:
    """Batch reads/exists round-trip vector bins across multiple keys.
    Mirrors the rust core's ``batch_read_returns_records_with_vector_bins``.
    """

    async def test_batch_read_returns_vector_bins(self, session_and_key):
        session, key = session_and_key
        keys = [await key(f"v_batch_{i}") for i in range(3)]
        vecs = [
            Vector([float(i), float(i) + 0.5], VectorElementType.FLOAT32)
            for i in range(3)
        ]
        for k, v in zip(keys, vecs):
            await session.upsert(k).put({"v": v, "i": 1}).execute()

        stream = await session.query(*keys).bins(["v"]).execute()
        results = await stream.collect()

        assert len(results) == 3
        by_value = {tuple(r.record_or_raise().bins["v"].value) for r in results}
        assert by_value == {tuple(v.value) for v in vecs}

    async def test_batch_exists_with_vector_records(self, session_and_key):
        session, key = session_and_key
        present = await key("v_batch_present")
        missing = await key("v_batch_missing")
        await session.upsert(present).put(
            {"v": Vector([1.0], VectorElementType.FLOAT32)},
        ).execute()

        stream = await (
            session.exists(present, missing).include_missing_keys().execute()
        )
        results = await stream.collect()

        assert results[0].as_bool() is True
        assert results[1].as_bool() is False


# ---------------------------------------------------------------------------
# Element-type fidelity across the round trip
# ---------------------------------------------------------------------------

_STORAGE_TYPES = [
    (VectorElementType.FLOAT32, [0.5, -1.5, 2.0]),
    (VectorElementType.FLOAT64, [0.25, -0.5, 1.0]),
    (VectorElementType.INT32, [-5, 0, 7]),
]


class TestVectorAllElementTypesInOneRecord:

    async def test_all_four_element_types_in_one_record(self, session_and_key):
        """A record with a bin per element type (float16 needs numpy)
        round-trips every bin independently."""
        np = pytest.importorskip("numpy")
        session, key = session_and_key
        k = await key("v_all_types")

        bins = {
            "f16": Vector(np.array([1.0, -2.5, 3.5], dtype=np.float16)),
            "f32": Vector([0.5, -1.5, 2.0], VectorElementType.FLOAT32),
            "f64": Vector([0.25, -0.5, 1.0], VectorElementType.FLOAT64),
            "i32": Vector([-5, 0, 7], VectorElementType.INT32),
        }
        await session.upsert(k).put(bins).execute()

        got = await _read_bins(session, k)
        for name, expected in bins.items():
            assert got[name] == expected, name
            assert got[name].element_type == expected.element_type, name


class TestVectorElementTypeFidelity:
    """Element-type and dimension edge cases on the round trip. Mirrors the
    rust core's ``element_type_is_preserved_distinctly_through_the_server`` and
    ``signed_zero_and_non_finite_survive_the_server_bit_exact``."""

    @pytest.mark.parametrize("element_type, values", _STORAGE_TYPES)
    async def test_single_dimension_vector_round_trips(
        self, session_and_key, element_type, values,
    ):
        session, key = session_and_key
        k = await key(f"v_one_dim_{element_type}")
        v = Vector([values[0]], element_type)
        assert v.dimensions == 1

        await session.upsert(k).put({"v": v}).execute()

        got = await _read_bin(session, k, "v")
        assert got == v
        assert got.dimensions == 1
        assert got.element_type == element_type

    async def test_same_value_preserves_element_type_distinctly(self, session_and_key):
        """The literal "one" stored as four element types stays distinct: the
        element-type byte is not coalesced, so two bins holding the same number
        with different element types compare unequal after the round trip."""
        np = pytest.importorskip("numpy")
        session, key = session_and_key
        k = await key("v_distinct_types")

        await session.upsert(k).put({
            "f16": Vector(np.array([1.0], dtype=np.float16)),
            "i32": Vector([1], VectorElementType.INT32),
            "f32": Vector([1.0], VectorElementType.FLOAT32),
            "f64": Vector([1.0], VectorElementType.FLOAT64),
        }).execute()

        bins = await _read_bins(session, k)
        assert bins["f16"].element_type == VectorElementType.FLOAT16
        assert bins["i32"].element_type == VectorElementType.INT32
        assert bins["f32"].element_type == VectorElementType.FLOAT32
        assert bins["f64"].element_type == VectorElementType.FLOAT64

        # Same number, different element type => not equal after the round trip.
        assert bins["f32"] != bins["f64"]
        assert bins["f32"] != bins["i32"]

    async def test_signed_zero_stays_distinct_from_positive_zero(self, session_and_key):
        """-0.0 must not be flattened to +0.0 by the round trip (guards the
        IEEE-754 bit pattern)."""
        np = pytest.importorskip("numpy")
        session, key = session_and_key
        k = await key("v_signed_zero")

        await session.upsert(k).put(
            {"v": Vector(np.array([-0.0], dtype=np.float32))},
        ).execute()

        got = (await _read_bin(session, k, "v")).numpy_value
        assert got.tobytes() == np.array([-0.0], dtype=np.float32).tobytes()
        assert got.tobytes() != np.array([0.0], dtype=np.float32).tobytes()

    async def test_int32_preserves_full_signed_range(self, session_and_key):
        session, key = session_and_key
        k = await key("v_i32_full_range")
        v = Vector([-2147483648, -1, 0, 1, 2147483647], VectorElementType.INT32)

        await session.upsert(k).put({"embedding": v}).execute()

        assert (await _read_bin(session, k, "embedding")) == v


# ---------------------------------------------------------------------------
# Deeper CDT nesting: every type in list/map, map-in-list, list-in-map
# ---------------------------------------------------------------------------

class TestVectorNestedAllTypes:

    @pytest.mark.parametrize("element_type, values", _STORAGE_TYPES)
    async def test_each_type_nested_in_list(self, session_and_key, element_type, values):
        session, key = session_and_key
        k = await key(f"v_nest_list_{element_type}")
        v = Vector(values, element_type)

        await session.upsert(k).put({"l": ["head", v, 0]}).execute()

        assert (await _read_bin(session, k, "l"))[1] == v

    @pytest.mark.parametrize("element_type, values", _STORAGE_TYPES)
    async def test_each_type_nested_in_map(self, session_and_key, element_type, values):
        session, key = session_and_key
        k = await key(f"v_nest_map_{element_type}")
        v = Vector(values, element_type)

        await session.upsert(k).put({"m": {"embedding": v, "n": 1}}).execute()

        assert (await _read_bin(session, k, "m"))["embedding"] == v

    async def test_vector_in_map_in_list(self, session_and_key):
        session, key = session_and_key
        k = await key("v_map_in_list")
        v = Vector([1.0, 2.0, 3.0], VectorElementType.FLOAT32)

        await session.upsert(k).put({"l": [{"embedding": v}]}).execute()

        assert (await _read_bin(session, k, "l"))[0]["embedding"] == v

    async def test_vector_in_list_in_map(self, session_and_key):
        session, key = session_and_key
        k = await key("v_list_in_map")
        v = Vector([1, 2, 3], VectorElementType.INT32)

        await session.upsert(k).put({"m": {"items": [v]}}).execute()

        assert (await _read_bin(session, k, "m"))["items"][0] == v


# ---------------------------------------------------------------------------
# Additional size / wire-length boundary: top-level (non-nested) vector
# ---------------------------------------------------------------------------

class TestVectorTopLevelSize:

    async def test_top_level_vector_crossing_16bit_length_boundary(self, session_and_key):
        """A top-level vector *bin* (not nested) whose particle exceeds the
        16-bit msgpack length boundary round-trips. 9000 f64 elements =>
        8 + 9000*8 = 72008 bytes, past 65_535. Mirrors the rust core's
        ``large_vector_crossing_16bit_length_boundary_round_trips``."""
        session, key = session_and_key
        k = await key("v_toplevel_16bit")
        v = Vector([i * 0.5 for i in range(9000)], VectorElementType.FLOAT64)
        assert v.dimensions * 8 + 8 > 65_535

        await session.upsert(k).put({"embedding": v}).execute()

        assert (await _read_bin(session, k, "embedding")) == v
