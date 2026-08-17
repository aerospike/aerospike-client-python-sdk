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

"""Unit tests for ``set_to_vector(...)`` and the ``Vector`` value type.

Covers the builder surface, ``Vector`` construction and element-type
defaults, invalid input, value semantics (equality, unhashability, ``len``),
and numpy-array construction/readback (including FLOAT16, which requires
numpy). Server round-trips are in ``tests/integration/async/vector_test.py``.
"""

import subprocess
import sys
import textwrap

import pytest

from aerospike_sdk import Key, Vector, VectorElementType

from aerospike_sdk.sync.operations.query import SyncWriteBinBuilder

from aerospike_sdk.aio.operations.query import (
    QueryBuilder,
    WriteBinBuilder,
    WriteSegmentBuilder,
)


def _make_qb() -> QueryBuilder:
    return QueryBuilder(client=object(), namespace="test", set_name="unit")


def _make_key() -> Key:
    return Key("test", "unit", 1)


def _bin_builder(bin_name: str) -> tuple[QueryBuilder, WriteSegmentBuilder, WriteBinBuilder]:
    qb = _make_qb()
    qb._single_key = _make_key()
    segment = WriteSegmentBuilder(qb)
    return qb, segment, WriteBinBuilder(segment, bin_name)


# ---------------------------------------------------------------------------
# Builder: set_to_vector
# ---------------------------------------------------------------------------

class TestWriteBinBuilderSetToVector:

    def test_queues_put_with_vector_value(self):
        qb, segment, wbb = _bin_builder("embedding")

        result = wbb.set_to_vector([1.0, 2.0, 3.0])

        assert result is segment
        assert len(qb._operations) == 1
        # The queued Operation.put wraps a Vector value; the call would fail
        # if PAC rejected the type.

    def test_queues_put_with_explicit_element_type(self):
        qb, segment, wbb = _bin_builder("embedding")

        result = wbb.set_to_vector([1, 2, 3], VectorElementType.INT32)

        assert result is segment
        assert len(qb._operations) == 1

    def test_accepts_existing_vector_instance(self):
        qb, segment, wbb = _bin_builder("embedding")

        result = wbb.set_to_vector(Vector([1.0, 2.0]))

        assert result is segment
        assert len(qb._operations) == 1

    def test_chaining_to_next_bin(self):
        qb = _make_qb()
        qb._single_key = _make_key()
        segment = WriteSegmentBuilder(qb)
        result = (
            WriteBinBuilder(segment, "embedding")
            .set_to_vector([1.0, 2.0, 3.0])
            .bin("name").set_to("alpha")
        )
        assert result is segment
        assert len(qb._operations) == 2

    def test_invalid_data_raises_at_call_site(self):
        # Vector(...) is constructed eagerly, so bad input raises here, not at execute().
        _, _, wbb = _bin_builder("embedding")
        with pytest.raises(TypeError):
            wbb.set_to_vector([1.5, 2.5], VectorElementType.INT32)

    def test_float16_rejected_at_call_site(self):
        _, _, wbb = _bin_builder("embedding")
        with pytest.raises(TypeError):
            wbb.set_to_vector([1.0, 2.0], VectorElementType.FLOAT16)

    def test_accepts_numpy_array(self):
        np = pytest.importorskip("numpy")
        qb, segment, wbb = _bin_builder("embedding")

        result = wbb.set_to_vector(np.array([1.0, 2.0, 3.0], dtype=np.float32))

        assert result is segment
        assert len(qb._operations) == 1

    def test_accepts_float16_numpy_array(self):
        np = pytest.importorskip("numpy")
        qb, segment, wbb = _bin_builder("embedding")

        result = wbb.set_to_vector(np.array([1.0, 2.0], dtype=np.float16))

        assert result is segment
        assert len(qb._operations) == 1


class TestSyncWriteBinBuilderSetToVector:

    def test_method_exists_and_returns_segment(self):
        # SyncWriteBinBuilder wraps the async builder; just verify the
        # method exists and is callable. Behavior is covered by the
        # async tests above plus the integration test.
        assert hasattr(SyncWriteBinBuilder, "set_to_vector")
        assert callable(SyncWriteBinBuilder.set_to_vector)


# ---------------------------------------------------------------------------
# Vector construction and defaults
# ---------------------------------------------------------------------------

class TestVectorConstruction:
    """Direct ``Vector(...)`` construction, re-exported from ``aerospike_sdk``."""

    def test_default_element_type_is_float32(self):
        v = Vector([1.0, 2.0, 3.0])
        assert v.element_type == VectorElementType.FLOAT32
        assert v.dimensions == 3
        assert list(v.value) == [1.0, 2.0, 3.0]

    def test_default_coerces_integers_to_float32(self):
        # No element_type => FLOAT32, so integer inputs come back as floats.
        v = Vector([1, 2, 3])
        assert v.element_type == VectorElementType.FLOAT32
        assert list(v.value) == [1.0, 2.0, 3.0]
        assert all(isinstance(x, float) for x in v.value)

    def test_float64_element_type(self):
        v = Vector([1.5, 2.5], VectorElementType.FLOAT64)
        assert v.element_type == VectorElementType.FLOAT64
        assert list(v.value) == [1.5, 2.5]

    def test_int32_preserves_integers(self):
        v = Vector([1, 2, 3], VectorElementType.INT32)
        assert v.element_type == VectorElementType.INT32
        assert list(v.value) == [1, 2, 3]
        assert all(isinstance(x, int) for x in v.value)

    def test_empty_vector(self):
        v = Vector([])
        assert v.dimensions == 0
        assert list(v.value) == []
        assert len(v) == 0

    def test_copy_construct_from_existing_vector(self):
        original = Vector([1.0, 2.0, 3.0])
        copy = Vector(original)
        assert copy == original
        assert copy.element_type == original.element_type
        assert list(copy.value) == list(original.value)

    def test_copy_construct_preserves_element_type(self):
        original = Vector([1, 2, 3], VectorElementType.INT32)
        copy = Vector(original)
        assert copy.element_type == VectorElementType.INT32
        assert list(copy.value) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Vector invalid input
# ---------------------------------------------------------------------------

class TestVectorInvalidInput:

    def test_non_list_raises(self):
        with pytest.raises(TypeError):
            Vector("not a list")

    def test_none_element_raises(self):
        with pytest.raises(TypeError):
            Vector([1.0, None])

    def test_nested_list_raises(self):
        with pytest.raises(TypeError):
            Vector([[1.0], [2.0]])

    def test_int32_with_float_data_raises(self):
        with pytest.raises(TypeError):
            Vector([1.5, 2.5], VectorElementType.INT32)

    def test_float16_from_list_raises(self):
        # FLOAT16 has no list form; requires a numpy.float16 array.
        with pytest.raises(TypeError):
            Vector([1.0, 2.0], VectorElementType.FLOAT16)


# ---------------------------------------------------------------------------
# Vector value semantics
# ---------------------------------------------------------------------------

class TestVectorSemantics:

    def test_len_matches_dimensions(self):
        v = Vector([1.0, 2.0, 3.0, 4.0])
        assert len(v) == 4
        assert len(v) == v.dimensions

    def test_equality_same_values(self):
        assert Vector([1.0, 2.0]) == Vector([1.0, 2.0])

    def test_inequality_different_values(self):
        assert Vector([1.0, 2.0]) != Vector([1.0, 3.0])

    def test_inequality_different_element_types(self):
        # Same numeric values but different element types are not equal.
        assert Vector([1.0, 2.0], VectorElementType.FLOAT32) != Vector(
            [1.0, 2.0], VectorElementType.FLOAT64,
        )

    def test_inequality_with_non_vector(self):
        assert Vector([1.0, 2.0]) != [1.0, 2.0]
        assert not (Vector([1.0, 2.0]) == [1.0, 2.0])

    def test_not_hashable(self):
        with pytest.raises(TypeError):
            hash(Vector([1.0, 2.0]))

    def test_cannot_be_dict_key(self):
        with pytest.raises(TypeError):
            {Vector([1.0]): "v"}

    def test_str_and_repr(self):
        v = Vector([1.0, 2.0])
        assert "float32" in str(v).lower()
        assert repr(v) == str(v)


class TestVectorElementTypeEnum:

    def test_members_present(self):
        for name in ("FLOAT16", "INT32", "FLOAT32", "FLOAT64"):
            assert hasattr(VectorElementType, name)

    def test_equality_and_identity(self):
        assert VectorElementType.FLOAT32 == VectorElementType.FLOAT32
        assert VectorElementType.FLOAT32 != VectorElementType.INT32


# ---------------------------------------------------------------------------
# Vector: numpy construction and ``.numpy_value`` readback
# ---------------------------------------------------------------------------

# (numpy dtype, element type, values) for the types with both a list and numpy form.
_LISTABLE_TYPES = [
    ("float32", VectorElementType.FLOAT32, [1.0, 2.0, 3.0]),
    ("float64", VectorElementType.FLOAT64, [1.0, 2.0, 3.0]),
    ("int32", VectorElementType.INT32, [1, 2, -3]),
]


class TestVectorNumpyConstruction:

    @pytest.mark.parametrize("np_dtype, element_type, values", _LISTABLE_TYPES)
    def test_dtype_infers_element_type(self, np_dtype, element_type, values):
        np = pytest.importorskip("numpy")
        v = Vector(np.array(values, dtype=np_dtype))
        assert v.element_type == element_type
        assert v.value == values

    @pytest.mark.parametrize("np_dtype, element_type, values", _LISTABLE_TYPES)
    def test_explicit_matching_element_type_accepted(self, np_dtype, element_type, values):
        np = pytest.importorskip("numpy")
        v = Vector(np.array(values, dtype=np_dtype), element_type)
        assert v.element_type == element_type

    def test_float16_requires_numpy_array(self):
        np = pytest.importorskip("numpy")
        v = Vector(np.array([1.0, 2.0, -3.5], dtype=np.float16))
        assert v.element_type == VectorElementType.FLOAT16
        assert v.dimensions == 3

    def test_empty_numpy_array(self):
        np = pytest.importorskip("numpy")
        v = Vector(np.array([], dtype=np.float32))
        assert v.dimensions == 0
        assert len(v) == 0

    def test_non_contiguous_array_is_handled(self):
        np = pytest.importorskip("numpy")
        base = np.array([1.0, 99.0, 2.0, 99.0, 3.0], dtype=np.float32)
        sliced = base[::2]
        assert not sliced.flags["C_CONTIGUOUS"]
        assert Vector(sliced).value == [1.0, 2.0, 3.0]

    def test_numpy_and_list_construction_are_equal(self):
        np = pytest.importorskip("numpy")
        assert Vector(np.array([1.0, 2.0], dtype=np.float32)) == Vector([1.0, 2.0])

    def test_multi_dimensional_array_raises(self):
        np = pytest.importorskip("numpy")
        with pytest.raises(TypeError):
            Vector(np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32))

    def test_zero_d_array_raises(self):
        np = pytest.importorskip("numpy")
        with pytest.raises(TypeError):
            Vector(np.array(5.0, dtype=np.float32))

    @pytest.mark.parametrize("np_dtype", ["int64", "int16", "uint8", "complex64"])
    def test_unsupported_dtype_raises(self, np_dtype):
        np = pytest.importorskip("numpy")
        with pytest.raises(TypeError):
            Vector(np.array([1, 2, 3], dtype=np_dtype))

    @pytest.mark.parametrize(
        "np_dtype, wrong_element_type",
        [
            ("float32", VectorElementType.FLOAT64),
            ("float32", VectorElementType.INT32),
            ("float32", VectorElementType.FLOAT16),
            ("int32", VectorElementType.FLOAT32),
            ("float16", VectorElementType.FLOAT32),
        ],
    )
    def test_element_type_mismatch_raises(self, np_dtype, wrong_element_type):
        np = pytest.importorskip("numpy")
        with pytest.raises(TypeError):
            Vector(np.array([1, 2, 3], dtype=np_dtype), wrong_element_type)


class TestVectorNumpyValue:
    """``.numpy_value`` readback, for all element types."""

    @pytest.mark.parametrize("np_dtype, element_type, values", _LISTABLE_TYPES)
    def test_matches_list_value(self, np_dtype, element_type, values):
        np = pytest.importorskip("numpy")
        v = Vector(values, element_type)
        arr = v.numpy_value
        assert arr.dtype == np.dtype(np_dtype)
        assert arr.tolist() == values

    def test_value_is_plain_list_not_numpy(self):
        pytest.importorskip("numpy")
        v = Vector([1.0, 2.0, 3.0])
        assert type(v.value) is list

    def test_float16_value_raises_use_numpy_value_instead(self):
        np = pytest.importorskip("numpy")
        v = Vector(np.array([1.0, 2.0], dtype=np.float16))
        with pytest.raises(TypeError):
            _ = v.value

    def test_float16_numpy_value_roundtrips(self):
        np = pytest.importorskip("numpy")
        arr_in = np.array([1.0, 2.0, -3.5], dtype=np.float16)
        v = Vector(arr_in)
        out = v.numpy_value
        assert out.dtype == np.float16
        assert out.tolist() == [1.0, 2.0, -3.5]
        assert Vector(out) == v

    def test_float16_special_values_roundtrip(self):
        np = pytest.importorskip("numpy")
        arr_in = np.array([np.inf, -np.inf, 0.0, -0.0, 1.5], dtype=np.float16)
        out = Vector(arr_in).numpy_value
        assert np.array_equal(out, arr_in)

    @pytest.mark.parametrize("np_dtype", ["float16", "float32", "float64", "int32"])
    def test_roundtrip_through_numpy_value(self, np_dtype):
        np = pytest.importorskip("numpy")
        values = [1, 2, 3] if np_dtype == "int32" else [1.0, 2.0, -3.5]
        v = Vector(np.array(values, dtype=np_dtype))
        assert Vector(v.numpy_value) == v


# ---------------------------------------------------------------------------
# Vector when numpy is NOT installed
#
# numpy is optional, so list-based Vectors must keep working without it and
# numpy-only paths must raise clean Python errors (never a Rust panic). Each
# test runs in a subprocess that makes `numpy` unimportable, since it mutates
# process-global `sys.modules`/`sys.meta_path`.
# ---------------------------------------------------------------------------

_BLOCK_NUMPY = """
import sys, importlib.abc

class _BlockNumpy(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path, target=None):
        if name == "numpy" or name.startswith("numpy."):
            raise ImportError("numpy blocked for test")
        return None

for _m in [m for m in list(sys.modules) if m == "numpy" or m.startswith("numpy.")]:
    del sys.modules[_m]
sys.meta_path.insert(0, _BlockNumpy())
"""


def _run_without_numpy(body: str) -> subprocess.CompletedProcess:
    script = _BLOCK_NUMPY + textwrap.dedent(body)
    return subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)


class TestVectorNumpyMissing:

    def test_list_vector_works_without_numpy(self):
        result = _run_without_numpy(
            """
            from aerospike_sdk import Vector, VectorElementType

            assert list(Vector([1.0, 2.0, 3.0]).value) == [1.0, 2.0, 3.0]
            assert list(Vector([1, 2, 3], VectorElementType.INT32).value) == [1, 2, 3]

            try:
                Vector([1.0, 2.0], VectorElementType.FLOAT16)
            except TypeError:
                pass
            else:
                raise AssertionError("expected TypeError for FLOAT16 from list")
            print("OK")
            """,
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout
        assert "PanicException" not in result.stderr

    def test_numpy_value_raises_importerror_without_numpy(self):
        pytest.importorskip("numpy")
        # Build a float16 vector while numpy is available, then make numpy
        # unimportable before exercising the accessors.
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(
                """
                import sys, importlib.abc
                import numpy as np
                from aerospike_sdk import Vector

                v16 = Vector(np.array([1.0, 2.0, -3.5], dtype=np.float16))
                v32 = Vector([1.0, 2.0, 3.0])

                class _BlockNumpy(importlib.abc.MetaPathFinder):
                    def find_spec(self, name, path, target=None):
                        if name == "numpy" or name.startswith("numpy."):
                            raise ImportError("numpy blocked")
                        return None

                for _m in [m for m in list(sys.modules) if m == "numpy" or m.startswith("numpy.")]:
                    del sys.modules[_m]
                sys.meta_path.insert(0, _BlockNumpy())

                assert v16.dimensions == 3  # non-numpy accessors still work

                try:
                    _ = v16.value  # TypeError, numpy-independent
                except TypeError:
                    pass
                else:
                    raise AssertionError("expected TypeError from FLOAT16 .value")

                for v in (v16, v32):
                    try:
                        _ = v.numpy_value  # needs numpy -> ImportError, not a panic
                    except ImportError:
                        pass
                    else:
                        raise AssertionError("expected ImportError from .numpy_value")
                print("OK")
                """,
            )],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout
        assert "PanicException" not in result.stderr


# ---------------------------------------------------------------------------
# Vector through generic CDT write builders
# ---------------------------------------------------------------------------

class TestVectorInCdtOps:
    """Vector values pass through the generic CDT write builders unmodified."""

    def test_list_append_accepts_vector(self):
        qb, segment, wbb = _bin_builder("embeddings")

        result = wbb.list_append(Vector([1.0, 2.0, 3.0]))

        assert result is segment
        assert len(qb._operations) == 1

    def test_list_insert_accepts_vector(self):
        qb, segment, wbb = _bin_builder("embeddings")

        result = wbb.list_insert(0, Vector([1.0, 2.0, 3.0]))

        assert result is segment
        assert len(qb._operations) == 1

    def test_map_upsert_items_accepts_vector(self):
        qb, segment, wbb = _bin_builder("embeddings_by_key")

        result = wbb.map_upsert_items({"a": Vector([1.0, 2.0])})

        assert result is segment
        assert len(qb._operations) == 1
