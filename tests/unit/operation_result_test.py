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

"""Unit tests for the typed-accessor wrapper :class:`OperationResult`.

Each accessor must:

* return the right type for matching values,
* return the documented default (``0``, ``0.0``, ``False``) for ``None``,
* propagate ``None`` for string / bytes / list / map (no useful default),
* raise :class:`TypeError` on type mismatch.
"""

import pytest

from aerospike_sdk import OperationResult


class TestNumericAccessors:

    @pytest.mark.parametrize("value,expected", [
        (42, 42),
        (0, 0),
        (-7, -7),
        (True, 1),
        (False, 0),
        (None, 0),
    ])
    def test_get_long(self, value, expected):
        assert OperationResult(value).get_long() == expected

    def test_get_int_alias(self):
        assert OperationResult(42).get_int() == 42

    @pytest.mark.parametrize("bad", ["str", 1.5, [1], {1: 2}, b"x"])
    def test_get_long_rejects_non_int(self, bad):
        with pytest.raises(TypeError, match="not int"):
            OperationResult(bad).get_long()

    @pytest.mark.parametrize("value,expected", [
        (3.14, 3.14),
        (42, 42.0),       # int widens
        (True, 1.0),      # bool widens
        (None, 0.0),
    ])
    def test_get_double(self, value, expected):
        assert OperationResult(value).get_double() == expected

    def test_get_float_alias(self):
        assert OperationResult(2.5).get_float() == 2.5

    @pytest.mark.parametrize("bad", ["str", [1], {1: 2}, b"x"])
    def test_get_double_rejects_non_numeric(self, bad):
        with pytest.raises(TypeError, match="not float"):
            OperationResult(bad).get_double()


class TestBooleanAccessor:

    @pytest.mark.parametrize("value,expected", [
        (True, True),
        (False, False),
        (1, True),         # legacy long-as-bool
        (0, False),
        (5, True),
        (None, False),
    ])
    def test_get_bool(self, value, expected):
        assert OperationResult(value).get_bool() is expected

    @pytest.mark.parametrize("bad", ["true", 1.5, [1], {1: 2}, b"x"])
    def test_get_bool_rejects_non_int_bool(self, bad):
        with pytest.raises(TypeError, match="not bool"):
            OperationResult(bad).get_bool()


class TestStringAccessor:

    def test_get_string_returns_str(self):
        assert OperationResult("hello").get_string() == "hello"

    def test_get_string_propagates_none(self):
        assert OperationResult(None).get_string() is None

    @pytest.mark.parametrize("bad", [42, 3.14, True, [1], {1: 2}, b"bytes"])
    def test_get_string_rejects_non_str(self, bad):
        with pytest.raises(TypeError, match="not str"):
            OperationResult(bad).get_string()


class TestBytesAccessor:

    def test_get_bytes_returns_bytes(self):
        assert OperationResult(b"data").get_bytes() == b"data"

    def test_get_bytes_converts_bytearray(self):
        assert OperationResult(bytearray(b"data")).get_bytes() == b"data"

    def test_get_bytes_propagates_none(self):
        assert OperationResult(None).get_bytes() is None

    @pytest.mark.parametrize("bad", [42, "str", [1], {1: 2}])
    def test_get_bytes_rejects_non_bytes(self, bad):
        with pytest.raises(TypeError, match="not bytes"):
            OperationResult(bad).get_bytes()


class TestCollectionAccessors:

    def test_get_list_returns_list(self):
        v = [1, 2, 3]
        assert OperationResult(v).get_list() == v

    def test_get_list_propagates_none(self):
        assert OperationResult(None).get_list() is None

    @pytest.mark.parametrize("bad", [42, "str", {1: 2}, (1, 2)])
    def test_get_list_rejects_non_list(self, bad):
        with pytest.raises(TypeError, match="not list"):
            OperationResult(bad).get_list()

    def test_get_map_returns_dict(self):
        v = {"a": 1}
        assert OperationResult(v).get_map() == v

    def test_get_map_propagates_none(self):
        assert OperationResult(None).get_map() is None

    @pytest.mark.parametrize("bad", [42, "str", [1], (1, 2)])
    def test_get_map_rejects_non_dict(self, bad):
        with pytest.raises(TypeError, match="not dict"):
            OperationResult(bad).get_map()


class TestRawValueAndDunders:

    def test_value_returns_raw(self):
        v = {"complex": ["object"]}
        assert OperationResult(v).value is v  # identity, no copy

    def test_repr(self):
        assert repr(OperationResult(42)) == "OperationResult(42)"

    def test_eq_compares_value(self):
        assert OperationResult(1) == OperationResult(1)
        assert OperationResult(1) != OperationResult(2)
        assert OperationResult(1) != 1  # different type

    def test_hashable_for_simple_values(self):
        assert hash(OperationResult(1)) == hash(OperationResult(1))
        assert hash(OperationResult("x")) == hash(OperationResult("x"))

    def test_hash_falls_back_to_identity_for_unhashable(self):
        # Lists are unhashable — wrapper falls back to id() rather than raising
        a = OperationResult([1, 2])
        # hash() must succeed
        assert isinstance(hash(a), int)
