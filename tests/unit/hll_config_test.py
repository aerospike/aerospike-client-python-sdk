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

"""Unit tests for :class:`HllConfig` and ``_resolve_hll_flags``."""

import dataclasses

import pytest

from aerospike_async import HLLWriteFlags

from aerospike_sdk import HllConfig
from aerospike_sdk.aio.operations.query import _resolve_hll_flags


class TestHllConfig:

    def test_of_index_bits_only(self):
        config = HllConfig.of(14)
        assert config.index_bit_count == 14
        assert config.min_hash_bit_count == -1

    def test_of_with_minhash(self):
        config = HllConfig.of(12, 20)
        assert config.index_bit_count == 12
        assert config.min_hash_bit_count == 20

    def test_constructor_direct(self):
        config = HllConfig(10, -1)
        assert config == HllConfig.of(10)

    def test_equality(self):
        assert HllConfig.of(14) == HllConfig.of(14)
        assert HllConfig.of(14) != HllConfig.of(12)
        assert HllConfig.of(12, 8) != HllConfig.of(12, -1)

    def test_hashable(self):
        # Frozen + slots → usable as a dict key.
        d = {HllConfig.of(14): "first", HllConfig.of(12, 8): "second"}
        assert d[HllConfig.of(14)] == "first"
        assert d[HllConfig.of(12, 8)] == "second"

    def test_frozen(self):
        config = HllConfig.of(14)
        with pytest.raises(dataclasses.FrozenInstanceError):
            config.index_bit_count = 16  # type: ignore[misc]

    def test_repr(self):
        assert repr(HllConfig.of(14)) == "HllConfig(index_bit_count=14, min_hash_bit_count=-1)"


class TestResolveHllFlags:

    def test_no_flags_returns_default(self):
        assert _resolve_hll_flags() == int(HLLWriteFlags.DEFAULT)

    def test_create_only(self):
        assert _resolve_hll_flags(create_only=True) == int(HLLWriteFlags.CREATE_ONLY)

    def test_update_only(self):
        assert _resolve_hll_flags(update_only=True) == int(HLLWriteFlags.UPDATE_ONLY)

    def test_no_fail(self):
        assert _resolve_hll_flags(no_fail=True) == int(HLLWriteFlags.NO_FAIL)

    def test_allow_fold(self):
        assert _resolve_hll_flags(allow_fold=True) == int(HLLWriteFlags.ALLOW_FOLD)

    def test_create_only_plus_no_fail(self):
        expected = int(HLLWriteFlags.CREATE_ONLY) | int(HLLWriteFlags.NO_FAIL)
        assert _resolve_hll_flags(create_only=True, no_fail=True) == expected

    def test_update_only_plus_allow_fold(self):
        expected = int(HLLWriteFlags.UPDATE_ONLY) | int(HLLWriteFlags.ALLOW_FOLD)
        assert _resolve_hll_flags(update_only=True, allow_fold=True) == expected

    def test_create_and_update_only_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _resolve_hll_flags(create_only=True, update_only=True)

    def test_create_and_update_only_raises_even_with_other_flags(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _resolve_hll_flags(
                create_only=True, update_only=True, no_fail=True, allow_fold=True,
            )
