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

"""HyperLogLog (HLL) bin configuration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HllConfig:
    """Configuration describing an HLL bin's index and minhash bit widths.

    Used as input to :meth:`~aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_init`
    and :meth:`~aerospike_sdk.aio.operations.query.WriteBinBuilder.hll_add` to size
    a new sketch, and returned from a ``hll_describe()`` round trip via
    :meth:`~aerospike_sdk.record_result.RecordResult.get_hll_config`.

    Args:
        index_bit_count: Number of index bits — controls register count and
            cardinality accuracy. Valid range is 4 to 16 inclusive.
        min_hash_bit_count: Number of minhash bits, or ``-1`` for no minhash.
            Valid range when set is 4 to 51 inclusive. ``index_bit_count +
            min_hash_bit_count`` must not exceed 64.

    Example:
        >>> config = HllConfig.of(14)
        >>> config.index_bit_count
        14
        >>> config.min_hash_bit_count
        -1
        >>> with_minhash = HllConfig.of(12, 20)
        >>> with_minhash.min_hash_bit_count
        20

    See Also:
        :meth:`~aerospike_sdk.record_result.RecordResult.get_hll_config`:
            Construct an ``HllConfig`` from a ``hll_describe()`` result.
    """

    index_bit_count: int
    min_hash_bit_count: int = -1

    @staticmethod
    def of(index_bit_count: int, min_hash_bit_count: int = -1) -> HllConfig:
        """Build an :class:`HllConfig`.

        Args:
            index_bit_count: Index bits, 4–16 inclusive.
            min_hash_bit_count: Minhash bits, 4–51 inclusive, or ``-1`` for no minhash.

        Returns:
            A frozen :class:`HllConfig` value.

        Example:
            >>> HllConfig.of(14)
            HllConfig(index_bit_count=14, min_hash_bit_count=-1)
        """
        return HllConfig(index_bit_count, min_hash_bit_count)
