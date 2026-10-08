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

"""``limit()`` and partition filters select which keys a key query reads.

Keys are selected client-side before anything is sent: the partition filter
first, then the limit, across chained reads in order. A chain that also
writes or calls a UDF is rejected, since the dropped keys' writes would
silently never run.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aerospike_async import Key, PartitionFilter, ReadPolicy

from aerospike_sdk import Behavior
from aerospike_sdk.aio.operations.query import QueryBuilder
from aerospike_sdk.sync.operations.query import QueryBuilder as SyncQueryBuilder

_RUNTIMES = pytest.mark.parametrize("qb_cls", [QueryBuilder, SyncQueryBuilder], ids=["async", "sync"])

_KEYS = [Key("test", "t", i) for i in range(1, 8)]


def _builder(qb_cls, *keys, client=None, **kwargs):
    qb = qb_cls(
        client=client if client is not None else MagicMock(),
        namespace="test",
        set_name="t",
        behavior=Behavior.DEFAULT,
        **kwargs,
    )
    qb._set_current_keys_from_varargs(keys)
    return qb


def _selected(qb):
    qb._finalize_chain()
    return [spec.keys for spec in qb._specs]


def _keys_split_by_partition():
    """Return (inside, outside, range) where *range* covers exactly the *inside* keys."""
    by_partition = sorted(_KEYS, key=lambda k: k.partition_id)
    pivot = by_partition[len(by_partition) // 2].partition_id
    inside = [k for k in _KEYS if k.partition_id >= pivot]
    outside = [k for k in _KEYS if k.partition_id < pivot]
    return inside, outside, (pivot, 4096)


async def _execute(qb):
    if isinstance(qb, SyncQueryBuilder):
        return qb.execute().collect()
    return await (await qb.execute()).collect()


@_RUNTIMES
def test_limit_keeps_keys_in_order(qb_cls):
    k1, k3, k5, k7 = _KEYS[0], _KEYS[2], _KEYS[4], _KEYS[6]
    assert _selected(_builder(qb_cls, k1, k3, k5, k7).limit(3)) == [[k1, k3, k5]]


@_RUNTIMES
def test_limit_across_chained_reads(qb_cls):
    """Whole operations while the budget lasts, the next one cut short, the rest dropped."""
    k1, k2, k3, k4, k5, k6 = _KEYS[:6]
    qb = _builder(qb_cls, k1, k2).query(k3, k4).query(k5, k6).limit(3)
    assert _selected(qb) == [[k1, k2], [k3]]


@_RUNTIMES
def test_partition_range_keeps_only_keys_in_range(qb_cls):
    inside, _outside, (begin, end) = _keys_split_by_partition()
    qb = _builder(qb_cls, *_KEYS).on_partition_range(begin, end)
    assert _selected(qb) == [inside]


@_RUNTIMES
def test_partition_filter_applies_before_limit(qb_cls):
    inside, outside, (begin, end) = _keys_split_by_partition()
    qb = _builder(qb_cls, outside[0], *inside).limit(1).on_partition_range(begin, end)
    assert _selected(qb) == [[inside[0]]]


@_RUNTIMES
def test_partition_range_filter_object_is_a_range_check(qb_cls):
    inside, _outside, (begin, end) = _keys_split_by_partition()
    qb = _builder(qb_cls, *_KEYS).partition(PartitionFilter.by_range(begin, end - begin))
    assert _selected(qb) == [inside]


@_RUNTIMES
def test_partition_filter_with_resume_state_raises(qb_cls):
    qb = _builder(qb_cls, *_KEYS).partition(PartitionFilter.by_key(_KEYS[0]))
    with pytest.raises(ValueError, match="resume state"):
        qb._finalize_chain()


@_RUNTIMES
@pytest.mark.parametrize("narrow", [
    pytest.param(lambda qb: qb.limit(1), id="limit"),
    pytest.param(lambda qb: qb.on_partition(0), id="partition"),
])
@pytest.mark.parametrize("add_write", [
    pytest.param(lambda qb: qb.upsert(_KEYS[5]).bin("x").set_to(1)._qb, id="write"),
    pytest.param(
        lambda qb: qb.execute_udf(_KEYS[5]).function("pkg", "fn")._qb, id="udf",
    ),
])
def test_key_selection_with_a_write_or_udf_raises(qb_cls, narrow, add_write):
    qb = add_write(narrow(_builder(qb_cls, _KEYS[0], _KEYS[1])))
    qb._finalize_udf_spec()
    with pytest.raises(ValueError, match="write or UDF"):
        qb._finalize_chain()


@_RUNTIMES
async def test_no_keys_in_range_returns_nothing_without_a_dataset_query(qb_cls):
    _inside, outside, (begin, end) = _keys_split_by_partition()
    qb = _builder(qb_cls, *outside).on_partition_range(begin, end)
    with (
        patch.object(QueryBuilder, "_execute_dataset_query", side_effect=AssertionError),
        patch.object(
            SyncQueryBuilder, "_execute_dataset_query_blocking", side_effect=AssertionError,
        ),
    ):
        assert await _execute(qb) == []


@_RUNTIMES
async def test_single_key_fast_path_honors_partition_filter(qb_cls):
    """A point read outside the partition is not read, even on the direct path."""
    client = MagicMock()
    client.get = AsyncMock()
    key = _KEYS[0]
    other = (key.partition_id + 1) % 4096
    qb = _builder(
        qb_cls, key, client=client,
        cached_read_policy=ReadPolicy(), cached_read_policy_sc=ReadPolicy(),
    ).on_partition(other)
    assert await _execute(qb) == []
    client.get.assert_not_called()
    client.get_blocking.assert_not_called()
