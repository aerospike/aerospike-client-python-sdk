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

"""``limit()`` and partition filters on key queries, against a server."""

import pytest

from aerospike_sdk import DataSet
from tests.integration.namespace import general_namespace

_IDS = range(1, 8)
_MISSING = 5


@pytest.fixture
async def seeded(cluster):
    session = cluster.create_session()
    ds = DataSet.of(general_namespace(), "query_key_selection")
    for i in _IDS:
        if i == _MISSING:
            await session.delete(ds.id(i)).execute()
        else:
            await session.upsert(ds.id(i)).put({"v": i}).execute()
    return session, ds


async def _ids(stream):
    return [result.key.value for result in await stream.collect()]


async def test_missing_keys_count_toward_the_limit(seeded):
    """Keys past the limit are never read, so a missing key still uses up one slot."""
    session, ds = seeded
    assert await _ids(await session.query(ds.ids(1, 3, _MISSING, 7)).limit(3).execute()) == [1, 3]


async def test_single_key_limit_returns_the_record(seeded):
    session, ds = seeded
    assert await _ids(await session.query(ds.id(1)).limit(1).execute()) == [1]


async def test_partition_range_reads_only_keys_in_range(seeded):
    session, ds = seeded
    keys = [ds.id(i) for i in _IDS if i != _MISSING]
    pivot = sorted(key.partition_id for key in keys)[len(keys) // 2]
    expected = [key.value for key in keys if key.partition_id >= pivot]

    stream = await session.query(keys).on_partition_range(pivot, 4096).execute()

    assert await _ids(stream) == expected
