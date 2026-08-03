# Copyright 2026 Aerospike, Inc.
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

"""Integration tests for partition-restricted queries — record-count parity.

These assert *which records come back*, not merely that the call succeeds:
a partition-restriction bug widens or narrows the result set silently, so
the assertions here are exact counts against independently computed
digest → partition assignments.
"""

from collections import Counter

from aerospike_sdk import DataSet

PART_SET = "query_partition"
NUM_KEYS = 200


def _partition_id(key) -> int:
    """Partition id = low 12 bits of the first two digest bytes (little-endian)."""
    d = bytes.fromhex(key.digest)
    return (d[0] | (d[1] << 8)) & 0xFFF


async def _count(stream) -> int:
    n = 0
    async for _ in stream:
        n += 1
    return n


async def _seed(session, ds):
    """Upsert NUM_KEYS records; return per-partition counts computed client-side."""
    per_partition: Counter = Counter()
    for i in range(NUM_KEYS):
        key = ds.id(f"pk{i}")
        per_partition[_partition_id(key)] += 1
        await session.upsert(key).put({"v": i}).execute()
    return per_partition


async def test_on_partition_returns_exactly_that_partitions_records(cluster):
    """on_partition(p) returns exactly the records whose digest maps to p."""
    session = cluster.create_session()
    ds = DataSet.of("test", PART_SET)
    await session.truncate(ds)
    per_partition = await _seed(session, ds)

    # The most-populated partition gives the strongest signal; a neighbor
    # with a different expected count guards against off-by-one widening.
    target, expected = per_partition.most_common(1)[0]
    got = await _count(await session.query(ds).on_partition(target).execute())
    assert got == expected

    neighbor = (target + 1) % 4096
    got_neighbor = await _count(
        await session.query(ds).on_partition(neighbor).execute()
    )
    assert got_neighbor == per_partition.get(neighbor, 0)


async def test_partition_range_halves_partition_the_keyspace(cluster):
    """The two half-ranges are disjoint and together cover every record."""
    session = cluster.create_session()
    ds = DataSet.of("test", PART_SET)
    await session.truncate(ds)
    await _seed(session, ds)

    total = await _count(await session.query(ds).execute())
    first = await _count(
        await session.query(ds).on_partition_range(0, 2048).execute()
    )
    second = await _count(
        await session.query(ds).on_partition_range(2048, 4096).execute()
    )
    assert total == NUM_KEYS
    assert first + second == total


async def test_on_partition_range_matches_computed_assignment(cluster):
    """An interior range returns exactly the records assigned to it."""
    session = cluster.create_session()
    ds = DataSet.of("test", PART_SET)
    await session.truncate(ds)
    per_partition = await _seed(session, ds)

    start, end = 1024, 2048
    expected = sum(n for pid, n in per_partition.items() if start <= pid < end)
    got = await _count(
        await session.query(ds).on_partition_range(start, end).execute()
    )
    assert got == expected


async def test_on_partitions_contiguous_matches_per_partition_sum(cluster):
    """on_partitions over a contiguous trio equals the per-partition sum."""
    session = cluster.create_session()
    ds = DataSet.of("test", PART_SET)
    await session.truncate(ds)
    per_partition = await _seed(session, ds)

    anchor, _ = per_partition.most_common(1)[0]
    ids = [anchor, (anchor + 1) % 4096, (anchor + 2) % 4096]
    if max(ids) - min(ids) != 2:
        ids = [1000, 1001, 1002]  # anchor wrapped; use a fixed interior trio
    expected = sum(per_partition.get(pid, 0) for pid in ids)
    got = await _count(await session.query(ds).on_partitions(*ids).execute())
    assert got == expected
