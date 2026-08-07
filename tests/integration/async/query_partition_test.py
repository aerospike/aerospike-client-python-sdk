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
from tests.integration.namespace import general_namespace

PART_SET = "query_partition"
HOT_SET = "query_partition_hot"
NUM_KEYS = 200

# A single partition holding more records than the limit under test, so the
# limit assertion is exact: one partition means one node, and max_records is
# distributed across nodes.
HOT_PARTITION = 10
HOT_RECORDS = 30
HOT_LIMIT = 18
HOT_CHUNK = 7


def _partition_id(key) -> int:
    """Partition id = low 12 bits of the first two digest bytes (little-endian)."""
    d = bytes.fromhex(key.digest)
    return (d[0] | (d[1] << 8)) & 0xFFF


async def _count(stream) -> int:
    n = 0
    async for _ in stream:
        n += 1
    return n


async def _drain_chunks(stream) -> tuple[int, int]:
    """Drain every chunk; returns (records, chunks).

    Plain iteration yields only the loaded chunk, so advancing the cursor is
    the caller's job via has_more_chunks().
    """
    records = chunks = 0
    while await stream.has_more_chunks():
        chunks += 1
        async for _ in stream:
            records += 1
    stream.close()
    return records, chunks


async def _seed(session, ds):
    """Upsert NUM_KEYS records; return [(partition_id, v)] computed client-side."""
    placed = []
    for i in range(NUM_KEYS):
        key = ds.id(f"pk{i}")
        placed.append((_partition_id(key), i))
        await session.upsert(key).put({"v": i}).execute()
    return placed


def _per_partition(placed) -> Counter:
    return Counter(pid for pid, _ in placed)


async def _seed_hot_partition(session, ds):
    """Upsert HOT_RECORDS records whose digests all map to HOT_PARTITION."""
    inserted = 0
    candidate = 0
    while inserted < HOT_RECORDS:
        key = ds.id(f"hot{candidate}")
        candidate += 1
        if _partition_id(key) != HOT_PARTITION:
            continue
        await session.upsert(key).put({"v": inserted}).execute()
        inserted += 1


async def test_on_partition_returns_exactly_that_partitions_records(cluster):
    """on_partition(p) returns exactly the records whose digest maps to p."""
    session = cluster.create_session()
    ds = DataSet.of(general_namespace(), PART_SET)
    await session.truncate(ds)
    per_partition = _per_partition(await _seed(session, ds))

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
    ds = DataSet.of(general_namespace(), PART_SET)
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
    ds = DataSet.of(general_namespace(), PART_SET)
    await session.truncate(ds)
    per_partition = _per_partition(await _seed(session, ds))

    start, end = 1024, 2048
    expected = sum(n for pid, n in per_partition.items() if start <= pid < end)
    got = await _count(
        await session.query(ds).on_partition_range(start, end).execute()
    )
    assert got == expected


async def test_on_partition_with_chunking_returns_every_record(cluster):
    """Chunked iteration inside a partition restriction covers the partition.

    Each chunk re-executes the query against the advanced partition cursor,
    so a cursor that failed to advance would silently truncate the results.
    """
    session = cluster.create_session()
    ds = DataSet.of(general_namespace(), HOT_SET)
    await session.truncate(ds)
    await _seed_hot_partition(session, ds)

    records, chunks = await _drain_chunks(
        await session.query(ds)
        .on_partition(HOT_PARTITION)
        .bins(["v"])
        .chunk_size(HOT_CHUNK)
        .execute()
    )
    assert records == HOT_RECORDS
    assert chunks == -(-HOT_RECORDS // HOT_CHUNK)  # ceil division


async def test_on_partition_with_limit_and_chunking_stops_at_limit(cluster):
    """A limit must survive chunk boundaries, capping the total returned.

    HOT_LIMIT is deliberately not a multiple of HOT_CHUNK so the cap has to
    take effect mid-chunk.
    """
    session = cluster.create_session()
    ds = DataSet.of(general_namespace(), HOT_SET)
    await session.truncate(ds)
    await _seed_hot_partition(session, ds)

    records, _ = await _drain_chunks(
        await session.query(ds)
        .on_partition(HOT_PARTITION)
        .bins(["v"])
        .limit(HOT_LIMIT)
        .chunk_size(HOT_CHUNK)
        .execute()
    )
    assert records == HOT_LIMIT


async def test_on_partition_range_with_where_returns_matching_subset(cluster):
    """A filter expression and a partition range compose: exactly their intersection."""
    session = cluster.create_session()
    ds = DataSet.of(general_namespace(), PART_SET)
    await session.truncate(ds)
    placed = await _seed(session, ds)

    start, end = 0, 2048
    threshold = 100
    expected = sum(1 for pid, v in placed if start <= pid < end and v >= threshold)
    got = await _count(
        await session.query(ds)
        .on_partition_range(start, end)
        .where(f"$.v >= {threshold}")
        .execute()
    )
    assert got == expected

    # The intersection is a strict subset of what the filter alone matches.
    filtered_total = await _count(
        await session.query(ds).where(f"$.v >= {threshold}").execute()
    )
    assert got <= filtered_total
