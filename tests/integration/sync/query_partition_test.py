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

"""Sync integration tests for partition-restricted queries (record-count parity).

The sync query dispatch is an independent implementation over the shared
builder base, so the partition-restriction arithmetic gets its own
end-to-end count assertions here.
"""

from collections import Counter

import pytest

from aerospike_sdk import DataSet

PART_SET = "query_partition_sync"
HOT_SET = "query_partition_hot_sync"
NUM_KEYS = 200

HOT_PARTITION = 10
HOT_RECORDS = 30
HOT_LIMIT = 18
HOT_CHUNK = 7


@pytest.fixture
def cluster(aerospike_host, make_cluster_definition):
    """Connected sync cluster for partition-parity tests."""
    with make_cluster_definition(aerospike_host, sync=True).connect() as cluster:
        yield cluster


def _partition_id(key) -> int:
    """Partition id = low 12 bits of the first two digest bytes (little-endian)."""
    d = bytes.fromhex(key.digest)
    return (d[0] | (d[1] << 8)) & 0xFFF


def _count(stream) -> int:
    return sum(1 for _ in stream)


def _drain_chunks(stream) -> int:
    """Drain every chunk; plain iteration alone yields only the loaded chunk."""
    n = 0
    while stream.has_more_chunks():
        n += sum(1 for _ in stream)
    stream.close()
    return n


def _seed(session, ds):
    per_partition: Counter = Counter()
    for i in range(NUM_KEYS):
        key = ds.id(f"pk{i}")
        per_partition[_partition_id(key)] += 1
        session.upsert(key).put({"v": i}).execute()
    return per_partition


def _seed_hot_partition(session, ds):
    """Upsert HOT_RECORDS records whose digests all map to HOT_PARTITION."""
    inserted = 0
    candidate = 0
    while inserted < HOT_RECORDS:
        key = ds.id(f"hot{candidate}")
        candidate += 1
        if _partition_id(key) != HOT_PARTITION:
            continue
        session.upsert(key).put({"v": inserted}).execute()
        inserted += 1


def test_partition_range_halves_partition_the_keyspace(cluster):
    """The two half-ranges are disjoint and together cover every record."""
    session = cluster.create_session()
    ds = DataSet.of("test", PART_SET)
    session.truncate(ds)
    _seed(session, ds)

    total = _count(session.query(ds).execute())
    first = _count(session.query(ds).on_partition_range(0, 2048).execute())
    second = _count(session.query(ds).on_partition_range(2048, 4096).execute())
    assert total == NUM_KEYS
    assert first + second == total


def test_on_partition_returns_exactly_that_partitions_records(cluster):
    """on_partition(p) returns exactly the records whose digest maps to p."""
    session = cluster.create_session()
    ds = DataSet.of("test", PART_SET)
    session.truncate(ds)
    per_partition = _seed(session, ds)

    target, expected = per_partition.most_common(1)[0]
    got = _count(session.query(ds).on_partition(target).execute())
    assert got == expected


def test_on_partition_with_chunking_returns_every_record(cluster):
    """Chunked iteration inside a partition restriction covers the partition.

    Sync chunk advancement is an independent implementation that reads the
    cursor synchronously, so it cannot rely on the async coverage.
    """
    session = cluster.create_session()
    ds = DataSet.of("test", HOT_SET)
    session.truncate(ds)
    _seed_hot_partition(session, ds)

    unlimited = _count(session.query(ds).on_partition(HOT_PARTITION).execute())
    assert unlimited == HOT_RECORDS

    got = _drain_chunks(
        session.query(ds)
        .on_partition(HOT_PARTITION)
        .bins(["v"])
        .chunk_size(HOT_CHUNK)
        .execute()
    )
    assert got == HOT_RECORDS


def test_on_partition_with_limit_and_chunking_stops_at_limit(cluster):
    """A limit must survive chunk boundaries, capping the total returned.

    HOT_LIMIT is deliberately not a multiple of HOT_CHUNK so the cap has to
    take effect mid-chunk.
    """
    session = cluster.create_session()
    ds = DataSet.of("test", HOT_SET)
    session.truncate(ds)
    _seed_hot_partition(session, ds)

    got = _drain_chunks(
        session.query(ds)
        .on_partition(HOT_PARTITION)
        .bins(["v"])
        .limit(HOT_LIMIT)
        .chunk_size(HOT_CHUNK)
        .execute()
    )
    assert got == HOT_LIMIT
