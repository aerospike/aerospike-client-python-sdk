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
NUM_KEYS = 200


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


def _seed(session, ds):
    per_partition: Counter = Counter()
    for i in range(NUM_KEYS):
        key = ds.id(f"pk{i}")
        per_partition[_partition_id(key)] += 1
        session.upsert(key).put({"v": i}).execute()
    return per_partition


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
