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

"""Sync integration tests for operate operations.

Covers:
- delete_record() atomically reads bins then deletes the record
- delete_record() followed by a bin write recreates the record with only the new bin
- touch_record() resets TTL within an atomic operate call
"""

import pytest

from aerospike_sdk import DataSet
from tests.integration.namespace import general_namespace


NS = general_namespace()
SET = "operate_record_sync"


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


@pytest.fixture
def session(cluster):
    return cluster.create_session()


@pytest.fixture
def ds():
    return DataSet.of(NS, SET)


def test_delete_record_reads_then_deletes(session, ds):
    """Read a bin and atomically delete the record in one operate call."""
    key = ds.id("del_read")
    session.upsert(key).put({"name": "Alice", "age": 30}).execute()

    stream = (
        session.upsert(key)
        .bin("name").get()
        .delete_record()
        .execute()
    )
    row = stream.first_or_raise()
    assert row.record.bins["name"] == "Alice"

    exists_stream = session.exists(key).execute()
    exists_row = exists_stream.first()
    assert exists_row is None or not exists_row.as_bool()


def test_delete_record_then_write_recreates(session, ds):
    """Delete the record and write a new bin in one atomic operate call."""
    key = ds.id("del_write")
    session.upsert(key).put({"a": 1, "b": 2}).execute()

    stream = (
        session.upsert(key)
        .bin("a").get()
        .delete_record()
        .bin("b").set_to(99)
        .bin("b").get()
        .execute()
    )
    row = stream.first_or_raise()
    assert row.record.bins["a"] == 1

    read_stream = session.query(key).execute()
    read_row = read_stream.first_or_raise()
    read_rec = read_row.record
    assert read_rec.bins["b"] == 99
    assert "a" not in read_rec.bins
    assert len(read_rec.bins) == 1


def test_touch_record_resets_ttl(session, ds):
    """Touch the record to reset its TTL within an atomic operate call."""
    key = ds.id("touch_ttl")
    session.upsert(key).put({"score": 42}).expire_record_after_seconds(60).execute()

    stream = (
        session.upsert(key)
        .bin("score").get()
        .touch_record()
        .expire_record_after_seconds(120)
        .execute()
    )
    row = stream.first_or_raise()
    assert row.record.bins["score"] == 42


def test_scalar_multi_op_results_are_op_aligned(session, ds):
    """get/add/get on one bin through the sync path: one slot per op
    positionally, while the bins view merges the reads and skips the
    write's empty slot."""
    key = ds.id("scalar_positional")
    session.delete(key).execute()
    session.upsert(key).bin("n").set_to(1).execute()

    result = (
        session.upsert(key)
        .bin("n").get()
        .bin("n").add(10)
        .bin("n").get()
        .execute()
    ).first_or_raise()
    rec = result.record_or_raise()

    assert rec.results == [1, None, 11]
    assert rec.bins["n"] == [1, 11]

    assert result.operation_result(1) is None
    assert result.operation_result(2) == 11
    typed = result.typed_operation_result(2)
    assert typed is not None and typed.get_long() == 11

    session.delete(key).execute()
