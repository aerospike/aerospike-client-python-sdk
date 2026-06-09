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

"""Integration tests for synchronous put/get and core SDK operations."""

import pytest
from aerospike_async.exceptions import ResultCode
from aerospike_sdk import DataSet, SyncClient
from aerospike_sdk.exceptions import AerospikeError


@pytest.fixture
def client(aerospike_host, client_policy):
    """Setup sync SDK client for testing."""
    with SyncClient(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        ds = DataSet.of("test", "test")
        try:
            session.delete(ds.id(1)).execute()
        except Exception:
            pass
        yield client


def test_put_get_basic(client):
    """Test basic put and get operations."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"name": "John", "age": 30}).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins["name"] == "John"
    assert record.bins["age"] == 30


def test_put_get_with_dataset(client):
    """Test put and get using DataSet."""
    session = client.create_session()
    users = DataSet.of("test", "test")
    key = users.id(2)
    session.upsert(key).put({"name": "Jane", "age": 28}).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins["name"] == "Jane"


def test_put_get_with_key_object(client):
    """Test put and get using Key object."""
    session = client.create_session()
    users = DataSet.of("test", "test")
    key = users.id(3)
    session.upsert(key).put({"name": "Bob", "age": 35}).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins["name"] == "Bob"


def test_exists(client):
    """Test exists operation."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)

    exists_stream = session.exists(key).execute()
    first = exists_stream.first()
    exists = first.as_bool() if first else False
    assert not exists


def test_delete(client):
    """Test delete operation."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"name": "John"}).execute()

    session.delete(key).execute()

    exists_stream = session.exists(key).execute()
    first = exists_stream.first()
    exists = first.as_bool() if first else False
    assert not exists


def test_get_with_bins(client):
    """Test get with specific bin selection."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"name": "John", "age": 30, "city": "NYC"}).execute()

    result = session.query(key).bins(["name", "age"]).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert "name" in record.bins
    assert "age" in record.bins
    assert "city" not in record.bins


def test_truncate(client):
    """Test that truncate succeeds and new writes after it are readable.

    Truncate is an async server-side operation that may not propagate
    instantly, so we verify the call completes without error and that
    records written *after* the truncate (whose timestamps exceed the
    cutoff) are immediately readable.
    """
    session = client.create_session()
    users = DataSet.of("test", "trunc_test_sync")

    key1 = users.id("trunc_old1")
    key2 = users.id("trunc_old2")

    session.upsert(key1).put({"v": 1}).execute()
    session.upsert(key2).put({"v": 2}).execute()

    client.truncate(users)

    key_new = users.id("trunc_new1")
    session.upsert(key_new).put({"v": 42}).execute()

    result = session.query(key_new).execute().first()
    assert result is not None
    assert result.record.bins["v"] == 42


def test_bin_chaining_set_to(client):
    """Test bin chaining API with set_to."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).bin("name").set_to("Tim").bin("age").set_to(1).bin("gender").set_to("male").execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "Tim", "age": 1, "gender": "male"}


def test_bin_chaining_add(client):
    """Test bin chaining API with add."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"age": 30}).execute()

    session.upsert(key).bin("age").add(1).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins["age"] == 31


def test_bin_chaining_mixed_operations(client):
    """Test bin chaining with both set_to and add."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"name": "Tim", "age": 1}).execute()

    session.upsert(key).bin("name").set_to("Tim Updated").bin("age").add(1).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "Tim Updated", "age": 2}


def test_and_remove_other_bins(client):
    """Test replace removes other bins (equivalent to and_remove_other_bins)."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"name": "Tim", "age": 30, "gender": "male", "city": "NYC"}).execute()

    session.replace(key).put({"name": "Tim Updated", "age": 26}).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "Tim Updated", "age": 26}


def test_set_bins_execute(client):
    """Test set_bins with execute method."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).set_bins({"name": "Tim", "age": 1, "gender": "male"}).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "Tim", "age": 1, "gender": "male"}


def test_with_durable_delete(client):
    """Test delete operations."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"name": "Tim"}).execute()

    session.delete(key).execute()

    stream = session.query(key).execute()
    first = stream.first()
    record = first.record if first and first.is_ok else None
    assert record is None


def test_insert_creates_new_record(client):
    """Test that insert() creates a new record successfully."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.insert(key).put({"name": "Alice", "age": 25}).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "Alice", "age": 25}


def test_insert_fails_if_record_exists(client):
    """Test that insert() fails if record already exists."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.insert(key).put({"name": "Alice"}).execute()

    with pytest.raises(AerospikeError):
        session.insert(key).put({"name": "Bob"}).execute()


def test_update_succeeds_if_record_exists(client):
    """Test that update() succeeds if record exists."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"name": "Alice", "age": 25}).execute()
    session.update(key).put({"age": 26}).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "Alice", "age": 26}


def test_update_fails_if_record_not_exists(client):
    """Test that update() raises KEY_NOT_FOUND_ERROR if record does not exist."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(88888)
    try:
        session.delete(key).execute()
    except Exception:
        pass
    with pytest.raises(AerospikeError) as exc_info:
        session.update(key).put({"name": "Bob"}).execute()
    assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR


def test_replace_succeeds_if_record_exists(client):
    """Test that replace() succeeds if record exists and replaces all bins."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    session.upsert(key).put({"name": "Alice", "age": 25, "city": "NYC"}).execute()
    session.replace(key).put({"name": "Bob"}).execute()

    result = session.query(key).execute().first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "Bob"}
    assert "age" not in record.bins
    assert "city" not in record.bins


def test_replace_if_exists_fails_if_record_not_exists(client):
    """Test that replace_if_exists() raises KEY_NOT_FOUND_ERROR if record does not exist."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(88888)
    try:
        session.delete(key).execute()
    except Exception:
        pass
    with pytest.raises(AerospikeError) as exc_info:
        session.replace_if_exists(key).put({"name": "Bob"}).execute()
    assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR
