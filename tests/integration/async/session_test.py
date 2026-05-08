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

"""Tests for Session wrapper."""

import logging

import pytest
from datetime import timedelta

log = logging.getLogger(__name__)

from aerospike_sdk import Behavior, DataSet, Client


@pytest.fixture
async def session(client):
    """Setup session with default behavior for testing."""
    return client.create_session(Behavior.DEFAULT)


async def test_session_creation_default_behavior(client):
    """Test creating a session with default behavior."""
    session = client.create_session()
    assert session is not None
    assert session.behavior.name == "DEFAULT"
    assert session.client is client


async def test_session_creation_custom_behavior(client):
    """Test creating a session with custom behavior."""
    custom_behavior = Behavior.DEFAULT.derive_with_changes(
        name="custom",
        total_timeout=timedelta(seconds=10),
        max_retries=5,
    )
    session = client.create_session(custom_behavior)
    assert session is not None
    assert session.behavior.name == "custom"
    assert session.behavior.total_timeout == timedelta(seconds=10)
    assert session.behavior.max_retries == 5
    assert session.client is client



async def test_session_repr(session):
    """Test session string representation."""
    repr_str = repr(session)
    assert "Session" in repr_str
    assert "DEFAULT" in repr_str


async def test_session_upsert_with_key(session):
    """Test session.upsert() with Key object."""
    users = DataSet.of("test", "users")
    key = users.id("user123")

    await session.upsert(key).put({"name": "John", "age": 30}).execute()

    stream = await session.query(key).execute()
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins == {"name": "John", "age": 30}


async def test_session_upsert_with_dataset(session):
    """Test session.upsert() with DataSet."""
    users = DataSet.of("test", "users")

    await session.upsert(dataset=users, key_value="user456").put(
        {"name": "Jane", "age": 25}
    ).execute()

    key = users.id("user456")
    stream = await session.query(key).execute()
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins == {"name": "Jane", "age": 25}


async def test_session_upsert_with_namespace_set(session):
    """Test session.upsert() with explicit namespace/set."""
    from aerospike_async import Key

    key = Key("test", "users", "user789")
    await session.upsert(
        namespace="test", set_name="users", key_value="user789"
    ).put({"name": "Bob", "age": 35}).execute()

    stream = await session.query(key).execute()
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins == {"name": "Bob", "age": 35}


async def test_session_insert(session):
    """Test session.insert() method."""
    users = DataSet.of("test", "users")
    key = users.id("insert_test")

    try:
        await session.delete(key).execute()
    except Exception:
        pass

    await session.insert(key).put({"name": "Insert", "value": 1}).execute()

    stream = await session.query(key).execute()
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins == {"name": "Insert", "value": 1}


async def test_session_update(session):
    """Test session.update() method."""
    users = DataSet.of("test", "users")
    key = users.id("update_test")

    await session.upsert(key).put({"name": "Original", "age": 20}).execute()
    await session.update(key).put({"age": 21}).execute()

    stream = await session.query(key).execute()
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins["age"] == 21


async def test_session_delete(session):
    """Test session.delete() method."""
    users = DataSet.of("test", "users")
    key = users.id("delete_test")

    await session.upsert(key).put({"name": "ToDelete"}).execute()

    exists_result = await session.exists(key).execute()
    first = await exists_result.first()
    assert first is not None and first.as_bool()

    await session.delete(key).execute()

    exists_result = await session.exists(key).execute()
    first = await exists_result.first()
    assert first is None or not first.as_bool()


async def test_session_touch(session):
    """Test session.touch() method."""
    users = DataSet.of("test", "users")
    key = users.id("touch_test")

    await session.upsert(key).put({"name": "TouchMe"}).execute()

    await session.touch(key).execute()

    exists_result = await session.exists(key).execute()
    assert (await exists_result.first()).as_bool() is True


async def test_session_exists(session):
    """Test session.exists() method."""
    users = DataSet.of("test", "users")
    key = users.id("exists_test")

    await session.delete(key).execute()

    exists_result = await session.exists(key).execute()
    first = await exists_result.first()
    assert first is None or not first.as_bool()

    await session.upsert(key).put({"name": "Exists"}).execute()

    exists_result = await session.exists(key).execute()
    first = await exists_result.first()
    assert first is not None and first.as_bool()


async def test_session_query_delegation(session):
    """Test that session.query() delegates to client correctly."""
    users = DataSet.of("test", "users")
    key = users.id("query_test")

    await session.upsert(key).put({"name": "QueryTest", "value": 42}).execute()

    stream = await session.query(key).execute()
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins == {"name": "QueryTest", "value": 42}


async def test_session_key_value_delegation(session):
    """Test that session.upsert/query work correctly for key-value operations."""
    users = DataSet.of("test", "users")
    key = users.id("kv_test")

    await session.upsert(key).put({"name": "KVTest"}).execute()

    result = await session.query(key).execute()
    first = await result.first_or_raise()
    assert first.is_ok
    assert first.record_or_raise().bins == {"name": "KVTest"}


async def test_session_index_delegation(session):
    """Test that session.index() delegates to client correctly."""
    users = DataSet.of("test", "users")

    index_builder = session.index(dataset=users)
    assert index_builder is not None
    assert index_builder._namespace == "test"
    assert index_builder._set_name == "users"


async def test_session_upsert_error_no_key(session):
    """Test that upsert raises error when no key is provided."""
    with pytest.raises(ValueError, match="At least one key must be provided"):
        await session.upsert().put({"name": "Test"}).execute()


async def test_session_multiple_sessions_different_behaviors(client):
    """Test creating multiple sessions with different behaviors."""
    default_session = client.create_session(Behavior.DEFAULT)
    fast_session = client.create_session(
        Behavior.DEFAULT.derive_with_changes(
            name="fast",
            total_timeout=timedelta(seconds=5),
        )
    )

    assert default_session.behavior.name == "DEFAULT"
    assert fast_session.behavior.name == "fast"
    assert fast_session.behavior.total_timeout == timedelta(seconds=5)
    assert default_session.behavior.total_timeout == timedelta(seconds=30)


async def test_session_transaction_session(session):
    """Test that session.transaction_session() works."""
    tx_session = session.transaction_session()
    assert tx_session is not None


async def test_session_behavior_immutability(session):
    """Test that behavior is immutable."""
    original_timeout = session.behavior.total_timeout

    new_behavior = session.behavior.derive_with_changes(
        name="new",
        total_timeout=timedelta(seconds=60),
    )

    assert session.behavior.total_timeout == original_timeout
    assert new_behavior.total_timeout == timedelta(seconds=60)
    assert new_behavior.name == "new"


async def test_session_query_with_dataset(session):
    """Test session.query() with DataSet."""
    users = DataSet.of("test", "users")

    key = users.id("dataset_query_test")
    await session.upsert(key).put({"name": "DatasetQuery"}).execute()

    stream = await session.query(key).execute()
    async for result in stream:
        rec = result.record_or_raise()
        assert rec.bins == {"name": "DatasetQuery"}


async def test_session_query_with_multiple_keys(session):
    """Test session.query() with multiple keys."""
    users = DataSet.of("test", "users")

    keys = users.ids("batch1", "batch2", "batch3")
    for key in keys:
        await session.upsert(key).put({"name": f"Batch{key.value}"}).execute()

    count = 0
    stream = await session.query(keys).execute()
    async for result in stream:
        rec = result.record_or_raise()
        count += 1
        assert "Batch" in rec.bins["name"]

    assert count == 3


async def test_session_truncate(session):
    """Test that truncate succeeds and new writes after it are readable.

    Truncate is an async server-side operation that may not propagate
    instantly, so we verify the call completes without error and that
    records written *after* the truncate (whose timestamps exceed the
    cutoff) are immediately readable.
    """
    users = DataSet.of("test", "trunc_test")

    key1 = users.id("trunc_old1")
    key2 = users.id("trunc_old2")

    await session.upsert(key1).put({"v": 1}).execute()
    await session.upsert(key2).put({"v": 2}).execute()

    await session.truncate(users)

    key_new = users.id("trunc_new1")
    await session.upsert(key_new).put({"v": 42}).execute()

    result = await (await session.query(key_new).execute()).first()
    assert result is not None
    assert result.record.bins["v"] == 42
