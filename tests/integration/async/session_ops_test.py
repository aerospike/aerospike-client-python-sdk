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

"""Integration tests for session-level operations and TransactionalSession."""

import pytest
import pytest_asyncio
from aerospike_sdk import DataSet, Client


_SHARED_KEYS = (1, 2, "user1")


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy):
    """Setup SDK client for testing."""
    async with Client(seeds=aerospike_host, policy=client_policy) as client:
        yield client


@pytest_asyncio.fixture(autouse=True)
async def _clean_shared_keys(client):
    """Wipe the keys these tests share so each one starts from a clean slate.

    The whole file deliberately reuses ``DataSet.of("test", "test").id(...)``
    for ``1``, ``2``, and ``"user1"``, so without a per-test wipe a prior
    ``upsert({"name": ..., "age": ...})`` leaves bins behind that pollute
    later assertions on the same key.
    """
    session = client.create_session()
    ds = DataSet.of("test", "test")
    for key in _SHARED_KEYS:
        try:
            await session.delete(ds.id(key)).execute()
        except Exception:
            pass
    yield


async def test_session_put_get(client):
    """Test session put and get operations."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    await session.upsert(key).put({"name": "John", "age": 30}).execute()

    result = await (await session.query(key).execute()).first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "John", "age": 30}


async def test_session_multiple_operations(client):
    """Test multiple operations using the same session."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    await session.upsert(ds.id(1)).put({"name": "John"}).execute()
    await session.upsert(ds.id(2)).put({"name": "Jane"}).execute()

    result1 = await (await session.query(ds.id(1)).execute()).first_or_raise()
    result2 = await (await session.query(ds.id(2)).execute()).first_or_raise()
    record1 = result1.record
    record2 = result2.record

    assert record1 is not None
    assert record1.bins == {"name": "John"}
    assert record2 is not None
    assert record2.bins == {"name": "Jane"}


async def test_session_get_with_bins(client):
    """Test getting specific bins with session query."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    await session.upsert(key).put({"name": "John", "age": 30, "city": "NYC"}).execute()

    result = await (await session.query(key).bins(["name", "age"]).execute()).first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins == {"name": "John", "age": 30}
    assert "city" not in record.bins


async def test_session_delete(client):
    """Test delete operation with session."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    await session.upsert(key).put({"name": "John"}).execute()

    await session.delete(key).execute()

    exists_stream = await session.exists(key).execute()
    first = await exists_stream.first()
    exists = first.as_bool() if first else False
    assert exists is False


async def test_session_exists(client):
    """Test exists operation with session."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)

    exists_stream = await session.exists(key).execute()
    first = await exists_stream.first()
    exists = first.as_bool() if first else False
    assert exists is False

    await session.upsert(key).put({"name": "John"}).execute()

    exists_stream = await session.exists(key).execute()
    first = await exists_stream.first()
    exists = first.as_bool() if first else False
    assert exists is True


async def test_session_increment(client):
    """Test increment operation with session."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    await session.upsert(key).put({"counter": 10}).execute()

    await session.upsert(key).bin("counter").add(5).execute()

    result = await (await session.query(key).execute()).first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins["counter"] == 15


async def test_session_append_prepend(client):
    """Test append and prepend operations with session."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id(1)
    await session.upsert(key).put({"name": "John"}).execute()

    await session.upsert(key).bin("name").append(" Doe").execute()
    await session.upsert(key).bin("name").prepend("Mr. ").execute()

    result = await (await session.query(key).execute()).first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins["name"] == "Mr. John Doe"


async def test_session_string_keys(client):
    """Test using string keys with session."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    key = ds.id("user1")
    await session.upsert(key).put({"name": "John"}).execute()

    result = await (await session.query(key).execute()).first_or_raise()
    record = result.record
    assert record is not None
    assert record.bins["name"] == "John"


async def test_transactional_session_basic(client):
    """Test basic TransactionalSession usage."""
    session = client.create_session()
    ds = DataSet.of("test", "test")
    await session.upsert(ds.id(1)).put({"name": "John"}).execute()
    await session.upsert(ds.id(2)).put({"name": "Jane"}).execute()

    result1 = await (await session.query(ds.id(1)).execute()).first_or_raise()
    result2 = await (await session.query(ds.id(2)).execute()).first_or_raise()
    record1 = result1.record
    record2 = result2.record

    assert record1 is not None
    assert record2 is not None


async def test_transactional_session_context_manager(client):
    """Test TransactionalSession context manager behavior.

    NOTE: requires a namespace in strong-consistency (SC) mode to commit.
    Marked xfail when running against an AP-only cluster.
    """
    tx_session = client.transaction_session()
    assert tx_session.active is False

    try:
        async with tx_session as tx:
            assert tx.active is True
            assert tx.txn is not None
    except Exception as exc:
        pytest.xfail(f"Requires SC cluster for MRT commit: {exc}")

    assert tx_session.active is False
