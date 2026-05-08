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

"""Tests for IndexBuilder SDK API."""

import asyncio
import os

import pytest
from aerospike_async import CTX, CollectionIndexType, Filter
from aerospike_sdk import DataSet
from aerospike_sdk.exceptions import AerospikeError


async def test_client_policy_use_services_alternate_from_env(client_policy, aerospike_host):
    """Verify AEROSPIKE_USE_SERVICES_ALTERNATE is loaded and applied to client_policy."""
    assert client_policy.use_services_alternate is True
    env_val = os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").strip().lower()
    assert env_val in ("true", "1", "yes", ""), f"unexpected AEROSPIKE_USE_SERVICES_ALTERNATE={env_val!r}"
    assert aerospike_host, "AEROSPIKE_HOST should be set (e.g. 127.0.0.1:3100)"


async def test_create_numeric_index(client):
    """Test creating a numeric index."""
    index_name = "test_numeric_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create numeric index
    await client.index("test", "test").on_bin("age").named(index_name).numeric().create()

    # Clean up
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

async def test_create_string_index(client):
    """Test creating a string index."""
    index_name = "test_string_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create string index
    await client.index("test", "test").on_bin("name").named(index_name).string().create()

    # Clean up
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

async def test_create_index_with_collection_type(client):
    """Test creating an index with collection index type."""
    index_name = "test_collection_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create index with collection type
    await (
        client.index("test", "test")
        .on_bin("roles")
        .named(index_name)
        .string()
        .collection(CollectionIndexType.LIST)
        .create()
    )

    # Clean up
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

async def test_drop_index(client):
    """Test dropping an index."""
    index_name = "test_drop_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create index first
    await client.index("test", "test").on_bin("age").named(index_name).numeric().create()

    # Drop the index
    await client.index("test", "test").named(index_name).drop()

async def test_drop_nonexistent_index(client):
    """Test dropping a non-existent index (should not raise error)."""
    # Dropping non-existent index should not raise error
    await client.index("test", "test").named("non_existent_idx").drop()

async def test_index_chaining(client):
    """Test method chaining on index builder."""
    index_name = "test_chain_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Test chaining
    await (
        client.index("test", "test")
        .on_bin("age")
        .named(index_name)
        .numeric()
        .create()
    )

    # Verify we can chain drop too
    await client.index("test", "test").named(index_name).drop()

async def test_create_index_missing_bin_name(client):
    """Test that creating index without bin name raises error."""
    with pytest.raises(ValueError, match="bin_name"):
        await client.index("test", "test").named("test_idx").numeric().create()

async def test_create_index_missing_index_name(client):
    """Test that creating index without index name raises error."""
    with pytest.raises(ValueError, match="index_name"):
        await client.index("test", "test").on_bin("age").numeric().create()

async def test_create_index_missing_index_type(client):
    """Test that creating index without index type raises error."""
    with pytest.raises(ValueError, match="index_type"):
        await client.index("test", "test").on_bin("age").named("test_idx").create()

async def test_create_duplicate_index_fails(client):
    """Test that creating duplicate index names fails."""
    index_name = "test_duplicate_idx"
    # Clean up any existing index
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    # Create first index
    await client.index("test", "test").on_bin("age").named(index_name).numeric().create()

    # Try to create another index with same name should fail
    with pytest.raises(AerospikeError):
        await client.index("test", "test").on_bin("name").named(index_name).string().create()

    # Clean up
    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass


async def test_create_index_with_cdt_context(client, enterprise, wait_for_index):
    """Create a numeric index on a nested map element via chainable .context()."""
    index_name = "test_ctx_idx"
    bin_name = "payload"
    ds = DataSet.of("test", "test")
    session = client.create_session()

    try:
        await client.index("test", "test").named(index_name).drop()
    except Exception:
        pass

    k1 = ds.id("ctx_idx_a")
    k2 = ds.id("ctx_idx_b")

    await (
        session.upsert(k1)
            .put({bin_name: {"inner": 10, "other": 99}})
            .execute()
    )
    await (
        session.upsert(k2)
            .put({bin_name: {"inner": 20, "other": 99}})
            .execute()
    )

    await (
        client.index("test", "test")
            .on_bin(bin_name)
            .named(index_name)
            .numeric()
            .context([CTX.map_key("inner")])
            .create()
    )

    flt = Filter.equal(bin_name, 10).context([CTX.map_key("inner")])
    await wait_for_index(client, "test", "test", flt)

    try:
        stream = await client.query("test", "test").filter(flt).bins([bin_name]).execute()
        results = []
        try:
            async for res in stream:
                results.append(res)
        finally:
            stream.close()

        matched = [r.record.key.value for r in results if r.is_ok and r.record]
        assert matched == ["ctx_idx_a"]
    finally:
        await session.delete(k1, k2).execute()
        try:
            await client.index("test", "test").named(index_name).drop()
        except Exception:
            pass
