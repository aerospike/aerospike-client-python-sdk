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

import os

import pytest

from tests.pac_compat import requires_server_compiled_ael
from aerospike_sdk import CollectionIndexType, CTX, DataSet, Filter
from aerospike_sdk.exceptions import AerospikeError
from tests.integration.namespace import general_namespace


async def test_client_policy_use_services_alternate_from_env(client_policy, aerospike_host):
    """Verify AEROSPIKE_USE_SERVICES_ALTERNATE is loaded and applied to client_policy."""
    assert client_policy.use_services_alternate is True
    env_val = os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").strip().lower()
    assert env_val in ("true", "1", "yes", ""), f"unexpected AEROSPIKE_USE_SERVICES_ALTERNATE={env_val!r}"
    assert aerospike_host, "AEROSPIKE_HOST should be set (e.g. 127.0.0.1:3100)"


async def test_create_numeric_index(cluster):
    """Test creating a numeric index."""
    index_name = "test_numeric_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

    # Create numeric index
    await cluster.create_session().index(general_namespace(), "test").on_bin("age").named(index_name).numeric().create()

    # Clean up
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

async def test_create_string_index(cluster):
    """Test creating a string index."""
    index_name = "test_string_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

    # Create string index
    await cluster.create_session().index(general_namespace(), "test").on_bin("name").named(index_name).string().create()

    # Clean up
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

async def test_create_index_with_collection_type(cluster):
    """Test creating an index with collection index type."""
    index_name = "test_collection_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

    # Create index with collection type
    await (
        cluster.create_session().index(general_namespace(), "test")
        .on_bin("roles")
        .named(index_name)
        .string()
        .collection(CollectionIndexType.LIST)
        .create()
    )

    # Clean up
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

async def test_drop_index(cluster):
    """Test dropping an index."""
    index_name = "test_drop_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

    # Create index first
    await cluster.create_session().index(general_namespace(), "test").on_bin("age").named(index_name).numeric().create()

    # Drop the index
    await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()

async def test_drop_nonexistent_index(cluster):
    """Test dropping a non-existent index (should not raise error)."""
    # Dropping non-existent index should not raise error
    await cluster.create_session().index(general_namespace(), "test").named("non_existent_idx").drop()

async def test_index_chaining(cluster):
    """Test method chaining on index builder."""
    index_name = "test_chain_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

    # Test chaining
    await (
        cluster.create_session().index(general_namespace(), "test")
        .on_bin("age")
        .named(index_name)
        .numeric()
        .create()
    )

    # Verify we can chain drop too
    await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()

async def test_create_index_missing_bin_name(cluster):
    """Test that creating index without bin name raises error."""
    with pytest.raises(ValueError, match="bin_name"):
        await cluster.create_session().index(general_namespace(), "test").named("test_idx").numeric().create()

async def test_create_index_missing_index_name(cluster):
    """Test that creating index without index name raises error."""
    with pytest.raises(ValueError, match="index_name"):
        await cluster.create_session().index(general_namespace(), "test").on_bin("age").numeric().create()

async def test_create_index_missing_index_type(cluster):
    """Test that creating index without index type raises error."""
    with pytest.raises(ValueError, match="index_type"):
        await cluster.create_session().index(general_namespace(), "test").on_bin("age").named("test_idx").create()

async def test_create_duplicate_index_fails(cluster):
    """Test that creating duplicate index names fails."""
    index_name = "test_duplicate_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass

    # Create first index
    await cluster.create_session().index(general_namespace(), "test").on_bin("age").named(index_name).numeric().create()

    # Try to create another index with same name should fail
    with pytest.raises(AerospikeError):
        await cluster.create_session().index(general_namespace(), "test").on_bin("name").named(index_name).string().create()

    # Clean up
    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
    except Exception:
        pass


@requires_server_compiled_ael
async def test_create_index_with_cdt_context(cluster, enterprise, wait_for_index):
    """Create a numeric index on a nested map element via chainable .context()."""
    index_name = "test_ctx_idx"
    bin_name = "payload"
    ds = DataSet.of(general_namespace(), "test")
    session = cluster.create_session()

    try:
        await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
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
        cluster.create_session().index(general_namespace(), "test")
        .on_bin(bin_name)
        .named(index_name)
        .numeric()
        .context([CTX.map_key("inner")])
        .create()
    )

    flt = Filter.equal(bin_name, 10).context([CTX.map_key("inner")])
    await wait_for_index(cluster, general_namespace(), "test", flt)

    try:
        stream = await session.query(general_namespace(), "test").filter(flt).bins([bin_name]).execute()
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
            await cluster.create_session().index(general_namespace(), "test").named(index_name).drop()
        except Exception:
            pass


@requires_server_compiled_ael
async def test_create_expression_index_and_query(cluster, server_version, wait_for_index):
    """Create an expression-based index, list it, query through it, drop it."""
    if server_version is None or server_version < (8, 1, 2, 0):
        pytest.skip("expression-based indexes require server 8.1.2+")
    from aerospike_sdk import Exp, Filter

    set_name = "exp_idx_set"
    index_name = "psdk_exp_age_idx"
    ds = DataSet.of(general_namespace(), set_name)
    session = cluster.create_session()

    try:
        await session.index(general_namespace(), set_name).named(index_name).drop()
    except Exception:
        pass

    keys = [ds.id(f"exp_u{i}") for i in range(5)]
    for i, k in enumerate(keys):
        await session.upsert(k).put({"age": 30 + i}).execute()

    expr = Exp.int_bin("age")
    try:
        await (
            session.index(general_namespace(), set_name)
            .on_expression(expr)
            .named(index_name)
            .numeric()
            .create()
        )

        listed = [i for i in await session.list_indexes() if i["name"] == index_name]
        assert listed, "expression index not visible in list_indexes"
        assert listed[0]["namespace"] == general_namespace()
        assert listed[0]["set"] == set_name

        flt = Filter.range("age", 31, 33).expression(expr)
        await wait_for_index(cluster, general_namespace(), set_name, flt)

        stream = await session.query(general_namespace(), set_name).filter(flt).bins(["age"]).execute()
        ages = sorted(
            [r.record.bins["age"] async for r in stream if r.is_ok and r.record],
        )
        assert ages == [31, 32, 33]
    finally:
        await session.delete(keys).execute()
        try:
            await session.index(general_namespace(), set_name).named(index_name).drop()
        except Exception:
            pass
    assert not any(
        i["name"] == index_name for i in await session.list_indexes()
    ), "expression index still listed after drop"

@requires_server_compiled_ael
async def test_create_blob_index_and_query(cluster, supports_blob_index, wait_for_index):
    """Create a blob index on a bytes bin, query through it, drop it."""
    if not supports_blob_index:
        pytest.skip("blob secondary indexes require server 7.0+")

    set_name = "blob_idx_set"
    index_name = "psdk_blob_payload_idx"
    ds = DataSet.of(general_namespace(), set_name)
    session = cluster.create_session()

    try:
        await session.index(general_namespace(), set_name).named(index_name).drop()
    except Exception:
        pass

    # The decoy shares a prefix with the needle, so a truncating comparison
    # would over-match.
    needle = b"\xde\xad\xbe\xef"
    blobs = [b"\x01\x02", needle, b"\xff", b"\xde\xad"]
    keys = [ds.id(f"blob_u{i}") for i in range(len(blobs))]
    for k, blob in zip(keys, blobs):
        await session.upsert(k).put({"payload": blob}).execute()

    try:
        await (
            session.index(general_namespace(), set_name)
            .on_bin("payload")
            .named(index_name)
            .blob()
            .create()
        )

        flt = Filter.equal("payload", needle)
        await wait_for_index(cluster, general_namespace(), set_name, flt)

        stream = await session.query(general_namespace(), set_name).filter(flt).bins(["payload"]).execute()
        matches = [r.record.bins["payload"] async for r in stream if r.is_ok and r.record]
        assert matches == [needle]
    finally:
        await session.delete(keys).execute()
        try:
            await session.index(general_namespace(), set_name).named(index_name).drop()
        except Exception:
            pass

@requires_server_compiled_ael
async def test_create_blob_list_collection_index_and_query(cluster, supports_blob_index, wait_for_index):
    """Blob index over LIST collection elements: create, query via contains, drop."""
    if not supports_blob_index:
        pytest.skip("blob secondary indexes require server 7.0+")

    set_name = "blob_list_idx_set"
    index_name = "psdk_blob_list_payloads_idx"
    ds = DataSet.of(general_namespace(), set_name)
    session = cluster.create_session()

    try:
        await session.index(general_namespace(), set_name).named(index_name).drop()
    except Exception:
        pass

    # Only one record's list holds the needle; another list holds a
    # prefix-sharing decoy that a truncating comparison would match.
    needle = b"\xde\xad\xbe\xef"
    lists = [
        [b"\x01\x02", b"\xff"],
        [b"\x0a", needle],
        [b"\xde\xad"],
    ]
    keys = [ds.id(f"blob_list_u{i}") for i in range(len(lists))]
    for k, blobs in zip(keys, lists):
        await session.upsert(k).put({"payloads": blobs}).execute()

    try:
        await (
            session.index(general_namespace(), set_name)
            .on_bin("payloads")
            .named(index_name)
            .blob()
            .collection(CollectionIndexType.LIST)
            .create()
        )

        flt = Filter.contains("payloads", needle, CollectionIndexType.LIST)
        await wait_for_index(cluster, general_namespace(), set_name, flt)

        stream = await session.query(general_namespace(), set_name).filter(flt).bins(["payloads"]).execute()
        matches = [r.record.bins["payloads"] async for r in stream if r.is_ok and r.record]
        assert matches == [[b"\x0a", needle]]
    finally:
        await session.delete(keys).execute()
        try:
            await session.index(general_namespace(), set_name).named(index_name).drop()
        except Exception:
            pass
