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
import pytest_asyncio

from aerospike_native import ClientPolicy as PncClientPolicy, FilterExpression, IndexType, new_client
from aerospike_sdk import (
    Behavior,
    ClusterDefinition,
    CollectionIndexType,
    CTX,
    DataSet,
    Exp,
    Filter,
    Host,
)
from aerospike_sdk.policy import Settings
from aerospike_sdk.exceptions import (
    AerospikeError,
    AuthorizationError,
    IndexAlreadyExistsError,
    ResultCode,
    SecurityNotEnabled,
)
from tests.integration.namespace import general_namespace
from tests.pnc_compat import requires_server_compiled_ael

TEST_DS = DataSet.of(general_namespace(), "test")


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
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

    # Create numeric index
    await cluster.create_session().index(TEST_DS).on_bin("age").named(index_name).integer().create()

    # Clean up
    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

async def test_create_string_index(cluster):
    """Test creating a string index."""
    index_name = "test_string_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

    # Create string index
    await cluster.create_session().index(TEST_DS).on_bin("name").named(index_name).string().create()

    # Clean up
    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

async def test_create_index_with_collection_type(cluster):
    """Test creating an index with collection index type."""
    index_name = "test_collection_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

    # Create index with collection type
    await (
        cluster.create_session().index(TEST_DS)
        .on_bin("roles")
        .named(index_name)
        .string()
        .collection(CollectionIndexType.LIST)
        .create()
    )

    # Clean up
    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

async def _index_exists_per_node(cluster, index_name):
    """``{node name: "true" | "false"}`` from each node's own ``sindex-exists``."""
    result = {}
    command = f"sindex-exists:namespace={general_namespace()};indexname={index_name}"
    for node in cluster.nodes():
        result[node.name] = (await node.info(command))[command]
    return result

async def test_drop_index(cluster):
    """Create and drop both reach every node once their tasks complete."""
    index_name = "test_drop_idx"
    session = cluster.create_session()
    # Clean up any existing index
    try:
        await session.index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

    task = await session.index(TEST_DS).on_bin("age").named(index_name).integer().create()
    await task.wait_till_complete()
    exists = await _index_exists_per_node(cluster, index_name)
    assert set(exists.values()) == {"true"}, exists

    task = await session.index(TEST_DS).named(index_name).drop()
    await task.wait_till_complete()
    exists = await _index_exists_per_node(cluster, index_name)
    assert set(exists.values()) == {"false"}, exists

async def test_create_set_index(cluster):
    """A set index round-trips with only a set and a name, and lists as one.

    Two create/drop rounds: the second create proves the drop really removed
    the index. The listing is checked between, because a set index row has
    no bin, type, or state and must still read as a ready set index.
    """
    index_name = "psdk_set_idx"
    session = cluster.create_session()
    try:
        await session.index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

    for _ in range(2):
        task = await session.index(TEST_DS).on_set().named(index_name).create()
        assert await task.wait_till_complete()
        exists = await _index_exists_per_node(cluster, index_name)
        assert set(exists.values()) == {"true"}, exists

        rows = [i for i in await session.info().secondary_indexes(general_namespace())
                if i.name == index_name]
        assert len(rows) == 1, rows
        (row,) = rows
        assert row.set_name == "test"
        assert row.is_set_index is True
        assert row.bin_name == ""
        assert row.index_type == ""
        assert row.is_ready is True

        task = await session.index(TEST_DS).named(index_name).drop()
        assert await task.wait_till_complete()
        exists = await _index_exists_per_node(cluster, index_name)
        assert set(exists.values()) == {"false"}, exists

async def test_create_index_and_drop_index_verbs(cluster):
    """The one-call verbs drive the builder: bin index, set index, and drop."""
    ds = TEST_DS
    session = cluster.create_session()
    for name in ("psdk_verb_city_idx", "psdk_verb_set_idx"):
        try:
            await session.drop_index(ds, name)
        except Exception:
            pass

    task = await session.create_index(ds, "psdk_verb_city_idx", "city", IndexType.STRING)
    assert await task.wait_till_complete()
    task = await session.create_index(ds, "psdk_verb_set_idx")
    assert await task.wait_till_complete()

    listed = {i.name: i for i in await session.info().secondary_indexes(general_namespace())}
    assert listed["psdk_verb_city_idx"].bin_name == "city"
    assert listed["psdk_verb_city_idx"].index_type == "string"
    assert listed["psdk_verb_set_idx"].is_set_index is True

    for name in ("psdk_verb_city_idx", "psdk_verb_set_idx"):
        task = await session.drop_index(ds, name)
        assert await task.wait_till_complete()
    exists = await _index_exists_per_node(cluster, "psdk_verb_city_idx")
    assert set(exists.values()) == {"false"}, exists

async def test_create_set_index_needs_a_set(cluster):
    """A namespace-wide set index is refused before anything reaches the wire."""
    whole_namespace = DataSet.of(general_namespace())
    with pytest.raises(ValueError, match="requires a set"):
        await cluster.create_session().index(whole_namespace).on_set().named("psdk_set_idx_ns").create()

async def test_drop_nonexistent_index(cluster):
    """Test dropping a non-existent index (should not raise error)."""
    # Dropping non-existent index should not raise error
    await cluster.create_session().index(TEST_DS).named("non_existent_idx").drop()

async def test_index_chaining(cluster):
    """Test method chaining on index builder."""
    index_name = "test_chain_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

    # Test chaining
    await (
        cluster.create_session().index(TEST_DS)
        .on_bin("age")
        .named(index_name)
        .integer()
        .create()
    )

    # Verify we can chain drop too
    await cluster.create_session().index(TEST_DS).named(index_name).drop()

async def test_create_index_missing_bin_name(cluster):
    """Test that creating index without bin name raises error."""
    with pytest.raises(ValueError, match="bin_name"):
        await cluster.create_session().index(TEST_DS).named("test_idx").integer().create()

async def test_create_index_missing_index_name(cluster):
    """Test that creating index without index name raises error."""
    with pytest.raises(ValueError, match="index_name"):
        await cluster.create_session().index(TEST_DS).on_bin("age").integer().create()

async def test_create_index_missing_index_type(cluster):
    """Test that creating index without index type raises error."""
    with pytest.raises(ValueError, match="index_type"):
        await cluster.create_session().index(TEST_DS).on_bin("age").named("test_idx").create()

async def test_create_duplicate_index_fails(cluster):
    """Test that creating duplicate index names fails."""
    index_name = "test_duplicate_idx"
    # Clean up any existing index
    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass

    # Create first index
    await cluster.create_session().index(TEST_DS).on_bin("age").named(index_name).integer().create()

    # Reusing the name for a different definition fails with the server's own
    # explanation kept as the base message, under the index result code.
    with pytest.raises(IndexAlreadyExistsError) as exc_info:
        await cluster.create_session().index(TEST_DS).on_bin("name").named(index_name).string().create()
    err = exc_info.value
    assert err.result_code == ResultCode.INDEX_FOUND
    assert err.base_message.startswith("Create index failed: ")
    assert "already exists with different definition" in err.base_message

    # Clean up
    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
    except Exception:
        pass


async def test_create_index_with_cdt_context(cluster, enterprise):
    """Create a numeric index on a nested map element via chainable .context()."""
    index_name = "test_ctx_idx"
    bin_name = "payload"
    ds = TEST_DS
    # The assertion reads the user key back off the query results, which
    # requires the key to be stored with the record — an explicit opt-in.
    session = cluster.create_session(
        Behavior.DEFAULT.derive_with_changes("ctx-idx-send-key", all=Settings(send_key=True))
    )

    try:
        await cluster.create_session().index(TEST_DS).named(index_name).drop()
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

    index_task = await (
        cluster.create_session().index(TEST_DS)
        .on_bin(bin_name)
        .named(index_name)
        .integer()
        .context([CTX.map_key("inner")])
        .create()
    )
    # The build task is authoritative; a query probe only infers readiness.
    await index_task.wait_till_complete()

    flt = Filter.equal(bin_name, 10).context([CTX.map_key("inner")])

    try:
        stream = await session.query(TEST_DS).filter(flt).bins([bin_name]).execute()
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
            await cluster.create_session().index(TEST_DS).named(index_name).drop()
        except Exception:
            pass


async def test_create_expression_index_and_query(cluster):
    """Create an expression-based index, list it, query through it, drop it."""

    set_name = "exp_idx_set"
    index_name = "psdk_exp_age_idx"
    ds = DataSet.of(general_namespace(), set_name)
    session = cluster.create_session()

    try:
        await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
    except Exception:
        pass

    keys = [ds.id(f"exp_u{i}") for i in range(5)]
    for i, k in enumerate(keys):
        await session.upsert(k).put({"age": 30 + i}).execute()

    expr = Exp.int_bin("age")
    try:
        index_task = await (
            session.index(DataSet.of(general_namespace(), set_name))
            .on_expression(expr)
            .named(index_name)
            .integer()
            .create()
        )
        # The build task is authoritative; a query probe only infers readiness.
        await index_task.wait_till_complete()

        listed = [i for i in await session.list_indexes() if i["name"] == index_name]
        assert listed, "expression index not visible in list_indexes"
        assert listed[0]["namespace"] == general_namespace()
        assert listed[0]["set"] == set_name
        # The server's own name for the type.
        assert listed[0]["type"] == "integer"

        flt = Filter.range("age", 31, 33).expression(expr)

        stream = await session.query(DataSet.of(general_namespace(), set_name)).filter(flt).bins(["age"]).execute()
        ages = sorted(
            [r.record.bins["age"] async for r in stream if r.is_ok and r.record],
        )
        assert ages == [31, 32, 33]
    finally:
        await session.delete(keys).execute()
        try:
            await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
        except Exception:
            pass
    assert not any(
        i["name"] == index_name for i in await session.list_indexes()
    ), "expression index still listed after drop"

async def test_create_blob_index_and_query(cluster):
    """Create a blob index on a bytes bin, query through it, drop it."""

    set_name = "blob_idx_set"
    index_name = "psdk_blob_payload_idx"
    ds = DataSet.of(general_namespace(), set_name)
    session = cluster.create_session()

    try:
        await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
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
        index_task = await (
            session.index(DataSet.of(general_namespace(), set_name))
            .on_bin("payload")
            .named(index_name)
            .blob()
            .create()
        )
        # The build task is authoritative; a query probe only infers readiness.
        await index_task.wait_till_complete()

        flt = Filter.equal("payload", needle)

        stream = await session.query(DataSet.of(general_namespace(), set_name)).filter(flt).bins(["payload"]).execute()
        matches = [r.record.bins["payload"] async for r in stream if r.is_ok and r.record]
        assert matches == [needle]
    finally:
        await session.delete(keys).execute()
        try:
            await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
        except Exception:
            pass

async def test_create_blob_list_collection_index_and_query(cluster):
    """Blob index over LIST collection elements: create, query via contains, drop."""

    set_name = "blob_list_idx_set"
    index_name = "psdk_blob_list_payloads_idx"
    ds = DataSet.of(general_namespace(), set_name)
    session = cluster.create_session()

    try:
        await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
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
        index_task = await (
            session.index(DataSet.of(general_namespace(), set_name))
            .on_bin("payloads")
            .named(index_name)
            .blob()
            .collection(CollectionIndexType.LIST)
            .create()
        )
        # The build task is authoritative; a query probe only infers readiness.
        await index_task.wait_till_complete()

        flt = Filter.contains("payloads", needle, CollectionIndexType.LIST)

        stream = await session.query(DataSet.of(general_namespace(), set_name)).filter(flt).bins(["payloads"]).execute()
        matches = [r.record.bins["payloads"] async for r in stream if r.is_ok and r.record]
        assert matches == [[b"\x0a", needle]]
    finally:
        await session.delete(keys).execute()
        try:
            await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
        except Exception:
            pass


@requires_server_compiled_ael
async def test_create_index_from_ael_string_and_query(cluster):
    """Create an expression index from an AEL string, list it, query through it, drop it."""

    set_name = "ael_idx_set"
    index_name = "psdk_ael_age_idx"
    ael = "$.age + 1"
    ds = DataSet.of(general_namespace(), set_name)
    session = cluster.create_session()

    try:
        await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
    except Exception:
        pass

    keys = [ds.id(f"ael_u{i}") for i in range(5)]
    for i, k in enumerate(keys):
        await session.upsert(k).put({"age": 30 + i}).execute()

    try:
        index_task = await (
            session.index(DataSet.of(general_namespace(), set_name))
            .on_expression(ael)
            .named(index_name)
            .integer()
            .create()
        )
        await index_task.wait_till_complete()

        listed = [i for i in await session.list_indexes() if i["name"] == index_name]
        assert listed, "AEL-string expression index not visible in list_indexes"

        flt = Filter.range("age", 32, 34).expression(
            FilterExpression.from_server_compiled_ael(ael),
        )
        stream = (
            await session.query(DataSet.of(general_namespace(), set_name))
            .filter(flt)
            .bins(["age"])
            .execute()
        )
        ages = sorted(
            [r.record.bins["age"] async for r in stream if r.is_ok and r.record],
        )
        assert ages == [31, 32, 33]
    finally:
        await session.delete(keys).execute()
        try:
            await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
        except Exception:
            pass


@requires_server_compiled_ael
async def test_create_index_from_boolean_ael_rejected(cluster):
    """The index basis must produce a value — a boolean AEL predicate is rejected."""
    session = cluster.create_session()
    with pytest.raises(AerospikeError):
        await (
            session.index(DataSet.of(general_namespace(), "ael_idx_set"))
            .on_expression("$.age > 31")
            .named("psdk_ael_bool_idx")
            .integer()
            .create()
        )


@requires_server_compiled_ael
async def test_where_selects_ael_expression_index(cluster):
    """A where() served through an index created from the same AEL.

    Server query planning began selecting expression-based indexes in the
    8.2.0.0 RC. ``requires_server_compiled_ael`` only asserts ``>= 8.2.0.0``,
    which a pre-RC build of that version also satisfies, so this fails with
    ``IndexNotFound`` there instead of skipping.
    """
    set_name = "ael_idx_sel_set"
    index_name = "psdk_ael_sel_idx"
    ael = "$.age + 1"
    ds = DataSet.of(general_namespace(), set_name)
    session = cluster.create_session()

    keys = [ds.id(f"sel_u{i}") for i in range(5)]
    for i, k in enumerate(keys):
        await session.upsert(k).put({"age": 30 + i}).execute()

    try:
        index_task = await (
            session.index(DataSet.of(general_namespace(), set_name))
            .on_expression(ael)
            .named(index_name)
            .integer()
            .create()
        )
        await index_task.wait_till_complete()

        stream = (
            await session.query(DataSet.of(general_namespace(), set_name))
            .where(f"{ael} == 32")
            .execute()
        )
        ages = [r.record.bins["age"] async for r in stream if r.is_ok and r.record]
        assert ages == [31]
    finally:
        await session.delete(keys).execute()
        try:
            await session.index(DataSet.of(general_namespace(), set_name)).named(index_name).drop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Privileges: the security-enabled node (AEROSPIKE_HOST_SEC)
# ---------------------------------------------------------------------------

def _services_alternate() -> bool:
    return os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "true").lower() == "true"


async def _wait_user(admin_pnc, username, *, present, timeout=5.0):
    """Poll ``query_users`` until *username* is (or is not) listed.

    A user is several system-metadata items committed one by one, so the
    only reliable signal is the asserted presence or absence itself.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        names = {u.user for u in await admin_pnc.query_users(None)}
        if (username in names) == present:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"user {username!r} {'not visible' if present else 'still listed'} after {timeout}s")


@pytest_asyncio.fixture
async def sec_admin_pnc(aerospike_host_sec):
    """PNC admin client on the security node, for user management only.

    PSDK does not expose user or role administration, so the users this
    test needs come from PNC's admin entries; everything under test goes
    through PSDK.
    """
    if not aerospike_host_sec:
        pytest.skip("AEROSPIKE_HOST_SEC unset; privilege tests need a security-enabled cluster")
    cp = PncClientPolicy()
    cp.use_services_alternate = _services_alternate()
    cp.user = os.environ.get("AEROSPIKE_AUTH_USER", "admin")
    cp.password = os.environ.get("AEROSPIKE_AUTH_PASSWORD", "admin")
    try:
        client = await new_client(cp, aerospike_host_sec)
    except Exception as exc:
        pytest.skip(f"could not connect as admin to {aerospike_host_sec}: {exc}")
    try:
        await client.query_users(None)
    except SecurityNotEnabled:
        await client.close()
        pytest.skip("security is not enabled on AEROSPIKE_HOST_SEC")
    yield client
    await client.close()


def _sec_definition(seed: str, user: str, password: str) -> ClusterDefinition:
    definition = ClusterDefinition(hosts=Host.parse_hosts(seed, 3000))
    if _services_alternate():
        definition.using_services_alternate()
    return definition.with_native_credentials(user, password)


async def test_set_index_needs_only_sindex_admin(aerospike_host_sec, sec_admin_pnc):
    """A set index is created and dropped by a user holding only ``sindex-admin``.

    The negative control is a user holding only ``read-write``: the server
    refuses with ROLE_VIOLATION naming the missing privilege, which pins
    the grant to that one role rather than to anything else the admin
    user happens to hold.
    """
    users = {"psdk_sidx_admin": ["sindex-admin"], "psdk_sidx_rw": ["read-write"]}
    password = "set_index_pw"
    for name in users:
        try:
            await sec_admin_pnc.drop_user(name)
        except Exception:
            pass
        await _wait_user(sec_admin_pnc, name, present=False)
    for name, roles in users.items():
        await sec_admin_pnc.create_user(name, password, roles)
        await _wait_user(sec_admin_pnc, name, present=True)

    ds = DataSet.of("test", "sidx_role")
    index_name = "psdk_sidx_role_idx"
    try:
        async with _sec_definition(aerospike_host_sec, "psdk_sidx_admin", password).connect() as cluster:
            session = cluster.create_session()
            task = await session.create_index(ds, index_name)
            assert await task.wait_till_complete()
            task = await session.drop_index(ds, index_name)
            assert await task.wait_till_complete()

        async with _sec_definition(aerospike_host_sec, "psdk_sidx_rw", password).connect() as cluster:
            with pytest.raises(AuthorizationError) as exc_info:
                await cluster.create_session().create_index(ds, index_name)
            assert exc_info.value.result_code == ResultCode.ROLE_VIOLATION
            assert "sindex-admin" in exc_info.value.base_message
    finally:
        for name in users:
            try:
                await sec_admin_pnc.drop_user(name)
            except Exception:
                pass
            await _wait_user(sec_admin_pnc, name, present=False)
