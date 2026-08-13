"""Smoke tests that exercise the code patterns shown in the docs.

If a doc example drifts from the actual API, a test here should break.
Each test is tagged with the doc page and section it validates.
"""

import os

import pytest

from tests.pac_compat import requires_server_compiled_ael
import pytest_asyncio

from aerospike_sdk import (
    Behavior,
    ClusterDefinition,
    DataSet,
)
from tests.integration.namespace import general_namespace
from tests.integration.general_auth import apply_general_auth, general_seed

SEEDS = general_seed()
USERS = DataSet.of(general_namespace(), "doc_smoke")


def _use_services_alternate() -> bool:
    return os.environ.get(
        "AEROSPIKE_USE_SERVICES_ALTERNATE", "",
    ).lower() in ("true", "1", "yes")


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def session(make_cluster_definition):
    """Provide a connected async session for the module."""
    async with await make_cluster_definition(SEEDS).connect() as cluster:
        s = cluster.create_session(Behavior.DEFAULT)
        await s.truncate(USERS)
        yield s


# ------------------------------------------------------------------
# docs/index.md — Quick Example (async)
# ------------------------------------------------------------------

@requires_server_compiled_ael
@pytest.mark.asyncio(loop_scope="session")
async def test_quick_example_async(session):
    """docs/index.md — Quick Example (async tab)."""
    key = USERS.id("qe_async")
    await (
        session.upsert(key)
        .bin("name").set_to("Alice")
        .bin("age").set_to(30)
        .execute()
    )

    stream = await session.query(key).execute()
    result = await stream.first_or_raise()
    assert result.record.bins["name"] == "Alice"
    assert result.record.bins["age"] == 30
    stream.close()

    stream = await session.query(USERS).where("$.age > 25").execute()
    found = False
    async for r in stream:
        if r.record.bins.get("name") == "Alice":
            found = True
    stream.close()
    assert found

    await session.delete(key).execute()


# ------------------------------------------------------------------
# docs/index.md — Quick Example (sync)
# ------------------------------------------------------------------

def test_quick_example_sync(make_cluster_definition):
    """docs/index.md — Quick Example (sync tab)."""
    with make_cluster_definition(SEEDS, sync=True).connect() as cluster:
        s = cluster.create_session(Behavior.DEFAULT)
        key = USERS.id("qe_sync")

        s.upsert(key).bin("name").set_to("Alice").bin("age").set_to(30).execute()

        stream = s.query(key).execute()
        result = stream.first_or_raise()
        assert result.record.bins["name"] == "Alice"
        stream.close()

        s.delete(key).execute()


# ------------------------------------------------------------------
# docs/guide/connecting.md — ClusterDefinition
# ------------------------------------------------------------------

@pytest.mark.asyncio(loop_scope="session")
async def test_cluster_definition_connect():
    """docs/guide/connecting.md — ClusterDefinition section."""
    host, port = SEEDS.split(",")[0].rsplit(":", 1)
    cluster_def = apply_general_auth(ClusterDefinition(host, int(port)))
    if _use_services_alternate():
        cluster_def = cluster_def.using_services_alternate()
    mode_str = os.environ.get("AEROSPIKE_AUTH_MODE", "").strip().upper()
    if mode_str == "INTERNAL":
        user = os.environ.get("AEROSPIKE_AUTH_USER", "")
        password = os.environ.get("AEROSPIKE_AUTH_PASSWORD", "")
        cluster_def = cluster_def.with_native_credentials(user, password)
    elif mode_str == "EXTERNAL":
        user = os.environ.get("AEROSPIKE_AUTH_USER", "")
        password = os.environ.get("AEROSPIKE_AUTH_PASSWORD", "")
        cluster_def = cluster_def.with_external_credentials(user, password)
    async with await cluster_def.connect() as cluster:
        s = cluster.create_session(Behavior.DEFAULT)
        key = USERS.id("cd_test")
        await s.upsert(key).put({"x": 1}).execute()
        stream = await s.query(key).execute()
        result = await stream.first_or_raise()
        assert result.record.bins["x"] == 1
        stream.close()
        await s.delete(key).execute()


# ------------------------------------------------------------------
# docs/guide/writes.md — Write verbs
# ------------------------------------------------------------------

@pytest.mark.asyncio(loop_scope="session")
async def test_write_verbs(session):
    """docs/guide/writes.md — upsert, insert, update, replace, delete."""
    key = USERS.id("write_verbs")

    # upsert
    await session.upsert(key).put({"name": "Bob", "age": 25}).execute()

    # update (bin chaining)
    await session.update(key).bin("age").set_to(26).execute()

    stream = await session.query(key).execute()
    r = await stream.first_or_raise()
    assert r.record.bins["age"] == 26
    assert r.record.bins["name"] == "Bob"
    stream.close()

    # replace
    await session.replace(key).put({"name": "Bob Replaced"}).execute()

    stream = await session.query(key).execute()
    r = await stream.first_or_raise()
    assert r.record.bins["name"] == "Bob Replaced"
    assert "age" not in r.record.bins
    stream.close()

    # delete
    await session.delete(key).execute()
    stream = await session.exists(key).execute()
    first = await stream.first()
    assert first is None or not first.as_bool()
    stream.close()


# ------------------------------------------------------------------
# docs/guide/reads.md — Selecting bins, batch read
# ------------------------------------------------------------------

@pytest.mark.asyncio(loop_scope="session")
async def test_bin_selection(session):
    """docs/guide/reads.md — Selecting Bins."""
    key = USERS.id("bin_sel")
    await session.upsert(key).put({"name": "Carol", "age": 40, "city": "Austin"}).execute()

    stream = await session.query(key).bins(["name", "age"]).execute()
    r = await stream.first_or_raise()
    assert "name" in r.record.bins
    assert "city" not in r.record.bins
    stream.close()

    await session.delete(key).execute()


@pytest.mark.asyncio(loop_scope="session")
async def test_batch_read(session):
    """docs/guide/reads.md — Batch Read (Multiple Keys)."""
    keys = USERS.ids("br1", "br2", "br3")
    for i, k in enumerate(keys):
        await session.upsert(k).put({"idx": i}).execute()

    stream = await session.query(*keys).execute()
    count = 0
    async for _ in stream:
        count += 1
    stream.close()
    assert count == 3

    for k in keys:
        await session.delete(k).execute()


# ------------------------------------------------------------------
# docs/guide/writes.md — Conditional writes
# ------------------------------------------------------------------

@requires_server_compiled_ael
@pytest.mark.asyncio(loop_scope="session")
async def test_conditional_write(session):
    """docs/guide/writes.md — Conditional Writes with where()."""
    key = USERS.id("cond_write")
    await session.upsert(key).put({"age": 20, "verified": False}).execute()

    # Should NOT match (age < 18 filter won't match age=20... wait, age >= 18 WILL match)
    await (
        session.update(key)
        .where("$.age >= 18")
        .bin("verified").set_to(True)
        .execute()
    )

    stream = await session.query(key).execute()
    r = await stream.first_or_raise()
    assert r.record.bins["verified"] is True
    stream.close()

    await session.delete(key).execute()
