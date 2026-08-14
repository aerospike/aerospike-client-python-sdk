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
# License for the specific language governing permissions and limitations
# under the License.

"""Integration tests for foreground UDF SDK API (async)."""

from __future__ import annotations

import importlib
import os
from datetime import timedelta

import pytest
from aerospike_sdk import Behavior, UDFLang
from aerospike_sdk.exceptions import AerospikeError, ResultCode, TimeoutError
from aerospike_sdk import ClusterDefinition, DataSet
from aerospike_sdk.policy.behavior_settings import Settings
from tests.integration.namespace import general_namespace
from tests.integration.general_auth import apply_general_auth

NS = general_namespace()
SET = "test"
DS = DataSet.of(NS, SET)
LUA_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "udf", "record_example.lua"),
)
SERVER_PATH = "record_example.lua"
MODULE = "record_example"


@pytest.fixture(scope="module")
async def cluster_with_udf(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as c:
        udf_session = c.create_session()
        try:
            rm = await udf_session.remove_udf(SERVER_PATH)
            await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        except Exception:
            pass
        reg = await udf_session.register_udf_from_file(LUA_FILE, SERVER_PATH, UDFLang.LUA)
        assert await reg.wait_till_complete(sleep_time=0.2, max_attempts=50)
        yield c
        try:
            rm = await udf_session.remove_udf(SERVER_PATH)
            await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        except Exception:
            pass

async def test_write_using_udf(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_write_1")
    await session.delete(k).execute()
    stream = await (
        session.execute_udf(k)
        .function(MODULE, "writeBin")
        .passing("udfbin1", "string value")
        .execute()
    )
    rr = await stream.first_or_raise()
    assert rr.is_ok
    rec = await (
        await session.query(k).bins(["udfbin1"]).execute()
    ).first_or_raise()
    assert rec.record is not None
    assert rec.record.bins.get("udfbin1") == "string value"

async def test_first_udf_result_read_bin(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_read_1")
    await session.upsert(k).put({"udfbin2": "stored"}).execute()
    stream = await (
        session.execute_udf(k)
        .function(MODULE, "readBin")
        .passing("udfbin2")
        .execute()
    )
    val = await stream.first_udf_result()
    assert val == "stored"

async def test_write_read_blob_via_udf(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_blob_rtt")
    await session.delete(k).execute()
    payload = b"\x00\x01\xfe\x2a"
    await (
        session.execute_udf(k)
        .function(MODULE, "writeBin")
        .passing("bbin", payload)
        .execute()
    )
    stream = await (
        session.execute_udf(k)
        .function(MODULE, "readBin")
        .passing("bbin")
        .execute()
    )
    val = await stream.first_udf_result()
    assert bytes(val) == payload

async def test_nested_list_map_round_trip_via_udf_read_bin(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_list_map_rtt")
    await session.delete(k).execute()
    expected = [
        {"id": 1, "tags": ["a", "b"]},
        {"meta": {"x": 2, "nested": [3, 4]}},
    ]
    await (
        session.execute_udf(k)
        .function(MODULE, "writeBin")
        .passing("complex", expected)
        .execute()
    )
    stream = await (
        session.execute_udf(k)
        .function(MODULE, "readBin")
        .passing("complex")
        .execute()
    )
    val = await stream.first_udf_result()
    assert val == expected

async def test_batch_udf(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k1 = DS.id("batch_udf_1")
    k2 = DS.id("batch_udf_2")
    await session.delete(k1, k2).execute()
    stream = await (
        session.execute_udf(k1, k2)
        .function(MODULE, "writeBin")
        .passing("B5", "value5")
        .execute()
    )
    results = await stream.collect()
    assert len(results) == 2
    assert all(r.is_ok for r in results)
    for k in (k1, k2):
        rr = await (
            await session.query(k).bins(["B5"]).execute()
        ).first_or_raise()
        assert rr.record is not None
        assert rr.record.bins.get("B5") == "value5"

async def test_batch_udf_validation_error_in_stream(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k1 = DS.id("batch_udf_err_1")
    k2 = DS.id("batch_udf_err_2")
    await session.delete(k1, k2).execute()
    stream = await (
        session.execute_udf(k1, k2)
        .function(MODULE, "writeWithValidation")
        .passing("B5", 999)
        .execute()
    )
    results = await stream.collect()
    assert len(results) == 2
    keys = [r.key for r in results]
    assert k1 in keys and k2 in keys
    for r in results:
        assert r.result_code == ResultCode.UDF_BAD_RESPONSE
        assert r.record is not None

async def test_batch_udf_include_missing_keys_includes_filtered_out(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k1 = DS.id("batch_udf_rak_1")
    k2 = DS.id("batch_udf_rak_2")
    await session.delete(k1, k2).execute()
    await session.upsert(k1).put({"v": 5}).execute()
    await session.upsert(k2).put({"v": 20}).execute()

    # Without include_missing_keys: filtered-out key is omitted
    stream = await (
        session.execute_udf(k1, k2)
        .function(MODULE, "writeBin")
        .passing("tag", "hit")
        .where("$.v < 10")
        .execute()
    )
    results = await stream.collect()
    assert len(results) == 1
    assert results[0].key == k1
    assert results[0].is_ok

    # With include_missing_keys: filtered-out key appears in stream
    stream = await (
        session.execute_udf(k1, k2)
        .function(MODULE, "writeBin")
        .passing("tag", "hit2")
        .where("$.v < 10")
        .include_missing_keys()
        .execute()
    )
    results = await stream.collect()
    assert len(results) == 2
    r1 = next(r for r in results if r.key == k1)
    r2 = next(r for r in results if r.key == k2)
    assert r1.is_ok
    assert r2.result_code == ResultCode.FILTERED_OUT

async def test_get_generation_udf_result(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_gen_read")
    await session.upsert(k).put({"gprobe": 1}).execute()
    stream = await (
        session.execute_udf(k)
        .function(MODULE, "getGeneration")
        .execute()
    )
    gen = await stream.first_udf_result()
    assert isinstance(gen, int)
    assert gen >= 1

async def test_write_if_generation_not_changed(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_gen_guard")
    await session.delete(k).execute()
    await session.upsert(k).put({"gcol": "a"}).execute()
    stream = await (
        session.execute_udf(k)
        .function(MODULE, "getGeneration")
        .execute()
    )
    gen = await stream.first_udf_result()
    assert isinstance(gen, int)
    stream = await (
        session.execute_udf(k)
        .function(MODULE, "writeIfGenerationNotChanged")
        .passing("gcol", "b", gen)
        .execute()
    )
    assert await stream.first_udf_result() is None
    rr = await (
        await session.query(k).bins(["gcol"]).execute()
    ).first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get("gcol") == "b"
    stale_gen = gen
    await (
        session.execute_udf(k)
        .function(MODULE, "writeIfGenerationNotChanged")
        .passing("gcol", "should_not_apply", stale_gen)
        .execute()
    )
    rr2 = await (
        await session.query(k).bins(["gcol"]).execute()
    ).first_or_raise()
    assert rr2.record is not None
    assert rr2.record.bins.get("gcol") == "b"

async def test_write_unique_idempotent(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_write_unique")
    await session.delete(k).execute()
    await (
        session.execute_udf(k)
        .function(MODULE, "writeUnique")
        .passing("ub", "first")
        .execute()
    )
    await (
        session.execute_udf(k)
        .function(MODULE, "writeUnique")
        .passing("ub", "second")
        .execute()
    )
    rr = await (
        await session.query(k).bins(["ub"]).execute()
    ).first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get("ub") == "first"

async def test_append_list_bin_via_udf(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_list_append")
    await session.delete(k).execute()
    await session.insert(k).put({"lb": []}).execute()
    for v in (10, 20, 30):
        await (
            session.execute_udf(k)
            .function(MODULE, "appendListBin")
            .passing("lb", v)
            .execute()
        )
    rr = await (
        await session.query(k).bins(["lb"]).execute()
    ).first_or_raise()
    assert rr.record is not None
    lst = rr.record.bins.get("lb")
    assert lst is not None
    assert list(lst) == [10, 20, 30]

async def test_process_record_even_adds_to_bin(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_proc_even")
    await session.delete(k).execute()
    await session.insert(k).put({"n1": 4, "n2": 1}).execute()
    await (
        session.execute_udf(k)
        .function(MODULE, "processRecord")
        .passing("n1", "n2", 3)
        .execute()
    )
    rr = await (
        await session.query(k).bins(["n1", "n2"]).execute()
    ).first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get("n1") == 7
    assert rr.record.bins.get("n2") == 1

async def test_process_record_multiple_of_five_clears_second_bin(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_proc_five")
    await session.delete(k).execute()
    await session.insert(k).put({"n1": 10, "n2": 99}).execute()
    await (
        session.execute_udf(k)
        .function(MODULE, "processRecord")
        .passing("n1", "n2", 1)
        .execute()
    )
    rr = await (
        await session.query(k).bins(["n1", "n2"]).execute()
    ).first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get("n1") == 10
    assert rr.record.bins.get("n2") is None

async def test_process_record_multiple_of_nine_removes_record(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_proc_nine")
    await session.delete(k).execute()
    await session.insert(k).put({"n1": 9, "n2": 1}).execute()
    await (
        session.execute_udf(k)
        .function(MODULE, "processRecord")
        .passing("n1", "n2", 1)
        .execute()
    )
    rs = await session.query(k).execute()
    first = await rs.first()
    assert first is None or not first.is_ok
    if first is not None:
        assert first.record is None

async def test_chained_udf(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k1 = DS.id("chain_udf_1")
    k2 = DS.id("chain_udf_2")
    await session.delete(k1, k2).execute()
    stream = await (
        session
        .execute_udf(k1)
        .function(MODULE, "writeBin")
        .passing("B5", "value1")
        .execute_udf(k2)
        .function(MODULE, "writeWithValidation")
        .passing("B5", 5)
        .execute()
    )
    rows = await stream.collect()
    assert len(rows) == 2
    for k in (k1, k2):
        rr = await (
            await session.query(k).bins(["B5"]).execute()
        ).first_or_raise()
        assert rr.record is not None
        assert "B5" in rr.record.bins

async def test_chained_udf_three_specs_mixed_ok_and_udf_bad_response(
    cluster_with_udf,
):
    """Chained UDF specs: first two succeed, third returns UDF_BAD_RESPONSE."""
    session = cluster_with_udf.create_session()
    k1 = DS.id("chain_udf_complex_1")
    k2 = DS.id("chain_udf_complex_2")
    k3 = DS.id("chain_udf_complex_3")
    await session.delete(k1, k2, k3).execute()
    stream = await (
        session
        .execute_udf(k1)
        .function(MODULE, "writeBin")
        .passing("cx", "ok1")
        .execute_udf(k2)
        .function(MODULE, "writeWithValidation")
        .passing("cx", 7)
        .execute_udf(k3)
        .function(MODULE, "writeWithValidation")
        .passing("cx", 999)
        .execute()
    )
    rows = await stream.collect()
    assert len(rows) == 3
    assert rows[0].is_ok
    assert rows[0].key == k1
    assert rows[1].is_ok
    assert rows[1].key == k2
    assert not rows[2].is_ok
    assert rows[2].key == k3
    assert rows[2].result_code == ResultCode.UDF_BAD_RESPONSE
    # Multiple UDF segments fold into one batch, so the server's UDF failure
    # detail is surfaced as a FAILURE bin rather than dropped.
    assert rows[2].record is not None
    assert "FAILURE" in rows[2].record.bins
    r1 = await (
        await session.query(k1).bins(["cx"]).execute()
    ).first_or_raise()
    assert r1.record is not None
    assert r1.record.bins.get("cx") == "ok1"
    r2 = await (
        await session.query(k2).bins(["cx"]).execute()
    ).first_or_raise()
    assert r2.record is not None
    assert r2.record.bins.get("cx") == 7

async def test_single_key_validation_raises(cluster_with_udf):
    session = cluster_with_udf.create_session()
    k = DS.id("udf_val_fail")
    await session.delete(k).execute()
    with pytest.raises(AerospikeError):
        await (
            session.execute_udf(k)
            .function(MODULE, "writeWithValidation")
            .passing("bx", 99)
            .execute()
        )

async def test_list_udf(aerospike_host, make_cluster_definition):
    """``list_udf`` reports name/hash/type and reflects register + remove."""
    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        path = "psdk_list_udf_probe.lua"
        with open(LUA_FILE, "rb") as f:
            body = f.read()
        try:
            rm = await session.remove_udf(path)
            await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        except Exception:
            pass
        assert not any(m["name"] == path for m in await session.list_udf())

        task = await session.register_udf(body, path, UDFLang.LUA)
        assert await task.wait_till_complete(sleep_time=0.2, max_attempts=50)

        mine = [m for m in await session.list_udf() if m["name"] == path]
        assert mine, "module not listed after register"
        (entry,) = mine
        assert entry["type"] == "LUA"
        assert entry["hash"]
        assert set(entry) == {"name", "hash", "type"}

        rm = await session.remove_udf(path)
        await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        assert not any(m["name"] == path for m in await session.list_udf())


async def test_register_udf_from_resource(aerospike_host, make_cluster_definition, tmp_path, monkeypatch):
    """``register_udf_from_resource`` loads a module from a Python package resource."""
    pkg = tmp_path / "psdk_udf_resource_pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "probe.lua").write_bytes(b"function noop(rec) return 1 end\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        session = cluster.create_session()
        server_path = "psdk_resource_probe.lua"
        try:
            rm = await session.remove_udf(server_path)
            await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        except Exception:
            pass

        task = await session.register_udf_from_resource(
            "psdk_udf_resource_pkg", "probe.lua", server_path)
        assert await task.wait_till_complete(sleep_time=0.2, max_attempts=50)
        assert any(m["name"] == server_path for m in await session.list_udf())

        rm = await session.remove_udf(server_path)
        await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        assert not any(m["name"] == server_path for m in await session.list_udf())


async def test_udf_admin_reachable_via_cluster_and_session(aerospike_host):
    """UDF admin works through the ClusterDefinition -> Cluster -> Session path.
       Registers via the Cluster, lists/removes via a Session obtained from it
    """
    if ":" in aerospike_host:
        hostname, port_str = aerospike_host.split(":", 1)
        port = int(port_str)
    else:
        hostname, port = aerospike_host, 3000
    path = "psdk_udf_via_cluster.lua"
    with open(LUA_FILE, "rb") as f:
        body = f.read()

    cluster = await apply_general_auth(ClusterDefinition(hostname, port)).connect()
    try:
        try:
            rm = await cluster.remove_udf(path)
            await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        except Exception:
            pass

        reg = await cluster.register_udf(body, path, UDFLang.LUA)
        assert await reg.wait_till_complete(sleep_time=0.2, max_attempts=50)

        session = cluster.create_session()
        assert any(m["name"] == path for m in await session.list_udf())

        rm = await session.remove_udf(path)
        assert await rm.wait_till_complete(sleep_time=0.2, max_attempts=50)
        assert not any(m["name"] == path for m in await cluster.list_udf())
    finally:
        await cluster.close()


async def test_write_chain_then_udf(cluster_with_udf):
    """A write segment chained into a UDF segment executes as one batch."""
    session = cluster_with_udf.create_session()
    k1 = DS.id("chain_w2u_1")
    k2 = DS.id("chain_w2u_2")
    await session.delete(k1, k2).execute()
    stream = await (
        session.upsert(k1).put({"wu": "written"})
        .execute_udf(k2)
        .function(MODULE, "writeBin")
        .passing("wu", "via_udf")
        .execute()
    )
    rows = await stream.collect()
    assert len(rows) == 2
    assert all(r.is_ok for r in rows)
    r1 = await (
        await session.query(k1).bins(["wu"]).execute()
    ).first_or_raise()
    assert r1.record is not None
    assert r1.record.bins.get("wu") == "written"
    r2 = await (
        await session.query(k2).bins(["wu"]).execute()
    ).first_or_raise()
    assert r2.record is not None
    assert r2.record.bins.get("wu") == "via_udf"


async def test_query_chain_then_udf(cluster_with_udf):
    """A read segment chained into a UDF segment returns both results in order."""
    session = cluster_with_udf.create_session()
    k1 = DS.id("chain_q2u_1")
    k2 = DS.id("chain_q2u_2")
    await session.upsert(k1).put({"qa": 1}).execute()
    await session.delete(k2).execute()
    stream = await (
        session.query(k1).bins(["qa"])
        .execute_udf(k2)
        .function(MODULE, "writeBin")
        .passing("qa", 2)
        .execute()
    )
    rows = await stream.collect()
    assert len(rows) == 2
    assert rows[0].is_ok
    assert rows[0].record is not None
    assert rows[0].record.bins.get("qa") == 1
    assert rows[1].is_ok
    r2 = await (
        await session.query(k2).bins(["qa"]).execute()
    ).first_or_raise()
    assert r2.record is not None
    assert r2.record.bins.get("qa") == 2


async def test_write_chain_udf_then_read_chain(cluster_with_udf):
    """Write -> UDF -> read: the forward transition composes with the existing
    UDF-to-read transition in a single three-segment batch."""
    session = cluster_with_udf.create_session()
    k1 = DS.id("chain_w2u2q_1")
    k2 = DS.id("chain_w2u2q_2")
    await session.delete(k1, k2).execute()
    await session.upsert(k2).put({"seed": "old"}).execute()
    stream = await (
        session.upsert(k1).put({"seed": "new"})
        .execute_udf(k2)
        .function(MODULE, "writeBin")
        .passing("seed", "udf")
        .query(k1)
        .bins(["seed"])
        .execute()
    )
    rows = await stream.collect()
    assert len(rows) == 3
    assert all(r.is_ok for r in rows)
    assert rows[2].record is not None
    assert rows[2].record.bins.get("seed") == "new"


SLEEP_LUA_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "udf", "sleep_example.lua"),
)
SLEEP_SERVER_PATH = "sleep_example.lua"
SLEEP_MODULE = "sleep_example"


@pytest.fixture(scope="module")
async def cluster_with_sleep_udf(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as c:
        udf_session = c.create_session()
        try:
            rm = await udf_session.remove_udf(SLEEP_SERVER_PATH)
            await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        except Exception:
            pass
        reg = await udf_session.register_udf_from_file(
            SLEEP_LUA_FILE, SLEEP_SERVER_PATH, UDFLang.LUA
        )
        assert await reg.wait_till_complete(sleep_time=0.2, max_attempts=50)
        yield c
        try:
            rm = await udf_session.remove_udf(SLEEP_SERVER_PATH)
            await rm.wait_till_complete(sleep_time=0.1, max_attempts=20)
        except Exception:
            pass


async def test_udf_client_timeout_marks_in_doubt(cluster_with_sleep_udf):
    """A client socket timeout on a write that reached the server marks the error in-doubt."""
    # The socket timeout must fire while the server is still executing the
    # UDF: the write reached the wire but its outcome is unknown, which is
    # exactly the in-doubt condition. total_timeout must stay 0: a nonzero
    # total makes the client send min(socket, total) as the server-side
    # deadline, and the server's own UDF abort then beats the client's
    # socket timer. With no server deadline the client's 250ms timer is the
    # only one racing the 1000ms UDF sleep, so the client-side timeout is
    # deterministic even under server load.
    behavior = Behavior.DEFAULT.derive_with_changes(
        "udf_client_timeout",
        writes=Settings(
            socket_timeout=timedelta(milliseconds=250),
            total_timeout=timedelta(0),
            max_retries=0,
        ),
    )
    session = cluster_with_sleep_udf.create_session(behavior)
    k = DS.id("udf_in_doubt_1")
    with pytest.raises(TimeoutError) as exc_info:
        await (
            session.execute_udf(k)
            .function(SLEEP_MODULE, "sleep")
            .passing(1000)
            .execute()
        )
    assert exc_info.value.in_doubt is True


async def test_udf_client_timeout_carries_retry_context(cluster_with_sleep_udf):
    """A retried client timeout surfaces the full retry context, SDK-typed.

    Same deterministic shape as the in-doubt test above (client socket timer
    racing a longer server-side UDF sleep, no server deadline), with retries
    enabled so prior attempts are recorded. Asserts the Phase carried by the
    boundary conversion: ``client`` provenance, ``node``, ``iteration``,
    ``base_message``, and ``sub_exceptions`` converted into the SDK hierarchy.
    """
    behavior = Behavior.DEFAULT.derive_with_changes(
        "udf_retry_context",
        writes=Settings(
            socket_timeout=timedelta(milliseconds=250),
            total_timeout=timedelta(0),
            max_retries=2,
        ),
    )
    session = cluster_with_sleep_udf.create_session(behavior)
    k = DS.id("udf_retry_ctx_1")
    with pytest.raises(TimeoutError) as exc_info:
        await (
            session.execute_udf(k)
            .function(SLEEP_MODULE, "sleep")
            .passing(1000)
            .execute()
        )
    err = exc_info.value
    assert err.in_doubt is True
    assert err.client is True
    assert err.node, "expected the attempted node on the exception"
    assert err.iteration is not None and err.iteration >= 1
    assert err.base_message
    assert err.sub_exceptions, "expected prior attempts in sub_exceptions"
    assert all(isinstance(s, TimeoutError) for s in err.sub_exceptions)
    assert all(isinstance(s, AerospikeError) for s in err.sub_exceptions)
