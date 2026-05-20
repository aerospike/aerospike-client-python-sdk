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

"""Integration tests for foreground UDF SDK API (sync)."""

from __future__ import annotations

import os

import pytest
from aerospike_async import UDFLang
from aerospike_async.exceptions import ResultCode

from aerospike_sdk import DataSet, SyncClient

NS = "test"
SET = "test"
DS = DataSet.of(NS, SET)
LUA_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "udf", "record_example.lua"),
)
SERVER_PATH = "record_example.lua"
MODULE = "record_example"


def _wait_task(client: SyncClient, task) -> bool:
    """Wait for ``task`` synchronously via PAC's blocking sibling."""
    return task.wait_till_complete_blocking(sleep_time=0.2, max_attempts=50)


@pytest.fixture
def client_with_udf(aerospike_host, client_policy):
    with SyncClient(seeds=aerospike_host, policy=client_policy) as client:
        try:
            rm = client.remove_udf(SERVER_PATH)
            _wait_task(client, rm)
        except Exception:
            pass
        reg = client.register_udf_from_file(LUA_FILE, SERVER_PATH, UDFLang.LUA)
        assert _wait_task(client, reg)
        yield client
        try:
            rm = client.remove_udf(SERVER_PATH)
            _wait_task(client, rm)
        except Exception:
            pass


def test_sync_write_using_udf(client_with_udf):
    session = client_with_udf.create_session()
    k = DS.id("sync_udf_w1")
    session.delete(k).execute()
    stream = (
        session.execute_udf(k)
            .function(MODULE, "writeBin")
            .passing("sb1", "sync val")
            .execute()
    )
    stream.first_or_raise()
    rr = session.query(k).bins(["sb1"]).execute().first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get("sb1") == "sync val"


def test_sync_register_udf_from_bytes(client_with_udf):
    with open(LUA_FILE, "rb") as f:
        body = f.read()
    path = "record_example_sync_dup.lua"
    try:
        rm = client_with_udf.remove_udf(path)
        _wait_task(client_with_udf, rm)
    except Exception:
        pass
    task = client_with_udf.register_udf(body, path, UDFLang.LUA)
    assert _wait_task(client_with_udf, task)
    rm = client_with_udf.remove_udf(path)
    _wait_task(client_with_udf, rm)


def test_sync_batch_udf_validation_errors_in_stream(client_with_udf):
    session = client_with_udf.create_session()
    k1 = DS.id("sync_batch_udf_err_1")
    k2 = DS.id("sync_batch_udf_err_2")
    session.delete(k1, k2).execute()
    stream = (
        session.execute_udf(k1, k2)
            .function(MODULE, "writeWithValidation")
            .passing("B5", 999)
            .execute()
    )
    results = stream.collect()
    assert len(results) == 2
    keys = [r.key for r in results]
    assert k1 in keys and k2 in keys
    for r in results:
        assert r.result_code == ResultCode.UDF_BAD_RESPONSE
        assert r.record is not None


def test_sync_batch_udf_respond_all_keys_includes_filtered_out(client_with_udf):
    session = client_with_udf.create_session()
    k1 = DS.id("sync_batch_udf_rak_1")
    k2 = DS.id("sync_batch_udf_rak_2")
    session.delete(k1, k2).execute()
    session.upsert(k1).put({"v": 5}).execute()
    session.upsert(k2).put({"v": 20}).execute()

    stream = (
        session.execute_udf(k1, k2)
            .function(MODULE, "writeBin")
            .passing("tag", "hit")
            .where("$.v < 10")
            .execute()
    )
    results = stream.collect()
    assert len(results) == 1
    assert results[0].key == k1
    assert results[0].is_ok

    stream = (
        session.execute_udf(k1, k2)
            .function(MODULE, "writeBin")
            .passing("tag", "hit2")
            .where("$.v < 10")
            .respond_all_keys()
            .execute()
    )
    results = stream.collect()
    assert len(results) == 2
    r1 = next(r for r in results if r.key == k1)
    r2 = next(r for r in results if r.key == k2)
    assert r1.is_ok
    assert r2.result_code == ResultCode.FILTERED_OUT


def test_sync_write_if_generation_not_changed(client_with_udf):
    session = client_with_udf.create_session()
    k = DS.id("sync_udf_gen_guard")
    session.delete(k).execute()
    session.upsert(k).put({"gcol": "a"}).execute()
    gen = (
        session.execute_udf(k)
            .function(MODULE, "getGeneration")
            .execute()
            .first_udf_result()
    )
    assert isinstance(gen, int)
    (
        session.execute_udf(k)
            .function(MODULE, "writeIfGenerationNotChanged")
            .passing("gcol", "b", gen)
            .execute()
    )
    rr = session.query(k).bins(["gcol"]).execute().first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get("gcol") == "b"
    (
        session.execute_udf(k)
            .function(MODULE, "writeIfGenerationNotChanged")
            .passing("gcol", "should_not_apply", gen)
            .execute()
    )
    rr2 = session.query(k).bins(["gcol"]).execute().first_or_raise()
    assert rr2.record is not None
    assert rr2.record.bins.get("gcol") == "b"


def test_sync_write_unique_idempotent(client_with_udf):
    session = client_with_udf.create_session()
    k = DS.id("sync_udf_write_unique")
    session.delete(k).execute()
    (
        session.execute_udf(k)
            .function(MODULE, "writeUnique")
            .passing("ub", "first")
            .execute()
    )
    (
        session.execute_udf(k)
            .function(MODULE, "writeUnique")
            .passing("ub", "second")
            .execute()
    )
    rr = session.query(k).bins(["ub"]).execute().first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get("ub") == "first"


def test_sync_append_list_bin_via_udf(client_with_udf):
    session = client_with_udf.create_session()
    k = DS.id("sync_udf_list_append")
    session.delete(k).execute()
    session.insert(k).put({"lb": []}).execute()
    for v in (10, 20, 30):
        (
            session.execute_udf(k)
                .function(MODULE, "appendListBin")
                .passing("lb", v)
                .execute()
        )
    rr = session.query(k).bins(["lb"]).execute().first_or_raise()
    assert rr.record is not None
    lst = rr.record.bins.get("lb")
    assert lst is not None
    assert list(lst) == [10, 20, 30]


def test_sync_chained_udf_three_specs_mixed_ok_and_udf_bad_response(
    client_with_udf,
):
    session = client_with_udf.create_session()
    k1 = DS.id("sync_chain_udf_complex_1")
    k2 = DS.id("sync_chain_udf_complex_2")
    k3 = DS.id("sync_chain_udf_complex_3")
    session.delete(k1, k2, k3).execute()
    stream = (
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
    rows = stream.collect()
    assert len(rows) == 3
    assert rows[0].is_ok
    assert rows[0].key == k1
    assert rows[1].is_ok
    assert rows[1].key == k2
    assert not rows[2].is_ok
    assert rows[2].key == k3
    assert rows[2].result_code == ResultCode.UDF_BAD_RESPONSE
    assert rows[2].record is None
    r1 = session.query(k1).bins(["cx"]).execute().first_or_raise()
    assert r1.record is not None
    assert r1.record.bins.get("cx") == "ok1"
    r2 = session.query(k2).bins(["cx"]).execute().first_or_raise()
    assert r2.record is not None
    assert r2.record.bins.get("cx") == 7
