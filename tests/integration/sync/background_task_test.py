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

"""Integration tests for session.background_task() (sync)."""

import pytest
from aerospike_async import UDFLang

from aerospike_sdk import DataSet, SyncClient

NS = "test"
SET = "pfc_bg_task"
DS = DataSet.of(NS, SET)
BG_BIN = "bgval"
BG_BIN2 = "bgval2"
UDF_PATH = "pfc_bg_udf.lua"
UDF_MODULE = "pfc_bg_udf"

BG_UDF_LUA = br"""
local function putBin(r, name, value)
    if not aerospike:exists(r) then aerospike:create(r) end
    r[name] = value
    aerospike:update(r)
end

function writeBin(r, name, value)
    putBin(r, name, value)
end

function writeWithValidation(r, name, value)
    if (value >= 1 and value <= 10) then
        putBin(r, name, value)
    else
        error("1000:Invalid value")
    end
end
"""


def _wait_task(client: SyncClient, task) -> bool:
    """Wait for ``task`` synchronously via PAC's blocking sibling."""
    return task.wait_till_complete_blocking()


@pytest.fixture
def client(aerospike_host, client_policy):
    with SyncClient(seeds=aerospike_host, policy=client_policy) as c:
        ac = c._ensure_connected()
        raw = ac._client
        assert raw is not None

        reg = raw.register_udf_blocking(BG_UDF_LUA, UDF_PATH, UDFLang.LUA)
        reg.wait_till_complete_blocking()
        session = c.create_session()
        for i in range(1, 60):
            try:
                session.delete(DS.id(f"bg_{i}")).execute()
            except Exception:
                pass
        yield c


def test_sync_background_update(client):
    session = client.create_session()
    for i in range(1, 11):
        (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = (
        session.background_task()
            .update(DS)
            .bin(BG_BIN2).set_to("sync_updated")
            .execute()
    )
    assert _wait_task(client, task)
    for i in range(1, 11):
        rr = session.query(DS.id(f"bg_{i}")).bins([BG_BIN2]).execute().first_or_raise()
        assert rr.record is not None
        assert rr.record.bins.get(BG_BIN2) == "sync_updated"


def test_sync_background_delete(client):
    session = client.create_session()
    for i in range(1, 11):
        session.upsert(DS.id(f"bg_{i}")).bin(BG_BIN).set_to(i).execute()
    task = (
        session.background_task()
            .delete(DS)
            .where("$.bgval > 8")
            .execute()
    )
    assert _wait_task(client, task)
    for i in range(1, 11):
        rr = session.query(DS.id(f"bg_{i}")).execute().first()
        if i > 8:
            assert rr is None
        else:
            assert rr is not None
            assert rr.is_ok


def test_sync_background_touch(client):
    session = client.create_session()
    for i in range(1, 11):
        session.upsert(DS.id(f"bg_{i}")).bin(BG_BIN).set_to(i).execute()
    task = (
        session.background_task()
            .touch(DS)
            .expire_record_after_seconds(60)
            .execute()
    )
    assert _wait_task(client, task)
    rr = session.query(DS.id("bg_1")).execute().first_or_raise()
    assert rr.record is not None
    assert rr.record.ttl is not None
    assert rr.record.ttl > 0


def test_sync_background_udf(client):
    session = client.create_session()
    for i in range(1, 11):
        (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = (
        session.background_task()
            .execute_udf(DS)
            .function(UDF_MODULE, "writeBin")
            .passing(BG_BIN2, "sync_udf")
            .execute()
    )
    assert _wait_task(client, task)
    rr = session.query(DS.id("bg_1")).bins([BG_BIN2]).execute().first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get(BG_BIN2) == "sync_udf"


def test_sync_background_udf_with_validation(client):
    session = client.create_session()
    for i in range(1, 11):
        (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = (
        session.background_task()
            .execute_udf(DS)
            .function(UDF_MODULE, "writeWithValidation")
            .passing(BG_BIN2, 5)
            .execute()
    )
    assert _wait_task(client, task)
    for i in range(1, 11):
        rr = session.query(DS.id(f"bg_{i}")).bins([BG_BIN2]).execute().first_or_raise()
        assert rr.record is not None
        assert rr.record.bins.get(BG_BIN2) == 5
