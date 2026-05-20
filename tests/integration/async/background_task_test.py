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

"""Integration tests for session.background_task() (async)."""

import pytest
import pytest_asyncio
from aerospike_async import Operation, UDFLang

from aerospike_sdk import DataSet, Client

NS = "test"
SET = "pfc_bg_task"
DS = DataSet.of(NS, SET)
BG_BIN = "bgval"
BG_BIN2 = "bgval2"
MARKER = "bg_marker"
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

function incrementBin(r, name, amount)
    if not aerospike:exists(r) then
        aerospike:create(r)
        r[name] = 0
    end
    r[name] = r[name] + amount
    aerospike:update(r)
end

function writeWithValidation(r, name, value)
    if (value >= 1 and value <= 10) then
        putBin(r, name, value)
    else
        error("1000:Invalid value")
    end
end
"""


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy):
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        session = c.create_session()
        raw = c._client
        assert raw is not None
        reg = await raw.register_udf(BG_UDF_LUA, UDF_PATH, UDFLang.LUA)
        await reg.wait_till_complete()
        for i in range(1, 60):
            try:
                await session.delete(DS.id(f"bg_{i}")).execute()
            except Exception:
                pass
            try:
                await session.delete(DS.id(i)).execute()
            except Exception:
                pass
        yield c


async def test_background_update(client):
    session = client.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = await (
        session.background_task()
            .update(DS)
            .bin(BG_BIN2).set_to("updated")
            .execute()
    )
    assert await task.wait_till_complete()
    for i in range(1, 11):
        rs = await session.query(DS.id(f"bg_{i}")).bins([BG_BIN2]).execute()
        rr = await rs.first_or_raise()
        assert rr.record is not None
        assert rr.record.bins.get(BG_BIN2) == "updated"


async def test_background_update_with_where(client):
    session = client.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = await (
        session.background_task()
            .update(DS)
            .where("$.bgval > 5")
            .bin(BG_BIN2).set_to("filtered")
            .execute()
    )
    assert await task.wait_till_complete()
    for i in range(1, 11):
        rs = await session.query(DS.id(f"bg_{i}")).bins([BG_BIN2]).execute()
        rr = await rs.first_or_raise()
        assert rr.record is not None
        if i > 5:
            assert rr.record.bins.get(BG_BIN2) == "filtered"
        else:
            assert rr.record.bins.get(BG_BIN2) == "original"


async def test_background_delete(client):
    session = client.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .execute()
        )
    task = await (
        session.background_task()
            .delete(DS)
            .where("$.bgval > 8")
            .execute()
    )
    assert await task.wait_till_complete()
    for i in range(1, 11):
        rs = await session.query(DS.id(f"bg_{i}")).execute()
        rr = await rs.first()
        if i > 8:
            assert rr is None
        else:
            assert rr is not None
            assert rr.is_ok


async def test_background_touch(client):
    session = client.create_session()
    for i in range(1, 11):
        await session.upsert(DS.id(f"bg_{i}")).bin(BG_BIN).set_to(i).execute()
    task = await (
        session.background_task()
            .touch(DS)
            .expire_record_after_seconds(60)
            .execute()
    )
    assert await task.wait_till_complete()
    rs = await session.query(DS.id("bg_1")).execute()
    rr = await rs.first_or_raise()
    assert rr.record is not None
    assert rr.record.ttl is not None
    assert rr.record.ttl > 0


async def test_background_udf(client):
    session = client.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = await (
        session.background_task()
            .execute_udf(DS)
            .function(UDF_MODULE, "writeBin")
            .passing(BG_BIN2, "udf_written")
            .execute()
    )
    assert await task.wait_till_complete()
    rs = await session.query(DS.id("bg_1")).bins([BG_BIN2]).execute()
    rr = await rs.first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get(BG_BIN2) == "udf_written"


async def test_background_udf_with_args(client):
    session = client.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .execute()
        )
    task = await (
        session.background_task()
            .execute_udf(DS)
            .function(UDF_MODULE, "incrementBin")
            .passing(BG_BIN, 100)
            .execute()
    )
    assert await task.wait_till_complete()
    for i in range(1, 11):
        rs = await session.query(DS.id(f"bg_{i}")).bins([BG_BIN]).execute()
        rr = await rs.first_or_raise()
        assert rr.record is not None
        assert rr.record.bins.get(BG_BIN) == i + 100


async def test_background_udf_with_where(client):
    session = client.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = await (
        session.background_task()
            .execute_udf(DS)
            .function(UDF_MODULE, "writeBin")
            .passing(BG_BIN2, "udf_filtered")
            .where("$.bgval <= 3")
            .execute()
    )
    assert await task.wait_till_complete()
    for i in range(1, 11):
        rs = await session.query(DS.id(f"bg_{i}")).bins([BG_BIN2]).execute()
        rr = await rs.first_or_raise()
        assert rr.record is not None
        if i <= 3:
            assert rr.record.bins.get(BG_BIN2) == "udf_filtered"
        else:
            assert rr.record.bins.get(BG_BIN2) == "original"


async def test_background_udf_with_records_per_second(client):
    session = client.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = await (
        session.background_task()
            .execute_udf(DS)
            .function(UDF_MODULE, "writeBin")
            .passing(BG_BIN2, "rate_limited")
            .records_per_second(100)
            .execute()
    )
    assert await task.wait_till_complete()
    rs = await session.query(DS.id("bg_1")).bins([BG_BIN2]).execute()
    rr = await rs.first_or_raise()
    assert rr.record is not None
    assert rr.record.bins.get(BG_BIN2) == "rate_limited"


async def test_background_udf_with_validation(client):
    session = client.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
                .bin(BG_BIN).set_to(i)
                .bin(BG_BIN2).set_to("original")
                .execute()
        )
    task = await (
        session.background_task()
            .execute_udf(DS)
            .function(UDF_MODULE, "writeWithValidation")
            .passing(BG_BIN2, 5)
            .execute()
    )
    assert await task.wait_till_complete()
    for i in range(1, 11):
        rs = await session.query(DS.id(f"bg_{i}")).bins([BG_BIN2]).execute()
        rr = await rs.first_or_raise()
        assert rr.record is not None
        assert rr.record.bins.get(BG_BIN2) == 5


async def test_legacy_query_builder_background_scan(client):
    session = client.create_session()
    for i in range(5):
        await session.upsert(DS.id(i)).put({BG_BIN: i}).execute()
    task = await (
        session.query(DS)
            .with_write_operations([Operation.put(MARKER, 1)])
            .execute_background_task()
    )
    assert await task.wait_till_complete()
    rec = await (
        await session.query(DS.id(0)).bins([MARKER]).execute()
    ).first_or_raise()
    assert rec.record.bins.get(MARKER) == 1


async def test_point_query_rejects_background_task(client):
    session = client.create_session()
    k = DS.id(40)
    await session.upsert(k).put({BG_BIN: 1}).execute()
    with pytest.raises(ValueError, match="dataset queries"):
        await (
            session.query(k)
                .with_write_operations([Operation.put(MARKER, 1)])
                .execute_background_task()
        )
