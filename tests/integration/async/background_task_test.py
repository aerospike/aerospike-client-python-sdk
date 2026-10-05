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

from tests.pac_compat import requires_server_compiled_ael
import pytest_asyncio
from aerospike_sdk import Exp, HllConfig, UDFLang
from aerospike_async import Filter, MapOperation, MapReturnType, Operation

from aerospike_sdk import DataSet
from tests.integration.namespace import general_namespace

NS = general_namespace()
SET = "pfc_bg_task"
DS = DataSet.of(NS, SET)
# Background jobs touch every record in their set; collection tests get their own.
CDT_DS = DataSet.of(NS, "pfc_bg_cdt")
CDT_KEYS = 3
CUTOFF = [1704067200]
BG_BIN = "bgval"
BG_BIN2 = "bgval2"
BG_INDEX = "pfc_bg_idx"
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
async def cluster(aerospike_host, make_cluster_definition):
    async with make_cluster_definition(aerospike_host).connect() as c:
        session = c.create_session()
        reg = await c.register_udf(BG_UDF_LUA, UDF_PATH, UDFLang.LUA)
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


async def _ensure_bg_index(session):
    task = await session.index(DS).on_bin(BG_BIN).named(BG_INDEX).integer().create()
    assert await task.wait_till_complete()


async def _seed_profiles(session):
    for i in range(CDT_KEYS):
        await session.replace(CDT_DS.id(i)).put({
            "segments": {"expired": [1700000000], "active": [1800000000]},
            "prefs": {"tags": ["news"]},
            "score": 10,
        }).execute()


async def _bins(session, i, *names):
    rs = await session.query(CDT_DS.id(i)).bins(list(names)).execute()
    return (await rs.first_or_raise()).record_or_raise().bins


async def test_background_update(cluster):
    session = cluster.create_session()
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


@requires_server_compiled_ael
async def test_background_update_with_where(cluster):
    session = cluster.create_session()
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


@requires_server_compiled_ael
async def test_background_delete(cluster):
    session = cluster.create_session()
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


async def test_background_touch(cluster):
    session = cluster.create_session()
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


async def test_background_udf(cluster):
    session = cluster.create_session()
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


async def test_background_udf_with_args(cluster):
    session = cluster.create_session()
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


@requires_server_compiled_ael
async def test_background_udf_with_where(cluster):
    session = cluster.create_session()
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


async def test_background_udf_with_records_per_second(cluster):
    session = cluster.create_session()
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


async def test_background_udf_with_validation(cluster):
    session = cluster.create_session()
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


async def test_legacy_query_builder_background_scan(cluster):
    session = cluster.create_session()
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


@requires_server_compiled_ael
async def test_query_builder_background_task_honors_a_string_where(cluster):
    session = cluster.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bg_{i}"))
            .bin(BG_BIN).set_to(i)
            .bin(BG_BIN2).set_to("original")
            .execute()
        )
    task = await (
        session.query(DS)
        .where("$.bgval > 5")
        .with_write_operations([Operation.put(BG_BIN2, "filtered")])
        .execute_background_task()
    )
    assert await task.wait_till_complete()
    for i in range(1, 11):
        rs = await session.query(DS.id(f"bg_{i}")).bins([BG_BIN2]).execute()
        rr = await rs.first_or_raise()
        assert rr.record.bins.get(BG_BIN2) == ("filtered" if i > 5 else "original")


async def test_point_query_rejects_background_task(cluster):
    session = cluster.create_session()
    k = DS.id(40)
    await session.upsert(k).put({BG_BIN: 1}).execute()
    with pytest.raises(ValueError, match="dataset queries"):
        await (
            session.query(k)
            .with_write_operations([Operation.put(MARKER, 1)])
            .execute_background_task()
        )


async def test_background_update_with_records_per_second(cluster):
    """A throttled background job still runs to completion and applies its writes.

    The rate itself is the server's to enforce, so this does not assert timing;
    what it covers is that a throttle the server accepts is carried on the wire
    without breaking the job. A rejected or malformed value would surface here
    as a failed task rather than a slower one.
    """
    session = cluster.create_session()
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bgrps_{i}"))
            .bin(BG_BIN).set_to(i)
            .bin(BG_BIN2).set_to("original")
            .execute()
        )
    task = await (
        session.background_task()
        .update(DS)
        .bin(BG_BIN2).set_to("throttled")
        .records_per_second(1000)
        .execute()
    )
    assert await task.wait_till_complete()
    for i in range(1, 11):
        rs = await session.query(DS.id(f"bgrps_{i}")).bins([BG_BIN2]).execute()
        rr = await rs.first_or_raise()
        assert rr.record is not None
        assert rr.record.bins.get(BG_BIN2) == "throttled"

@requires_server_compiled_ael
async def test_background_update_with_index_filter_and_where(cluster):
    """The index selects the candidates; the predicate decides which get written.

    The predicate excludes the records an earlier run already processed, so a
    second run must change nothing.
    """
    session = cluster.create_session()
    await _ensure_bg_index(session)
    for i in range(1, 11):
        await (
            session.upsert(DS.id(f"bgif_{i}"))
            .bin(BG_BIN).set_to(i)
            .bin(BG_BIN2).set_to("original")
            .execute()
        )

    task = await (
        session.background_task()
        .update(DS)
        .filter(Filter.range(BG_BIN, 4, 8))
        .where("not($.bgval2.exists()) or $.bgval2 == 'original'")
        .bin(BG_BIN2).set_to("touched")
        .execute()
    )
    assert await task.wait_till_complete()

    async def marker(i):
        rs = await session.query(DS.id(f"bgif_{i}")).bins([BG_BIN2]).execute()
        return (await rs.first_or_raise()).record.bins.get(BG_BIN2)

    # Only the index range was written; neither narrowing alone would do this.
    for i in range(1, 11):
        assert await marker(i) == ("touched" if 4 <= i <= 8 else "original")

    # The predicate reached the server: a second run finds nothing left to do.
    task = await (
        session.background_task()
        .update(DS)
        .filter(Filter.range(BG_BIN, 4, 8))
        .where("not($.bgval2.exists()) or $.bgval2 == 'original'")
        .bin(BG_BIN2).set_to("second_pass")
        .execute()
    )
    assert await task.wait_till_complete()
    for i in range(4, 9):
        assert await marker(i) == "touched"


async def test_background_delete_with_index_filter_and_where(cluster):
    """Only records inside the index range that also match the predicate are deleted."""
    session = cluster.create_session()
    await _ensure_bg_index(session)
    values = range(101, 111)
    for i in values:
        await session.upsert(DS.id(f"bgdel_{i}")).bin(BG_BIN).set_to(i).execute()

    task = await (
        session.background_task()
        .delete(DS)
        .filter(Filter.range(BG_BIN, 104, 108))
        .where(Exp.ne(Exp.int_bin(BG_BIN), Exp.int_val(106)))
        .execute()
    )
    assert await task.wait_till_complete()

    for i in values:
        rr = await (await session.query(DS.id(f"bgdel_{i}")).execute()).first()
        if 104 <= i <= 108 and i != 106:
            assert rr is None
        else:
            assert rr is not None and rr.is_ok


async def test_background_touch_with_index_filter(cluster):
    """A filter alone confines the job to the index range."""
    session = cluster.create_session()
    await _ensure_bg_index(session)
    values = range(201, 211)
    for i in values:
        await session.upsert(DS.id(f"bgtouch_{i}")).bin(BG_BIN).set_to(i).execute()

    async def generation(i):
        rs = await session.query(DS.id(f"bgtouch_{i}")).execute()
        return (await rs.first_or_raise()).record.generation

    before = {i: await generation(i) for i in values}
    task = await (
        session.background_task()
        .touch(DS)
        .filter(Filter.range(BG_BIN, 204, 208))
        .expire_record_after_seconds(600)
        .execute()
    )
    assert await task.wait_till_complete()

    for i in values:
        touched = 204 <= i <= 208
        assert await generation(i) == before[i] + (1 if touched else 0)


async def test_background_update_map_value_range_remove(cluster):
    session = cluster.create_session()
    await _seed_profiles(session)
    task = await (
        session.background_task()
        .update(CDT_DS)
        .bin("segments").on_map_value_range(None, CUTOFF).remove()
        .execute()
    )
    assert await task.wait_till_complete()
    for i in range(CDT_KEYS):
        assert (await _bins(session, i, "segments"))["segments"] == {"active": [1800000000]}


async def test_background_update_nested_list_append(cluster):
    """The write lands in the list under the map key, not on the bin itself."""
    session = cluster.create_session()
    await _seed_profiles(session)
    task = await (
        session.background_task()
        .update(CDT_DS)
        .bin("prefs").on_map_key("tags").list_append_items(["sports"])
        .execute()
    )
    assert await task.wait_till_complete()
    for i in range(CDT_KEYS):
        assert (await _bins(session, i, "prefs"))["prefs"] == {"tags": ["news", "sports"]}


async def test_background_update_hll_add(cluster):
    session = cluster.create_session()
    await _seed_profiles(session)
    task = await (
        session.background_task()
        .update(CDT_DS)
        .bin("visitors").hll_add(["alice", "bob", "carol"], config=HllConfig.of(8))
        .execute()
    )
    assert await task.wait_till_complete()
    for i in range(CDT_KEYS):
        rs = await session.query(CDT_DS.id(i)).bin("visitors").hll_get_count().execute()
        assert (await rs.first_or_raise()).record_or_raise().bins["visitors"] == 3


@requires_server_compiled_ael
async def test_background_update_expression_write(cluster):
    session = cluster.create_session()
    await _seed_profiles(session)
    task = await (
        session.background_task()
        .update(CDT_DS)
        .bin("doubled").upsert_from("$.score * 2")
        .execute()
    )
    assert await task.wait_till_complete()
    for i in range(CDT_KEYS):
        assert (await _bins(session, i, "doubled"))["doubled"] == 20


async def test_query_builder_background_task_with_map_operation(cluster):
    session = cluster.create_session()
    await _seed_profiles(session)
    task = await (
        session.query(CDT_DS)
        .with_write_operations([MapOperation.remove_by_value_range(
            "segments", None, CUTOFF, MapReturnType.NONE,
        )])
        .execute_background_task()
    )
    assert await task.wait_till_complete()
    for i in range(CDT_KEYS):
        assert (await _bins(session, i, "segments"))["segments"] == {"active": [1800000000]}
