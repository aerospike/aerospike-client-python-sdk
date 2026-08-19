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

"""Integration tests for a UDF apply folded into a heterogeneous batch (async)."""

from __future__ import annotations

import os

import pytest
from aerospike_sdk import DataSet, UDFLang
from tests.integration.namespace import general_namespace

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
        reg = await udf_session.register_udf_from_file(LUA_FILE, SERVER_PATH, UDFLang.LUA)
        assert await reg.wait_till_complete(sleep_time=0.2, timeout=10.0)
        yield c
        try:
            rm = await udf_session.remove_udf(SERVER_PATH)
            await rm.wait_till_complete(sleep_time=0.1, timeout=2.0)
        except Exception:
            pass


async def test_udf_folds_into_mixed_batch(cluster_with_udf):
    """A UDF apply combined with a read and a delete runs in one mixed batch.

    Exercises the ``BatchUDFOp`` path: a heterogeneous batch carrying a read,
    a per-key UDF apply, and a delete resolves to a single ``client.batch``
    call, and the UDF actually writes its bin.
    """
    session = cluster_with_udf.create_session()
    k_read = DS.id("bu_read")
    k_udf = DS.id("bu_udf")
    k_del = DS.id("bu_del")

    await session.upsert(k_read).put({"n": 1}).execute()
    await session.upsert(k_del).put({"x": 9}).execute()
    await session.delete(k_udf).execute()

    stream = await (
        session.query(k_read)
        .execute_udf(k_udf).function(MODULE, "writeBin").passing("mbin", 42)
        .delete(k_del)
        .execute()
    )
    results = {rr.key.value: rr async for rr in stream}

    assert results["bu_read"].is_ok
    assert results["bu_read"].record.bins.get("n") == 1
    assert results["bu_udf"].is_ok
    assert results["bu_del"].is_ok

    # The UDF ran as part of the batch and persisted its write.
    after = await (
        await session.query(k_udf).bins(["mbin"]).execute()
    ).first_or_raise()
    assert after.record.bins.get("mbin") == 42
