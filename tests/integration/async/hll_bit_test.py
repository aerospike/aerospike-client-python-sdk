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
# distributed under the License is distributed on an "AS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Integration tests for HyperLogLog and bit-operation fluent builders."""

import pytest
import pytest_asyncio

from aerospike_sdk import Client
from aerospike_sdk.dataset import DataSet


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy):
    async with Client(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        test_ds = DataSet.of("test", "test")
        await session.delete(test_ds.id("hll_bit_fluent_1")).execute()
        await session.delete(test_ds.id("hll_bit_fluent_2")).execute()
        yield client


async def test_hll_init_add_and_get_count(client):
    from aerospike_sdk import HllConfig
    session = client.create_session()
    k = DataSet.of("test", "test").id("hll_bit_fluent_1")
    await (
        session.upsert(k)
        .bin("hll")
        .hll_init(HllConfig.of(12))
        .bin("hll")
        .hll_add(["alpha", "beta", "gamma"])
        .execute()
    )
    rs = await session.query(k).bin("hll").hll_get_count().execute()
    first = await rs.first_or_raise()
    assert first.is_ok
    count = first.record_or_raise().bins["hll"]
    assert isinstance(count, int)
    assert count >= 1


async def test_bit_resize_set_and_get(client):
    session = client.create_session()
    k = DataSet.of("test", "test").id("hll_bit_fluent_2")
    await (
        session.upsert(k)
        .bin("bits")
        .bit_resize(2)
        .bin("bits")
        .bit_set(0, 8, b"\xab")
        .execute()
    )
    rs = await session.query(k).bin("bits").bit_get(0, 8).execute()
    first = await rs.first_or_raise()
    assert first.is_ok
    raw = first.record_or_raise().bins["bits"]
    assert raw == b"\xab"
