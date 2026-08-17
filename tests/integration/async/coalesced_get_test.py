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

"""Transparent same-tick read coalescer on :meth:`Session.get`.

The coalescer is on by default and must be invisible: a concurrent burst of
``get`` calls fuses into one PAC crossing, a lone/sequential ``get`` dispatches
directly, and a missing key raises — every result byte-identical to a read with
the coalescer disabled.
"""

import asyncio

import pytest

import aerospike_sdk.aio.session as session_mod
from aerospike_sdk.exceptions import RecordNotFoundError
from aerospike_sdk.dataset import DataSet
from tests.integration.namespace import general_namespace

_DS = DataSet.of(general_namespace(), "coalesced_get")


@pytest.fixture(scope="module")
async def session(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        yield cluster.create_session()


async def test_concurrent_burst_delivers_positionally(session):
    """A same-tick burst coalesces yet each get returns its own record."""
    keys = [_DS.id(i) for i in range(64)]
    for i, key in enumerate(keys):
        await session.put(key, {"v": i})
    records = await asyncio.gather(*(session.get(key) for key in keys))
    assert [r.bins["v"] for r in records] == list(range(64))


async def test_sequential_gets_are_correct(session):
    """z=1: a lone get per tick dispatches directly and reads correctly."""
    for i in range(10):
        await session.put(_DS.id(i), {"v": i})
    for i in range(10):
        record = await session.get(_DS.id(i))
        assert record.bins["v"] == i


async def test_missing_key_raises(session):
    """A not-found read raises ``RecordNotFoundError`` — identical to a direct get."""
    with pytest.raises(RecordNotFoundError):
        await session.get(_DS.id(9_999_999))


async def test_results_match_with_coalescer_disabled(session, monkeypatch):
    """Coalescer on vs off yields identical records (transparency)."""
    keys = [_DS.id(i) for i in range(16)]
    for i, key in enumerate(keys):
        await session.put(key, {"v": i})

    coalesced = await asyncio.gather(*(session.get(key) for key in keys))
    monkeypatch.setattr(session_mod, "_COALESCE", False)
    direct = await asyncio.gather(*(session.get(key) for key in keys))

    assert [r.bins["v"] for r in coalesced] == [r.bins["v"] for r in direct]
