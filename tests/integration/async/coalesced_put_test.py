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

"""Transparent same-tick write coalescer on :meth:`Session.put`.

The coalescer is on by default and must be invisible: a concurrent burst of
``put`` calls fuses into one PAC crossing while each key keeps its own payload,
a lone/sequential ``put`` dispatches directly, and errors surface per call —
every outcome identical to a write with the coalescer disabled.
"""

import asyncio

import pytest

import aerospike_sdk.aio.session as session_mod
from aerospike_sdk.dataset import DataSet

_DS = DataSet.of("test", "coalesced_put")


@pytest.fixture
async def session(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as cluster:
        yield cluster.create_session()


async def test_concurrent_burst_keeps_per_key_payloads(session):
    """A same-tick burst coalesces yet every key keeps its own payload."""
    keys = [_DS.id(i) for i in range(64)]
    await asyncio.gather(
        *(session.put(key, {"v": i, "name": f"user-{i}"}) for i, key in enumerate(keys))
    )
    records = await asyncio.gather(*(session.get(key) for key in keys))
    assert [r.bins["v"] for r in records] == list(range(64))
    assert [r.bins["name"] for r in records] == [f"user-{i}" for i in range(64)]

async def test_put_returns_none(session):
    """A coalesced write resolves to ``None``, as a direct write does."""
    results = await asyncio.gather(
        *(session.put(_DS.id(f"ret-{i}"), {"v": i}) for i in range(8))
    )
    assert results == [None] * 8

async def test_sequential_puts_are_correct(session):
    """z=1: a lone put per tick dispatches directly and writes correctly."""
    for i in range(10):
        await session.put(_DS.id(f"seq-{i}"), {"v": i})
    for i in range(10):
        assert (await session.get(_DS.id(f"seq-{i}"))).bins["v"] == i

async def test_reused_payload_dict_is_snapshotted(session):
    """A dict mutated between buffered writes must not leak into its peers.

    The buffered path defers submission to the flush, so it copies; without
    that, every key in the burst would land the dict's final contents.
    """
    payload = {"v": None}
    keys = [_DS.id(f"reuse-{i}") for i in range(16)]

    async def write(key, value):
        payload["v"] = value
        await session.put(key, payload)

    await asyncio.gather(*(write(key, i) for i, key in enumerate(keys)))
    records = await asyncio.gather(*(session.get(key) for key in keys))
    assert [r.bins["v"] for r in records] == list(range(16))

async def test_mixed_tick_coalesces_both_directions(session):
    """Reads and writes in one tick fuse independently, not into each other.

    The two halves target disjoint keys: a read and a write racing on the same
    key has no defined order, coalesced or not.
    """
    read_keys = [_DS.id(f"mixed-r-{i}") for i in range(16)]
    write_keys = [_DS.id(f"mixed-w-{i}") for i in range(16)]
    for i, key in enumerate(read_keys):
        await session.put(key, {"v": i})

    results = await asyncio.gather(
        *(session.get(key) for key in read_keys),
        *(session.put(key, {"v": i + 100}) for i, key in enumerate(write_keys)),
    )
    assert [r.bins["v"] for r in results[:16]] == list(range(16))
    assert results[16:] == [None] * 16
    written = await asyncio.gather(*(session.get(key) for key in write_keys))
    assert [r.bins["v"] for r in written] == [i + 100 for i in range(16)]

async def test_results_match_with_coalescer_disabled(session, monkeypatch):
    """Coalescer on vs off yields identical written records (transparency)."""
    keys = [_DS.id(f"parity-{i}") for i in range(16)]

    await asyncio.gather(*(session.put(k, {"v": i}) for i, k in enumerate(keys)))
    coalesced = await asyncio.gather(*(session.get(k) for k in keys))

    monkeypatch.setattr(session_mod, "_COALESCE_WRITES", False)
    await asyncio.gather(*(session.put(k, {"v": i}) for i, k in enumerate(keys)))
    direct = await asyncio.gather(*(session.get(k) for k in keys))

    assert [r.bins["v"] for r in coalesced] == [r.bins["v"] for r in direct]

async def test_write_gate_leaves_reads_coalescing(session, monkeypatch):
    """``PSDK_COALESCE_WRITES=0`` sends writes direct without touching reads."""
    monkeypatch.setattr(session_mod, "_COALESCE_WRITES", False)
    keys = [_DS.id(f"wgate-{i}") for i in range(16)]

    await asyncio.gather(*(session.put(k, {"v": i}) for i, k in enumerate(keys)))
    # No write ever buffers, so the write buffer stays empty even mid-burst.
    assert session._coalesce_write_keys == []

    records = await asyncio.gather(*(session.get(k) for k in keys))
    assert [r.bins["v"] for r in records] == list(range(16))

async def test_unconvertible_payload_fails_only_its_own_caller(session):
    """A bad payload raises to its caller instead of stalling the window.

    Payload conversion happens during the fused crossing, after the buffers have
    been drained, so an unconvertible value must not leave any caller — itself
    included — awaiting a future that nothing will complete.
    """
    good = [_DS.id(f"badpay-ok-{i}") for i in range(8)]

    results = await asyncio.wait_for(
        asyncio.gather(
            *(session.put(k, {"v": i}) for i, k in enumerate(good)),
            session.put(_DS.id("badpay-bad"), {"v": object()}),
            return_exceptions=True,
        ),
        timeout=10.0,
    )

    assert results[:-1] == [None] * 8
    assert isinstance(results[-1], TypeError)
    records = await asyncio.gather(*(session.get(k) for k in good))
    assert [r.bins["v"] for r in records] == list(range(8))
