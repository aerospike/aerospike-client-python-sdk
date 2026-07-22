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

"""AsyncPool: multi-loop / multi-member dispatch and lifecycle.

The pool spins up N event loops on N OS threads, each with its own
:class:`~aerospike_sdk.aio.cluster.Cluster` member built from one shared
:class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`.  Each
member's PAC ``CompletionBridge`` is bound to its own loop, so completions
never cross loops and the cross-loop guard in the bridge never fires during
normal use.
"""

import asyncio
import pytest

from aerospike_sdk import AsyncPool
from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.cluster import Cluster
from aerospike_sdk.dataset import DataSet
from aerospike_sdk import index_monitor as _index_monitor_module


class TestAsyncPoolLifecycle:
    """Start / stop / repeat-use semantics."""

    async def test_context_manager_starts_and_closes(
        self, aerospike_host, make_cluster_definition
    ):
        pool = AsyncPool(make_cluster_definition(aerospike_host), loop_count=2)
        assert not pool.is_started
        async with pool:
            assert pool.is_started
            assert not pool.is_closed
            assert pool.loop_count == 2
        assert pool.is_closed

    async def test_run_before_start_raises(
        self, aerospike_host, make_cluster_definition
    ):
        pool = AsyncPool(make_cluster_definition(aerospike_host), loop_count=2)
        with pytest.raises(RuntimeError, match="not started"):
            await pool.run(lambda c: _noop(c))

    async def test_run_after_close_raises(
        self, aerospike_host, make_cluster_definition
    ):
        pool = AsyncPool(make_cluster_definition(aerospike_host), loop_count=2)
        await pool.start()
        await pool.aclose()
        with pytest.raises(RuntimeError, match="closed"):
            await pool.run(lambda c: _noop(c))

    async def test_double_start_raises(
        self, aerospike_host, make_cluster_definition
    ):
        pool = AsyncPool(make_cluster_definition(aerospike_host), loop_count=2)
        await pool.start()
        try:
            with pytest.raises(RuntimeError, match="already started"):
                await pool.start()
        finally:
            await pool.aclose()

    async def test_default_loop_count_is_cpu_count(
        self, aerospike_host, make_cluster_definition
    ):
        import os
        pool = AsyncPool(make_cluster_definition(aerospike_host))
        assert pool.loop_count == (os.cpu_count() or 4)


class TestAsyncPoolDispatch:
    """run() / map() correctness and round-robin behavior."""

    async def test_run_roundtrips_on_pool_loop(
        self, aerospike_host, make_cluster_definition
    ):
        """Each `run` call dispatches a put+get; completions land on the right loop.

        The cross-loop guard in PAC's CompletionBridge is what makes this a
        real test: if the pool wired up loops incorrectly, the operation
        would fail with the owning-loop RuntimeError.
        """
        ds = DataSet.of("test", "asyncpool_run")
        async with AsyncPool(
            make_cluster_definition(aerospike_host), loop_count=4
        ) as pool:
            async def roundtrip(cluster: Cluster) -> int:
                session = cluster.create_session()
                key = ds.id("k0")
                await session.upsert(key).bin("v").set_to(99).execute()
                stream = await session.query(key).execute()
                row = await stream.first_or_raise()
                return row.record_or_raise().bins["v"]

            assert await pool.run(roundtrip) == 99

    async def test_map_dispatches_one_per_input_in_order(
        self, aerospike_host, make_cluster_definition
    ):
        """map() returns results in input order even though dispatch is round-robin."""
        ds = DataSet.of("test", "asyncpool_map")
        async with AsyncPool(
            make_cluster_definition(aerospike_host), loop_count=3
        ) as pool:
            async def put_and_read(cluster: Cluster, i: int) -> int:
                session = cluster.create_session()
                key = ds.id(f"k{i}")
                await session.upsert(key).bin("v").set_to(i * 10).execute()
                stream = await session.query(key).execute()
                row = await stream.first_or_raise()
                return row.record_or_raise().bins["v"]

            inputs = list(range(8))
            results = await pool.map(put_and_read, inputs)
            assert results == [i * 10 for i in inputs]

    async def test_pick_selects_specific_loop(
        self, aerospike_host, make_cluster_definition
    ):
        """`pick=` routes to a specific loop; every member is a Cluster."""
        async with AsyncPool(
            make_cluster_definition(aerospike_host), loop_count=3
        ) as pool:
            async def id_self(cluster: Cluster) -> str:
                return type(cluster).__name__

            for i in range(3):
                assert await pool.run(id_self, pick=i) == "Cluster"
            assert await pool.run(id_self, pick=10) == "Cluster"

    async def test_each_loop_gets_distinct_member(
        self, aerospike_host, make_cluster_definition
    ):
        """Identity-check: pick=i and pick=j return distinct members for i != j."""
        async with AsyncPool(
            make_cluster_definition(aerospike_host), loop_count=3
        ) as pool:
            async def whoami(cluster: Cluster) -> int:
                return id(cluster)

            ids = [await pool.run(whoami, pick=i) for i in range(3)]
            assert len(set(ids)) == 3, f"expected 3 distinct members, got ids={ids}"


class TestAsyncPoolDeprecatedFactory:
    """The one-cycle client_factory adapter: warns, and hands raw Clients."""

    async def test_client_factory_warns_and_dispatch_hands_client(
        self, aerospike_host, client_policy
    ):
        with pytest.warns(DeprecationWarning, match="client_factory"):
            pool = AsyncPool(
                client_factory=lambda: Client(
                    seeds=aerospike_host, policy=client_policy
                ),
                loop_count=2,
            )
        async with pool:
            async def type_name(member) -> str:
                return type(member).__name__

            assert await pool.run(type_name) == "Client"


class TestAsyncPoolLoopType:
    """Pool loop type follows the ``use_uvloop`` switch.

    uvloop 0.22.x has a libuv free-threading race on ``loop._ready_len``
    (MagicStack/uvloop issues #720, #721) that stalls a multi-loop pool when
    the GIL is disabled — the per-loop race fires across all loops at once and
    wedges (a hard hang on the fast-path pool path). PAC's drainer thread tames
    the single-loop case but not N concurrent loops, so ``AsyncPool`` defaults
    to the stdlib selector loop under free-threading (auto-decide tracks the
    GIL) and exposes ``use_uvloop`` to force either way.

    These guard the contract so a regression in loop selection fails loudly.
    """

    @staticmethod
    def _is_uvloop(loop: asyncio.AbstractEventLoop) -> bool:
        return "uvloop" in type(loop).__module__.lower()

    async def test_default_avoids_uvloop_under_free_threading(
        self, aerospike_host, make_cluster_definition
    ):
        """Default (``use_uvloop=None``): under free-threading the pool loops
        must NOT be uvloop — that's the multi-loop stall condition. Under
        GIL-on the uvloop default is safe, so the guard only bites under FT."""
        from aerospike_sdk.aio.pool import _gil_is_enabled

        if _gil_is_enabled():
            pytest.skip("GIL enabled: uvloop default is safe (FT race can't fire)")
        async with AsyncPool(
            make_cluster_definition(aerospike_host), loop_count=2
        ) as pool:
            for i, loop in enumerate(pool._loops):
                assert loop is not None, f"loop {i} is None"
                assert not self._is_uvloop(loop), (
                    f"loop {i} is uvloop ({type(loop).__module__}) under "
                    f"free-threading — exposes the multi-loop libuv stall"
                )

    async def test_use_uvloop_false_forces_selector(
        self, aerospike_host, make_cluster_definition
    ):
        """Explicit ``use_uvloop=False`` forces the stdlib selector loop
        regardless of the global uvloop policy or GIL state."""
        async with AsyncPool(
            make_cluster_definition(aerospike_host),
            loop_count=2,
            use_uvloop=False,
        ) as pool:
            for i, loop in enumerate(pool._loops):
                assert loop is not None, f"loop {i} is None"
                assert not self._is_uvloop(loop), (
                    f"loop {i} is {type(loop).__module__}; use_uvloop=False "
                    f"must force the stdlib selector loop"
                )


class TestAsyncPoolGuards:
    """Misuse detection."""

    async def test_self_dispatch_guard_raises(
        self, aerospike_host, make_cluster_definition
    ):
        """Running run() from within a pool loop deadlocks; the guard prevents it."""
        async with AsyncPool(
            make_cluster_definition(aerospike_host), loop_count=2
        ) as pool:
            async def recursive(cluster: Cluster) -> None:
                # Called on a pool loop — dispatching back into the pool from
                # here would deadlock the originating loop.
                await pool.run(lambda c: _noop(c))

            with pytest.raises(RuntimeError, match="within a pool loop"):
                await pool.run(recursive)


class TestAsyncPoolSharedMonitor:
    """Index metadata is cluster-scoped, not member-scoped — one monitor for the pool."""

    async def test_all_members_share_one_monitor_instance(
        self, aerospike_host, make_cluster_definition
    ):
        """Identity check: every member client's monitor is the same object."""
        async with AsyncPool(
            make_cluster_definition(aerospike_host), loop_count=4
        ) as pool:
            async def grab_monitor_id(cluster: Cluster) -> int:
                return id(cluster._client._indexes_monitor)

            monitor_ids = [
                await pool.run(grab_monitor_id, pick=i) for i in range(4)
            ]
            assert len(set(monitor_ids)) == 1, (
                f"expected one shared IndexesMonitor across the pool, "
                f"got distinct ids={monitor_ids}"
            )
            assert monitor_ids[0] == id(pool._shared_monitor)

    async def test_only_one_poll_per_refresh_interval(
        self, aerospike_host, make_cluster_definition, monkeypatch
    ):
        """N members should produce 1 poll per refresh_interval, not N."""
        call_count = 0
        real_fetch = _index_monitor_module._fetch_indexes_blocking

        def counting_fetch(pac_client):
            nonlocal call_count
            call_count += 1
            return real_fetch(pac_client)

        monkeypatch.setattr(
            _index_monitor_module, "_fetch_indexes_blocking", counting_fetch,
        )

        async with AsyncPool(
            make_cluster_definition(aerospike_host),
            loop_count=4,
            index_refresh_interval=0.3,
        ) as pool:
            # Trigger lazy-start by issuing one AEL query against the pool's
            # shared monitor — pool monitors no longer eager-start on entry.
            _ = pool  # silence unused
            # Wait for the first refresh to land plus a full cycle.
            await asyncio.sleep(0.7)

        # The monitor lazy-starts now, so call_count may be 0 if no AEL
        # query touched the daemon thread. When it does start, expect
        # ≥1 and ≤3 calls.
        assert call_count <= 3, (
            f"expected ≤3 _fetch_indexes_blocking calls with a shared "
            f"monitor; got {call_count}"
        )


async def _noop(cluster: Cluster) -> None:
    return None
