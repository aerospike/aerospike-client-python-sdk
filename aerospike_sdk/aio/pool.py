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

"""Multi-loop async pool for scaling past the single-event-loop ceiling.

Each pool thread runs its own event loop with its own
:class:`~aerospike_sdk.aio.client.Client`.  Because each PAC ``Client``
carries its own ``CompletionBridge``, completions never cross loops —
loop A's completions enqueue into client-A's bridge and drain on loop A's
thread.

**Free-threading required for throughput gains.**  On a GIL-built
interpreter (stock CPython ≤ 3.12) an ``AsyncPool`` is *correct* but
delivers no TPS scaling — N loops still serialize on the GIL for Python
work.  The throughput benefit materializes under a free-threaded build
(3.13t / 3.14t).
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import threading
from typing import (
    Callable,
    Coroutine,
    Iterable,
    List,
    Optional,
    TypeVar,
)

from aerospike_sdk.aio.client import Client
from aerospike_sdk.index_monitor import IndexesMonitor

log = logging.getLogger(__name__)

T = TypeVar("T")
X = TypeVar("X")


class AsyncPool:
    """Pool of event loops + paired clients for parallel async work.

    Each loop runs on a dedicated OS thread with its own
    :class:`~aerospike_sdk.aio.client.Client` (and therefore its own PAC
    ``CompletionBridge``).  Submitted coroutines are dispatched round-robin
    (or by explicit index) across loops.

    **Free-threading required for throughput gains.**  On a GIL-built
    interpreter (stock CPython ≤ 3.12) an ``AsyncPool`` is *correct* — N
    loops still serialize on the GIL for Python work, so TPS does not
    scale with ``loop_count``.  The throughput benefit only materializes
    under a free-threaded build (3.13t / 3.14t).

    **Shared IndexesMonitor.**  Index metadata is cluster-scoped, so the
    pool runs one shared :class:`IndexesMonitor` (anchored to loop 0,
    issuing info commands through ``clients[0]``) instead of one per
    client.  The factory's per-client monitor is replaced before
    ``connect()``, so cluster-side ``sindex-list`` load is independent
    of ``loop_count``.  Tune via the ``index_refresh_interval`` kwarg
    on :class:`AsyncPool` itself.

    **Tuning notes** (from 12-core measurement run, client+server colocated):

    * **Tasks-per-loop floor.**  Below ~16–32 concurrent asyncio tasks
      per loop, per-call dispatch overhead (``run_coroutine_threadsafe`` +
      ``asyncio.wrap_future``) dominates the savings from parallelism — a
      4-loop pool with 8 tasks/loop measured *slower* than a 1-loop client
      with 32 tasks total.  Keep tasks-per-loop in the same regime that
      saturates a single client.
    * **Throughput vs tail latency.**  Adding loops trades p99 for TPS.
      In one sample, going from 1 → 8 loops took p99 from 0.6 ms to 6.1 ms
      while TPS rose then fell.  Latency-sensitive workloads should pick a
      ``loop_count`` based on the p99 budget; throughput-only workloads can
      push higher.
    * **Sweet spot is hardware-dependent.**  With colocated client+server
      the sweet spot is at 1–2 loops because they share CPU; with a remote
      server on dedicated hardware the curve flattens later.  Always
      validate against your target deployment.

    Example::

        pool = AsyncPool(
            client_factory=lambda: Client("127.0.0.1:3000"),
            loop_count=4,
        )
        async with pool:
            result = await pool.run(
                lambda client: client.create_session().get(key)
            )

    See Also:
        :class:`~aerospike_sdk.aio.client.Client`: Single-loop async API.
    """

    def __init__(
        self,
        client_factory: Callable[[], Client],
        loop_count: Optional[int] = None,
        *,
        index_refresh_interval: float = 5.0,
    ) -> None:
        """Configure the pool.  Call :meth:`start` or use ``async with``.

        Args:
            client_factory: Zero-argument callable returning an *unconnected*
                :class:`~aerospike_sdk.aio.client.Client`.  Called
                ``loop_count`` times — once per pool thread.  Each client
                connects on its own loop, binding its PAC
                ``CompletionBridge`` to that loop.

                **Connection-pool sizing:** with N clients, total connections
                per server node = N × ``max_conns_per_node``.  To keep the
                aggregate budget constant, set
                ``ClientPolicy.max_conns_per_node`` in the factory to
                ``default / loop_count``.
            loop_count: Number of event loops / OS threads.  Defaults to
                ``os.cpu_count()`` (or ``4`` if indeterminate).
            index_refresh_interval: Seconds between secondary-index cache
                refreshes for the pool's *single shared* ``IndexesMonitor``
                (default 5.0).  Index metadata is cluster-scoped, so one
                monitor serves all pool clients — the per-Client monitor
                each ``client_factory()`` would create is replaced before
                connect, eliminating N×polling load.

        Example::

            from aerospike_async import ClientPolicy
            from aerospike_sdk import Client

            N = 4
            def make_client() -> Client:
                policy = ClientPolicy()
                policy.max_conns_per_node = 300 // N
                return Client("127.0.0.1:3000", policy=policy)

            pool = AsyncPool(client_factory=make_client, loop_count=N)
        """
        self._factory = client_factory
        self._n = loop_count or os.cpu_count() or 4
        self._loops: List[Optional[asyncio.AbstractEventLoop]] = [None] * self._n
        self._threads: List[threading.Thread] = []
        self._clients: List[Client] = []
        self._rr = itertools.cycle(range(self._n))
        self._started = False
        self._closed = False
        self._loop_ready: List[threading.Event] = [
            threading.Event() for _ in range(self._n)
        ]
        # Shared monitor: one instance for all pool clients.  Constructed
        # here; started on loop 0 in `start()`, stopped before client 0 in
        # `aclose()`.
        self._shared_monitor = IndexesMonitor(
            refresh_interval=index_refresh_interval
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spin up pool threads, event loops, and connect all clients.

        Each thread starts an ``asyncio`` event loop, then the pool creates
        and connects one :class:`Client` per loop (via
        ``run_coroutine_threadsafe``).  Because ``Client.connect()`` calls
        ``await new_client(…)`` on the pool loop, the PAC
        ``CompletionBridge`` is naturally bound to the correct loop.

        Raises:
            RuntimeError: If already started or closed.
        """
        if self._started:
            raise RuntimeError("AsyncPool is already started")
        if self._closed:
            raise RuntimeError("AsyncPool is closed; create a new one")

        for i in range(self._n):
            t = threading.Thread(
                target=self._run_loop_thread,
                args=(i,),
                name=f"asyncpool-{i}",
                daemon=True,
            )
            self._threads.append(t)
            t.start()

        for ev in self._loop_ready:
            ev.wait()

        # Schedule all connects concurrently on their respective loops.
        # `run_coroutine_threadsafe` returns a `concurrent.futures.Future`;
        # we wrap each so `gather` can await them without blocking the
        # caller's event loop (sequential `.result()` would freeze the
        # caller's loop for up to N × connect_timeout seconds).
        clients: List[Optional[Client]] = [None] * self._n
        afuts: List[asyncio.Future[None]] = []
        for i in range(self._n):
            client = self._factory()
            # Replace the factory-created per-Client monitor with the pool's
            # shared one before `connect()` runs.  `_owns_monitor = False`
            # makes the per-Client lazy-start path skip start (so only one
            # daemon thread polls, not N).  The pool drives the shared
            # monitor's lifecycle (stop on aclose).
            client._indexes_monitor = self._shared_monitor
            client._owns_monitor = False
            clients[i] = client
            loop = self._loops[i]
            assert loop is not None
            cfut = asyncio.run_coroutine_threadsafe(client.connect(), loop)
            afuts.append(asyncio.wrap_future(cfut))

        results = await asyncio.gather(*afuts, return_exceptions=True)
        errors: List[Exception] = [r for r in results if isinstance(r, Exception)]
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                log.error("AsyncPool: client %d failed to connect: %s", i, r)

        if errors:
            # Close the clients that did connect, also concurrently.
            close_afuts: List[asyncio.Future[None]] = []
            for i in range(self._n):
                c = clients[i]
                if c is not None and c.is_connected:
                    loop = self._loops[i]
                    assert loop is not None
                    cfut = asyncio.run_coroutine_threadsafe(c.close(), loop)
                    close_afuts.append(asyncio.wrap_future(cfut))
            if close_afuts:
                await asyncio.gather(*close_afuts, return_exceptions=True)
            for loop in self._loops:
                if loop is not None:
                    loop.call_soon_threadsafe(loop.stop)
            for t in self._threads:
                t.join(timeout=5.0)
            raise errors[0]

        self._clients = [c for c in clients if c is not None]

        # The shared monitor is now a daemon-thread poller (no loop affinity).
        # It starts lazily on the first AEL ``where()`` query through any of
        # the pool's builders; cache reads from any pool loop are plain
        # ``dict.get()`` calls — safe under both GIL and free-threading.

        self._started = True
        log.debug(
            "AsyncPool started: %d loops, %d clients, 1 shared monitor",
            self._n,
            len(self._clients),
        )

    async def aclose(self) -> None:
        """Ordered shutdown.

        Protocol:

        1. **Fence** — reject new ``run``/``map`` calls.
        2. **Close each client** — stops new PAC operations, flushes
           connection pools.  Runs on each client's own loop so
           ``Client.close()`` awaits properly.
        3. **Stop event loops** — ``loop.stop()`` is scheduled via
           ``call_soon_threadsafe``, so any pending drain callbacks
           (from completions delivered between close and stop) run first.
        4. **Join threads.**

        Completions that arrive *after* the loop stops hit the
        ``CompletionBridge.closed`` latch and resolve their Python
        futures with ``RuntimeError("event loop is closed")`` — callers
        fail fast instead of hanging.
        """
        if self._closed:
            return
        self._closed = True

        # Stop the shared monitor before closing clients — the daemon thread
        # issues info commands through clients[0]'s PAC client, so it must
        # be torn down before that client closes. No-op if it was never
        # started (lazy-start: only triggered on first AEL query).
        if self._started:
            try:
                self._shared_monitor.stop()
            except Exception as exc:
                log.warning("AsyncPool: error stopping shared monitor: %s", exc)

        # Close all clients concurrently on their own loops. Sequential
        # `.result()` would freeze the caller's loop for up to
        # N × close_timeout seconds.
        close_afuts: List[asyncio.Future[None]] = []
        indexed: List[int] = []
        for i, client in enumerate(self._clients):
            loop = self._loops[i]
            if loop is None:
                continue
            cfut = asyncio.run_coroutine_threadsafe(client.close(), loop)
            close_afuts.append(asyncio.wrap_future(cfut))
            indexed.append(i)
        if close_afuts:
            results = await asyncio.gather(*close_afuts, return_exceptions=True)
            for i, r in zip(indexed, results):
                if isinstance(r, Exception):
                    log.warning("AsyncPool: error closing client %d: %s", i, r)

        for loop in self._loops:
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)

        for t in self._threads:
            t.join(timeout=10.0)

        self._clients.clear()
        self._threads.clear()
        # Hygiene: drop the round-robin iterator so its captured range() can GC.
        self._rr = itertools.cycle(range(0))
        log.debug("AsyncPool closed")

    async def __aenter__(self) -> AsyncPool:
        """Async context manager: start the pool."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Async context manager: close the pool."""
        await self.aclose()

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    async def run(
        self,
        fn: Callable[[Client], Coroutine[object, object, T]],
        pick: Optional[int] = None,
    ) -> T:
        """Dispatch ``fn(client_i)`` to one of the pool's loops.

        Args:
            fn: Async callable receiving one of the pool's
                :class:`~aerospike_sdk.aio.client.Client` instances.
            pick: Explicit loop index (modulo ``loop_count``).  ``None``
                selects round-robin.

        Returns:
            The awaited result of ``fn``.

        Raises:
            RuntimeError: If the pool is not started, is closed, or if
                called from within one of the pool's own loops (which
                would deadlock).

        Example::

            result = await pool.run(
                lambda c: c.create_session().get(key)
            )
        """
        self._check_usable()
        self._guard_self_dispatch()

        idx = (pick % self._n) if pick is not None else next(self._rr)
        client = self._clients[idx]
        loop = self._loops[idx]
        assert loop is not None

        cfut = asyncio.run_coroutine_threadsafe(fn(client), loop)
        return await asyncio.wrap_future(cfut)

    async def map(
        self,
        fn: Callable[[Client, X], Coroutine[object, object, T]],
        inputs: Iterable[X],
    ) -> List[T]:
        """Dispatch ``fn`` across *inputs*, sharded round-robin across loops.

        Args:
            fn: Async callable receiving ``(client, input_item)``.
            inputs: Items to distribute across the pool.

        Returns:
            Results in the same order as *inputs*.

        Raises:
            RuntimeError: If the pool is not usable or called from a pool loop.

        Example::

            async def do_get(client: Client, key: Key) -> RecordResult:
                return await client.create_session().get(key)

            results = await pool.map(do_get, keys)
        """
        self._check_usable()
        self._guard_self_dispatch()

        wrapped: List[asyncio.Future[T]] = []
        for item in inputs:
            idx = next(self._rr)
            client = self._clients[idx]
            loop = self._loops[idx]
            assert loop is not None
            cfut = asyncio.run_coroutine_threadsafe(fn(client, item), loop)
            wrapped.append(asyncio.wrap_future(cfut))

        result = list(await asyncio.gather(*wrapped))
        return result

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def loop_count(self) -> int:
        """Number of event loops / OS threads in the pool."""
        return self._n

    @property
    def is_started(self) -> bool:
        """``True`` after :meth:`start` succeeds."""
        return self._started

    @property
    def is_closed(self) -> bool:
        """``True`` after :meth:`aclose` is called."""
        return self._closed

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_loop_thread(self, index: int) -> None:
        """Thread target: create and run an event loop forever.

        Uses the stdlib ``SelectorEventLoop`` explicitly rather than
        ``asyncio.new_event_loop()`` because a third-party loop policy
        (e.g. uvloop installed by PAC) may not be safe to instantiate
        on multiple threads under free-threaded Python.  The caller's
        own loop — typically the single "main" loop — is unaffected and
        keeps whatever policy is in place.
        """
        loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(loop)
        self._loops[index] = loop
        self._loop_ready[index].set()
        try:
            loop.run_forever()
        finally:
            loop.close()

    def _check_usable(self) -> None:
        if self._closed:
            raise RuntimeError("AsyncPool is closed")
        if not self._started:
            raise RuntimeError(
                "AsyncPool is not started; call start() or use async with"
            )

    def _guard_self_dispatch(self) -> None:
        """Raise if the caller is running on one of the pool's own loops.

        Submitting work to the same loop that is blocked awaiting the
        result would deadlock.  Mirrors the equivalent Tokio-context misuse
        check on the Rust side.
        """
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            return
        if running in self._loops:
            raise RuntimeError(
                "AsyncPool.run() called from within a pool loop. "
                "Use `await fn(client)` directly, or dispatch to a "
                "different loop with the `pick` argument."
            )
