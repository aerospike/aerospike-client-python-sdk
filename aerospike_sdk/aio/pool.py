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
(3.14t).
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import sys
import threading
import warnings
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


def _gil_is_enabled() -> bool:
    """Return True if Python's GIL is currently enabled.

    On regular CPython (no free-threading build), ``sys._is_gil_enabled``
    is absent and the GIL is always on. On free-threaded builds
    (3.14t) the GIL state is dynamic and depends on the
    ``PYTHON_GIL`` env var plus any C extensions that re-enable it.
    """
    return getattr(sys, "_is_gil_enabled", lambda: True)()


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
    under a free-threaded build (3.14t).

    **Shared IndexesMonitor.**  Index metadata is cluster-scoped, so the
    pool runs one shared :class:`IndexesMonitor` (anchored to loop 0,
    issuing info commands through ``clients[0]``) instead of one per
    client.  The factory's per-client monitor is replaced before
    ``connect()``, so cluster-side ``sindex-list`` load is independent
    of ``loop_count``.  Tune via the ``index_refresh_interval`` kwarg
    on :class:`AsyncPool` itself.

    **Per-Client Tokio runtime.**  When ``loop_count >= 4``, AsyncPool
    automatically configures each Client to use its own dedicated PAC
    Tokio runtime instead of the shared global one. This eliminates the
    cross-loop scheduler contention that previously caused throughput to
    collapse beyond 4 loops. Controlled via the ``per_client_runtime``
    kwarg; see its docstring for the threshold rationale and override.

    **Event loop policy.**  Pool loops default to the stdlib selector loop
    on free-threaded (GIL-off) builds.  uvloop's libuv free-threading race
    on ``loop._ready_len`` (MagicStack/uvloop issues #720, #721) stalls a
    multi-loop pool when the GIL is disabled — the per-loop race fires
    across all loops at once and wedges.  Override with the ``use_uvloop``
    kwarg.

    **Tuning notes** (8-core remote-cluster measurement, FT 3.14t):

    * **Tasks-per-loop floor.**  Below ~16–32 concurrent asyncio tasks
      per loop, per-call dispatch overhead (``run_coroutine_threadsafe`` +
      ``asyncio.wrap_future``) dominates the savings from parallelism — a
      4-loop pool with 8 tasks/loop measured *slower* than a 1-loop client
      with 32 tasks total.  Keep tasks-per-loop in the same regime that
      saturates a single client.
    * **Throughput scales monotonically with loops** under per-Client
      runtime (4×64 = 167K, 8×64 = 178K, 12×64 = 180K TPS measured).  TPS
      ceiling on 8-core hardware is ~180K, capped by Python interpreter
      self-time across loops.
    * **Tail latency degrades with loops.**  4×64 has p99 = 4.3 ms; 12×64
      has p99 = 15.5 ms. Latency-sensitive workloads should pick
      ``loop_count`` based on the p99 budget; throughput-only workloads
      can push higher.
    * **Sweet spot is hardware-dependent.**  With colocated client+server
      the sweet spot shifts down because they share CPU; with more cores
      the ceiling shifts up. Always validate against your target deployment.

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
        per_client_runtime: Optional[bool] = None,
        use_uvloop: Optional[bool] = None,
    ) -> None:
        """Configure the pool.  Call :meth:`start` or use ``async with``.

        Args:
            client_factory: Zero-argument callable returning an *unconnected*
                :class:`~aerospike_sdk.aio.client.Client`.  Called
                ``loop_count`` times — once per pool thread.  Each client
                connects on its own loop, binding its PAC
                ``CompletionBridge`` to that loop.

                **Shared-policy invariant:** when ``per_client_runtime`` is
                enabled (auto at ``loop_count >= 4`` on free-threaded Python),
                the factory MUST return Clients sharing a single
                ``ClientPolicy`` PyO3 object — typically via a closure that
                captures one policy: ``policy = ClientPolicy(); factory =
                lambda: Client(seeds, policy=policy)``. AsyncPool applies a
                one-shot mutation to that shared policy before any loop
                thread starts; constructing a fresh ``ClientPolicy`` per
                call would land the mutation on client 0 only and silently
                disable the per-Client runtime for the rest. Violations
                raise ``RuntimeError`` from :meth:`start`.

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
            per_client_runtime: Whether each pool Client should run on its
                own dedicated PAC Tokio runtime (per-loop runtime isolation,
                eliminates cross-loop scheduler contention).

                * ``None`` (default): auto-enable when ``loop_count >= 4``.
                  Below 4 loops the shared global runtime wins on the
                  per-loop worker budget; at 4+ loops per-Client runtimes
                  scale monotonically (measured: AsyncPool 8×64 lifts from
                  ~59K TPS collapsed to ~184K with per-Client runtimes).
                * ``True``: always enable. Worker count auto-sized to
                  ``max(2, os.cpu_count() // loop_count)``.
                * ``False``: never enable; use the shared global runtime
                  regardless of ``loop_count``.
            use_uvloop: Whether the pool's event loops may use uvloop (when a
                uvloop policy is installed process-wide).

                * ``None`` (default): auto — **disabled** on free-threaded
                  (GIL-off) builds, enabled otherwise. uvloop's libuv
                  free-threading race (MagicStack/uvloop #720, #721) stalls a
                  multi-loop pool when the GIL is off, so the stdlib selector
                  loop is used instead. Under GIL-on Python the race can't
                  fire, so uvloop is left on (preserving prior behavior).
                * ``True``: force uvloop on the pool loops (only takes effect
                  if a uvloop policy is installed). Known to stall fast-path
                  pools under free-threading — opt in only after validating
                  your workload.
                * ``False``: force the stdlib ``SelectorEventLoop`` on every
                  pool loop regardless of the global event-loop policy.

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
        # Auto-decide per-Client runtime: enable at 4+ loops where it scales,
        # leave alone below where the shared global runtime wins. ALSO gate
        # on GIL being disabled — under GIL-on Python the per-Client Tokio
        # workers all serialize on one GIL when delivering completions back
        # to asyncio, which deadlocks (every worker stuck in futex_do_wait
        # while the main loop blocks on epoll). Threshold + GIL check are
        # both empirical (8-core measurements); revisit on other hardware.
        if per_client_runtime is None:
            per_client_runtime = self._n >= 4 and not _gil_is_enabled()
        elif per_client_runtime and _gil_is_enabled():
            # Explicit opt-in on GIL-on is a footgun — known to deadlock.
            # Warn loudly but honor the user's choice; they may know
            # something we don't (e.g., a tiny synthetic test).
            warnings.warn(
                "AsyncPool: per_client_runtime=True requested but the GIL is "
                "enabled. This combination deadlocks under load — the "
                "per-Client Tokio workers serialize on one GIL when delivering "
                "completions. Either run on a free-threaded Python build with "
                "PYTHON_GIL=0, or set per_client_runtime=False (or None for "
                "the safe auto-decide).",
                RuntimeWarning,
                stacklevel=2,
            )
        self._per_client_runtime = per_client_runtime
        # Worker count: divide CPUs across loops, floor at 2 so each runtime
        # has at least one extra worker to absorb tail-latency bursts.
        n_cpu = os.cpu_count() or 4
        self._per_client_runtime_workers = max(2, n_cpu // self._n)
        # Event-loop policy. uvloop's libuv free-threading race on
        # `loop._ready_len` (MagicStack/uvloop #720/#721) stalls a multi-loop
        # pool when the GIL is disabled: the per-loop (waker-thread vs
        # loop-thread) race fires across all N loops and wedges (a hard hang on
        # the fast-path pool path). PAC's drainer thread tames the single-loop
        # case but not N concurrent loops. Default uvloop OFF under FT; leave it
        # on under GIL-on (race can't fire, and the pool gives no scaling there
        # anyway, so this just preserves prior behavior).
        if use_uvloop is None:
            use_uvloop = _gil_is_enabled()
        self._use_uvloop = use_uvloop
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

        # Phase 1: construct all N clients on the main thread, BEFORE any
        # loop threads exist. Each call to `self._factory()` returns a
        # fresh `Client` but typically shares the same `ClientPolicy`
        # PyO3 object (the standard factory shape is
        # `lambda: Client(seeds, policy=shared_policy)`). Constructing
        # the clients here keeps them on a single thread until policy
        # mutation is done.
        clients: List[Optional[Client]] = [None] * self._n
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

        # Phase 2: one-shot policy mutation.  Per-Client Tokio runtime must
        # be set BEFORE connect() because PAC's new_client() reads this
        # field at construction.  This relies on the documented invariant
        # that the factory returns Clients sharing a single ClientPolicy
        # PyO3 object — verified by `_assert_shared_policy_invariant()`
        # below. A single mutation on clients[0]._policy then applies to
        # all clients via shared reference. Doing this once, BEFORE any
        # loop threads exist, avoids the race where the previous
        # per-iteration mutation could collide with already-running loop
        # threads — on 3.14t free-threading PyO3's RefCell-style borrow
        # checker raised `RuntimeError: Already borrowed` because the
        # mutation took `&mut ClientPolicy` while a peer loop thread held
        # a shared borrow via `Client.connect()`'s policy read.
        if self._per_client_runtime:
            self._assert_shared_policy_invariant(clients)
            clients[0]._policy.per_client_runtime_workers = (
                self._per_client_runtime_workers
            )

        # Phase 3: spawn the N loop threads. Safe now because clients are
        # fully constructed and their shared policy is finalized.
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

        # Phase 4: schedule all connects concurrently on their respective
        # loops.  `run_coroutine_threadsafe` returns a
        # `concurrent.futures.Future`; we wrap each so `gather` can await
        # them without blocking the caller's event loop (sequential
        # `.result()` would freeze the caller's loop for up to
        # N × connect_timeout seconds).
        afuts: List[asyncio.Future[None]] = []
        for i in range(self._n):
            client = clients[i]
            assert client is not None
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

        The loop type honors :attr:`_use_uvloop` (the ``use_uvloop`` kwarg).
        When enabled, ``asyncio.new_event_loop()`` picks up any globally
        installed uvloop policy; when disabled, a stdlib
        ``asyncio.SelectorEventLoop`` is constructed directly, bypassing the
        global policy.  uvloop is disabled by default on free-threaded builds:
        its libuv ``loop._ready_len`` race (MagicStack/uvloop #720, #721)
        stalls multi-loop pools when the GIL is off — the per-loop race fires
        across all loops at once and wedges (a hard hang on the fast-path
        pool path).
        """
        loop = asyncio.new_event_loop() if self._use_uvloop else asyncio.SelectorEventLoop()
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

    def _assert_shared_policy_invariant(self, clients: List[Optional[Client]]) -> None:
        """Verify all clients share a single ``ClientPolicy`` PyO3 object.

        The Phase-2 one-shot mutation in :meth:`start` relies on this
        invariant: it mutates ``clients[0]._policy`` and expects the change
        to be visible to all other clients via shared reference. The
        documented factory shape (``lambda: Client(seeds, policy=shared)``)
        produces this; an unusual factory that constructs a fresh
        ``ClientPolicy`` per call would silently land
        ``per_client_runtime_workers`` on client 0 only and break the
        per-Client-runtime promise for clients 1..N-1.

        Raises:
            RuntimeError: If the factory produced clients with differing
                ``ClientPolicy`` identities while ``per_client_runtime``
                is enabled.
        """
        first = clients[0]._policy
        for i, c in enumerate(clients[1:], start=1):
            if c._policy is not first:
                raise RuntimeError(
                    f"AsyncPool with per_client_runtime requires the "
                    f"factory to return Clients sharing a single "
                    f"ClientPolicy object; client {i}'s policy is a "
                    f"different object than client 0's. Use a closure "
                    f"that captures one policy: "
                    f"`policy = ClientPolicy(); "
                    f"factory = lambda: Client(seeds, policy=policy)`."
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
