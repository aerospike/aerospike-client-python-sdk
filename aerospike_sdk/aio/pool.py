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
:class:`~aerospike_sdk.aio.cluster.Cluster` (backed by one PAC client
apiece).  Because each PAC client carries its own ``CompletionBridge``,
completions never cross loops — loop A's completions enqueue into loop A's
Cluster bridge and drain on loop A's thread.

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
    cast,
)

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.cluster import Cluster
from aerospike_sdk.aio.cluster_definition import ClusterDefinition
from aerospike_sdk.index_monitor import IndexesMonitor

from aerospike_sdk.loggers import SdkLoggers

log = logging.getLogger(SdkLoggers.POOL)

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
    """Pool of event loops, each with its own :class:`Cluster` handle, for
    parallel async work.

    Each loop runs on a dedicated OS thread with its own
    :class:`~aerospike_sdk.aio.cluster.Cluster` (and therefore its own PAC
    ``CompletionBridge``).  Submitted coroutines are dispatched round-robin
    (or by explicit index) across loops.  The pool is defined by a
    :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`; one
    :class:`Cluster` connects per loop from that single definition.

    All N handles target the **same** Aerospike cluster — the pool holds one
    :class:`Cluster` per loop purely for loop affinity (each handle binds its
    own ``CompletionBridge`` to its loop), not because there is more than one
    cluster.

    **Free-threading required for throughput gains.**  On a GIL-built
    interpreter (stock CPython ≤ 3.12) an ``AsyncPool`` is *correct* — N
    loops still serialize on the GIL for Python work, so TPS does not
    scale with ``loop_count``.  The throughput benefit only materializes
    under a free-threaded build (3.14t).

    **Shared IndexesMonitor.**  Index metadata is cluster-scoped, so the
    pool runs one shared :class:`IndexesMonitor` (anchored to loop 0,
    issuing info commands through loop 0's Cluster) instead of one per
    loop, so cluster-side ``sindex-list`` load is independent of
    ``loop_count``.  Tune via ``index_refresh_interval`` — either the
    kwarg on :class:`AsyncPool` or
    :meth:`ClusterDefinition.with_index_refresh_interval`.

    **Per-Client Tokio runtime.**  When ``loop_count >= 4``, AsyncPool
    automatically configures each per-loop Cluster to use its own dedicated
    PAC Tokio runtime instead of the shared global one. This eliminates the
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
            ClusterDefinition("127.0.0.1", 3000),
            loop_count=4,
        )
        async with pool:
            result = await pool.run(
                lambda cluster: cluster.create_session().get(key)
            )

    See Also:
        :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`:
            Single-loop entry point; the pool connects one :class:`Cluster`
            per loop from the same definition.
    """

    def __init__(
        self,
        cluster_definition: Optional[ClusterDefinition] = None,
        loop_count: Optional[int] = None,
        *,
        index_refresh_interval: Optional[float] = None,
        per_client_runtime: Optional[bool] = None,
        use_uvloop: Optional[bool] = None,
        client_factory: Optional[Callable[[], Client]] = None,
    ) -> None:
        """Configure the pool.  Call :meth:`start` or use ``async with``.

        Args:
            cluster_definition: The
                :class:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition`
                describing the cluster (seeds, auth, TLS, system settings).
                The pool builds ``loop_count`` :class:`Cluster` handles from
                this single definition; each connects on its own loop, binding
                its PAC ``CompletionBridge`` to that loop.  One definition
                builds one ``ClientPolicy``, shared by every handle — which is
                the invariant the one-shot per-Cluster-runtime policy mutation
                relies on.  Config-file hot-reload is not armed for pool
                Clusters (restart the pool to pick up config changes).

                **Connection-pool sizing:** with N handles (N loops), total
                connections per server node = N × ``max_connections_per_node``.
                To keep
                the aggregate budget constant, size
                ``SystemSettings(max_connections_per_node=default //
                loop_count)`` via
                :meth:`ClusterDefinition.with_system_settings`.
            loop_count: Number of event loops / OS threads.  Defaults to
                ``os.cpu_count()`` (or ``4`` if indeterminate).
            index_refresh_interval: Seconds between secondary-index cache
                refreshes for the pool's *single shared* ``IndexesMonitor``.
                Defaults to the definition's
                :meth:`~ClusterDefinition.with_index_refresh_interval` value
                (itself 5.0 by default).  Index metadata is cluster-scoped,
                so one monitor serves all pool Clusters, eliminating
                N×polling load.
            client_factory: **Deprecated** — pass ``cluster_definition``
                instead.  Zero-argument callable returning an *unconnected*
                :class:`~aerospike_sdk.aio.client.Client`, called once per
                pool thread; dispatch callbacks then receive the raw
                ``Client`` the factory made (not a ``Cluster``).  When
                ``per_client_runtime`` is enabled the factory MUST return
                Clients sharing a single ``ClientPolicy`` PyO3 object
                (violations raise ``RuntimeError`` from :meth:`start`).
                Removed after one deprecation cycle.
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

            from aerospike_sdk import ClusterDefinition
            from aerospike_sdk.policy.system_settings import SystemSettings

            N = 4
            definition = (
                ClusterDefinition("127.0.0.1", 3000)
                .with_system_settings(SystemSettings(max_connections_per_node=300 // N))
            )
            pool = AsyncPool(definition, loop_count=N)
        """
        if cluster_definition is not None and not isinstance(
            cluster_definition, ClusterDefinition
        ):
            # Old positional shape: AsyncPool(factory, loop_count). Shift the
            # callable into the deprecated kwarg path below.
            if callable(cluster_definition) and client_factory is None:
                client_factory = cluster_definition
                cluster_definition = None
            else:
                raise TypeError(
                    "cluster_definition must be a ClusterDefinition; got "
                    f"{type(cluster_definition).__name__}"
                )
        if client_factory is not None:
            if cluster_definition is not None:
                raise ValueError(
                    "Pass either cluster_definition or the deprecated "
                    "client_factory, not both"
                )
            warnings.warn(
                "AsyncPool(client_factory=...) is deprecated; pass a "
                "ClusterDefinition instead: AsyncPool(ClusterDefinition(...), "
                "loop_count=N). Dispatch callbacks then receive a Cluster.",
                DeprecationWarning,
                stacklevel=2,
            )
        elif cluster_definition is None:
            raise ValueError("cluster_definition is required")
        self._definition = cluster_definition
        self._factory = client_factory
        if index_refresh_interval is None:
            index_refresh_interval = (
                cluster_definition._index_refresh_interval
                if cluster_definition is not None
                else 5.0
            )
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
        # What dispatch hands to callbacks: Cluster handles (or, on the
        # deprecated client_factory path, the raw Clients the factory made).
        self._members: List[Cluster] = []
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
        """Spin up pool threads, event loops, and connect all Clusters.

        Each thread starts an ``asyncio`` event loop, then the pool connects
        one :class:`Cluster` per loop (via
        ``run_coroutine_threadsafe``).  Because the connect awaits
        ``new_client(…)`` on the pool loop, the PAC ``CompletionBridge`` is
        naturally bound to the correct loop.

        Raises:
            RuntimeError: If already started or closed.
        """
        if self._started:
            raise RuntimeError("AsyncPool is already started")
        if self._closed:
            raise RuntimeError("AsyncPool is closed; create a new one")

        # Construct all N Clients on the main thread, BEFORE
        # any loop threads exist — this keeps them on a single thread until
        # policy mutation is done. The definition path builds every Client
        # from one ClusterDefinition, so a single ClientPolicy is shared by
        # construction and the shared monitor is injected at __init__ time.
        if self._definition is not None:
            clients: List[Client] = self._definition._build_pool_members(
                self._n, self._shared_monitor
            )
        else:
            # Deprecated client_factory path. Each factory call returns a
            # fresh `Client` but must share the same `ClientPolicy` PyO3
            # object (the documented factory shape is
            # `lambda: Client(seeds, policy=shared_policy)`).
            assert self._factory is not None
            clients = []
            for _ in range(self._n):
                client = self._factory()
                # Replace the factory-created per-Client monitor with the
                # pool's shared one before `connect()` runs.
                # `_owns_monitor = False` makes the per-Client lazy-start
                # path skip start (so only one daemon thread polls, not N).
                # The pool drives the shared monitor's lifecycle (stop on
                # aclose).
                client._indexes_monitor = self._shared_monitor
                client._owns_monitor = False
                clients.append(client)

        # One-shot policy mutation.  Per-Client Tokio runtime must
        # be set BEFORE connect() because PAC's new_client() reads this
        # field at construction.  All Clients share a single ClientPolicy
        # PyO3 object — by construction on the definition path, and by
        # documented factory contract on the deprecated path (verified by
        # `_assert_shared_policy_invariant()`). A single mutation on
        # clients[0]._policy then applies to all Clients via shared
        # reference. Doing this once, BEFORE any loop threads exist, avoids
        # the race where a per-iteration mutation could collide with
        # already-running loop threads — on 3.14t free-threading PyO3's
        # RefCell-style borrow checker raised `RuntimeError: Already
        # borrowed` because the mutation took `&mut ClientPolicy` while a
        # peer loop thread held a shared borrow via the connect-time policy
        # read.
        if self._per_client_runtime:
            self._assert_shared_policy_invariant(clients)
            clients[0]._policy.per_client_runtime_workers = (
                self._per_client_runtime_workers
            )

        # Spawn the N loop threads. Safe now because clients are
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

        # Schedule all connects concurrently on their respective
        # loops.  `run_coroutine_threadsafe` returns a
        # `concurrent.futures.Future`; we wrap each so `gather` can await
        # them without blocking the caller's event loop (sequential
        # `.result()` would freeze the caller's loop for up to
        # N × connect_timeout seconds).  On the definition path each Client
        # is connected, validated, and wrapped into a Cluster on its own
        # loop; the deprecated factory path connects the bare Client as
        # before.
        afuts: List[asyncio.Future[object]] = []
        for i in range(self._n):
            loop = self._loops[i]
            assert loop is not None
            if self._definition is not None:
                coro: Coroutine[object, object, object] = (
                    Cluster._connect_and_wrap(clients[i])
                )
            else:
                coro = clients[i].connect()
            cfut = asyncio.run_coroutine_threadsafe(coro, loop)
            afuts.append(asyncio.wrap_future(cfut))

        results = await asyncio.gather(*afuts, return_exceptions=True)
        errors: List[Exception] = [r for r in results if isinstance(r, Exception)]
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                log.error("AsyncPool: Cluster %d failed to connect: %s", i, r)

        if errors:
            # Close the Clusters that did connect, also concurrently.
            # `_connect_and_wrap` already closed its client on validation
            # failure, so only successful results need cleanup here.
            close_afuts: List[asyncio.Future[None]] = []
            for i, r in enumerate(results):
                loop = self._loops[i]
                assert loop is not None
                if isinstance(r, Cluster):
                    cfut = asyncio.run_coroutine_threadsafe(r.close(), loop)
                elif clients[i].is_connected:
                    cfut = asyncio.run_coroutine_threadsafe(
                        clients[i].close(), loop
                    )
                else:
                    continue
                close_afuts.append(asyncio.wrap_future(cfut))
            if close_afuts:
                await asyncio.gather(*close_afuts, return_exceptions=True)
            for loop in self._loops:
                if loop is not None:
                    loop.call_soon_threadsafe(loop.stop)
            for t in self._threads:
                t.join(timeout=5.0)
            raise errors[0]

        if self._definition is not None:
            self._members = cast(List[Cluster], list(results))
            self._clients = [m._client for m in self._members]
        else:
            # Deprecated factory path: callbacks receive the raw Clients.
            self._members = cast(List[Cluster], clients)
            self._clients = clients

        # The shared monitor is now a daemon-thread poller (no loop affinity).
        # It starts lazily on the first AEL ``where()`` query through any of
        # the pool's builders; cache reads from any pool loop are plain
        # ``dict.get()`` calls — safe under both GIL and free-threading.

        self._started = True
        log.info(
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
        self._members.clear()
        self._threads.clear()
        # Hygiene: drop the round-robin iterator so its captured range() can GC.
        self._rr = itertools.cycle(range(0))
        log.info("AsyncPool closed")

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
        fn: Callable[[Cluster], Coroutine[object, object, T]],
        pick: Optional[int] = None,
    ) -> T:
        """Dispatch ``fn(cluster)`` to one of the pool's loops.

        Args:
            fn: Async callable receiving one of the pool's
                :class:`~aerospike_sdk.aio.cluster.Cluster` handles.  (On
                the deprecated ``client_factory`` path the callback receives
                the raw ``Client`` the factory made instead.)
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
                lambda cluster: cluster.create_session().get(key)
            )
        """
        self._check_usable()
        self._guard_self_dispatch()

        idx = (pick % self._n) if pick is not None else next(self._rr)
        member = self._members[idx]
        loop = self._loops[idx]
        assert loop is not None

        cfut = asyncio.run_coroutine_threadsafe(fn(member), loop)
        return await asyncio.wrap_future(cfut)

    async def map(
        self,
        fn: Callable[[Cluster, X], Coroutine[object, object, T]],
        inputs: Iterable[X],
    ) -> List[T]:
        """Dispatch ``fn`` across *inputs*, sharded round-robin across loops.

        Args:
            fn: Async callable receiving ``(cluster, input_item)``.  (On
                the deprecated ``client_factory`` path the first argument is
                the raw ``Client`` the factory made instead.)
            inputs: Items to distribute across the pool.

        Returns:
            Results in the same order as *inputs*.

        Raises:
            RuntimeError: If the pool is not usable or called from a pool loop.

        Example::

            async def do_get(cluster: Cluster, key: Key) -> RecordResult:
                return await cluster.create_session().get(key)

            results = await pool.map(do_get, keys)
        """
        self._check_usable()
        self._guard_self_dispatch()

        wrapped: List[asyncio.Future[T]] = []
        for item in inputs:
            idx = next(self._rr)
            member = self._members[idx]
            loop = self._loops[idx]
            assert loop is not None
            cfut = asyncio.run_coroutine_threadsafe(fn(member, item), loop)
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

    def _assert_shared_policy_invariant(self, clients: List[Client]) -> None:
        """Verify all clients share a single ``ClientPolicy`` PyO3 object.

        The one-shot policy mutation in :meth:`start` relies on this
        invariant: it mutates ``clients[0]._policy`` and expects the change
        to be visible to all other clients via shared reference. The
        definition path satisfies it by construction (one
        ``ClusterDefinition`` builds one policy); on the deprecated factory
        path the documented shape (``lambda: Client(seeds, policy=shared)``)
        produces it, and an unusual factory that constructs a fresh
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
