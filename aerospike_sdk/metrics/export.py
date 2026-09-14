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

"""Metrics export: where a snapshot goes once the client has taken it.

The client *pushes* a snapshot to one registered exporter every export
interval. An exporter decides what to do with it -- append to a file,
translate to OTEL, batch, drop. To reach several destinations, register one
:class:`MultipleMetricsExporter` rather than several exporters.

There are two exporter protocols because there are two clients. The async
client awaits :class:`AsyncMetricsExporter` on its loop; the sync client calls
:class:`MetricsExporter` directly. Exporting is IO -- a file write, an HTTP
post -- and an export interval is long enough that running it on the async
client's event loop would stall every in-flight operation behind it. Writing
an async exporter for the async client keeps that IO off the critical path
without the SDK moving user code onto a thread behind their back.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Protocol, Sequence, runtime_checkable

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.metrics import LatencyType, MetricsSnapshot

log = logging.getLogger(SdkLoggers.BEHAVIOR)

# How often a snapshot is pushed when the configuration does not say.
DEFAULT_EXPORT_INTERVAL_SECONDS = 30.0

__all__ = [
    "DEFAULT_EXPORT_INTERVAL_SECONDS",
    "AsyncMetricsExportTimer",
    "AsyncMetricsExporter",
    "AsyncMultipleMetricsExporter",
    "AsyncNoOpMetricsExporter",
    "LearnMetricsFileExporter",
    "MetricsExporter",
    "MultipleMetricsExporter",
    "NoOpMetricsExporter",
    "SyncMetricsExportTimer",
    "built_in_exporter",
    "check_exporter",
    "dispatch_disable",
]


@runtime_checkable
class MetricsExporter(Protocol):
    """Receives metrics snapshots from a synchronous cluster.

    Implement this for :class:`~aerospike_sdk.sync.cluster.SyncCluster`. The
    callbacks run on the export thread, so a slow one delays the next export
    but never a request.

    Example::

        class JsonLinesExporter:
            def __init__(self, path):
                self._file = open(path, "a")

            def on_enable(self, cluster, settings):
                pass

            def on_snapshot(self, snapshot):
                self._file.write(json.dumps(snapshot.to_canonical_dict()) + "\\n")
                self._file.flush()

            def on_node_close(self, host, snapshot):
                pass

            def on_disable(self, cluster):
                self._file.close()

        cluster.metrics_exporter = JsonLinesExporter("/var/log/aerospike/metrics.jsonl")

    See Also:
        :class:`AsyncMetricsExporter`: The same contract for the async client.
        :class:`MultipleMetricsExporter`: Fan out to several exporters.
    """

    def on_enable(self, cluster: Any, settings: Any) -> None:
        """Metrics collection was turned on for *cluster*."""
        ...

    def on_snapshot(self, snapshot: MetricsSnapshot) -> None:
        """A snapshot was taken, once per export interval."""
        ...

    def on_node_close(self, host: str, snapshot: MetricsSnapshot) -> None:
        """*host* left the cluster; this is its final snapshot."""
        ...

    def on_disable(self, cluster: Any) -> None:
        """Metrics collection was turned off; flush and release."""
        ...


@runtime_checkable
class AsyncMetricsExporter(Protocol):
    """Receives metrics snapshots from an asynchronous cluster.

    The same contract as :class:`MetricsExporter` with awaitable callbacks, so
    an exporter can use an async HTTP client without blocking the event loop
    the cluster's own operations run on.

    Example::

        class OtelExporter:
            def __init__(self, client):
                self._client = client

            async def on_enable(self, cluster, settings):
                pass

            async def on_snapshot(self, snapshot):
                await self._client.post("/v1/metrics", json=snapshot.to_canonical_dict())

            async def on_node_close(self, host, snapshot):
                pass

            async def on_disable(self, cluster):
                await self._client.aclose()

        cluster.metrics_exporter = OtelExporter(httpx.AsyncClient())

    See Also:
        :class:`MetricsExporter`: The same contract for the sync client.
    """

    async def on_enable(self, cluster: Any, settings: Any) -> None:
        """Metrics collection was turned on for *cluster*."""
        ...

    async def on_snapshot(self, snapshot: MetricsSnapshot) -> None:
        """A snapshot was taken, once per export interval."""
        ...

    async def on_node_close(self, host: str, snapshot: MetricsSnapshot) -> None:
        """*host* left the cluster; this is its final snapshot."""
        ...

    async def on_disable(self, cluster: Any) -> None:
        """Metrics collection was turned off; flush and release."""
        ...


class NoOpMetricsExporter:
    """Accepts snapshots and discards them.

    What ``exporter: none`` installs, and what the built-in file exporter
    degrades to when no ``report_dir`` is configured. Satisfies both protocols
    so either cluster can hold it.
    """

    def on_enable(self, cluster: Any, settings: Any) -> None:
        """Ignore the enable."""

    def on_snapshot(self, snapshot: MetricsSnapshot) -> None:
        """Ignore the snapshot."""

    def on_node_close(self, host: str, snapshot: MetricsSnapshot) -> None:
        """Ignore the node close."""

    def on_disable(self, cluster: Any) -> None:
        """Ignore the disable."""


class AsyncNoOpMetricsExporter:
    """Accepts snapshots and discards them, for the async client."""

    async def on_enable(self, cluster: Any, settings: Any) -> None:
        """Ignore the enable."""

    async def on_snapshot(self, snapshot: MetricsSnapshot) -> None:
        """Ignore the snapshot."""

    async def on_node_close(self, host: str, snapshot: MetricsSnapshot) -> None:
        """Ignore the node close."""

    async def on_disable(self, cluster: Any) -> None:
        """Ignore the disable."""


class MultipleMetricsExporter:
    """Fans one snapshot out to several exporters, in order.

    A cluster holds exactly one exporter, so this is how a snapshot reaches
    more than one destination. An exporter that raises is logged and skipped;
    the ones after it still run, because one broken destination should not
    silence the rest.

    Delegates are called in the order given. Sync and async delegates cannot
    be mixed in one composite -- use the kind that matches the cluster.

    Args:
        *exporters: The delegates, invoked in order.

    Example::

        cluster.metrics_exporter = MultipleMetricsExporter(
            LearnMetricsFileExporter("/var/log/aerospike"),
            my_otel_exporter,
        )

    See Also:
        :class:`MetricsExporter`, :class:`AsyncMetricsExporter`
    """

    def __init__(self, *exporters: Any) -> None:
        self._exporters: Sequence[Any] = exporters

    @property
    def exporters(self) -> Sequence[Any]:
        """The delegates, in call order."""
        return self._exporters

    def _each(self, method: str, *args: Any) -> None:
        for exporter in self._exporters:
            try:
                getattr(exporter, method)(*args)
            except Exception:
                log.warning(
                    "Metrics exporter %r raised in %s; continuing",
                    type(exporter).__name__, method, exc_info=True,
                )

    def on_enable(self, cluster: Any, settings: Any) -> None:
        """Forward the enable to every delegate."""
        self._each("on_enable", cluster, settings)

    def on_snapshot(self, snapshot: MetricsSnapshot) -> None:
        """Forward the snapshot to every delegate."""
        self._each("on_snapshot", snapshot)

    def on_node_close(self, host: str, snapshot: MetricsSnapshot) -> None:
        """Forward the node close to every delegate."""
        self._each("on_node_close", host, snapshot)

    def on_disable(self, cluster: Any) -> None:
        """Forward the disable to every delegate."""
        self._each("on_disable", cluster)


class AsyncMultipleMetricsExporter:
    """Fans one snapshot out to several async exporters, in order.

    The :class:`MultipleMetricsExporter` contract for the async client. Each
    delegate is awaited in turn; one that raises is logged and skipped so a
    single broken destination does not silence the others.

    Args:
        *exporters: The delegates, awaited in order.

    Example::

        cluster.metrics_exporter = AsyncMultipleMetricsExporter(
            my_otel_exporter, my_audit_exporter,
        )

    See Also:
        :class:`AsyncMetricsExporter`, :class:`MultipleMetricsExporter`
    """

    def __init__(self, *exporters: Any) -> None:
        self._exporters: Sequence[Any] = exporters

    @property
    def exporters(self) -> Sequence[Any]:
        """The delegates, in call order."""
        return self._exporters

    async def _each(self, method: str, *args: Any) -> None:
        for exporter in self._exporters:
            try:
                await getattr(exporter, method)(*args)
            except Exception:
                log.warning(
                    "Metrics exporter %r raised in %s; continuing",
                    type(exporter).__name__, method, exc_info=True,
                )

    async def on_enable(self, cluster: Any, settings: Any) -> None:
        """Forward the enable to every delegate."""
        await self._each("on_enable", cluster, settings)

    async def on_snapshot(self, snapshot: MetricsSnapshot) -> None:
        """Forward the snapshot to every delegate."""
        await self._each("on_snapshot", snapshot)

    async def on_node_close(self, host: str, snapshot: MetricsSnapshot) -> None:
        """Forward the node close to every delegate."""
        await self._each("on_node_close", host, snapshot)

    async def on_disable(self, cluster: Any) -> None:
        """Forward the disable to every delegate."""
        await self._each("on_disable", cluster)


# Fields the legacy line format carries that this client cannot measure. Named
# in the header so whoever owns the log shipper learns it from the file rather
# than from a parse failure downstream.
_UNAVAILABLE_FIELDS = ("inUse", "inPool", "recoverQueueSize", "commandCount", "retryCount")

# Latency segment order, as the legacy format writes it.
_LATENCY_ORDER = (
    LatencyType.CONN,
    LatencyType.WRITE,
    LatencyType.READ,
    LatencyType.BATCH,
    LatencyType.QUERY,
)


class LearnMetricsFileExporter:
    """Writes the legacy line-oriented metrics log under a report directory.

    The default exporter, kept so existing log shippers keep working. Field
    names inside the segments stay camelCase for exactly that reason -- it is
    the one place this SDK does not use snake_case, and it is a compatibility
    exception rather than a style choice.

    Five fields the format defines cannot be filled: ``inUse`` and ``inPool``
    (the client keeps a single open-connection gauge, not the split),
    ``recoverQueueSize``, and cluster-level ``commandCount`` / ``retryCount``.
    They are omitted rather than written as zero, and the header line names
    them so a downstream parser sees why.

    Feature usage counters are deliberately **not** written here. This is an
    externally defined format with consumers this SDK does not control, and its
    field list does not include them; inventing a segment would make the file
    non-interoperable with the other clients reading and writing it. Usage
    counters reach a consumer through the canonical snapshot instead --
    :meth:`~aerospike_sdk.MetricsSnapshot.to_canonical_dict` carries them under
    ``usage`` -- which is the route the cross-SDK specification defines for
    anything outside the legacy field list.

    The ``latency(columns,shift)`` pair is written whatever the latency unit
    is. The format has no field for the unit -- it predates microsecond
    buckets, where milliseconds was the only resolution -- so a reader that
    assumes the legacy meaning will read microsecond buckets as milliseconds.
    Writing the pair anyway is the lesser problem: omitting it for microseconds
    would leave the histogram shape undescribed as well as its unit. Configure
    ``latency_unit: microseconds`` only where whatever consumes these files
    knows to expect it.

    Args:
        report_dir: Directory for log files; created if absent. An empty
            directory makes the exporter a no-op, matching the config default.
        report_size_limit: Rotate once the file exceeds this many bytes.
            ``0`` never rotates.

    Example::

        cluster.metrics_exporter = LearnMetricsFileExporter("/var/log/aerospike")

    See Also:
        :class:`MetricsExporter`: The protocol this implements.
    """

    def __init__(self, report_dir: str, report_size_limit: int = 0) -> None:
        self._dir = report_dir
        self._limit = report_size_limit
        self._file: Optional[Any] = None
        self._written = 0
        # Histogram shape of the snapshot being written; the latency segment
        # prints it, so it is captured when the cluster line is built.
        self._columns: Any = ""
        self._shift: Any = ""

    # -- lifecycle ----------------------------------------------------------

    def on_enable(self, cluster: Any, settings: Any) -> None:
        """Open a new log file and write its header."""
        if not self._dir:
            return
        # Enabling twice reuses this instance, so the previous handle has to go
        # or it is orphaned open for the life of the process.
        self.on_disable(cluster)
        try:
            os.makedirs(self._dir, exist_ok=True)
            self._open()
        except OSError:
            log.warning("Metrics: cannot open report_dir %r; not exporting",
                        self._dir, exc_info=True)
            self._file = None

    def on_snapshot(self, snapshot: MetricsSnapshot) -> None:
        """Append one cluster line for this snapshot."""
        self._write(self._cluster_line(snapshot))

    def on_node_close(self, host: str, snapshot: MetricsSnapshot) -> None:
        """Append a final line for a node that left the cluster."""
        for node in snapshot.to_canonical_dict().get("nodes", []):
            if f"{node.get('address')}:{node.get('port')}" == host:
                self._write(f"node[{self._node_segment(node)}]")

    def on_disable(self, cluster: Any) -> None:
        """Close the log file."""
        if self._file is not None:
            try:
                self._file.close()
            except OSError:
                pass
            self._file = None

    # -- file handling ------------------------------------------------------

    def _open(self) -> None:
        # A timestamp alone is not unique: rotation can fire several times
        # within one second, and every one of those would reopen and append to
        # the same file rather than starting a new one.
        stamp = time.strftime("%Y%m%d%H%M%S")
        path = os.path.join(self._dir, f"metrics-{stamp}.log")
        sequence = 1
        while os.path.exists(path):
            path = os.path.join(self._dir, f"metrics-{stamp}-{sequence}.log")
            sequence += 1
        self._file = open(path, "a", encoding="utf-8")
        self._written = 0
        header = (
            "header(1) cluster[name,clientType,clientVersion,appId,label[],node[]] "
            "node[name,address,port,conns[opened,closed,open],namespace[]] "
            "namespace[name,errors,timeouts,keyBusy,bytesIn,bytesOut,latency[]] "
            "latency(columns,shift)[type[buckets]] "
            f"unavailable[{','.join(_UNAVAILABLE_FIELDS)}]"
        )
        # Not through _write: the header must never trip rotation, or a limit
        # smaller than the header rotates forever.
        self._append(header)

    def _append(self, line: str) -> None:
        """Write one line, without considering rotation."""
        if self._file is None:
            return
        try:
            self._file.write(line + "\n")
            self._file.flush()
            self._written += len(line) + 1
        except OSError:
            log.warning("Metrics: write to the report file failed", exc_info=True)

    def _write(self, line: str) -> None:
        if self._file is None:
            return
        self._append(line)
        if self._limit and self._written >= self._limit:
            self.on_disable(None)
            try:
                self._open()
            except OSError:
                log.warning("Metrics: rotation failed", exc_info=True)

    # -- formatting ---------------------------------------------------------

    def _cluster_line(self, snapshot: MetricsSnapshot) -> str:
        doc = snapshot.to_canonical_dict()
        # Recorded before the node segments are built: they print the shape.
        self._columns = doc.get("latency_columns", "")
        self._shift = doc.get("latency_shift", "")
        labels = ",".join(f"{k}={v}" for k, v in sorted(doc.get("labels", {}).items()))
        nodes = ",".join(self._node_segment(n) for n in doc.get("nodes", []))
        return (
            f"cluster[{doc.get('cluster_name', '')},{doc.get('client_type', '')},"
            f"{doc.get('client_version', '')},{doc.get('app_id', '')},"
            f"label[{labels}],node[{nodes}]]"
        )

    def _node_segment(self, node: Dict[str, Any]) -> str:
        conns = node.get("connections", {})
        namespaces = ",".join(
            self._namespace_segment(ns) for ns in node.get("namespaces", [])
        )
        return (
            f"{node.get('name', '')},{node.get('address', '')},{node.get('port', '')},"
            f"conns[{conns.get('opened', 0)},{conns.get('closed', 0)},"
            f"{conns.get('open', 0)}],namespace[{namespaces}]"
        )

    def _namespace_segment(self, namespace: Dict[str, Any]) -> str:
        latency = namespace.get("latency", {})
        segments: List[str] = []
        for kind in _LATENCY_ORDER:
            buckets = latency.get(kind.value)
            if buckets:
                segments.append(f"{kind.value}[{','.join(str(b) for b in buckets)}]")
        return (
            f"{namespace.get('name', '')},{namespace.get('errors', 0)},"
            f"{namespace.get('timeouts', 0)},{namespace.get('key_busy', 0)},"
            f"{namespace.get('bytes_in', 0)},{namespace.get('bytes_out', 0)},"
            f"latency({self._columns},{self._shift})[{','.join(segments)}]"
        )


class _NodeCloseTracker:
    """Decides which hosts have left the cluster since the last snapshot.

    Not a diff of consecutive snapshots: the underlying client accumulates
    per-host metrics and never drops a host from the map once seen, so
    comparing successive snapshots reports a departure exactly never. The live
    node list is the only thing that shrinks, so membership is judged by
    joining against it. Each host fires once.
    """

    def __init__(self) -> None:
        self._closed: set = set()

    def departed(self, snapshot_hosts: Sequence[str], live_hosts: Sequence[str]) -> List[str]:
        """Hosts present in the snapshot, absent from the cluster, not yet fired."""
        live = set(live_hosts)
        gone = [h for h in snapshot_hosts if h not in live and h not in self._closed]
        self._closed.update(gone)
        return gone


class _MetricsExportTimer:
    """Shared state for the two export timers.

    Holds the exporter and the close tracker; the async and sync timers differ only
    in how they wait and how they invoke.
    """

    def __init__(
        self, cluster: Any, exporter: Any, interval_seconds: float, settings: Any = None
    ) -> None:
        self.cluster = cluster
        self.exporter = exporter
        self.interval = interval_seconds
        self.settings = settings
        self.tracker = _NodeCloseTracker()

    @staticmethod
    def hosts_of(snapshot: MetricsSnapshot) -> List[str]:
        """Host keys the snapshot reports, in ``address:port`` form."""
        return [
            f"{n.get('address')}:{n.get('port')}"
            for n in snapshot.to_canonical_dict().get("nodes", [])
        ]


class AsyncMetricsExportTimer:
    """Pushes snapshots to an async exporter on the cluster's event loop.

    Runs as an ``asyncio.Task``. Snapshotting and exporting are both cold-path
    work; nothing here touches a request.
    """

    def __init__(
        self, cluster: Any, exporter: Any, interval_seconds: float, settings: Any = None
    ) -> None:
        self._state = _MetricsExportTimer(cluster, exporter, interval_seconds, settings)
        self._task: Optional[Any] = None

    def start(self) -> None:
        """Start the export task; requires a running event loop."""
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def _run(self) -> None:
        state = self._state
        # Announced from inside the task so the callback can be awaited; the
        # method that starts the timer is synchronous.
        try:
            await state.exporter.on_enable(state.cluster, state.settings)
        except Exception:
            log.warning("Metrics exporter raised in on_enable", exc_info=True)
        while True:
            await asyncio.sleep(state.interval)
            try:
                await self._export_once()
            except Exception:
                log.warning("Metrics export failed; will retry next interval", exc_info=True)

    async def _export_once(self) -> None:
        state = self._state
        snapshot = await state.cluster.metrics()
        await state.exporter.on_snapshot(snapshot)
        live = await state.cluster._sdk_client.underlying_client.nodes()
        live_hosts = [f"{n.host[0]}:{n.host[1]}" for n in live if n.host is not None]
        for host in state.tracker.departed(state.hosts_of(snapshot), live_hosts):
            await state.exporter.on_node_close(host, snapshot)

    def request_stop(self) -> None:
        """Cancel the export task without awaiting it.

        For callers that are not coroutines -- ``disable_metrics`` is a plain
        method. :meth:`stop` is the awaiting form used when shutting down.
        """
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def stop(self) -> None:
        """Cancel the export task and wait for it to finish."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None


class SyncMetricsExportTimer:
    """Pushes snapshots to an exporter from a daemon thread."""

    def __init__(
        self, cluster: Any, exporter: Any, interval_seconds: float, settings: Any = None
    ) -> None:
        self._state = _MetricsExportTimer(cluster, exporter, interval_seconds, settings)
        self._stop: Optional[threading.Event] = None
        self._thread: Optional[Any] = None

    def start(self) -> None:
        """Start the daemon export thread."""
        if self._thread is None:
            self._stop = threading.Event()
            self._thread = threading.Thread(
                target=self._run, name="aerospike-metrics-export", daemon=True,
            )
            self._thread.start()

    def _run(self) -> None:
        state = self._state
        stop = self._stop
        assert stop is not None  # set by start() before the thread begins
        try:
            state.exporter.on_enable(state.cluster, state.settings)
        except Exception:
            log.warning("Metrics exporter raised in on_enable", exc_info=True)
        while not stop.wait(state.interval):
            try:
                self._export_once()
            except Exception:
                log.warning("Metrics export failed; will retry next interval", exc_info=True)

    def _export_once(self) -> None:
        state = self._state
        snapshot = state.cluster.metrics()
        state.exporter.on_snapshot(snapshot)
        live = state.cluster._sdk_client.underlying_client.nodes_blocking()
        live_hosts = [f"{n.host[0]}:{n.host[1]}" for n in live if n.host is not None]
        for host in state.tracker.departed(state.hosts_of(snapshot), live_hosts):
            state.exporter.on_node_close(host, snapshot)

    def stop(self) -> None:
        """Signal the export thread to exit and join it briefly."""
        if self._thread is not None and self._stop is not None:
            self._stop.set()
            self._thread.join(timeout=2.0)
            self._thread = None


def built_in_exporter(metrics: Any, *, awaitable: bool = False) -> Optional[Any]:
    """Pick the built-in exporter the configuration asks for.

    Returns ``None`` when the configuration names no exporter, leaving
    whatever the caller already had in place.

    Args:
        metrics: The resolved ``metrics`` settings group, or ``None``.
        awaitable: Wrap the result for the async client, whose exporters are
            awaited. The built-in writes files, so its callbacks run off-loop.

    Returns:
        A :class:`LearnMetricsFileExporter`, a :class:`NoOpMetricsExporter`,
        or ``None`` to leave the current exporter alone.
    """
    if metrics is None:
        return None
    name = (metrics.exporter or "").strip().lower()
    if name == "none":
        return AsyncNoOpMetricsExporter() if awaitable else NoOpMetricsExporter()
    if name in ("", "learn_metrics_file"):
        report_dir = metrics.report_dir or ""
        if not report_dir:
            # The documented default with nowhere to write is a no-op, not an error.
            return None
        exporter = LearnMetricsFileExporter(report_dir, metrics.report_size_limit or 0)
        return _AsyncExporterAdapter(exporter) if awaitable else exporter
    log.warning("Metrics: unknown exporter %r; leaving the current exporter in place", name)
    return None


class _AsyncExporterAdapter:
    """Presents a synchronous exporter to the async client.

    Only used for the built-in file exporter, whose callbacks are blocking
    file writes: those run on a worker thread so they never sit on the event
    loop. A user's own exporter is never adapted -- the async client expects
    an :class:`AsyncMetricsExporter` and awaits it directly, so nobody's code
    is moved onto a thread without them choosing it.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    @property
    def inner(self) -> Any:
        """The wrapped synchronous exporter."""
        return self._inner

    async def on_enable(self, cluster: Any, settings: Any) -> None:
        """Await the wrapped enable off-loop."""
        await asyncio.to_thread(self._inner.on_enable, cluster, settings)

    async def on_snapshot(self, snapshot: MetricsSnapshot) -> None:
        """Await the wrapped snapshot write off-loop."""
        await asyncio.to_thread(self._inner.on_snapshot, snapshot)

    async def on_node_close(self, host: str, snapshot: MetricsSnapshot) -> None:
        """Await the wrapped node close off-loop."""
        await asyncio.to_thread(self._inner.on_node_close, host, snapshot)

    async def on_disable(self, cluster: Any) -> None:
        """Await the wrapped disable off-loop."""
        await asyncio.to_thread(self._inner.on_disable, cluster)


# Fire-and-forget disable tasks, held so the loop does not collect them mid-flight.
_PENDING_DISABLES: set = set()


def dispatch_disable(exporter: Any, cluster: Any) -> None:
    """Run an exporter's disable callback, whichever protocol it implements.

    ``disable_metrics`` is a plain method on both clusters, so an async
    exporter's coroutine cannot be awaited there. Scheduling it keeps the
    final flush from being dropped on the floor, which is what calling a
    coroutine and discarding it would do.
    """
    try:
        result = exporter.on_disable(cluster)
    except Exception:
        log.warning("Metrics exporter raised in on_disable", exc_info=True)
        return
    if inspect.isawaitable(result):
        try:
            task = asyncio.ensure_future(result)
        except RuntimeError:
            # No running loop: nothing can await the flush, and a warning here
            # would fire on every interpreter shutdown.
            return
        _PENDING_DISABLES.add(task)
        task.add_done_callback(_PENDING_DISABLES.discard)


def check_exporter(exporter: Any, *, awaitable: bool) -> None:
    """Reject an exporter written against the other client's protocol.

    ``runtime_checkable`` only proves the four method names exist -- it cannot
    tell a coroutine from a plain function, and both protocols declare the same
    names. So the check is whether ``on_snapshot`` is a coroutine function,
    which is the thing that actually differs and the thing that breaks at
    export time if it is wrong.

    Args:
        exporter: The candidate exporter.
        awaitable: Whether this cluster awaits its exporter.

    Raises:
        TypeError: If the exporter is missing a callback, or implements the
            opposite protocol.
    """
    for name in ("on_enable", "on_snapshot", "on_node_close", "on_disable"):
        if not callable(getattr(exporter, name, None)):
            raise TypeError(
                f"{type(exporter).__name__} is not a metrics exporter: "
                f"missing {name}()"
            )
    is_async = inspect.iscoroutinefunction(exporter.on_snapshot)
    if awaitable and not is_async:
        raise TypeError(
            f"{type(exporter).__name__} defines synchronous callbacks; the "
            f"async cluster needs an AsyncMetricsExporter (async def)"
        )
    if not awaitable and is_async:
        raise TypeError(
            f"{type(exporter).__name__} defines async callbacks; the sync "
            f"cluster needs a MetricsExporter (plain def)"
        )
