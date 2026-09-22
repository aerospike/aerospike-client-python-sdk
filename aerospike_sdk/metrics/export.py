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

The client *pushes* a snapshot to every registered exporter each export
interval. An exporter implements one method -- ``export(snapshot)`` -- and
decides what to do with what it receives: append to a file, translate to
OTEL, batch, drop. It never sees the cluster, nodes, or collection lifecycle;
opening and closing whatever it writes to belongs to the exporter and the
application that constructed it.

There are two exporter protocols because there are two clients. The async
client awaits :class:`AsyncMetricsExporter` on its loop; the sync client calls
:class:`MetricsExporter` directly. Exporting is IO -- a file write, an HTTP
post -- and an export interval is long enough that running it on the async
client's event loop would stall every in-flight operation behind it. Writing
an async exporter for the async client keeps that IO off the critical path
without the SDK moving user code onto a thread behind their back.

An exporter that keeps failing is suspended rather than allowed to fail every
interval forever: after three consecutive failures it is skipped, then retried
every tenth interval, and a success puts it back on the normal cadence. Other
exporters and collection itself are unaffected throughout.
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

# An exporter failing this many exports in a row is suspended...
SUSPEND_AFTER_CONSECUTIVE_FAILURES = 3
# ...and retried once every this many intervals until it succeeds again.
SUSPENDED_RETRY_EVERY_INTERVALS = 10

__all__ = [
    "DEFAULT_EXPORT_INTERVAL_SECONDS",
    "SUSPEND_AFTER_CONSECUTIVE_FAILURES",
    "SUSPENDED_RETRY_EVERY_INTERVALS",
    "AsyncMetricsExportTimer",
    "AsyncMetricsExporter",
    "LearnMetricsFileExporter",
    "MetricsExporter",
    "SyncMetricsExportTimer",
    "built_in_exporter",
    "check_exporter",
]


@runtime_checkable
class MetricsExporter(Protocol):
    """Receives metrics snapshots from a synchronous cluster.

    Implement this for :class:`~aerospike_sdk.sync.cluster.SyncCluster`. The
    call runs on the export thread, so a slow exporter delays the next export
    but never a request.

    The snapshot argument is an owned copy: keeping a reference past the call
    is safe. Resources the exporter writes to are its own to manage -- open
    them in the constructor or on first call, and close them from application
    code when done.

    Example::

        class JsonLinesExporter:
            def __init__(self, path):
                self._file = open(path, "a")

            def export(self, snapshot):
                self._file.write(json.dumps(snapshot.to_canonical_dict()) + "\\n")
                self._file.flush()

            def close(self):
                self._file.close()

        exporter = JsonLinesExporter("/var/log/aerospike/metrics.jsonl")
        cluster.add_exporter(exporter)
        ...
        exporter.close()   # the application closes it, never the client

    See Also:
        :class:`AsyncMetricsExporter`: The same contract for the async client.
        :meth:`~aerospike_sdk.sync.cluster.SyncCluster.add_exporter`
    """

    def export(self, snapshot: MetricsSnapshot) -> None:
        """A snapshot was taken, once per export interval."""
        ...


@runtime_checkable
class AsyncMetricsExporter(Protocol):
    """Receives metrics snapshots from an asynchronous cluster.

    The same contract as :class:`MetricsExporter` with an awaitable method, so
    an exporter can use an async HTTP client without blocking the event loop
    the cluster's own operations run on.

    Example::

        class OtelExporter:
            def __init__(self, client):
                self._client = client

            async def export(self, snapshot):
                await self._client.post("/v1/metrics", json=snapshot.to_canonical_dict())

        cluster.add_exporter(OtelExporter(httpx.AsyncClient()))

    See Also:
        :class:`MetricsExporter`: The same contract for the sync client.
        :meth:`~aerospike_sdk.aio.cluster.Cluster.add_exporter`
    """

    async def export(self, snapshot: MetricsSnapshot) -> None:
        """A snapshot was taken, once per export interval."""
        ...


# Fields the legacy line format carries that this client cannot measure. Named
# in the header so whoever owns the log shipper learns it from the file rather
# than from a parse failure downstream.
_UNAVAILABLE_FIELDS = ("cpu", "mem", "inUse", "inPool", "recoverQueueSize", "invalidNodeCount")

# Usage counters on the cluster line, in the order and under the names the
# format defines. Values come from the canonical snapshot's ``usage``.
_USAGE_FILE_COLUMNS = (
    ("singleCount", "feature.shape.point"),
    ("batchCount", "feature.shape.batch"),
    ("queryCount", "feature.shape.query"),
    ("blockingCount", "feature.api.blocking"),
    ("deferredCount", "feature.api.deferred"),
    ("backgroundCount", "feature.api.background"),
)

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

    Kept so existing log shippers keep working. Data lines are positional;
    the header line declares the schema with the format's legacy camelCase
    field names (``keyBusy``, ``bytesIn``) because that is what its existing
    consumers parse -- a compatibility requirement, not a style choice.

    An ordinary exporter, not a client lifecycle: the first :meth:`export`
    creates the directory, opens ``metrics-<timestamp>.log`` and writes the
    header; each later one appends a cluster line, plus a final node line for
    every entry the snapshot reports departed. The application calls
    :meth:`close` when done -- the client never closes it. IO errors propagate
    to the export timer, which logs them and suspends the exporter after
    repeated failures.

    Six fields the format defines cannot be filled: ``cpu`` and ``mem`` (process
    statistics this client does not sample), ``inUse`` and ``inPool`` (the
    client keeps a single open-connection gauge, not the split),
    ``recoverQueueSize`` and ``invalidNodeCount``. They are omitted rather than
    written as zero, and the header line names them so a downstream parser sees
    why.

    The six feature-usage columns (``singleCount`` through ``backgroundCount``)
    carry the counters of the same name from the canonical snapshot's ``usage``
    mapping, and ``retryCount`` carries its ``cluster.command_retries``. The
    format has no field for the per-call ``command_count``; read that off
    :meth:`~aerospike_sdk.metrics.MetricsSnapshot.to_canonical_dict` instead,
    under ``cluster.command_count``.

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

        exporter = LearnMetricsFileExporter("/var/log/aerospike")
        cluster.add_exporter(exporter)
        ...
        exporter.close()

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

    # -- exporter contract ----------------------------------------------------

    def export(self, snapshot: MetricsSnapshot) -> None:
        """Append one cluster line, and a final line per departed node."""
        if not self._dir:
            return
        if self._file is None:
            os.makedirs(self._dir, exist_ok=True)
            self._open()
        doc = snapshot.to_canonical_dict()
        self._write(self._cluster_line(doc))
        for node in doc.get("nodes_departed", []):
            self._write(f"node[{self._node_segment(node)}]")

    def close(self) -> None:
        """Close the log file. Exporting again afterwards opens a new one."""
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
        usage_names = ",".join(name for name, _ in _USAGE_FILE_COLUMNS)
        header = (
            "header(3) cluster[name,clientType,clientVersion,appId,label[],"
            f"{usage_names},retryCount,node[]] "
            "label[name,value] "
            "node[name,address,port,conn,namespace[]] "
            "conn[opened,closed,open] "
            "namespace[name,errors,timeouts,keyBusy,bytesIn,bytesOut,latency[]] "
            "latency(unit,columns,shift)[type[buckets]] "
            f"unavailable[{','.join(_UNAVAILABLE_FIELDS)}]"
        )
        # Not through _write: the header must never trip rotation, or a limit
        # smaller than the header rotates forever.
        self._append(header)

    def _append(self, line: str) -> None:
        """Write one line, without considering rotation."""
        if self._file is None:
            return
        self._file.write(line + "\n")
        self._file.flush()
        self._written += len(line) + 1

    def _write(self, line: str) -> None:
        if self._file is None:
            return
        self._append(line)
        if self._limit and self._written >= self._limit:
            self.close()
            self._open()

    # -- formatting ---------------------------------------------------------

    def _cluster_line(self, doc: Dict[str, Any]) -> str:
        # Recorded before the node segments are built: they print the shape.
        self._columns = doc.get("latency_columns", "")
        self._shift = doc.get("latency_shift", "")
        labels = ",".join(f"{k}={v}" for k, v in sorted(doc.get("labels", {}).items()))
        usage = doc.get("usage") or {}
        counts = ",".join(str(int(usage.get(key, 0))) for _, key in _USAGE_FILE_COLUMNS)
        # The line format's own name for the canonical `command_retries`.
        retry_count = int((doc.get("cluster") or {}).get("command_retries", 0) or 0)
        nodes = ",".join(self._node_segment(n) for n in doc.get("nodes", []))
        return (
            f"cluster[{doc.get('cluster_name', '')},{doc.get('client_type', '')},"
            f"{doc.get('client_version', '')},{doc.get('app_id', '')},"
            f"label[{labels}],{counts},{retry_count},node[{nodes}]]"
        )

    def _node_segment(self, node: Dict[str, Any]) -> str:
        conns = node.get("connections", {})
        namespaces = ",".join(
            self._namespace_segment(ns) for ns in node.get("namespaces", [])
        )
        return (
            f"{node.get('name', '')},{node.get('address', '')},{node.get('port', '')},"
            f"conn[{conns.get('opened', 0)},{conns.get('closed', 0)},"
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
    joining against it. Each host is reported once.
    """

    def __init__(self) -> None:
        self._closed: set = set()

    def departed(self, snapshot_hosts: Sequence[str], live_hosts: Sequence[str]) -> List[str]:
        """Hosts present in the snapshot, absent from the cluster, not yet reported."""
        live = set(live_hosts)
        gone = [h for h in snapshot_hosts if h not in live and h not in self._closed]
        self._closed.update(gone)
        return gone


class _ExporterHealth:
    """Suspension state for one registered exporter."""

    __slots__ = ("failures", "suspended", "skipped")

    def __init__(self) -> None:
        self.failures = 0
        self.suspended = False
        self.skipped = 0

    def should_attempt(self) -> bool:
        """Whether this cycle calls the exporter, advancing the retry countdown."""
        if not self.suspended:
            return True
        self.skipped += 1
        if self.skipped >= SUSPENDED_RETRY_EVERY_INTERVALS:
            self.skipped = 0
            return True
        return False


class _MetricsExportTimer:
    """Shared state for the two export timers.

    Holds the departure tracker and per-exporter health; the async and sync
    timers differ only in how they wait and how they invoke. The exporter list
    is read from the cluster on every cycle, so registrations made after the
    timer started still take effect.
    """

    def __init__(self, cluster: Any, interval_seconds: float, settings: Any = None) -> None:
        self.cluster = cluster
        self.interval = interval_seconds
        self.settings = settings
        self.tracker = _NodeCloseTracker()
        self._health: Dict[int, _ExporterHealth] = {}

    def exporters_to_run(self) -> List[Any]:
        """The exporters this cycle calls, honoring suspensions."""
        current = list(self.cluster._exporters)
        ids = {id(e) for e in current}
        # Health for a removed exporter would pin its id forever, and a new
        # exporter can land on a recycled id; prune to the live list.
        self._health = {k: v for k, v in self._health.items() if k in ids}
        return [
            exporter for exporter in current
            if self._health.setdefault(id(exporter), _ExporterHealth()).should_attempt()
        ]

    def record_success(self, exporter: Any) -> None:
        health = self._health.get(id(exporter))
        if health is None:
            return
        if health.suspended:
            log.info(
                "Metrics exporter %r recovered; resuming its normal export cadence",
                type(exporter).__name__,
            )
        health.failures = 0
        health.suspended = False
        health.skipped = 0

    def record_failure(self, exporter: Any) -> None:
        """Log the failure; suspend after enough of them in a row.

        Called from an ``except`` block so the log line carries the traceback.
        """
        health = self._health.setdefault(id(exporter), _ExporterHealth())
        health.failures += 1
        if health.suspended:
            log.warning(
                "Metrics exporter %r failed its retry; staying suspended",
                type(exporter).__name__, exc_info=True,
            )
            return
        if health.failures >= SUSPEND_AFTER_CONSECUTIVE_FAILURES:
            health.suspended = True
            health.skipped = 0
            log.warning(
                "Metrics exporter %r failed %d consecutive exports; suspending it "
                "and retrying every %d intervals",
                type(exporter).__name__, health.failures,
                SUSPENDED_RETRY_EVERY_INTERVALS, exc_info=True,
            )
        else:
            log.warning(
                "Metrics exporter %r raised; continuing with the others",
                type(exporter).__name__, exc_info=True,
            )


class AsyncMetricsExportTimer:
    """Pushes snapshots to the cluster's exporters on its event loop.

    Runs as an ``asyncio.Task``. Snapshotting and exporting are both cold-path
    work; nothing here touches a request.
    """

    def __init__(self, cluster: Any, interval_seconds: float, settings: Any = None) -> None:
        self._state = _MetricsExportTimer(cluster, interval_seconds, settings)
        self._task: Optional[Any] = None

    def start(self) -> None:
        """Start the export task; requires a running event loop."""
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def _run(self) -> None:
        state = self._state
        while True:
            await asyncio.sleep(state.interval)
            try:
                await self._export_once()
            except Exception:
                log.warning("Metrics export failed; will retry next interval", exc_info=True)

    async def _export_once(self) -> None:
        state = self._state
        runnable = state.exporters_to_run()
        if not runnable:
            # Taking a snapshot drains and aggregates per-node state in the
            # client core; skip that work when nothing would consume it.
            return
        snapshot = await state.cluster.metrics()
        snapshot._mark_departed(state.tracker)
        for exporter in runnable:
            try:
                await exporter.export(snapshot)
            except Exception:
                state.record_failure(exporter)
            else:
                state.record_success(exporter)

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
    """Pushes snapshots to the cluster's exporters from a daemon thread."""

    def __init__(self, cluster: Any, interval_seconds: float, settings: Any = None) -> None:
        self._state = _MetricsExportTimer(cluster, interval_seconds, settings)
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
        while not stop.wait(state.interval):
            try:
                self._export_once()
            except Exception:
                log.warning("Metrics export failed; will retry next interval", exc_info=True)

    def _export_once(self) -> None:
        state = self._state
        runnable = state.exporters_to_run()
        if not runnable:
            return
        snapshot = state.cluster.metrics()
        snapshot._mark_departed(state.tracker)
        for exporter in runnable:
            try:
                exporter.export(snapshot)
            except Exception:
                state.record_failure(exporter)
            else:
                state.record_success(exporter)

    def stop(self) -> None:
        """Signal the export thread to exit and join it briefly."""
        if self._thread is not None and self._stop is not None:
            self._stop.set()
            self._thread.join(timeout=2.0)
            self._thread = None


def built_in_exporter(metrics: Any, *, awaitable: bool = False) -> Optional[Any]:
    """Build the exporter the configuration asks for, if it asks for one.

    Only consulted when the application registered no exporter of its own:
    a configured exporter is a convenience install, never a replacement for
    what application code set up.

    Args:
        metrics: The resolved ``metrics`` settings group, or ``None``.
        awaitable: Wrap the result for the async client, whose exporters are
            awaited. The built-in writes files, so its call runs off-loop.

    Returns:
        A :class:`LearnMetricsFileExporter` when ``exporter`` names it (the
        default) and ``report_dir`` is set; otherwise ``None`` -- nothing to
        install.
    """
    if metrics is None:
        return None
    name = (metrics.exporter or "").strip().lower()
    if name == "none":
        return None
    if name in ("", "learn_metrics_file"):
        report_dir = metrics.report_dir or ""
        if not report_dir:
            # The documented default with nowhere to write installs nothing.
            return None
        exporter = LearnMetricsFileExporter(report_dir, metrics.report_size_limit or 0)
        return _AsyncExporterAdapter(exporter) if awaitable else exporter
    log.warning("Metrics: unknown exporter %r; not installing a built-in exporter", name)
    return None


class _AsyncExporterAdapter:
    """Presents a synchronous exporter to the async client.

    Only used for the built-in file exporter, whose call is a blocking file
    write: it runs on a worker thread so it never sits on the event loop. A
    user's own exporter is never adapted -- the async client expects an
    :class:`AsyncMetricsExporter` and awaits it directly, so nobody's code is
    moved onto a thread without them choosing it.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    @property
    def inner(self) -> Any:
        """The wrapped synchronous exporter."""
        return self._inner

    async def export(self, snapshot: MetricsSnapshot) -> None:
        """Await the wrapped export off-loop."""
        await asyncio.to_thread(self._inner.export, snapshot)

    def close(self) -> None:
        """Close the wrapped exporter."""
        self._inner.close()


def check_exporter(exporter: Any, *, awaitable: bool) -> None:
    """Reject an exporter written against the other client's protocol.

    ``runtime_checkable`` only proves the method name exists -- it cannot tell
    a coroutine from a plain function, and both protocols declare the same
    name. So the check is whether ``export`` is a coroutine function, which is
    the thing that actually differs and the thing that breaks at export time
    if it is wrong.

    Args:
        exporter: The candidate exporter.
        awaitable: Whether this cluster awaits its exporters.

    Raises:
        TypeError: If the exporter has no ``export`` method, or implements the
            opposite protocol.
    """
    method = getattr(exporter, "export", None)
    if not callable(method):
        raise TypeError(
            f"{type(exporter).__name__} is not a metrics exporter: missing export()"
        )
    is_async = inspect.iscoroutinefunction(method)
    if awaitable and not is_async:
        raise TypeError(
            f"{type(exporter).__name__} defines a synchronous export(); the "
            f"async cluster needs an AsyncMetricsExporter (async def export)"
        )
    if not awaitable and is_async:
        raise TypeError(
            f"{type(exporter).__name__} defines an async export(); the sync "
            f"cluster needs a MetricsExporter (plain def export)"
        )
