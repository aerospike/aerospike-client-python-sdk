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

Which exporters receive the push is chosen by the ``metrics.exporter``
setting (:class:`MetricsExporterType`): ``file``, the default, sends it to
the built-in log writer only; ``custom`` to the exporters registered with
``add_exporter`` only; and ``none`` to nobody. The modes do not combine, so an
application that registers its own exporter also selects ``custom``.

An exporter that keeps failing is suspended rather than allowed to fail every
interval forever: after three consecutive failures it is skipped, then retried
every tenth interval, and a success puts it back on the normal cadence. Other
exporters and collection itself are unaffected throughout.

Disabling metrics or closing the cluster pushes one last snapshot, so the
window since the previous export is not lost. Restarting the timer on a
policy change does not.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from enum import Enum
import inspect
import logging
import os
import threading
import time
from typing import (
    Any,
    Coroutine,
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    runtime_checkable,
)

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.metrics import LatencyType, MetricsSnapshot

log = logging.getLogger(SdkLoggers.BEHAVIOR)

# How often a snapshot is pushed when the configuration does not say.
DEFAULT_EXPORT_INTERVAL_SECONDS = 30.0

# An exporter failing this many exports in a row is suspended...
SUSPEND_AFTER_CONSECUTIVE_FAILURES = 3
# ...and retried once every this many intervals until it succeeds again.
SUSPENDED_RETRY_EVERY_INTERVALS = 10



class MetricsExporterType(str, Enum):
    """Where the periodic push goes: the ``metrics.exporter`` setting.

    The values are the configuration-file spellings; construction is
    case-insensitive, so ``MetricsExporterType("FILE")`` is :attr:`FILE`.

    Attributes:
        FILE: The built-in log writer under ``report_dir``, and nothing else.
            The default.
        CUSTOM: The exporters registered with ``add_exporter``, in
            registration order, and nothing else.
        NONE: No periodic push; snapshots remain available by polling.

    Example::

        settings = SystemSettings(metrics=MetricsSettings(exporter=MetricsExporterType.CUSTOM))
        cluster = await ClusterDefinition(host).with_system_settings(settings).connect()
        cluster.add_exporter(my_exporter)
    """

    FILE = "file"
    CUSTOM = "custom"
    NONE = "none"

    @classmethod
    def _missing_(cls, value: object) -> Optional["MetricsExporterType"]:
        if isinstance(value, str):
            lowered = value.strip().lower()
            for member in cls:
                if member.value == lowered:
                    return member
        return None

    def __str__(self) -> str:
        return self.value


# Smallest non-zero rotation size. Below this a busy client would rotate
# every few intervals and scatter one run across a directory of files.
MIN_REPORT_SIZE_LIMIT = 1_000_000

__all__ = [
    "DEFAULT_EXPORT_INTERVAL_SECONDS",
    "MIN_REPORT_SIZE_LIMIT",
    "SUSPEND_AFTER_CONSECUTIVE_FAILURES",
    "SUSPENDED_RETRY_EVERY_INTERVALS",
    "AsyncMetricsExportTimer",
    "AsyncMetricsExporter",
    "MetricsExporter",
    "MetricsExporterType",
    "MetricsWriter",
    "SyncMetricsExportTimer",
    "built_in_exporter",
    "check_exporter",
    "export_mode",
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


# Cluster-line usage columns, in format order, keyed by the usage counter each
# one reports.
_USAGE_FILE_COLUMNS = (
    "feature.api.blocking",
    "feature.api.deferred",
    "feature.api.background",
    "feature.transaction",
)

# Latency segment order on every namespace, as the format writes it.
_LATENCY_ORDER = (
    LatencyType.CONN,
    LatencyType.WRITE,
    LatencyType.READ,
    LatencyType.BATCH,
    LatencyType.QUERY,
)

_HEADER_FIELDS = (
    "header(5)"
    " cluster[cluster_name,client_type,client_version,app_id,labels[],cpu,mem,"
    "recover_queue_size,nodes_invalid,command_count,blocking_count,deferred_count,"
    "background_count,tran_count,command_retries,nodes[]]"
    " labels[name,value]"
    " nodes[name,address,port,conns_in_use,conns_in_pool,conns_opened,conns_closed,"
    "namespaces[]]"
    " namespaces[name,errors,timeouts,key_busy,bytes_in,bytes_out,latency[]]"
)


class MetricsWriter:
    """Writes the line-oriented metrics log under a report directory.

    Every line starts with a local timestamp. The first line of each file is
    a header declaring the schema; each export appends one positional
    ``cluster[...]`` line carrying the cluster totals, its labels, and every
    node with its namespaces. A node that left the cluster since the previous
    export is written once more in that list, with its final counters.

    The four usage columns (``blocking_count``, ``deferred_count``,
    ``background_count``, ``tran_count``) carry the canonical snapshot's
    ``feature.api.*`` and ``feature.transaction`` counters, and read 0 unless
    usage counters are enabled. ``command_count`` and ``command_retries``
    read 0 unless operational metrics are enabled. A namespace's ``errors``
    excludes the failures counted under ``timeouts`` and ``key_busy``, and
    ``timeouts`` counts server-reported timeouts.

    The header names the latency unit, column count and shift. A file only
    ever describes one histogram shape: an export whose shape differs from
    the header's starts a new file.

    An ordinary exporter, not a client lifecycle: the first :meth:`export`
    creates the directory and opens ``metrics-<timestamp>.log``. The
    application calls :meth:`close` when done -- the client never closes an
    exporter it did not install. IO errors propagate to the export timer,
    which logs them and suspends the exporter after repeated failures.

    Args:
        report_dir: Directory for log files; created if absent. An empty
            directory makes the exporter a no-op, matching the config default.
        report_size_limit: Rotate once the file reaches this many bytes.
            ``0`` never rotates.

    Raises:
        ValueError: If ``report_size_limit`` is neither 0 nor at least
            1,000,000.

    Example::

        exporter = MetricsWriter("/var/log/aerospike", 10_000_000)
        cluster.add_exporter(exporter)
        ...
        exporter.close()

    See Also:
        :class:`MetricsExporter`: The protocol this implements.
    """

    def __init__(self, report_dir: str, report_size_limit: int = 0) -> None:
        if report_size_limit and report_size_limit < MIN_REPORT_SIZE_LIMIT:
            raise ValueError(
                f"report_size_limit must be 0 or at least {MIN_REPORT_SIZE_LIMIT}, "
                f"got {report_size_limit}"
            )
        self._dir = report_dir
        self._limit = report_size_limit
        self._file: Optional[Any] = None
        self._written = 0
        # Histogram shape the open file's header declares.
        self._shape: Optional[Tuple[str, Any, Any]] = None

    # -- exporter contract ----------------------------------------------------

    def export(self, snapshot: MetricsSnapshot) -> None:
        """Append one cluster line, opening a file first when needed."""
        if not self._dir:
            return
        doc = snapshot.to_canonical_dict()
        shape = (
            str(doc.get("latency_unit", "milliseconds")).upper(),
            doc.get("latency_columns", ""),
            doc.get("latency_shift", ""),
        )
        if self._file is not None and shape != self._shape:
            self.close()
        if self._file is None:
            os.makedirs(self._dir, exist_ok=True)
            self._open(shape)
        self._write(self._cluster_line(doc))

    def close(self) -> None:
        """Close the log file. Exporting again afterwards opens a new one."""
        if self._file is not None:
            try:
                self._file.close()
            except OSError:
                pass
            self._file = None

    # -- file handling ------------------------------------------------------

    def _open(self, shape: Tuple[str, Any, Any]) -> None:
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
        self._shape = shape
        unit, columns, shift = shape
        # Not through _write: the header must never trip rotation.
        self._append(
            f"{_line_time(datetime.now())} {_HEADER_FIELDS}"
            f" latency({unit},{columns},{shift})[type[l1,l2,l3...]]"
        )

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
            shape = self._shape
            self.close()
            if shape is not None:
                self._open(shape)

    # -- formatting ---------------------------------------------------------

    def _cluster_line(self, doc: Dict[str, Any]) -> str:
        # Positional, and bracketed exactly as existing parsers of this format
        # expect, which is not balanced. Do not "fix" the brackets.
        labels = ",".join(f"[{k},{v}]" for k, v in (doc.get("labels") or {}).items())
        usage = doc.get("usage") or {}
        counts = ",".join(str(int(usage.get(key, 0) or 0)) for key in _USAGE_FILE_COLUMNS)
        cluster = doc.get("cluster") or {}
        columns = int(doc.get("latency_columns") or 0)
        nodes = ",".join(
            self._node_segment(node, columns)
            for node in [*doc.get("nodes", []), *doc.get("nodes_departed", [])]
        )
        return (
            f"{_line_time(_parse_timestamp(doc.get('timestamp')))} cluster["
            f"{doc.get('cluster_name', '')},{doc.get('client_type', '')},"
            f"{doc.get('client_version', '')},{doc.get('app_id', '')},[{labels}],"
            f"{float(cluster.get('cpu_percent', 0.0) or 0.0)},"
            f"{int(cluster.get('memory_bytes', 0) or 0)},"
            f"{int((cluster.get('recover_queue') or {}).get('size', 0) or 0)},"
            f"{int((cluster.get('nodes') or {}).get('invalid', 0) or 0)},"
            f"{int(cluster.get('command_count', 0) or 0)},{counts},"
            f"{int(cluster.get('command_retries', 0) or 0)},[{nodes}]"
        )

    @staticmethod
    def _node_segment(node: Dict[str, Any], columns: int) -> str:
        conns = node.get("connections") or {}
        parts = [
            f"[{node.get('name', '')},{node.get('address', '')},{node.get('port', '')},"
            f"{conns.get('in_use', 0)},{conns.get('in_pool', 0)},"
            f"{conns.get('opened', 0)},{conns.get('closed', 0)},["
        ]
        for i, namespace in enumerate(node.get("namespaces") or []):
            if i:
                parts.append(",[")
            latency = namespace.get("latency") or {}
            types = ",".join(
                f"{kind.value}[{','.join(str(b) for b in (latency.get(kind.value) or [0] * columns))}]"
                for kind in _LATENCY_ORDER
            )
            parts.append(
                f"{namespace.get('name', '')},{namespace.get('errors', 0)},"
                f"{namespace.get('timeouts', 0)},{namespace.get('key_busy', 0)},"
                f"{namespace.get('bytes_in', 0)},{namespace.get('bytes_out', 0)},"
                f"[{types}]]"
            )
        parts.append("]]")
        return "".join(parts)


def _parse_timestamp(value: Any) -> datetime:
    """The snapshot's own wall time in local time, or now if it has none."""
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).astimezone()
        except ValueError:
            pass
    return datetime.now()


def _line_time(moment: datetime) -> str:
    """The line prefix: local time to the millisecond."""
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + f"{moment.microsecond // 1000:03d}"


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
        """The exporters this cycle calls, honoring the mode and suspensions."""
        current = self.cluster._export_targets()
        ids = {id(e) for e in current}
        # Health for a removed exporter would pin its id forever, and a new
        # exporter can land on a recycled id; prune to the live list.
        self._health = {k: v for k, v in self._health.items() if k in ids}
        return [
            exporter for exporter in current
            if self._health.setdefault(id(exporter), _ExporterHealth()).should_attempt()
        ]

    def take(self) -> Optional[Tuple[Any, List[Any]]]:
        """This cycle's snapshot and its recipients; ``None`` when nobody would consume it."""
        runnable = self.exporters_to_run()
        if not runnable:
            # Taking a snapshot drains and aggregates per-node state in the
            # client core; skip that work when nothing would consume it.
            return None
        snapshot = self.cluster.metrics_snapshot()
        snapshot._mark_departed(self.tracker)
        return snapshot, runnable

    def take_final(self) -> Optional[Tuple[Any, List[Any]]]:
        """:meth:`take` for the closing push, which must never fail a disable or close."""
        try:
            return self.take()
        except Exception:
            log.warning(
                "Final metrics snapshot failed; the last window is not exported",
                exc_info=True,
            )
            return None

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

    ``after`` is a final push still in flight from the timer this one
    replaces; the first export waits for it, so an exporter never sees two
    exports at once.
    """

    def __init__(
        self,
        cluster: Any,
        interval_seconds: float,
        settings: Any = None,
        *,
        after: Optional[asyncio.Future] = None,
    ) -> None:
        self._state = _MetricsExportTimer(cluster, interval_seconds, settings)
        self._task: Optional[asyncio.Task] = None
        self._after = after

    def start(self) -> None:
        """Start the export task; requires a running event loop."""
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def _run(self) -> None:
        state = self._state
        if self._after is not None:
            await asyncio.wait({self._after})
        while True:
            await asyncio.sleep(state.interval)
            try:
                await self._export_once()
            except Exception:
                log.warning("Metrics export failed; will retry next interval", exc_info=True)

    async def _export_once(self) -> None:
        batch = self._state.take()
        if batch is not None:
            await self._deliver(*batch)

    async def _deliver(self, snapshot: Any, runnable: List[Any]) -> None:
        state = self._state
        for exporter in runnable:
            try:
                await exporter.export(snapshot)
            except Exception:
                state.record_failure(exporter)
            else:
                state.record_success(exporter)

    async def _deliver_after(
        self, task: Optional[asyncio.Task], snapshot: Any, runnable: List[Any],
    ) -> None:
        if task is not None:
            await asyncio.wait({task})
        await self._deliver(snapshot, runnable)

    def _cancel(self) -> Optional[asyncio.Task]:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
        return task

    def request_stop(self, *, final: bool = False) -> Optional[Coroutine[Any, Any, None]]:
        """Cancel the export task without awaiting it.

        For callers that are not coroutines -- ``disable_metrics`` is a plain
        method. :meth:`stop` is the awaiting form used when shutting down.

        With ``final``, the closing snapshot is taken now, while collection is
        still on, and the coroutine that delivers it once the cancelled task
        has unwound is returned for the caller to schedule. ``None`` when
        nothing would receive it.
        """
        task = self._cancel()
        if not final:
            return None
        batch = self._state.take_final()
        return None if batch is None else self._deliver_after(task, *batch)

    async def stop(self, *, final: bool = False) -> None:
        """Cancel the export task and wait for it; with ``final``, then push one closing snapshot."""
        task = self._cancel()
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass
        if final:
            batch = self._state.take_final()
            if batch is not None:
                await self._deliver(*batch)


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
        batch = self._state.take()
        if batch is not None:
            self._deliver(*batch)

    def _deliver(self, snapshot: Any, runnable: List[Any]) -> None:
        state = self._state
        for exporter in runnable:
            try:
                exporter.export(snapshot)
            except Exception:
                state.record_failure(exporter)
            else:
                state.record_success(exporter)

    def stop(self, *, final: bool = False) -> None:
        """Signal the export thread to exit and join it briefly.

        With ``final``, push one closing snapshot once the thread has exited.
        """
        if self._thread is None or self._stop is None:
            return
        self._stop.set()
        self._thread.join(timeout=2.0)
        # A thread still inside an export is pushing nearly the same window;
        # a closing push alongside it would hand exporters two at once.
        exited = not self._thread.is_alive()
        self._thread = None
        if final and exited:
            batch = self._state.take_final()
            if batch is not None:
                self._deliver(*batch)


def export_mode(metrics: Any) -> MetricsExporterType:
    """Resolve ``metrics.exporter`` to the mode the push runs in.

    Args:
        metrics: The resolved ``metrics`` settings group, or ``None``.

    Returns:
        The configured :class:`MetricsExporterType`;
        :attr:`~MetricsExporterType.FILE` when unset.
    """
    mode = getattr(metrics, "exporter", None)
    return MetricsExporterType.FILE if mode is None else MetricsExporterType(mode)


def built_in_exporter(metrics: Any, *, awaitable: bool = False) -> Optional[Any]:
    """Build the built-in log writer when the configuration selects it.

    Built when ``exporter`` is ``file`` (or unset, which means the same) and
    ``report_dir`` is set. Choosing ``file`` by name with nowhere to write is
    logged, since that configuration exports nothing; the default with no
    directory is silent.

    Args:
        metrics: The resolved ``metrics`` settings group, or ``None``.
        awaitable: Wrap the result for the async client, whose exporters are
            awaited. The built-in writes files, so its call runs off-loop.

    Returns:
        A :class:`MetricsWriter`, or ``None`` when there is
        nothing to install.
    """
    if metrics is None or export_mode(metrics) is not MetricsExporterType.FILE:
        return None
    report_dir = metrics.report_dir or ""
    if not report_dir:
        if metrics.exporter is not None:
            log.warning(
                "Metrics: exporter is 'file' but report_dir is empty; snapshots are not exported"
            )
        return None
    exporter = MetricsWriter(report_dir, metrics.report_size_limit or 0)
    return _AsyncExporterAdapter(exporter) if awaitable else exporter


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
