# Copyright 2025-2026 Aerospike, Inc.
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

"""Metrics snapshot: the collected values and their canonical form.

:class:`MetricsSnapshot` wraps what the client core recorded and derives the
five-way latency grouping plus :meth:`MetricsSnapshot.to_canonical_dict`, the
stable document an exporter serializes. :mod:`.policy` holds the input side.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from collections.abc import Mapping
from functools import lru_cache
from importlib.metadata import version
from datetime import datetime, timezone
import os
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence, Set, Tuple

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes
else:
    import resource

from aerospike_native import (
    ClusterMetrics as _PncClusterMetrics,
    LatencyUnit,
    NodeMetricsSnapshot,
)

from aerospike_sdk.metrics.policy import (
    LatencyType,
    MetricsPolicy,
    _DEFAULT_LATENCY_COLUMNS,
    _DEFAULT_LATENCY_SHIFT,
    _LATENCY_TYPE_COMMANDS,
    _ALL_COMMAND_TYPES,
)

__all__ = ["DerivedHistogram", "MetricsSnapshot"]

@dataclass(frozen=True)
class DerivedHistogram:
    """A latency histogram summed across command categories.

    Produced by :meth:`MetricsSnapshot.latency`; bucket values are in the
    snapshot's latency unit (see :attr:`latency_unit`).
    """

    buckets: tuple
    count: int
    min: int
    max: int
    sum: float
    latency_unit: LatencyUnit

    @property
    def average(self) -> float:
        """Mean recorded value, or 0.0 when nothing was recorded."""
        return self.sum / self.count if self.count else 0.0


class _HistogramTotals(NamedTuple):
    buckets: List[int]
    count: int
    min: int
    max: int
    sum: float


def _sum_histograms(histograms: Iterable[Any]) -> _HistogramTotals:
    """Sum same-shaped histograms.

    Histograms whose bucket count differs from the first non-empty one are
    skipped, the same way the core's own aggregation skips mismatched shapes.
    With no samples at all the buckets are zeros in the first histogram's
    shape, so a series that recorded nothing still has one count per column.
    """
    buckets: List[int] = []
    count = 0
    minimum = 0
    maximum = 0
    total = 0.0
    zeros: List[int] = []
    for hist in histograms:
        if hist is None:
            continue
        if hist.count == 0:
            if not zeros:
                zeros = [0] * len(hist.buckets)
            continue
        hb = hist.buckets
        if not buckets:
            buckets = list(hb)
        elif len(hb) != len(buckets):
            continue
        else:
            for i, value in enumerate(hb):
                buckets[i] += value
        minimum = hist.min if count == 0 else min(minimum, hist.min)
        maximum = max(maximum, hist.max)
        count += hist.count
        total += hist.sum
    return _HistogramTotals(buckets or zeros, count, minimum, maximum, total)


def _merge_histograms(histograms, latency_unit: LatencyUnit) -> DerivedHistogram:
    """Sum same-shaped histograms into one derived view."""
    totals = _sum_histograms(histograms)
    return DerivedHistogram(
        buckets=tuple(totals.buckets),
        count=totals.count,
        min=totals.min,
        max=totals.max,
        sum=totals.sum,
        latency_unit=latency_unit,
    )


def _raw_histogram(entry: Mapping) -> _HistogramTotals:
    """A histogram from a snapshot's serialized form."""
    return _HistogramTotals(
        list(entry.get("buckets") or []),
        int(entry.get("count") or 0),
        int(entry.get("min") or 0),
        int(entry.get("max") or 0),
        float(entry.get("sum") or 0.0),
    )


def _histogram_view(totals: Any) -> Dict[str, Any]:
    """One latency series in the canonical document."""
    return {
        "buckets": list(totals.buckets),
        "sum": totals.sum,
        "count": totals.count,
        "min": totals.min,
        "max": totals.max,
    }


class ProcessSampler:
    """This process's CPU share and resident memory, read at snapshot time.

    CPU is the process's CPU time consumed since the previous sample as a
    percentage of the wall time elapsed, so a busy process on several cores
    can read above 100. The first sample measures from the sampler's
    creation. Memory is the resident set size in bytes: current on Linux,
    from the process's own accounting, and on Windows, where it is the
    working set; the process peak elsewhere, which is what the platform
    reports without a third-party dependency.
    """

    def __init__(self) -> None:
        # The export thread and application threads pulling snapshots sample
        # concurrently; each sample must see the previous one's pair whole.
        self._lock = threading.Lock()
        self._cpu_time = time.process_time()
        self._wall = time.monotonic()

    def sample(self) -> Tuple[float, int]:
        """Return ``(cpu_percent, rss_bytes)`` for the interval since the last call."""
        with self._lock:
            cpu_time = time.process_time()
            wall = time.monotonic()
            elapsed = wall - self._wall
            cpu = 100.0 * (cpu_time - self._cpu_time) / elapsed if elapsed > 0 else 0.0
            self._cpu_time, self._wall = cpu_time, wall
        return max(cpu, 0.0), _resident_bytes()


if sys.platform == "win32":
    class _ProcessMemoryCounters(ctypes.Structure):
        """The Win32 ``PROCESS_MEMORY_COUNTERS`` structure."""

        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    _kernel32 = ctypes.WinDLL("kernel32")
    _kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    _get_process_memory_info = _kernel32.K32GetProcessMemoryInfo
    _get_process_memory_info.argtypes = (
        wintypes.HANDLE, ctypes.POINTER(_ProcessMemoryCounters), wintypes.DWORD)
    _get_process_memory_info.restype = wintypes.BOOL
    _CURRENT_PROCESS = _kernel32.GetCurrentProcess()

    def _resident_bytes() -> int:
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if not _get_process_memory_info(_CURRENT_PROCESS, ctypes.byref(counters), counters.cb):
            return 0
        return counters.WorkingSetSize
else:
    def _resident_bytes() -> int:
        try:
            with open("/proc/self/statm", encoding="ascii") as f:
                pages = int(f.read().split()[1])
            return pages * os.sysconf("SC_PAGE_SIZE")
        except (OSError, ValueError, IndexError):
            pass
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports kilobytes here; macOS and the BSDs report bytes.
        return int(peak) if sys.platform == "darwin" or "bsd" in sys.platform else int(peak) * 1024


# Departed nodes retained at once. Past this, a departure is counted rather
# than retained, so a churning cluster cannot grow the set without bound.
MAX_RETAINED_DEPARTED_NODES = 256


class _Departures(NamedTuple):
    hosts: Tuple[str, ...]
    names: Dict[str, str]
    sequence: int
    dropped: int


class DepartedNodes:
    """Nodes that left the cluster, kept until every exporter has received them.

    Departure is judged by joining a snapshot's hosts against the live node
    list, not by diffing consecutive snapshots: the underlying client keeps
    every host it ever measured, so a snapshot's own host list never shrinks.

    Each departure is numbered, and each exporter is owed every departure
    numbered above the last one it exported successfully, so an exporter that
    fails the export after a node leaves still receives that node later. A
    departure is forgotten once every exporter has received it, or as soon as
    the node rejoins. With no exporters, nothing drains the set. On-demand
    snapshots list every retained departure without draining any.

    The export thread and application threads pulling snapshots share one
    instance, so every method takes the lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Hosts known to be out of the cluster, retained or not, so a host
        # already handled is not taken for a new departure on every snapshot.
        self._absent: Set[str] = set()
        self._retained: Dict[str, int] = {}
        # Each host's node name as last seen live: a departed node is no
        # longer in the node list to ask.
        self._names: Dict[str, str] = {}
        self._sequence = 0
        # The last departure number each exporter exported, keyed by id().
        # Each entry holds the exporter itself, so its id cannot be reused by
        # a new exporter while the entry exists.
        self._delivered: Dict[int, Tuple[Any, int]] = {}
        self._dropped = 0

    def observe(self, snapshot_hosts: Iterable[str], live: Mapping[str, str]) -> _Departures:
        """Record departures and rejoins seen in one snapshot.

        Args:
            snapshot_hosts: Every host the snapshot carries metrics for.
            live: Node name by host, for the nodes in the cluster now.

        Returns:
            Every retained departure with its node name, the newest departure
            number, and the count of departures dropped past the cap.
        """
        with self._lock:
            self._names.update(live)
            for host in self._absent & live.keys():
                self._absent.discard(host)
                self._retained.pop(host, None)
            for host in snapshot_hosts:
                if host in live or host in self._absent:
                    continue
                self._absent.add(host)
                if len(self._retained) >= MAX_RETAINED_DEPARTED_NODES:
                    self._dropped += 1
                    continue
                self._sequence += 1
                self._retained[host] = self._sequence
            return _Departures(
                tuple(self._retained),
                {host: self._names.get(host, "") for host in self._retained},
                self._sequence,
                self._dropped,
            )

    def track(self, exporters: Sequence[Any]) -> None:
        """Keep delivery state for exactly these exporters.

        A new exporter is owed every retained departure. A removed exporter
        stops holding departures back, and its id may be reused.
        """
        with self._lock:
            self._delivered = {
                id(e): (e, self._delivered.get(id(e), (e, 0))[1]) for e in exporters
            }
            self._forget_delivered()

    def owed(self, exporter: Any, sequence: int) -> Tuple[str, ...]:
        """Departures numbered up to ``sequence`` that ``exporter`` has not exported."""
        with self._lock:
            delivered = self._delivered.get(id(exporter), (exporter, 0))[1]
            return tuple(h for h, n in self._retained.items() if delivered < n <= sequence)

    def delivered(self, exporter: Any, sequence: int) -> None:
        """Record that ``exporter`` exported every departure up to ``sequence``."""
        with self._lock:
            key = id(exporter)
            entry = self._delivered.get(key)
            if entry is not None and entry[1] < sequence:
                self._delivered[key] = (exporter, sequence)
                self._forget_delivered()

    def _forget_delivered(self) -> None:
        if not self._delivered:
            return
        floor = min(n for _, n in self._delivered.values())
        if floor:
            self._retained = {h: n for h, n in self._retained.items() if n > floor}


class MetricsSnapshot:
    """A point-in-time view of accumulated client metrics.

    Counter values are cumulative since metrics were enabled; connection
    gauges are point-in-time. The canonical detail is the per-node,
    per-command-type breakdown reachable through :attr:`nodes` and
    :attr:`cluster_aggregated`; :meth:`latency` adds the classic five-way
    grouped view on top.

    Example::

        cluster.enable_metrics()
        ...
        snapshot = cluster.metrics_snapshot()
        reads = snapshot.latency(LatencyType.READ)
        print(f"{reads.count} reads, avg {reads.average:.1f} {reads.latency_unit}")
        for host, node in snapshot.nodes.items():
            print(host, node.connections_attempts)
    """

    __slots__ = (
        "_pnc", "_raw", "_timestamp", "_policy", "_nodes", "_usage", "_command_count", "_departed",
        "_departed_names", "_departed_sequence", "_departed_dropped", "_app_id", "_process",
        "_metrics_enabled", "_usage_enabled",
    )

    def __init__(
        self,
        pnc_metrics: _PncClusterMetrics,
        *,
        policy: Optional[MetricsPolicy] = None,
        nodes: Optional[List[Any]] = None,
        usage: Optional[Dict[str, int]] = None,
        command_count: Optional[int] = None,
        app_id: Optional[str] = None,
        process: Optional[Tuple[float, int]] = None,
        metrics_enabled: bool = False,
        usage_enabled: bool = False,
    ) -> None:
        """Wrap a raw PNC snapshot.

        Args:
            pnc_metrics: The snapshot from the underlying client.
            policy: Policy in effect, supplying the histogram shape that the
                snapshot itself does not carry.
            nodes: Live cluster nodes, joined by host to name the nodes in
                :meth:`to_canonical_dict`.
            usage: Feature-usage counters recorded by this SDK.
            command_count: User API calls counted by this SDK, or ``None``
                when the caller has no count to report.
            app_id: Application identity to report when the underlying client
                carries none, normally the authenticated user. ``None`` falls
                back to ``"not-set"``.
            metrics_enabled: Whether collection was on when the snapshot was
                taken.
            usage_enabled: Whether the usage counters were being recorded.
        """
        self._pnc = pnc_metrics
        self._raw: Optional[Dict[str, Any]] = None
        # Taken once, so every exporter of the same window reports one time.
        self._timestamp = datetime.now(timezone.utc)
        self._policy = policy
        self._nodes = nodes
        self._usage = usage or {}
        self._command_count = command_count
        self._departed: Tuple[str, ...] = ()
        self._departed_names: Dict[str, str] = {}
        self._departed_sequence = 0
        self._departed_dropped = 0
        self._app_id = app_id
        self._process = process
        self._metrics_enabled = metrics_enabled
        self._usage_enabled = usage_enabled

    def _mark_departed(self, departures: DepartedNodes) -> None:
        """Record this snapshot's departures in the cluster's departure state.

        Lists every retained departure under ``nodes_departed``, which is
        what an on-demand pull reports; the export timer narrows that per
        exporter with :meth:`_with_departed`.
        """
        live = {
            f"{host[0]}:{host[1]}": getattr(node, "name", "")
            for node in (self._nodes or [])
            if (host := getattr(node, "host", None)) is not None
        }
        snapshot_hosts = [host for host, _ in _node_entries(self._raw_metrics())]
        (self._departed, self._departed_names, self._departed_sequence,
         self._departed_dropped) = departures.observe(snapshot_hosts, live)

    def _with_departed(self, hosts: Tuple[str, ...]) -> MetricsSnapshot:
        """This snapshot with ``nodes_departed`` narrowed to ``hosts``.

        Shares the serialized form, so every exporter's view renders from
        one serialization.
        """
        view = copy.copy(self)
        view._departed = hosts
        return view

    def _raw_metrics(self) -> Dict[str, Any]:
        """The underlying snapshot serialized, computed once.

        The snapshot is immutable, and serializing it copies every per-node
        histogram, so it is done at most once per snapshot. Callers must
        not mutate the result.
        """
        if self._raw is None:
            self._raw = self._pnc.to_dict()
        return self._raw

    @property
    def nodes(self) -> Dict[str, NodeMetricsSnapshot]:
        """Per-node snapshots keyed by host address."""
        return self._pnc.nodes

    @property
    def cluster_aggregated(self) -> NodeMetricsSnapshot:
        """All node snapshots aggregated into one view."""
        return self._pnc.cluster_aggregated

    @property
    def total_nodes(self) -> int:
        """Number of nodes in the snapshot."""
        return self._pnc.total_nodes

    @property
    def metrics_enabled(self) -> bool:
        """Whether collection was on when the snapshot was taken.

        A snapshot can be pulled while collection is off; its counters are
        then frozen at the values they had when collection was last on.
        """
        return self._metrics_enabled

    @property
    def operational_metrics_enabled(self) -> bool:
        """Whether the operational tier (latency, errors, bytes) was being recorded."""
        return (
            self._metrics_enabled
            and self._policy is not None
            and self._policy.operational_enabled
        )

    @property
    def usage_metrics_enabled(self) -> bool:
        """Whether the feature-usage counters were being recorded."""
        return self._metrics_enabled and self._usage_enabled

    @property
    def command_retries(self) -> int:
        """Wire retries across the cluster, cumulative since metrics enable.

        The per-node retry counters summed: every retry increments on the node
        it was sent to, and the aggregate retains departed hosts, so the total
        survives node churn. Retries happen inside the client core, so unlike
        :attr:`command_count` this covers all traffic on the client, however
        it was issued.

        Example::

            snapshot = cluster.metrics_snapshot()
            retries_per_call = snapshot.command_retries / max(snapshot.command_count, 1)
        """
        aggregated = self._pnc.cluster_aggregated
        if aggregated is None:
            return 0
        return aggregated.transaction_retry_count

    def latency(
        self, latency_type: LatencyType, node: Optional[str] = None
    ) -> DerivedHistogram:
        """The derived latency histogram for one legacy grouping.

        Args:
            latency_type: Which grouped view to compute (see
                :class:`LatencyType` for the category mapping).
            node: Host address of a single node, or ``None`` for the
                cluster-aggregated view.

        Returns:
            A :class:`DerivedHistogram` in the snapshot's latency unit.
            ``CONN`` sums the connection-acquisition phase across all
            namespaces and command types.

        Raises:
            KeyError: If ``node`` names a host not present in the snapshot.

        Example::

            writes = snapshot.latency(LatencyType.WRITE)
            slowest_bucket = writes.buckets[-1]
        """
        source = self.nodes[node] if node is not None else self.cluster_aggregated
        unit = source.latency_unit
        if latency_type is LatencyType.CONN:
            histograms = [
                metric.connection_aq
                for namespace in source.detailed_namespaces()
                for ct in _ALL_COMMAND_TYPES
                if (metric := source.detailed_metric(namespace, ct)) is not None
            ]
            return _merge_histograms(histograms, unit)
        return _merge_histograms(_command_histograms(source, latency_type), unit)

    def _cluster_latency(self) -> Dict[str, Dict[str, Any]]:
        """Whole-call latency per group, for the canonical ``cluster.latency``."""
        # One aggregate for all four groups: each read of it is a fresh copy.
        aggregated = self._pnc.cluster_aggregated
        return {
            group.value: _histogram_view(
                _sum_histograms(
                    _command_histograms(aggregated, group) if aggregated is not None else ()
                )
            )
            for group in _CLUSTER_LATENCY_GROUPS
        }

    @property
    def usage(self) -> Dict[str, int]:
        """Feature-usage counters, cumulative since the client started.

        Empty unless :attr:`MetricsPolicy.usage_enabled` was set. Counts calls
        made through this SDK, so traffic issued through the underlying client
        directly is not represented.

        Returns:
            Counter name to count, e.g. ``{"feature.shape.point": 42}``.

        Example::

            snapshot = cluster.metrics_snapshot()
            print(snapshot.usage.get("feature.filter.ael", 0))
        """
        return dict(self._usage)

    @property
    def command_count(self) -> int:
        """User API calls counted while operational metrics were on, cumulative.

        Counted by this SDK — one increment per data-path call (point, batch,
        query, UDF, background job) made through this API, whatever the
        sampler decides — so it is an exact total, and traffic issued through
        the underlying client directly is not represented. ``0`` when the
        operational tier was never enabled.

        Example::

            snapshot = cluster.metrics_snapshot()
            errors_per_call = errors / max(snapshot.command_count, 1)
        """
        return self._command_count or 0

    def to_canonical_dict(self) -> Dict[str, Any]:
        """The snapshot in the cross-SDK structured form exporters consume.

        A stable, ``snake_case`` document that a JSON, OTEL or Prometheus
        exporter can serialize directly, independent of how the underlying
        client happens to name its fields.

        ``timestamp`` is when the snapshot was taken, not when it was
        rendered, so every exporter of one window reports the same time.

        The cluster section carries the pool occupancy split, recover-queue
        depth and invalid-node count the underlying client reports, this
        process's CPU share and resident memory when the cluster sampled them,
        and each node its circuit-breaker ``error_rate`` and the
        ``pool_exhausted`` count among its connection counters. The cluster
        ``command_retries`` is the per-node retry counters summed;
        ``command_count`` is present when this SDK counted calls — see
        :attr:`command_count` for its scope.

        Each namespace's ``errors`` is the remainder: failures not already
        counted under ``timeouts``, ``key_busy``, ``record_too_big`` or
        ``device_overload``, so summing the five never counts a failure twice.
        ``timeouts`` counts server-reported timeouts; a command that expired on
        the client side has no result code and is counted once, cluster-wide,
        under ``command_timeout_client``, while ``command_timeout_server`` is
        the namespace ``timeouts`` summed.

        ``app_id`` is always populated, so a consumer can group by application
        without a missing-field case: the identity the application set on the
        cluster definition, else the authenticated user, else ``"not-set"``.

        Every latency series -- ``cluster.latency.{read,write,batch,query}``
        and each namespace's ``latency`` groups -- is an object of
        ``buckets``, ``sum``, ``count``, ``min`` and ``max``, in the
        snapshot's ``latency_unit``. ``cluster.latency`` is whole-call
        latency, first attempt through the last retry, recorded once per
        successful call; a namespace's ``latency`` times each network attempt.

        ``nodes`` lists the nodes still in the cluster. A node that left
        appears under ``nodes_departed`` -- same shape and name, final
        counters -- so
        an exporter can flush its series without a callback. Each exporter
        receives a departed node until it exports one snapshot carrying it
        successfully; a snapshot pulled through ``metrics_snapshot()`` lists
        every departure some exporter has yet to receive, without consuming
        any. A node that rejoins leaves ``nodes_departed``. At most 256
        departures are retained; ``nodes_departed_dropped`` counts those
        dropped past that cap since the cluster connected.

        Returns:
            The structured snapshot: ``timestamp``, ``client_type``,
            ``client_version``, ``cluster_name``, ``app_id``, ``labels``, the
            three tier flags, the histogram shape, ``cluster``, ``nodes`` as a
            list, ``nodes_departed`` and ``nodes_departed_dropped``.

        Example::

            snapshot = cluster.metrics_snapshot()
            payload = json.dumps(snapshot.to_canonical_dict())
        """
        raw = self._raw_metrics()
        aggregated = raw.get("cluster_aggregated_metrics") or {}
        labels = _split_labels(aggregated.get("labels") or [])

        # A snapshot taken before collection was ever enabled has no recorded
        # policy. Fall back to the defaults rather than dropping the keys: the
        # canonical document is a fixed schema, and an exporter should not have
        # to special-case a field's absence to read the histogram shape.
        policy = self._policy
        columns = policy.latency_columns if policy is not None else _DEFAULT_LATENCY_COLUMNS
        shift = policy.latency_shift if policy is not None else _DEFAULT_LATENCY_SHIFT

        nodes_by_host = {}
        for node in self._nodes or []:
            host = getattr(node, "host", None)
            if host is not None:
                nodes_by_host[f"{host[0]}:{host[1]}"] = node

        nodes: List[Dict[str, Any]] = []
        departed: List[Dict[str, Any]] = []
        departed_hosts = set(self._departed)
        for host, node_raw in _node_entries(raw):
            if host in departed_hosts:
                departed.append(self._canonical_node(
                    host, node_raw, self._departed_names.get(host, ""), columns,
                ))
                continue
            if self._nodes is not None and host not in nodes_by_host:
                # Retained by the underlying client but no longer in the
                # cluster, and not owed as a departure: its final counters
                # were already delivered. Listing it under ``nodes`` would
                # claim membership it no longer has.
                continue
            nodes.append(self._canonical_node(
                host, node_raw, getattr(nodes_by_host.get(host), "name", ""), columns,
            ))

        document: Dict[str, Any] = {
            "timestamp": self._timestamp.isoformat(),
            "client_type": CLIENT_TYPE,
            "client_version": _client_version(),
            "cluster_name": labels.reserved.get("cluster", ""),
            "app_id": labels.reserved.get("app_id") or self._app_id or _APP_ID_UNSET,
            "labels": labels.user,
            "metrics_enabled": self.metrics_enabled,
            "operational_metrics_enabled": self.operational_metrics_enabled,
            "usage_metrics_enabled": self.usage_metrics_enabled,
            "latency_unit": _canonical_unit(aggregated.get("latency_unit")),
            "cluster": {
                # Cluster rollups summed across nodes by the aggregate, which
                # retains departed hosts, so the totals survive node churn.
                # The gauges come from the snapshot's own cluster totals.
                "connections": {
                    "opened": aggregated.get("connections_successful", 0),
                    "closed": aggregated.get("closed_connections", 0),
                    "in_use": int(raw.get("connections_in_use", 0) or 0),
                    "in_pool": int(raw.get("connections_in_pool", 0) or 0),
                },
                "nodes": {
                    "active": self.total_nodes,
                    "invalid": int(raw.get("nodes_invalid", 0) or 0),
                },
                "recover_queue": {"size": int(raw.get("recover_queue_size", 0) or 0)},
                # Both cluster counters are client-side timeouts (the retry
                # budget and the deadline are the two ways a client gives up),
                # so their sum is the client-reported total; the server-
                # reported total is the TIMEOUT answers across namespaces.
                # Derived from the same counts the nodes carry, never counted
                # a second time here.
                "command_timeout_client": (
                    self._pnc.exceeded_max_retries + self._pnc.exceeded_total_timeout
                ),
                "command_timeout_server": sum(
                    ns["timeouts"] for node in nodes for ns in node["namespaces"]
                ),
                **({"cpu_percent": self._process[0], "memory_bytes": self._process[1]}
                   if self._process is not None else {}),
                # The per-node retry counters summed (the canonical schema's
                # name for the cluster retry total).
                "command_retries": aggregated.get("transaction_retry_count", 0),
                # Counted by this SDK, not the underlying client: one per user
                # API call made through this API while the operational tier is
                # on, never reduced by the sampler.
                **({"command_count": self._command_count}
                   if self._command_count is not None else {}),
                "latency": self._cluster_latency(),
            },
            "nodes": nodes,
            "nodes_departed": departed,
            "nodes_departed_dropped": self._departed_dropped,
        }
        if self._usage:
            document["usage"] = dict(self._usage)
        document["latency_columns"] = columns
        document["latency_shift"] = shift
        return document

    @staticmethod
    def _canonical_node(host: str, node_raw: Mapping, name: str, columns: int) -> Dict[str, Any]:
        """One canonical per-node object; ``name`` is empty when unknown."""
        address, _, port = host.rpartition(":")
        namespaces = sorted(
            set(node_raw.get("detailed_resultcode_counts") or {})
            | set(node_raw.get("detailed_metrics") or {})
        )
        return {
            "name": name,
            # Identity comes from the snapshot's own host key: the live node's
            # `address` carries its port too, and a departed node has no live
            # counterpart to ask, so the key is the one form both share.
            "address": address,
            "port": int(port) if port.isdigit() else None,
            # The circuit-breaker window count, zeroed by the client every
            # error-rate window.
            "error_rate": int(node_raw.get("error_rate", 0) or 0),
            # `open_failure` stays the undifferentiated rollup; the TLS and
            # auth counters beside it name two of its causes rather than
            # partitioning it, so they are reported alongside, not subtracted.
            "connections": {
                "opened": node_raw.get("connections_successful", 0),
                "closed": node_raw.get("closed_connections", 0),
                "in_use": node_raw.get("connections_in_use", 0),
                "in_pool": node_raw.get("connections_in_pool", 0),
                "open_failure": node_raw.get("connections_failed", 0),
                "tls_handshake_failure": node_raw.get("connections_error_tls", 0),
                "auth_failure": node_raw.get("connections_error_auth", 0),
                "closed_idle": node_raw.get("connections_idle_dropped", 0),
                "closed_error": node_raw.get("connections_closed_error", 0),
                "closed_node_removed": node_raw.get("connections_closed_node_removed", 0),
                # A poll against a pool at capacity with no idle socket.
                "pool_exhausted": node_raw.get("connections_pool_empty", 0),
            },
            "namespaces": [
                _namespace_view(
                    node_raw.get("detailed_resultcode_counts") or {},
                    node_raw.get("detailed_metrics") or {},
                    namespace,
                    columns,
                )
                for namespace in namespaces
            ],
        }

    def __repr__(self) -> str:
        return (
            f"MetricsSnapshot(total_nodes={self.total_nodes}, "
            f"metrics_enabled={self.metrics_enabled}, "
            f"command_count={self.command_count}, "
            f"command_retries={self.command_retries})"
        )


def _command_histograms(source: NodeMetricsSnapshot, group: LatencyType) -> Iterable[Any]:
    """The whole-call histogram of each command type in ``group``."""
    return (source.command_histogram(ct) for ct in _LATENCY_TYPE_COMMANDS[group])


# The groups whole-call latency is reported for: connection acquisition is a
# phase of an attempt, not a call.
_CLUSTER_LATENCY_GROUPS = (LatencyType.READ, LatencyType.WRITE, LatencyType.BATCH, LatencyType.QUERY)

# A namespace also carries connection acquisition, a per-attempt phase.
_NAMESPACE_LATENCY_GROUPS = (*_CLUSTER_LATENCY_GROUPS, LatencyType.CONN)


def _node_entries(raw: Mapping[str, Any]) -> Iterable[Tuple[str, Mapping]]:
    """The per-node entries of a serialized snapshot, keyed ``host:port``."""
    return (
        (host, value) for host, value in raw.items()
        if isinstance(value, Mapping) and ":" in host
    )


# Labels the underlying client stamps into every snapshot itself. They identify
# the node and cluster and are promoted to their own canonical fields, so they
# are kept out of the user's `labels` map rather than duplicated into it.
_RESERVED_LABELS = frozenset({"node", "host", "cluster", "app_id"})

# Reported for `app_id` when the application named none and the connection is
# unauthenticated. A placeholder rather than an empty string, so a consumer
# grouping by application never has to tell "" apart from a missing field.
_APP_ID_UNSET = "not-set"

# `client_type` on every snapshot and log line. Distinct from the standalone
# Python client's identifier, so the two are never merged downstream.
CLIENT_TYPE = "python-sdk"


class _SplitLabels(NamedTuple):
    reserved: Dict[str, str]
    user: Dict[str, str]


def _split_labels(label_maps: Any) -> _SplitLabels:
    """Separate the client-stamped identity labels from the user's own."""
    reserved: Dict[str, str] = {}
    user: Dict[str, str] = {}
    for entry in label_maps or []:
        if not isinstance(entry, Mapping):
            continue
        for key, value in entry.items():
            (reserved if key in _RESERVED_LABELS else user)[str(key)] = str(value)
    return _SplitLabels(reserved, user)


def _canonical_unit(raw: Any) -> str:
    """Spell the latency unit the way the structured schema does."""
    return "microseconds" if str(raw) == "us" else "milliseconds"


@lru_cache(maxsize=1)
def _client_version() -> str:
    """This SDK's version, or an empty string when it cannot be determined.

    Cached: it reads installed package metadata, and a snapshot may be built
    every export interval.
    """
    try:
        return version("aerospike-sdk")
    except Exception:
        return ""


# Result codes that the canonical schema counts separately from the general
# error total. Everything else non-OK rolls up into `errors`.
# The serialized result-code names, exactly as they appear in a snapshot.
# These are the underlying client's display strings, not its enum variant
# names -- "Hot key", not "KeyBusy" -- so they cannot be inferred from the
# Python ResultCode members.
_RESULT_OK = "ok"
_RESULT_TIMEOUT = "Timeout"
_RESULT_KEY_BUSY = "Hot key"
_RESULT_RECORD_TOO_BIG = "Record too big"
_RESULT_DEVICE_OVERLOAD = "Device overload"

# Canonical latency group for each command type, inverted from the five-way
# grouping so a per-namespace command histogram can be filed under conn / read /
# write / batch / query.
# Keyed by the command's serialized name ("Get", "BatchRead"), which is how it
# appears in the raw snapshot — not by the enum member.
_COMMAND_LATENCY_GROUP: Dict[str, str] = {
    str(command): group.value
    for group, commands in _LATENCY_TYPE_COMMANDS.items()
    for command in commands
}


def _histogram_sum(entry: Any) -> int:
    """Total of a raw histogram's ``sum`` field, 0 when absent."""
    if isinstance(entry, Mapping):
        try:
            return int(entry.get("sum") or 0)
        except (TypeError, ValueError):
            return 0
    return 0


def _namespace_view(
    result_codes: Mapping[str, Any], detailed: Mapping[str, Any], namespace: str, columns: int,
) -> Dict[str, Any]:
    """Build one canonical per-namespace object from the two raw trees.

    ``detailed_resultcode_counts`` carries the outcome counts and
    ``detailed_metrics`` the byte and latency histograms; both are keyed
    namespace -> command -> value. Every latency group is present, with
    ``columns`` zero buckets when the namespace recorded none.
    """
    # `errors` is the remainder after the named causes, so no failure is
    # counted in two places. The counts are server answers only: a
    # client-side deadline never reaches a result code here and is counted
    # under the cluster's `command_timeout_client` instead.
    errors = timeouts = key_busy = record_too_big = device_overload = 0
    for counts in (result_codes.get(namespace) or {}).values():
        if not isinstance(counts, Mapping):
            continue
        for code, count in counts.items():
            if code == _RESULT_OK:
                continue
            value = int(count or 0)
            if code == _RESULT_TIMEOUT:
                timeouts += value
            elif code == _RESULT_KEY_BUSY:
                key_busy += value
            elif code == _RESULT_RECORD_TOO_BIG:
                record_too_big += value
            elif code == _RESULT_DEVICE_OVERLOAD:
                device_overload += value
            else:
                errors += value

    bytes_in = bytes_out = 0
    by_group: Dict[str, List[_HistogramTotals]] = {}
    for command, entry in (detailed.get(namespace) or {}).items():
        if not isinstance(entry, Mapping):
            continue
        bytes_in += _histogram_sum(entry.get("bytes_received"))
        bytes_out += _histogram_sum(entry.get("bytes_sent"))
        group = _COMMAND_LATENCY_GROUP.get(command) or str(command).lower()
        histogram = entry.get("latency")
        if isinstance(histogram, Mapping):
            by_group.setdefault(group, []).append(_raw_histogram(histogram))
        acquisition = entry.get("connection_aq")
        if isinstance(acquisition, Mapping):
            by_group.setdefault(LatencyType.CONN.value, []).append(_raw_histogram(acquisition))
    empty = _HistogramTotals([0] * columns, 0, 0, 0, 0.0)
    latency = {}
    for group in _NAMESPACE_LATENCY_GROUPS:
        histograms = by_group.pop(group.value, None)
        latency[group.value] = _histogram_view(_sum_histograms(histograms) if histograms else empty)
    # A command outside the five groups keeps its own key rather than vanish.
    for group_name, histograms in by_group.items():
        latency[group_name] = _histogram_view(_sum_histograms(histograms))

    return {
        "name": namespace,
        "errors": errors,
        "timeouts": timeouts,
        "key_busy": key_busy,
        "record_too_big": record_too_big,
        "device_overload": device_overload,
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
        "latency": latency,
    }
