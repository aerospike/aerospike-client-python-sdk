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

from dataclasses import dataclass
from collections.abc import Mapping
from functools import lru_cache
from importlib.metadata import version
from datetime import datetime, timezone
import os
import resource
import sys
import time
from types import MappingProxyType
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

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
class _SnapshotPolicy:
    operational_enabled: bool
    latency_columns: int
    latency_shift: int

    @classmethod
    def capture(cls, policy: Any) -> Optional["_SnapshotPolicy"]:
        if policy is None:
            return None
        return cls(
            operational_enabled=bool(policy.operational_enabled),
            latency_columns=int(policy.latency_columns),
            latency_shift=int(policy.latency_shift),
        )


@dataclass(frozen=True)
class _NodeIdentity:
    name: str
    host: tuple


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


def _merge_histograms(histograms, latency_unit: LatencyUnit) -> DerivedHistogram:
    """Sum same-shaped histograms into one derived view.

    Histograms whose bucket count differs from the first are skipped, the
    same way the core's own aggregation skips mismatched shapes.
    """
    buckets: List[int] = []
    count = 0
    minimum = 0
    maximum = 0
    total = 0.0
    for hist in histograms:
        if hist is None or hist.count == 0:
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
    return DerivedHistogram(
        buckets=tuple(buckets),
        count=count,
        min=minimum,
        max=maximum,
        sum=total,
        latency_unit=latency_unit,
    )


class ProcessSampler:
    """This process's CPU share and resident memory, read at snapshot time.

    CPU is the process's CPU time consumed since the previous sample as a
    percentage of the wall time elapsed, so a busy process on several cores
    can read above 100. The first sample measures from the sampler's
    creation. Memory is the resident set size in bytes: current on Linux,
    from the process's own accounting; the process peak elsewhere, which is
    what the platform reports without a third-party dependency.
    """

    def __init__(self) -> None:
        self._cpu_time = time.process_time()
        self._wall = time.monotonic()

    def sample(self) -> Tuple[float, int]:
        """Return ``(cpu_percent, rss_bytes)`` for the interval since the last call."""
        cpu_time = time.process_time()
        wall = time.monotonic()
        elapsed = wall - self._wall
        cpu = 100.0 * (cpu_time - self._cpu_time) / elapsed if elapsed > 0 else 0.0
        self._cpu_time, self._wall = cpu_time, wall
        return max(cpu, 0.0), _resident_bytes()


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
        "_pnc", "_policy", "_nodes", "_usage", "_command_count", "_departed", "_app_id", "_process",
        "_metrics_enabled", "_usage_enabled", "_timestamp",
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
        identities = None if nodes is None else tuple(
            _NodeIdentity(
                name=str(getattr(node, "name", "")),
                host=tuple(getattr(node, "host")),
            )
            for node in nodes
            if getattr(node, "host", None) is not None
        )
        object.__setattr__(self, "_pnc", pnc_metrics)
        object.__setattr__(self, "_policy", _SnapshotPolicy.capture(policy))
        object.__setattr__(self, "_nodes", identities)
        object.__setattr__(self, "_usage", MappingProxyType(dict(usage or {})))
        object.__setattr__(self, "_command_count", command_count)
        object.__setattr__(self, "_departed", ())
        object.__setattr__(self, "_app_id", app_id)
        object.__setattr__(self, "_process", None if process is None else tuple(process))
        object.__setattr__(self, "_metrics_enabled", bool(metrics_enabled))
        object.__setattr__(self, "_usage_enabled", bool(usage_enabled))
        object.__setattr__(self, "_timestamp", datetime.now(timezone.utc))

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def _with_departed(self, tracker: Any) -> "MetricsSnapshot":
        """Return a snapshot carrying all pending ``nodes_departed``.

        The cluster-owned tracker retains departures until a successful
        export drains them. This snapshot is not mutated.
        """
        raw = self._pnc.to_dict()
        snapshot_hosts = [
            host for host, value in raw.items()
            if isinstance(value, Mapping) and ":" in host
        ]
        live_hosts = [
            f"{host[0]}:{host[1]}"
            for node in (self._nodes or ())
            if (host := node.host)
        ]
        departed = tuple(tracker.peek(snapshot_hosts, live_hosts))
        clone = object.__new__(type(self))
        for slot in self.__slots__:
            object.__setattr__(
                clone, slot, departed if slot == "_departed" else getattr(self, slot)
            )
        return clone

    @property
    def _departed_hosts(self) -> Tuple[str, ...]:
        return tuple(self._departed)

    @property
    def nodes(self) -> Dict[str, NodeMetricsSnapshot]:
        """Per-node snapshots keyed by host address."""
        return dict(self._pnc.nodes)

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
        commands = _LATENCY_TYPE_COMMANDS[latency_type]
        return _merge_histograms(
            (source.command_histogram(ct) for ct in commands), unit
        )

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

        Cluster totals are top-level. Whole-call read/write/batch/query
        histograms are in ``cluster_latencies``. Each node has a four-field
        connection-pool block, connection-acquisition latency, existing-node
        open/TLS/auth failures, and namespace RPC metrics.

        Each namespace's ``errors`` is the remainder: failures not already
        counted under ``server_timeouts``, ``key_busy``, ``record_too_big`` or
        ``device_overload``, so summing the five never counts a failure twice.
        Shared fields that the native client cannot populate yet remain in the
        fixed document with value zero; their TODOs mark the native boundary.

        ``app_id`` is always populated, so a consumer can group by application
        without a missing-field case: the identity the application set on the
        cluster definition, else the authenticated user, else ``"not-set"``.

        ``nodes`` lists current nodes. Pending final samples stay in
        ``nodes_departed`` until a successful export drains them; on-demand
        snapshots peek without consuming them.

        Returns:
            The structured snapshot: ``timestamp``, ``client_type``,
            ``client_version``, ``cluster_name``, ``app_id``, ``labels``, the
            three tier flags, histogram shape, top-level totals, latency
            blocks, usage, ``nodes``, and ``nodes_departed``.

        Example::

            snapshot = cluster.metrics_snapshot()
            payload = json.dumps(snapshot.to_canonical_dict())
        """
        raw = self._pnc.to_dict()
        aggregated = raw.get("cluster_aggregated_metrics") or {}
        labels = _split_labels(aggregated.get("labels") or [])

        nodes_by_host = {}
        for node in self._nodes or ():
            host = node.host
            nodes_by_host[f"{host[0]}:{host[1]}"] = node

        nodes: List[Dict[str, Any]] = []
        departed: List[Dict[str, Any]] = []
        departed_hosts = set(self._departed)
        for host, node_raw in raw.items():
            if not isinstance(node_raw, Mapping) or ":" not in host:
                continue
            if host in departed_hosts:
                departed.append(self._canonical_node(host, node_raw, None))
                continue
            if self._nodes is not None and host not in nodes_by_host:
                # Retained by the underlying client but no longer in the
                # cluster, and not newly departed this cycle: it was already
                # reported (or the snapshot was pulled outside the export
                # cycle). Listing it under ``nodes`` would claim membership
                # it no longer has.
                continue
            nodes.append(self._canonical_node(host, node_raw, nodes_by_host.get(host)))

        # A snapshot taken before collection was ever enabled has no recorded
        # policy. Fall back to the defaults rather than dropping the keys: the
        # canonical document is a fixed schema, and an exporter should not have
        # to special-case a field's absence to read the histogram shape.
        policy = self._policy
        columns = policy.latency_columns if policy is not None else _DEFAULT_LATENCY_COLUMNS
        shift = policy.latency_shift if policy is not None else _DEFAULT_LATENCY_SHIFT
        cluster_latencies = (
            [
                _latency_view(latency_type.value, self.latency(latency_type))
                for latency_type in (
                    LatencyType.READ,
                    LatencyType.WRITE,
                    LatencyType.BATCH,
                    LatencyType.QUERY,
                )
            ]
            if self.cluster_aggregated is not None
            else []
        )
        open_connections = raw.get("open_connections")
        if open_connections is None:
            open_connections = sum(
                int(value.get("open_connections", 0) or 0)
                for key, value in raw.items()
                if isinstance(value, Mapping) and ":" in key
            )
        cpu_percent, memory_bytes = self._process or (0.0, 0)
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
            "latency_columns": columns,
            "latency_shift": shift,
            "total_nodes": self.total_nodes,
            "recover_queue_size": int(raw.get("recover_queue_size", 0) or 0),
            "invalid_node_count": int(raw.get("nodes_invalid", 0) or 0),
            # TODO(native): the client snapshot does not report delay-queue
            # timeouts yet. The field stays 0 until that count exists.
            "delay_queue_timeout_count": int(raw.get("delay_queue_timeout_count", 0) or 0),
            # TODO(native): node validation failures are not separated from
            # later connection errors. These stay 0 until native reports them.
            "validation_open_failure": int(raw.get("validation_open_failure", 0) or 0),
            "validation_tls_handshake_failure": int(
                raw.get("validation_tls_handshake_failure", 0) or 0
            ),
            "validation_auth_failure": int(raw.get("validation_auth_failure", 0) or 0),
            "open_connections": int(open_connections or 0),
            "command_count": self.command_count,
            "command_retries": int(aggregated.get("transaction_retry_count", 0) or 0),
            # TODO(native): no authoritative client-wide partial-batch count
            # is exposed yet. Keep the shared field stable until it is.
            "batch_partial_failure": int(raw.get("batch_partial_failure", 0) or 0),
            "cpu_percent": cpu_percent,
            "memory_bytes": memory_bytes,
            "cluster_latencies": cluster_latencies,
            "nodes": nodes,
            "nodes_departed": departed,
            "usage": _usage_view(self._usage),
        }
        return document

    @staticmethod
    def _canonical_node(
        host: str, node_raw: Mapping, node: Optional[Any]
    ) -> Dict[str, Any]:
        """One canonical per-node object, joined with its live node when known."""
        address, _, port = host.rpartition(":")
        namespaces = sorted(
            set(node_raw.get("detailed_resultcode_counts") or {})
            | set(node_raw.get("detailed_metrics") or {})
        )
        labels = _split_labels(node_raw.get("labels") or [])
        detailed = node_raw.get("detailed_metrics") or {}
        return {
            "node_name": node.name if node is not None else labels.reserved.get("node", ""),
            # Identity comes from the snapshot's own host key: the live node's
            # `address` carries its port too, and a departed node has no live
            # counterpart to ask, so the key is the one form both share.
            "node_address": address,
            "node_port": int(port) if port.isdigit() else None,
            "connections": {
                "opened": node_raw.get("connections_successful", 0),
                "closed": node_raw.get("closed_connections", 0),
                "in_use": node_raw.get("connections_in_use", 0),
                "in_pool": node_raw.get("connections_in_pool", 0),
            },
            "conn_latency": _latency_view(
                LatencyType.CONN.value,
                _merge_raw_histograms(
                    entry.get("connection_aq")
                    for commands in detailed.values()
                    if isinstance(commands, Mapping)
                    for entry in commands.values()
                    if isinstance(entry, Mapping)
                ),
            ),
            "connection_open_failure": node_raw.get("connections_failed", 0),
            "connection_tls_handshake_failure": node_raw.get("connections_error_tls", 0),
            "connection_auth_failure": node_raw.get("connections_error_auth", 0),
            "namespaces": [
                _namespace_view(
                    node_raw.get("detailed_resultcode_counts") or {},
                    detailed,
                    namespace,
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


def _merge_raw_histograms(histograms: Any) -> Dict[str, Any]:
    """Merge serialized histograms, rejecting incompatible bucket shapes."""
    buckets: List[int] = []
    count = 0
    minimum = 0
    maximum = 0
    total = 0.0
    for histogram in histograms:
        if not isinstance(histogram, Mapping):
            continue
        candidate = list(histogram.get("buckets") or [])
        if buckets and len(candidate) != len(buckets):
            continue
        histogram_count = int(histogram.get("count", 0) or 0)
        if not buckets:
            buckets = candidate
        else:
            for index, value in enumerate(candidate):
                buckets[index] += int(value or 0)
        histogram_min = int(histogram.get("min", 0) or 0)
        minimum = histogram_min if count == 0 else min(minimum, histogram_min)
        maximum = max(maximum, int(histogram.get("max", 0) or 0))
        count += histogram_count
        total += float(histogram.get("sum", 0) or 0)
    return {
        "buckets": buckets,
        "count": count,
        "min": minimum,
        "max": maximum,
        "sum": total,
    }


def _latency_view(latency_type: str, histogram: Any) -> Dict[str, Any]:
    """Convert either a native/derived or serialized histogram to the contract."""
    if isinstance(histogram, Mapping):
        get = histogram.get
    else:
        def get(name: str, default: Any = 0) -> Any:
            return getattr(histogram, name, default)
    return {
        "type": latency_type,
        "bucket_counts": list(get("buckets", ()) or ()),
        "min": int(get("min", 0) or 0),
        "max": int(get("max", 0) or 0),
        "sum": float(get("sum", 0) or 0),
        "count": int(get("count", 0) or 0),
    }


_USAGE_FIELDS = {
    "blocking": "feature.api.blocking",
    "deferred": "feature.api.deferred",
    "background": "feature.api.background",
    "shape_point": "feature.shape.point",
    "shape_batch": "feature.shape.batch",
    "shape_query": "feature.shape.query",
    "object_mapping_read": "feature.object_mapping.read",
    "object_mapping_write": "feature.object_mapping.write",
    "filter_ael": "feature.filter.ael",
    "filter_exp": "feature.filter.exp",
}


def _usage_view(usage: Mapping[str, int]) -> Dict[str, int]:
    """Expose only the identifiers approved by the revised usage contract."""
    return {field: int(usage.get(counter, 0) or 0) for field, counter in _USAGE_FIELDS.items()}


def _namespace_view(
    result_codes: Mapping[str, Any], detailed: Mapping[str, Any], namespace: str
) -> Dict[str, Any]:
    """Build one canonical per-namespace object from the two raw trees.

    ``detailed_resultcode_counts`` carries the outcome counts and
    ``detailed_metrics`` the byte and latency histograms; both are keyed
    namespace -> command -> value.
    """
    # `errors` is the remainder after the named causes, so no failure is
    # counted in two places. The counts are server answers only: the native
    # layer cannot yet attribute a client-side deadline to a namespace.
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
    latency: Dict[str, List[Mapping[str, Any]]] = {}
    for command, entry in (detailed.get(namespace) or {}).items():
        if not isinstance(entry, Mapping):
            continue
        bytes_in += _histogram_sum(entry.get("bytes_received"))
        bytes_out += _histogram_sum(entry.get("bytes_sent"))
        group = _COMMAND_LATENCY_GROUP.get(command)
        histogram = entry.get("latency")
        if (
            group in {
                LatencyType.READ.value,
                LatencyType.WRITE.value,
                LatencyType.BATCH.value,
                LatencyType.QUERY.value,
            }
            and isinstance(histogram, Mapping)
        ):
            latency.setdefault(group, []).append(histogram)

    return {
        "namespace": namespace,
        "errors": errors,
        "server_timeouts": timeouts,
        "key_busy": key_busy,
        "record_too_big": record_too_big,
        "device_overload": device_overload,
        # TODO(native): a client-side deadline cannot yet be attributed to a
        # namespace, so this stays 0 instead of a fabricated split of errors.
        "client_timeouts": 0,
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
        "latencies": [
            _latency_view(group, _merge_raw_histograms(latency.get(group, ())))
            for group in (
                LatencyType.READ.value,
                LatencyType.WRITE.value,
                LatencyType.BATCH.value,
                LatencyType.QUERY.value,
            )
        ],
    }
