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
from typing import Any, Dict, List, NamedTuple, Optional

from aerospike_async import (
    ClusterMetrics as _PacClusterMetrics,
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
        snapshot = await cluster.metrics()
        reads = snapshot.latency(LatencyType.READ)
        print(f"{reads.count} reads, avg {reads.average:.1f} {reads.latency_unit}")
        for host, node in snapshot.nodes.items():
            print(host, node.connections_attempts, node.open_connections)
    """

    __slots__ = (
        "_pac", "_policy", "_nodes", "_usage", "_command_count", "_departed", "_app_id",
    )

    def __init__(
        self,
        pac_metrics: _PacClusterMetrics,
        *,
        policy: Optional[MetricsPolicy] = None,
        nodes: Optional[List[Any]] = None,
        usage: Optional[Dict[str, int]] = None,
        command_count: Optional[int] = None,
        app_id: Optional[str] = None,
    ) -> None:
        """Wrap a raw PAC snapshot.

        Args:
            pac_metrics: The snapshot from the underlying client.
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
        """
        self._pac = pac_metrics
        self._policy = policy
        self._nodes = nodes
        self._usage = usage or {}
        self._command_count = command_count
        self._departed: tuple = ()
        self._app_id = app_id

    def _mark_departed(self, tracker: Any) -> None:
        """Record which hosts left the cluster, for ``nodes_departed``.

        Called by the export timer, which owns the departure state: the
        underlying client retains every host it ever measured, so departure is
        judged by joining the snapshot's hosts against the live node list, and
        the tracker remembers what was already reported. A snapshot pulled
        outside the export cycle carries an empty ``nodes_departed``.
        """
        raw = self._pac.to_dict()
        snapshot_hosts = [
            host for host, value in raw.items()
            if isinstance(value, Mapping) and ":" in host
        ]
        live_hosts = [
            f"{host[0]}:{host[1]}"
            for node in (self._nodes or [])
            if (host := getattr(node, "host", None)) is not None
        ]
        self._departed = tuple(tracker.departed(snapshot_hosts, live_hosts))

    @property
    def nodes(self) -> Dict[str, NodeMetricsSnapshot]:
        """Per-node snapshots keyed by host address."""
        return self._pac.nodes

    @property
    def cluster_aggregated(self) -> NodeMetricsSnapshot:
        """All node snapshots aggregated into one view."""
        return self._pac.cluster_aggregated

    @property
    def total_nodes(self) -> int:
        """Number of nodes in the snapshot."""
        return self._pac.total_nodes

    @property
    def open_connections(self) -> int:
        """Open connections across the cluster (point-in-time gauge)."""
        return self._pac.open_connections

    @property
    def exceeded_max_retries(self) -> int:
        """Commands that failed after exhausting max retries (cumulative)."""
        return self._pac.exceeded_max_retries

    @property
    def exceeded_total_timeout(self) -> int:
        """Commands that failed on total timeout (cumulative)."""
        return self._pac.exceeded_total_timeout

    @property
    def command_retries(self) -> int:
        """Wire retries across the cluster, cumulative since metrics enable.

        The per-node retry counters summed: every retry increments on the node
        it was sent to, and the aggregate retains departed hosts, so the total
        survives node churn. Retries happen inside the client core, so unlike
        :attr:`command_count` this covers all traffic on the client, however
        it was issued.

        Example::

            snapshot = await cluster.metrics()
            retries_per_call = snapshot.command_retries / max(snapshot.command_count, 1)
        """
        aggregated = self._pac.cluster_aggregated
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

            snapshot = await cluster.metrics()
            print(snapshot.usage.get("feature.filter.ael", 0))
        """
        return dict(self._usage)

    @property
    def command_count(self) -> int:
        """User API calls counted while metrics were enabled, cumulative.

        Counted by this SDK — one increment per data-path call (point, batch,
        query, UDF) made through this API, whatever the sampler decides —
        so it is an exact total, and traffic issued through the underlying
        client directly is not represented. ``0`` when metrics were never
        enabled.

        Example::

            snapshot = await cluster.metrics()
            errors_per_call = errors / max(snapshot.command_count, 1)
        """
        return self._command_count or 0

    def to_dict(self) -> Dict[str, Any]:
        """The raw snapshot as the underlying client serializes it.

        Node snapshots appear under their host address; the aggregate under
        ``"cluster_aggregated_metrics"``. Field names are the underlying
        client's own (hyphenated), which is why this is not the shape
        exporters consume — see :meth:`to_canonical_dict`.
        """
        return self._pac.to_dict()

    def to_canonical_dict(self) -> Dict[str, Any]:
        """The snapshot in the cross-SDK structured form exporters consume.

        A stable, ``snake_case`` document that a JSON, OTEL or Prometheus
        exporter can serialize directly, independent of how the underlying
        client happens to name its fields.

        The cluster section carries the pool occupancy split, recover-queue
        depth and invalid-node count the underlying client reports, and each
        node its circuit-breaker ``error_rate``. The cluster
        ``command_retries`` is the per-node retry counters summed;
        ``command_count`` is present when this SDK counted calls — see
        :attr:`command_count` for its scope.

        ``app_id`` is always populated, so a consumer can group by application
        without a missing-field case: the identity the application set on the
        cluster definition, else the authenticated user, else ``"not-set"``.

        ``nodes`` lists the nodes still in the cluster. A node that left since
        the previous export appears once under ``nodes_departed`` -- same
        shape, final counters -- so an exporter can flush its series without a
        callback. Departure is tracked by the export timer; a snapshot pulled
        directly through ``metrics()`` always carries an empty
        ``nodes_departed``.

        Returns:
            The structured snapshot: ``timestamp``, ``client_type``,
            ``client_version``, ``cluster_name``, ``app_id``, ``labels``,
            the histogram shape, ``cluster``, ``nodes`` as a list, and
            ``nodes_departed``.

        Example::

            snapshot = await cluster.metrics()
            payload = json.dumps(snapshot.to_canonical_dict())

        See Also:
            :meth:`to_dict`: The underlying client's own serialization.
        """
        raw = self._pac.to_dict()
        aggregated = raw.get("cluster_aggregated_metrics") or {}
        labels = _split_labels(aggregated.get("labels") or [])

        nodes_by_host = {}
        for node in self._nodes or []:
            host = getattr(node, "host", None)
            if host is not None:
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
        document: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "client_type": "python",
            "client_version": _client_version(),
            "cluster_name": labels.reserved.get("cluster", ""),
            "app_id": labels.reserved.get("app_id") or self._app_id or _APP_ID_UNSET,
            "labels": labels.user,
            "latency_unit": _canonical_unit(aggregated.get("latency_unit")),
            "cluster": {
                # Cluster rollups summed across nodes by the aggregate, which
                # retains departed hosts, so the totals survive node churn.
                # The gauges come from the snapshot's own cluster totals.
                "connections": {
                    "opened": aggregated.get("connections_successful", 0),
                    "closed": aggregated.get("closed_connections", 0),
                    "open": self.open_connections,
                    "in_use": int(raw.get("connections_in_use", 0) or 0),
                    "in_pool": int(raw.get("connections_in_pool", 0) or 0),
                },
                "nodes": {
                    "active": self.total_nodes,
                    "invalid": int(raw.get("nodes_invalid", 0) or 0),
                },
                "recover_queue": {"size": int(raw.get("recover_queue_size", 0) or 0)},
                "exceeded_max_retries": self.exceeded_max_retries,
                "exceeded_total_timeout": self.exceeded_total_timeout,
                # The per-node retry counters summed (the canonical schema's
                # name for the cluster retry total).
                "command_retries": aggregated.get("transaction_retry_count", 0),
                # Counted by this SDK, not the underlying client: one per user
                # API call made through this API, whenever metrics are on, and
                # never reduced by the sampler.
                **({"command_count": self._command_count}
                   if self._command_count is not None else {}),
            },
            "nodes": nodes,
            "nodes_departed": departed,
        }
        if self._usage:
            document["usage"] = dict(self._usage)
        document["latency_columns"] = columns
        document["latency_shift"] = shift
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
        return {
            "name": getattr(node, "name", "") if node is not None else "",
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
                "open": node_raw.get("open_connections", 0),
                "in_use": node_raw.get("connections_in_use", 0),
                "in_pool": node_raw.get("connections_in_pool", 0),
                "recovering": node_raw.get("connections_recovering", 0),
                "open_failure": node_raw.get("connections_failed", 0),
                "tls_handshake_failure": node_raw.get("connections_error_tls", 0),
                "auth_failure": node_raw.get("connections_error_auth", 0),
                "closed_idle": node_raw.get("connections_idle_dropped", 0),
                "closed_error": node_raw.get("connections_closed_error", 0),
                "closed_node_removed": node_raw.get("connections_closed_node_removed", 0),
            },
            "namespaces": [
                _namespace_view(
                    node_raw.get("detailed_resultcode_counts") or {},
                    node_raw.get("detailed_metrics") or {},
                    namespace,
                )
                for namespace in namespaces
            ],
        }

    def __repr__(self) -> str:
        return (
            f"MetricsSnapshot(total_nodes={self.total_nodes}, "
            f"open_connections={self.open_connections}, "
            f"exceeded_max_retries={self.exceeded_max_retries}, "
            f"exceeded_total_timeout={self.exceeded_total_timeout})"
        )


# Labels the underlying client stamps into every snapshot itself. They identify
# the node and cluster and are promoted to their own canonical fields, so they
# are kept out of the user's `labels` map rather than duplicated into it.
_RESERVED_LABELS = frozenset({"node", "host", "cluster", "app_id"})

# Reported for `app_id` when the application named none and the connection is
# unauthenticated. A placeholder rather than an empty string, so a consumer
# grouping by application never has to tell "" apart from a missing field.
_APP_ID_UNSET = "not-set"


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
    result_codes: Mapping[str, Any], detailed: Mapping[str, Any], namespace: str
) -> Dict[str, Any]:
    """Build one canonical per-namespace object from the two raw trees.

    ``detailed_resultcode_counts`` carries the outcome counts and
    ``detailed_metrics`` the byte and latency histograms; both are keyed
    namespace -> command -> value.
    """
    # `errors` is every non-OK outcome, the named causes included; those are
    # also reported separately, so they are counted twice by design. The
    # counts are server answers only: a client-side deadline never reaches a
    # result code here and is counted as the cluster's
    # `exceeded_total_timeout` instead.
    errors = timeouts = key_busy = record_too_big = device_overload = 0
    for counts in (result_codes.get(namespace) or {}).values():
        if not isinstance(counts, Mapping):
            continue
        for code, count in counts.items():
            if code == _RESULT_OK:
                continue
            value = int(count or 0)
            errors += value
            if code == _RESULT_TIMEOUT:
                timeouts += value
            elif code == _RESULT_KEY_BUSY:
                key_busy += value
            elif code == _RESULT_RECORD_TOO_BIG:
                record_too_big += value
            elif code == _RESULT_DEVICE_OVERLOAD:
                device_overload += value

    bytes_in = bytes_out = 0
    latency: Dict[str, List[int]] = {}
    for command, entry in (detailed.get(namespace) or {}).items():
        if not isinstance(entry, Mapping):
            continue
        bytes_in += _histogram_sum(entry.get("bytes_received"))
        bytes_out += _histogram_sum(entry.get("bytes_sent"))
        group = _COMMAND_LATENCY_GROUP.get(command) or str(command).lower()
        histogram = entry.get("latency")
        if isinstance(histogram, Mapping):
            buckets = list(histogram.get("buckets") or [])
            existing = latency.get(group)
            latency[group] = (
                buckets if existing is None
                else [a + b for a, b in zip(existing, buckets)]
            )
        acquisition = entry.get("connection_aq")
        if isinstance(acquisition, Mapping):
            buckets = list(acquisition.get("buckets") or [])
            existing = latency.get(LatencyType.CONN.value)
            latency[LatencyType.CONN.value] = (
                buckets if existing is None
                else [a + b for a, b in zip(existing, buckets)]
            )

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
