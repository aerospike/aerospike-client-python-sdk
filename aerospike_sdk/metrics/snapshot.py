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

    __slots__ = ("_pac", "_policy", "_nodes", "_usage")

    def __init__(
        self,
        pac_metrics: _PacClusterMetrics,
        *,
        policy: Optional[MetricsPolicy] = None,
        nodes: Optional[List[Any]] = None,
        usage: Optional[Dict[str, int]] = None,
    ) -> None:
        """Wrap a raw PAC snapshot.

        Args:
            pac_metrics: The snapshot from the underlying client.
            policy: Policy in effect, supplying the histogram shape that the
                snapshot itself does not carry.
            nodes: Live cluster nodes, joined by host to name the nodes in
                :meth:`to_canonical_dict`.
            usage: Feature-usage counters recorded by this SDK.
        """
        self._pac = pac_metrics
        self._policy = policy
        self._nodes = nodes
        self._usage = usage or {}

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

    def to_dict(self) -> Dict[str, Any]:
        """The raw snapshot as the underlying client serializes it.

        Node snapshots appear under their host address; the aggregate under
        ``"cluster-aggregated-metrics"``. Field names are the underlying
        client's own (hyphenated), which is why this is not the shape
        exporters consume — see :meth:`to_canonical_dict`.
        """
        return self._pac.to_dict()

    def to_canonical_dict(self) -> Dict[str, Any]:
        """The snapshot in the cross-SDK structured form exporters consume.

        A stable, ``snake_case`` document that a JSON, OTEL or Prometheus
        exporter can serialize directly, independent of how the underlying
        client happens to name its fields.

        Fields the underlying client does not measure are **omitted rather
        than zeroed**, so a consumer can tell "not collected" from "zero":
        per-node ``in_use`` / ``in_pool`` connection splits, recover-queue
        depth, and cluster-level command and retry counts.

        Returns:
            The structured snapshot: ``timestamp``, ``client_type``,
            ``client_version``, ``cluster_name``, ``app_id``, ``labels``,
            the histogram shape, ``cluster``, and ``nodes`` as a list.

        Example::

            snapshot = await cluster.metrics()
            payload = json.dumps(snapshot.to_canonical_dict())

        See Also:
            :meth:`to_dict`: The underlying client's own serialization.
        """
        raw = self._pac.to_dict()
        aggregated = raw.get("cluster-aggregated-metrics") or {}
        labels = _split_labels(aggregated.get("labels") or [])

        nodes_by_host = {}
        for node in self._nodes or []:
            host = getattr(node, "host", None)
            if host is not None:
                nodes_by_host[f"{host[0]}:{host[1]}"] = node

        nodes: List[Dict[str, Any]] = []
        for host, node_raw in raw.items():
            if not isinstance(node_raw, Mapping) or ":" not in host:
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
            "app_id": labels.reserved.get("app-id", ""),
            "labels": labels.user,
            "latency_unit": _canonical_unit(aggregated.get("latency-unit")),
            "cluster": {
                "total_nodes": self.total_nodes,
                "open_connections": self.open_connections,
                "exceeded_max_retries": self.exceeded_max_retries,
                "exceeded_total_timeout": self.exceeded_total_timeout,
            },
            "nodes": nodes,
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
            set(node_raw.get("detailed-resultcode-counts") or {})
            | set(node_raw.get("detailed-metrics") or {})
        )
        return {
            "name": getattr(node, "name", "") if node is not None else "",
            "address": getattr(node, "address", address) if node is not None else address,
            "port": int(port) if port.isdigit() else None,
            # `in_use` / `in_pool` are absent: the underlying client keeps a
            # single open-connection gauge rather than the split.
            "connections": {
                "opened": node_raw.get("connections-successful", 0),
                "closed": node_raw.get("closed-connections", 0),
                "open": node_raw.get("open-connections", 0),
            },
            "namespaces": [
                _namespace_view(
                    node_raw.get("detailed-resultcode-counts") or {},
                    node_raw.get("detailed-metrics") or {},
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
_RESERVED_LABELS = frozenset({"node", "host", "cluster", "app-id"})


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

    ``detailed-resultcode-counts`` carries the outcome counts and
    ``detailed-metrics`` the byte and latency histograms; both are keyed
    namespace -> command -> value.
    """
    # `errors` is every non-OK outcome, timeouts and hot keys included; those
    # two are also reported separately, so they are counted twice by design.
    errors = timeouts = key_busy = 0
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

    bytes_in = bytes_out = 0
    latency: Dict[str, List[int]] = {}
    for command, entry in (detailed.get(namespace) or {}).items():
        if not isinstance(entry, Mapping):
            continue
        bytes_in += _histogram_sum(entry.get("bytes-received"))
        bytes_out += _histogram_sum(entry.get("bytes-sent"))
        group = _COMMAND_LATENCY_GROUP.get(command) or str(command).lower()
        histogram = entry.get("latency")
        if isinstance(histogram, Mapping):
            buckets = list(histogram.get("buckets") or [])
            existing = latency.get(group)
            latency[group] = (
                buckets if existing is None
                else [a + b for a, b in zip(existing, buckets)]
            )
        acquisition = entry.get("connection-aq")
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
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
        "latency": latency,
    }
