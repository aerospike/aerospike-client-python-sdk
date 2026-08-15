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

"""Client metrics: policy, snapshot, and the derived latency views.

Collection happens in the client core; this module shapes the configuration
going in (:class:`MetricsPolicy`) and the snapshot coming out
(:class:`MetricsSnapshot`). Enable and poll through
:class:`~aerospike_sdk.aio.cluster.Cluster` (or the sync twin).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from aerospike_async import (
    ClusterMetrics as _PacClusterMetrics,
    CommandType,
    LatencyUnit,
    MetricsPolicy as _PacMetricsPolicy,
    NodeMetricsSnapshot,
    Sampler,
)

__all__ = [
    "CommandType",
    "DerivedHistogram",
    "LatencyType",
    "LatencyUnit",
    "MetricsPolicy",
    "MetricsSnapshot",
    "Sampler",
]


class LatencyType(Enum):
    """Legacy five-way latency grouping, derived from the command categories.

    The canonical detail is the per-command-type breakdown
    (:class:`~aerospike_async.CommandType`); these groups exist for
    compatibility with the classic latency views (``conn``/``write``/``read``/
    ``batch``/``query``) and are computed from the canonical histograms:

    - ``READ`` = get + get-header + exists
    - ``WRITE`` = put + delete + operate + udf
    - ``BATCH`` = batch-read + batch-write
    - ``QUERY`` = query + scan
    - ``CONN`` = connection acquisition (pool checkout; includes creation on a
      pool miss)
    """

    CONN = "conn"
    WRITE = "write"
    READ = "read"
    BATCH = "batch"
    QUERY = "query"


_LATENCY_TYPE_COMMANDS: Dict[LatencyType, tuple] = {
    LatencyType.READ: (CommandType.GET, CommandType.GET_HEADER, CommandType.EXISTS),
    LatencyType.WRITE: (
        CommandType.PUT,
        CommandType.DELETE,
        CommandType.OPERATE,
        CommandType.UDF,
    ),
    LatencyType.BATCH: (CommandType.BATCH_READ, CommandType.BATCH_WRITE),
    LatencyType.QUERY: (CommandType.QUERY, CommandType.SCAN),
}

# Every category with its own histogram (NONE has none).
_ALL_COMMAND_TYPES: tuple = tuple(
    ct for group in _LATENCY_TYPE_COMMANDS.values() for ct in group
)


class MetricsPolicy:
    """Configuration for client metrics collection.

    The defaults are the cross-SDK metrics defaults: latency recorded in
    milliseconds across 7 logarithmic buckets whose boundaries double per
    column (``<= 1``, ``> 1``, ``> 2``, ``> 4``, ``> 8``, ``> 16``, ``> 32``
    ms), with every command recorded. Choose
    :attr:`~aerospike_async.LatencyUnit.MICROSECONDS` with more columns when
    sub-millisecond resolution matters.

    Re-enabling metrics with a changed latency unit or histogram shape
    discards the accumulated latency samples (counters are retained).

    Example::

        from aerospike_sdk import LatencyUnit, MetricsPolicy, Sampler

        # Classic milliseconds view, sampling 10% of calls.
        policy = MetricsPolicy(sampler=Sampler.probability(0.1))
        cluster.enable_metrics(policy)

        # Sub-millisecond resolution for a low-latency deployment.
        fine = MetricsPolicy(
            latency_unit=LatencyUnit.MICROSECONDS,
            latency_columns=18,
        )

    Args:
        latency_unit: Unit latency histograms are recorded in. Defaults to
            milliseconds.
        latency_columns: Number of histogram buckets. Defaults to 7.
        latency_shift: Bucket-boundary spacing exponent — each boundary after
            the first bucket multiplies by ``2**latency_shift``. Defaults to
            1 (no skipped powers of two). Must be at least 1.
        sampler: Record-time gate for the per-command metrics; a fractional
            sampler decides once per call (not per retry) whether the whole
            call is measured. Defaults to recording every command.
        labels: Static label maps attached to every snapshot, e.g.
            ``[{"team": "billing"}]``.

    Raises:
        ValueError: If ``latency_shift`` is less than 1.

    See Also:
        :meth:`aerospike_sdk.aio.cluster.Cluster.enable_metrics`
    """

    __slots__ = ("latency_unit", "latency_columns", "latency_shift", "sampler", "labels")

    def __init__(
        self,
        *,
        latency_unit: LatencyUnit = LatencyUnit.MILLISECONDS,
        latency_columns: int = 7,
        latency_shift: int = 1,
        sampler: Optional[Sampler] = None,
        labels: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        if latency_shift < 1:
            raise ValueError(f"latency_shift must be at least 1, got {latency_shift}")
        self.latency_unit = latency_unit
        self.latency_columns = latency_columns
        self.latency_shift = latency_shift
        self.sampler = sampler if sampler is not None else Sampler.all()
        self.labels = labels if labels is not None else []

    def _to_pac(self) -> _PacMetricsPolicy:
        """Translate to the PAC policy (boundary spacing is a direct
        multiplier there: ``base = 2**shift``)."""
        pac = _PacMetricsPolicy()
        pac.latency_unit = self.latency_unit
        pac.latency_columns = self.latency_columns
        pac.latency_base = 1 << self.latency_shift
        pac.sampler = self.sampler
        pac.labels = self.labels
        return pac

    def __repr__(self) -> str:
        return (
            f"MetricsPolicy(latency_unit={self.latency_unit}, "
            f"latency_columns={self.latency_columns}, "
            f"latency_shift={self.latency_shift}, sampler={self.sampler!r})"
        )


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

    __slots__ = ("_pac",)

    def __init__(self, pac_metrics: _PacClusterMetrics) -> None:
        self._pac = pac_metrics

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

    def to_dict(self) -> Dict[str, Any]:
        """The full snapshot as a plain dict with cross-client-stable names.

        Node snapshots appear under their host address; the aggregate under
        ``"cluster-aggregated-metrics"``.
        """
        return self._pac.to_dict()

    def __repr__(self) -> str:
        return (
            f"MetricsSnapshot(total_nodes={self.total_nodes}, "
            f"open_connections={self.open_connections}, "
            f"exceeded_max_retries={self.exceeded_max_retries}, "
            f"exceeded_total_timeout={self.exceeded_total_timeout})"
        )
