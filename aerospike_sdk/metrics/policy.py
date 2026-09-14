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

"""Metrics policy: what to collect and at what shape.

The policy is the input side of metrics -- histogram unit and shape, the
sampler, labels, and whether usage counters run. :mod:`.snapshot` holds the
output side.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from aerospike_async import (
    CommandType,
    LatencyUnit,
    MetricsPolicy as _PacMetricsPolicy,
    Sampler,
)

if TYPE_CHECKING:
    from aerospike_sdk.policy.system_settings import MetricsSettings

__all__ = [
    "CommandType",
    "LatencyType",
    "LatencyUnit",
    "MetricsPolicy",
    "Sampler",
    "apply_metrics_settings",
    "policy_from_settings",
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


# Histogram shape used when a policy does not say otherwise, and reported as
# the shape of a snapshot taken before collection was ever enabled.
_DEFAULT_LATENCY_COLUMNS = 7
_DEFAULT_LATENCY_SHIFT = 1


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
        usage_enabled: Record which SDK features the application uses
            (:mod:`aerospike_sdk.metrics.usage`). Off by default. Recorded by this SDK
            rather than the client core, and independent of latency
            collection -- usage counters ignore the sampler.
        labels: Static label maps attached to every snapshot, e.g.
            ``[{"team": "billing"}]``.

    Raises:
        ValueError: If ``latency_shift`` is less than 1.

    See Also:
        :meth:`aerospike_sdk.aio.cluster.Cluster.enable_metrics`
    """

    __slots__ = (
        "latency_unit",
        "latency_columns",
        "latency_shift",
        "sampler",
        "labels",
        "usage_enabled",
    )

    def __init__(
        self,
        *,
        latency_unit: LatencyUnit = LatencyUnit.MILLISECONDS,
        latency_columns: int = _DEFAULT_LATENCY_COLUMNS,
        latency_shift: int = _DEFAULT_LATENCY_SHIFT,
        sampler: Optional[Sampler] = None,
        labels: Optional[List[Dict[str, str]]] = None,
        usage_enabled: bool = False,
    ) -> None:
        if latency_shift < 1:
            raise ValueError(f"latency_shift must be at least 1, got {latency_shift}")
        self.latency_unit = latency_unit
        self.latency_columns = latency_columns
        self.latency_shift = latency_shift
        self.sampler = sampler if sampler is not None else Sampler.all()
        self.labels = labels if labels is not None else []
        self.usage_enabled = usage_enabled

    def _to_pac(self) -> _PacMetricsPolicy:
        """Translate to the PAC policy.

        ``usage_enabled`` is deliberately not passed on: usage counters are
        recorded in this SDK, not below it.
        """
        pac = _PacMetricsPolicy()
        pac.latency_unit = self.latency_unit
        pac.latency_columns = self.latency_columns
        pac.latency_shift = self.latency_shift
        pac.sampler = self.sampler
        pac.labels = self.labels
        return pac

    def __repr__(self) -> str:
        return (
            f"MetricsPolicy(latency_unit={self.latency_unit}, "
            f"latency_columns={self.latency_columns}, "
            f"latency_shift={self.latency_shift}, sampler={self.sampler!r})"
        )


def policy_from_settings(settings: "MetricsSettings") -> MetricsPolicy:
    """Build a :class:`MetricsPolicy` from configuration-file settings.

    Fields the file did not set keep the policy default, so a block naming
    only ``enabled`` still gets the standard millisecond histogram.

    Args:
        settings: The ``system.<cluster>.metrics`` group.

    Returns:
        The equivalent policy.
    """
    kwargs: Dict[str, Any] = {}
    if settings.latency_unit is not None:
        kwargs["latency_unit"] = (
            LatencyUnit.MICROSECONDS
            if settings.latency_unit == "microseconds"
            else LatencyUnit.MILLISECONDS
        )
    if settings.latency_columns is not None:
        kwargs["latency_columns"] = settings.latency_columns
    if settings.latency_shift is not None:
        kwargs["latency_shift"] = settings.latency_shift
    if settings.labels is not None:
        # The file carries one label map; the policy takes a list of them.
        kwargs["labels"] = [settings.labels]
    if settings.sampler_range is not None and settings.sampler_threshold is not None:
        kwargs["sampler"] = Sampler(settings.sampler_range, settings.sampler_threshold)
    if settings.usage_enabled is not None:
        kwargs["usage_enabled"] = settings.usage_enabled
    return MetricsPolicy(**kwargs)


def apply_metrics_settings(underlying_client: Any, settings: "MetricsSettings") -> None:
    """Turn core metrics on or off to match *settings*.

    A block that never names ``enabled`` leaves collection exactly as it is —
    so a file that configures only the histogram shape does not switch
    collection on by itself, and a client that enabled metrics in code is not
    silently switched off by a file that is silent on the subject.

    Args:
        underlying_client: The PAC client to enable or disable collection on.
        settings: The resolved ``metrics`` group.
    """
    if settings.enabled is None:
        return
    if settings.enabled:
        underlying_client.enable_metrics(policy_from_settings(settings)._to_pac())
    else:
        underlying_client.disable_metrics()
