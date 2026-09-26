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

"""SystemSettings - Cluster-wide system configuration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Optional

from aerospike_async import ClientPolicy


@dataclass(frozen=True)
class TransactionSettings:
    """SDK-runtime transaction behavior, read at operation time.

    Unlike the connection and refresh groups on :class:`SystemSettings`,
    these fields do not map onto :class:`~aerospike_async.ClientPolicy`;
    they configure how the SDK itself drives multi-record transactions.
    ``None`` means "not set" — the value falls through to the next
    configuration layer (see :class:`SystemSettings` for layering), ending
    at the hard defaults: ``implicit_batch_write_transactions`` ``True``,
    ``number_of_attempts`` ``5``, ``sleep_between_attempts`` one second.

    ``implicit_batch_write_transactions`` controls whether a multi-key
    write batch on a strong-consistency namespace (MRT-capable cluster,
    no explicit transaction active) is wrapped in an implicit
    multi-record transaction so its writes commit atomically.
    ``number_of_attempts`` and ``sleep_between_attempts`` drive the retry
    loop for those implicit transactions when the server reports a
    transient conflict.

    Example::

        settings = SystemSettings(
            transactions=TransactionSettings(
                implicit_batch_write_transactions=False,
            ),
        )
        cluster = await (
            ClusterDefinition("localhost", 3000)
            .with_system_settings(settings)
            .connect()
        )

    See Also:
        :class:`SystemSettings`: Carrier for these settings.
    """

    implicit_batch_write_transactions: Optional[bool] = None
    sleep_between_attempts: Optional[timedelta] = None
    number_of_attempts: Optional[int] = None


@dataclass(frozen=True)
class MetricsSettings:
    """Client metrics configuration from the ``system.<cluster>.metrics`` block.

    Every field is ``None`` when the file did not set it, so an absent block
    changes nothing and a partial block only overrides what it names.

    The two extended tiers each have their own ``enabled`` flag, under the
    cross-SDK key names: ``metrics.extended.operational.enabled`` gates the
    latency histograms and per-command counters, and carries the histogram
    shape keys that only mean anything while it is on;
    ``metrics.extended.usage.enabled`` gates the feature-usage counters, which
    this SDK records itself rather than reading from the client core.

    ``metrics.enabled`` sits above both: it turns collection on at all, and on
    its own yields the always-on gauges.

    Example::

        # system:
        #   DEFAULT:
        #     metrics:
        #       enabled: true
        #       labels:
        #         owner: platform-team
        #       extended:
        #         operational:
        #           enabled: true
        #           latency_unit: microseconds
        #           latency_columns: 18

    See Also:
        :class:`~aerospike_sdk.metrics.MetricsPolicy`: The runtime policy these
            settings build.
    """

    enabled: Optional[bool] = None
    latency_unit: Optional[str] = None
    latency_columns: Optional[int] = None
    latency_shift: Optional[int] = None
    export_interval: Optional[timedelta] = None
    exporter: Optional[str] = None
    report_dir: Optional[str] = None
    report_size_limit: Optional[int] = None
    sampler_range: Optional[int] = None
    sampler_threshold: Optional[int] = None
    labels: Optional[Dict[str, str]] = None
    operational_enabled: Optional[bool] = None
    usage_enabled: Optional[bool] = None


@dataclass(frozen=True)
class SystemSettings:
    """Cluster-wide settings that apply to an entire cluster instance.

    These settings cannot vary per Behavior -- they are inherently global
    to the connection pool and cluster maintenance.

    Settings may also come from an SDK configuration file (the
    ``AEROSPIKE_SDK_CONFIG_URL`` environment variable). File-provided
    values take precedence over values set here, field by field: a field
    the file does not provide falls through to this object, then to the
    hard default.

    Example::

        settings = SystemSettings(
            max_connections_per_node=200,
            max_socket_idle_time=timedelta(seconds=30),
        )
        cluster = await (
            ClusterDefinition("localhost", 3000)
            .with_system_settings(settings)
            .connect()
        )

    ``wait_for_connection_to_complete`` bounds how long the pool may spend
    establishing one connection (TCP connect, TLS, authentication). It lives
    here rather than on a Behavior because the pool does the connecting and
    the pool is client-scoped: commands never open connections inline, so
    there is no per-operation moment for a connect bound to govern. Unset, it
    falls back to the client policy's total timeout.

    ``tend_timeout`` bounds each cluster-tend info call, the first contact
    with a seed included; ``login_timeout`` bounds the login exchange on every
    new connection when the cluster has security enabled. Both are
    client-wide for the same reason, so they live here rather than on the
    cluster definition. Unset, the hard defaults are one second and five
    seconds. Neither has a key in the configuration file.

    Connection-pool sizing is the group most worth setting explicitly. Left
    unset, the hard defaults are ``min_connections_per_node=0``,
    ``max_connections_per_node=100``, and ``conn_pools_per_node=1``. The
    maximum is a fail-fast cap rather than a queue, so a client whose
    concurrency outgrows it becomes pool-bound -- throughput flattens and
    latency climbs while the cluster still has headroom. Size it to total
    client concurrency, not to concurrency divided by node count, since
    bursts do not spread evenly across nodes.

    See Also:
        :class:`TransactionSettings`: The SDK-runtime transaction group.
        :meth:`~aerospike_sdk.aio.cluster_definition.ClusterDefinition.with_system_settings`
    """

    min_connections_per_node: Optional[int] = None
    max_connections_per_node: Optional[int] = None
    conn_pools_per_node: Optional[int] = None
    wait_for_connection_to_complete: Optional[timedelta] = None
    tend_timeout: Optional[timedelta] = None
    login_timeout: Optional[timedelta] = None
    max_socket_idle_time: Optional[timedelta] = None
    tend_interval: Optional[timedelta] = None
    num_tend_intervals_in_error_window: Optional[int] = None
    max_errors_in_error_window: Optional[int] = None
    transactions: TransactionSettings = TransactionSettings()
    metrics: MetricsSettings = MetricsSettings()

    def apply_to(self, policy: ClientPolicy) -> ClientPolicy:
        """Apply non-None fields to *policy*, returning the same object.

        The :attr:`transactions` group is SDK-runtime configuration and is
        deliberately not applied; it is read at operation time.
        """
        if self.min_connections_per_node is not None:
            policy.min_conns_per_node = self.min_connections_per_node
        if self.max_connections_per_node is not None:
            policy.max_conns_per_node = self.max_connections_per_node
        if self.conn_pools_per_node is not None:
            policy.conn_pools_per_node = self.conn_pools_per_node
        if self.wait_for_connection_to_complete is not None:
            policy.connect_timeout = int(
                self.wait_for_connection_to_complete.total_seconds() * 1000
            )
        if self.tend_timeout is not None:
            policy.timeout = int(self.tend_timeout.total_seconds() * 1000)
        if self.login_timeout is not None:
            policy.login_timeout = int(self.login_timeout.total_seconds() * 1000)
        if self.max_socket_idle_time is not None:
            policy.idle_timeout = int(self.max_socket_idle_time.total_seconds() * 1000)
        if self.tend_interval is not None:
            policy.tend_interval = int(self.tend_interval.total_seconds() * 1000)
        if self.num_tend_intervals_in_error_window is not None:
            policy.error_rate_window = self.num_tend_intervals_in_error_window
        if self.max_errors_in_error_window is not None:
            policy.max_error_rate = self.max_errors_in_error_window
        return policy
