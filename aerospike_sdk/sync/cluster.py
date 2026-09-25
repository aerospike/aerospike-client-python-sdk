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

"""Cluster - Represents a connection to an Aerospike cluster (sync version)."""

from __future__ import annotations

import weakref

import types
import typing
from typing import Any, Optional

from aerospike_async import ClientPolicy, UDFLang, Version

from aerospike_sdk import capabilities
from aerospike_sdk.cluster_shared import ClusterBase
from aerospike_sdk.exceptions import ConnectionError, ResultCode
from aerospike_sdk.metrics.export import (
    DEFAULT_EXPORT_INTERVAL_SECONDS,
    built_in_exporter,
    check_exporter,
    SyncMetricsExportTimer,
)
from aerospike_sdk.metrics import (
    MetricsPolicy,
    MetricsSnapshot,
    policy_from_settings,
)
from aerospike_sdk.metrics.usage import COMMAND_COUNT
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sdk_config_monitor import SdkConfigSource, adopt_discovered_cluster_name
from aerospike_sdk.sync.client import SyncClient

if typing.TYPE_CHECKING:
    from aerospike_async import AdminPolicy, RegisterTask, UdfRemoveTask
    # These resolve the ClusterBase[_S, _TS] string forward-refs; ruff reads them as unused
    # (F401) because it doesn't count string-subscript usage.
    from aerospike_sdk.sync.session import Session  # noqa: F401
    from aerospike_sdk.sync.transactional_session import TransactionalSession  # noqa: F401


class Cluster(ClusterBase["Session", "TransactionalSession"]):
    """Synchronous cluster handle from ``sync.cluster_definition.ClusterDefinition.connect``.

    Mirrors :class:`~aerospike_sdk.aio.cluster.Cluster` but uses
    ``SyncClient`` and
    :class:`~aerospike_sdk.sync.session.Session`. The
    :meth:`create_session` / :meth:`transaction` / :meth:`is_connected`
    factories are inherited from
    :class:`~aerospike_sdk.cluster_shared.ClusterBase`.

    Example::

            with ClusterDefinition("localhost", 3100).connect() as cluster:
                session = cluster.create_session(Behavior.DEFAULT)

    See Also:
        :class:`~aerospike_sdk.aio.cluster.Cluster`
    """
    
    def __init__(self, sdk_client: SyncClient) -> None:
        """
        Initialize a Cluster instance.
        
        Args:
            sdk_client: The underlying SyncClient instance
        
        Note:
            This should not be called directly. Use ClusterDefinition.connect() instead.
        """
        self._sdk_client = sdk_client
        self._metrics_policy: Optional[MetricsPolicy] = None
        self._exporters: list = []
        # The exporter this cluster installed from configuration, as opposed
        # to one the application registered: the only one the cluster closes.
        self._installed_exporter: Any = None
        self._export_timer: Any = None
        sdk_client._owner_cluster = weakref.ref(self)
    
    @classmethod
    def _create(
        cls,
        policy: ClientPolicy,
        seeds: str,
        sdk_settings: Optional[SystemSettings] = None,
        sdk_config_source: Optional[SdkConfigSource] = None,
    ) -> Cluster:
        """
        Internal method to create a new Cluster instance.

        Args:
            policy: The ClientPolicy configuration
            seeds: The seeds string (e.g., "localhost:3000")
            sdk_settings: Resolved SDK settings to store for runtime reads
            sdk_config_source: When set, arms config hot-reload on the client

        Returns:
            A new Cluster instance

        Raises:
            ConnectionError: If post-connect validation fails
        """
        sdk_client = SyncClient(
            seeds=seeds,
            policy=policy,
        )
        if sdk_settings is not None:
            sdk_client._sdk_settings = sdk_settings
        sdk_client.connect()

        # Bypass asyncio for the post-connect sanity check — `is_connected`
        # on PAC is a non-blocking synchronous probe (no I/O).
        if not sdk_client._pac_client().is_connected_blocking():
            sdk_client.close()
            raise ConnectionError(
                f"Connected to seeds '{seeds}' but cluster reports not connected",
                result_code=ResultCode.SERVER_NOT_AVAILABLE,
            )
        cluster = cls(sdk_client)
        if sdk_settings is not None and sdk_config_source is not None:
            # The server-reported name can select a `system.<clusterName>`
            # block, so this has to happen before the settings are used.
            sdk_settings, sdk_config_source = adopt_discovered_cluster_name(
                sdk_client.underlying_client.server_cluster_name,
                sdk_settings,
                sdk_config_source,
            )
            sdk_client._sdk_settings = sdk_settings
        if sdk_settings is not None:
            # After connect: enabling collection needs a live client, and it
            # goes through the cluster so the export timer starts with it.
            cluster._apply_metrics_settings(sdk_settings.metrics)

        if sdk_config_source is not None:
            sdk_client._start_sdk_config_monitor(sdk_config_source)
        return cluster
    
    def __enter__(self) -> Cluster:
        """Context manager entry."""
        return self
    
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        """Context manager exit."""
        self.close()
    
    @property
    def _client(self) -> SyncClient:
        """Get the underlying SyncClient."""
        return self._sdk_client

    def register_udf(
        self,
        body: bytes,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF module from bytes (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf`
        """
        return self._sdk_client._register_udf(body, server_path, language, policy=policy)

    def register_udf_from_file(
        self,
        client_path: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF module from a local file (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf_from_file`
        """
        return self._sdk_client._register_udf_from_file(
            client_path, server_path, language, policy=policy)

    def register_udf_from_resource(
        self,
        package: str,
        resource: str,
        server_path: str,
        language: UDFLang = UDFLang.LUA,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "RegisterTask":
        """Register a UDF from a Python package resource (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.register_udf_from_resource`
        """
        return self._sdk_client._register_udf_from_resource(
            package, resource, server_path, language, policy=policy)

    def remove_udf(
        self,
        server_path: str,
        *,
        policy: Optional["AdminPolicy"] = None,
    ) -> "UdfRemoveTask":
        """Remove a UDF module from the cluster (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.remove_udf`
        """
        return self._sdk_client._remove_udf(server_path, policy=policy)

    def list_udf(self) -> list[dict[str, str]]:
        """List the UDF modules registered on the cluster (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.list_udf`
        """
        return self._sdk_client._list_udf()

    def list_indexes(self) -> list[dict[str, str]]:
        """List the secondary indexes defined on the cluster (synchronous).

        Raises:
            RuntimeError: If not connected.
            AerospikeError: On cluster errors (via PAC).

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.list_indexes`
        """
        return self._sdk_client._list_indexes()

    # -- Server-capability probes ---------------------------------------------
    # Guard feature use against the cluster's least-capable node. Sync
    # counterparts of the async Cluster probes; see there for detail.

    def server_version(self) -> Optional[Version]:
        """The minimum server version across connected nodes.

        Returns:
            The least-capable node's :class:`~aerospike_async.Version`, or
            ``None`` when the cluster reports no nodes.

        Example::

            v = cluster.server_version()
            if v is not None and (v.major, v.minor, v.patch) >= (8, 2, 0):
                ...
        """
        return capabilities.min_version(self._sdk_client._cluster_versions_blocking())

    def supports_ael(self) -> bool:
        """Whether every node parses server-compiled AEL (filters, exp reads/writes)."""
        return capabilities.supports_ael(self._sdk_client._cluster_versions_blocking())

    def supports_query_operations(self) -> bool:
        """Whether every node supports read operations inside an index query."""
        return capabilities.supports_query_operations(
            self._sdk_client._cluster_versions_blocking())

    def supports_string_operations(self) -> bool:
        """Whether every node supports the server-side string operations."""
        return capabilities.supports_string_operations(
            self._sdk_client._cluster_versions_blocking())

    def supports_query_selection(self) -> bool:
        """Whether every node supports server-led index selection (>= 8.2.0)."""
        return capabilities.supports_query_selection(
            self._sdk_client._cluster_versions_blocking())

    # -- Metrics ---------------------------------------------------------------
    # Collection lives in the client core and is cluster-scoped; these
    # configure it and pull snapshots.

    @property
    def exporters(self) -> tuple:
        """The registered exporters, in the order snapshots reach them."""
        return tuple(self._exporters)

    def add_exporter(self, exporter: Any) -> None:
        """Register an exporter; snapshots are pushed to each one registered.

        Exporters receive every snapshot in registration order. One that keeps
        raising is suspended and periodically retried without affecting the
        others. The cluster never closes an exporter registered here -- the
        application owns its lifecycle.

        Args:
            exporter: A :class:`~aerospike_sdk.metrics.MetricsExporter`
                (plain ``def export``).

        Raises:
            TypeError: If ``exporter`` has no ``export`` method or defines an
                async one.

        Example::

            exporter = LearnMetricsFileExporter("/var/log/aerospike")
            cluster.add_exporter(exporter)

        See Also:
            :meth:`remove_exporter`, :meth:`enable_metrics`
        """
        # The two protocols are easy to confuse and a mismatch produces no
        # data at all -- only a warning once per interval, far from the
        # registration that caused it. Fail here instead.
        check_exporter(exporter, awaitable=False)
        self._exporters.append(exporter)

    def remove_exporter(self, exporter: Any) -> bool:
        """Unregister an exporter previously passed to :meth:`add_exporter`.

        Args:
            exporter: The exporter object to remove.

        Returns:
            ``True`` if it was registered, ``False`` if it was not found.
        """
        try:
            self._exporters.remove(exporter)
        except ValueError:
            return False
        return True

    def _apply_metrics_settings(self, metrics: Any) -> None:
        """Turn collection on or off to match configuration-file settings.

        Silent settings leave collection alone, so a file that tunes only the
        histogram shape does not switch collection on by itself.
        """
        if metrics is None or metrics.enabled is None:
            return
        if metrics.enabled:
            self.enable_metrics(policy_from_settings(metrics))
        else:
            self.disable_metrics()

    def _start_export_timer(self, policy: MetricsPolicy) -> None:
        """Begin pushing snapshots to the registered exporters.

        Replaces any running timer, and re-evaluates the configured built-in
        exporter: the config file installs one only while the application has
        registered nothing itself. The timer runs whenever metrics are on --
        an interval with no exporters skips the snapshot, so a registration
        made after enabling still takes effect.
        """
        self._stop_export_timer()
        self._uninstall_built_in()
        settings = getattr(self._sdk_client, "_sdk_settings", None)
        metrics = getattr(settings, "metrics", None)
        interval = DEFAULT_EXPORT_INTERVAL_SECONDS
        if metrics is not None and metrics.export_interval is not None:
            interval = metrics.export_interval.total_seconds()
        if not self._exporters and metrics is not None:
            built_in = built_in_exporter(metrics)
            if built_in is not None:
                self._exporters.append(built_in)
                self._installed_exporter = built_in
        self._export_timer = SyncMetricsExportTimer(self, interval, metrics)
        self._export_timer.start()

    def _uninstall_built_in(self) -> None:
        """Retire the config-installed exporter, closing what it opened.

        Only ever the cluster's own install: exporters the application
        registered are its to close.
        """
        installed, self._installed_exporter = self._installed_exporter, None
        if installed is None:
            return
        if installed in self._exporters:
            self._exporters.remove(installed)
        try:
            installed.close()
        except OSError:
            pass

    def _stop_export_timer(self) -> None:
        """Stop the export timer if one is running."""
        if self._export_timer is not None:
            self._export_timer.stop()
            self._export_timer = None

    def enable_metrics(self, policy: Optional[MetricsPolicy] = None) -> None:
        """Enable metrics collection for this cluster.

        Collection is off until enabled. Re-enabling with a changed latency
        unit or histogram shape discards the accumulated latency samples;
        counters are retained.

        Args:
            policy: Collection configuration. Defaults to
                :class:`~aerospike_sdk.metrics.MetricsPolicy`'s milliseconds/7-column
                scheme with every command recorded.

        Example::

            cluster.enable_metrics(MetricsPolicy(sampler=Sampler.probability(0.1)))

        See Also:
            :meth:`metrics`, :meth:`disable_metrics`
        """
        effective = policy if policy is not None else MetricsPolicy()
        # Kept because the snapshot does not carry its own histogram shape,
        # which the structured export has to report.
        self._metrics_policy = effective
        client = self._sdk_client
        client._usage_on = effective.usage_enabled
        # The command count rides on metrics being enabled at all; `_record_on`
        # is the single flag the per-op paths test.
        client._cmd_count_on = True
        client._record_on = True
        client.underlying_client.enable_metrics(effective._to_pac())
        self._start_export_timer(effective)

    def disable_metrics(self) -> None:
        """Disable metrics collection. Accumulated data is retained.

        Stops the export push; exporters the application registered stay
        registered and are not closed -- their lifecycle belongs to the
        application.
        """
        client = self._sdk_client
        client.underlying_client.disable_metrics()
        client._usage_on = False
        client._cmd_count_on = False
        client._record_on = False
        self._stop_export_timer()
        self._uninstall_built_in()

    def metrics_enabled(self) -> bool:
        """Whether metrics collection is currently enabled."""
        return self._sdk_client.underlying_client.metrics_enabled()

    def metrics(self) -> MetricsSnapshot:
        """Snapshot the accumulated cluster metrics.

        Values are cumulative since metrics were enabled (connection gauges
        are point-in-time). Snapshotting drains and aggregates per-node
        state, so poll at an export interval rather than per operation.

        Returns:
            A :class:`~aerospike_sdk.metrics.MetricsSnapshot`; empty (zeroed) if
            metrics were never enabled.

        Example::

            snapshot = cluster.metrics()
            reads = snapshot.latency(LatencyType.READ)
            print(f"{reads.count} reads, avg {reads.average:.1f}")
        """
        client = self._sdk_client
        pac = client.underlying_client
        client_policy = client._policy
        return MetricsSnapshot(
            pac.metrics(),
            policy=self._metrics_policy,
            nodes=pac.nodes_blocking(),
            usage=client._usage_counters.totals(),
            command_count=client._command_counts.totals().get(COMMAND_COUNT, 0),
            # With no application identity of its own, the snapshot reports the
            # authenticated user -- the identity the server already knows this
            # connection by, and so the one that joins the two views.
            app_id=client_policy.application_id or client_policy.user,
        )

    def close(self) -> None:
        """
        Closes the cluster connection and releases all associated resources.
        
        This method closes the underlying client connection. It should be called
        when the cluster is no longer needed to ensure proper resource cleanup.
        
        This method is automatically called when using context manager::

                with ClusterDefinition("localhost", 3100).connect() as cluster:
                    # Use the cluster...
                # cluster.close() is automatically called here
        """
        # Stop exporting before the client goes away: the timer polls the
        # client every interval and would otherwise keep firing against a
        # closed one.
        self._stop_export_timer()
        self._uninstall_built_in()
        self._sdk_client.close()

