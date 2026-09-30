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

"""Routing capability cache shared by async and sync clients.

Field ``43`` (server-compiled AEL) and field ``44`` (query selection) gates are
derived from the node list and cached on the client for builder hot paths. Mixed
into ``Client`` and
``SyncClient``.

The cache is warmed at connect and re-derived once a tend interval has elapsed,
so a node joining with an older version closes the gates and the client reports
a clean ``OP_NOT_APPLICABLE`` instead of letting that node reject the filter
mid-stream. It reopens once that node leaves. PAC exposes no tend callback, so
this reads the node list PAC's own tend loop already publishes — an in-memory
walk, not a round trip, so it runs inline on both runtimes. See
:meth:`RoutingCapabilitiesMixin._routing_capability_ttl_seconds` for the window.

Not every client can read node versions: the ``current_thread_runtime`` proxy has
no node-listing surface, so the cluster version there is *undeterminable* rather
than old. See :meth:`RoutingCapabilitiesMixin._apply_undeterminable_routing_capabilities`
for how the two gates resolve in that case.
"""

from __future__ import annotations

import logging
import time
from typing import Any, List, Optional, Protocol, Tuple

from aerospike_async import ClientPolicy

from aerospike_sdk import capabilities
from aerospike_sdk.policy.system_settings import SystemSettings

log = logging.getLogger(__name__)

_pac_default_tend_interval_seconds_value: Optional[float] = None


def _pac_default_tend_interval_seconds() -> float:
    """PAC's default tend interval, read once on first use (not at import)."""
    global _pac_default_tend_interval_seconds_value
    if _pac_default_tend_interval_seconds_value is None:
        _pac_default_tend_interval_seconds_value = ClientPolicy().tend_interval / 1000.0
    return _pac_default_tend_interval_seconds_value


class _RoutingCapabilitiesClient(Protocol):
    _client: Any
    _connected: bool
    _sdk_settings: SystemSettings
    _cached_supports_query_selection: Optional[bool]
    _cached_supports_server_compiled_ael: Optional[bool]
    _routing_capability_stamp: Optional[float]
    _routing_capability_ttl: Optional[Tuple[Any, float]]
    _supports_mrt_cache: Optional[bool]

    # Declared so the mixin's own ``self``-annotated methods may call each other.
    def _client_can_list_nodes(self) -> bool: ...
    def _cluster_versions(self) -> List[Any]: ...
    def _apply_routing_capabilities_from_versions(self, versions: List[Any]) -> None: ...
    def _apply_undeterminable_routing_capabilities(self) -> None: ...
    def _routing_capability_ttl_seconds(self) -> float: ...
    def _resolve_routing_capabilities(self) -> None: ...
    def _refresh_routing_capabilities_if_stale(self) -> None: ...


class RoutingCapabilitiesMixin:
    """Tend-interval-refreshed cache for field ``43`` / ``44`` routing gates."""

    _cached_supports_query_selection: Optional[bool]
    _cached_supports_server_compiled_ael: Optional[bool]
    _routing_capability_stamp: Optional[float]
    _routing_capability_ttl: Optional[Tuple[Any, float]]
    _supports_mrt_cache: Optional[bool]

    def _init_routing_capability_cache(self) -> None:
        """Initialize routing caches; call from client ``__init__``."""
        self._cached_supports_query_selection = None
        self._cached_supports_server_compiled_ael = None
        self._routing_capability_stamp = None
        self._routing_capability_ttl = None

    def _clear_routing_capability_cache(self) -> None:
        """Drop routing caches; call from client close paths."""
        self._cached_supports_query_selection = None
        self._cached_supports_server_compiled_ael = None
        self._routing_capability_stamp = None
        self._routing_capability_ttl = None

    def _client_can_list_nodes(self: _RoutingCapabilitiesClient) -> bool:
        """Whether this client's PAC handle exposes a node list.

        The ``current_thread_runtime`` proxy wraps PAC's ``_LocalClient``, which
        has no node-listing surface. Probed on ``type(pac)`` rather than the
        instance: an instance lookup falls through the proxy's ``__getattr__``,
        which would build a per-thread client just to answer this question.
        """
        pac = self._client
        if pac is None:
            return False
        return hasattr(type(pac), "nodes")

    def _cluster_versions(self: _RoutingCapabilitiesClient) -> List[Any]:
        """Live node versions, or ``[]`` when this client cannot list nodes.

        Read live rather than from the routing cache: capability probes are a
        cold introspection path, and a caller asking outright deserves the
        current membership rather than a value up to a tend interval old.
        """
        if not self._client_can_list_nodes():
            return []
        return [node.version for node in self._client.nodes()]

    def _supports_mrt(self: _RoutingCapabilitiesClient) -> bool:
        """Whether every cluster node supports multi-record transactions.

        An MRT spans the cluster, so the aggregate is all-nodes: a single
        node below the MRT server version makes the answer ``False``. A
        client that cannot list nodes (the ``current_thread_runtime`` proxy)
        reports ``False``, so implicit batch-write transactions stay off
        there. Cached for the client's lifetime; the client clears it on
        close.
        """
        if self._supports_mrt_cache is None:
            versions = self._cluster_versions()
            self._supports_mrt_cache = bool(versions) and all(
                version.supports_mrt() for version in versions
            )
        return self._supports_mrt_cache

    def _apply_routing_capabilities_from_versions(
        self: _RoutingCapabilitiesClient,
        versions: List[Any],
    ) -> None:
        self._cached_supports_query_selection = capabilities.supports_query_selection(
            versions,
        )
        self._cached_supports_server_compiled_ael = capabilities.supports_ael(versions)

    def _apply_undeterminable_routing_capabilities(
        self: _RoutingCapabilitiesClient,
    ) -> None:
        """Resolve both gates for a client that cannot read node versions.

        The two gates diverge deliberately. Field ``43`` is the only encoding
        left for string AEL, so it stays open and a cluster below 8.2.0 rejects
        the filter itself — pre-failing here would reject string AEL against a
        capable cluster. Field ``44`` has a working field-``43`` execute path to
        fall back on, so an unverifiable cluster keeps it closed.
        """
        self._cached_supports_query_selection = False
        self._cached_supports_server_compiled_ael = True

    def _routing_capability_ttl_seconds(self: _RoutingCapabilitiesClient) -> float:
        """How long a derived gate stays trusted: one PAC tend interval.

        Re-deriving faster than PAC tends would walk the same node list and reach
        the same answer, so the tend cadence is the useful floor. ``None`` means
        the caller never overrode it and PAC's own default applies. The float is
        cached and recomputed when ``_sdk_settings`` is swapped wholesale.
        """
        settings = self._sdk_settings
        cached = self._routing_capability_ttl
        if cached is not None and cached[0] is settings:
            return cached[1]
        interval = settings.tend_interval
        if interval is None:
            ttl = _pac_default_tend_interval_seconds()
        else:
            ttl = interval.total_seconds()
        # The settings marker and the value travel as one tuple through one
        # attribute, so a concurrent reader sees either the previous pair or
        # this complete one, never the marker without its value.
        self._routing_capability_ttl = (settings, ttl)
        return ttl

    def _resolve_routing_capabilities(self: _RoutingCapabilitiesClient) -> None:
        """Derive both gates from the current node list."""
        if self._client_can_list_nodes():
            self._apply_routing_capabilities_from_versions(self._cluster_versions())
        else:
            self._apply_undeterminable_routing_capabilities()

    def _refresh_routing_capabilities_if_stale(
        self: _RoutingCapabilitiesClient,
    ) -> None:
        """Re-derive the gates once the last result is a tend interval old.

        The stamp moves before the work starts, so a failed read keeps the
        previous gates and retries after the next interval instead of failing
        the operation that happened to trigger it.
        """
        stamp = self._routing_capability_stamp
        if stamp is not None:
            if time.monotonic() - stamp < self._routing_capability_ttl_seconds():
                return
        self._routing_capability_stamp = time.monotonic()
        try:
            self._resolve_routing_capabilities()
        except Exception as exc:
            log.debug("Routing capability refresh failed: %s", exc, exc_info=True)

    def _warm_routing_capabilities(self: _RoutingCapabilitiesClient) -> None:
        """Resolve both gates at connect so the first operation pays nothing."""
        if not self._connected or self._client is None:
            return
        self._resolve_routing_capabilities()
        self._routing_capability_stamp = time.monotonic()

    @property
    def supports_query_selection(self: _RoutingCapabilitiesClient) -> bool:
        """``True`` when all cluster nodes support field ``44`` query selection."""
        if not self._connected or self._client is None:
            return False
        self._refresh_routing_capabilities_if_stale()
        return bool(self._cached_supports_query_selection)

    @property
    def supports_server_compiled_ael(self: _RoutingCapabilitiesClient) -> bool:
        """``True`` when server-compiled AEL filters are usable on this connection."""
        if not self._connected or self._client is None:
            return False
        self._refresh_routing_capabilities_if_stale()
        return bool(self._cached_supports_server_compiled_ael)
