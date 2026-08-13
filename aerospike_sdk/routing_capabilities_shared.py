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

"""Connect-time routing capability cache shared by async and sync clients.

Field ``43`` (server-compiled AEL) and field ``44`` (query selection) gates are
resolved once at connect from the live node list and stored on the client for
builder hot paths. Mixed into :class:`~aerospike_sdk.aio.client.Client` and
:class:`~aerospike_sdk.sync.client.SyncClient`.
"""

from __future__ import annotations

from typing import Any, List, Optional, Protocol

from aerospike_sdk import capabilities


class _RoutingCapabilitiesClient(Protocol):
    _client: Any
    _connected: bool
    _cached_supports_query_selection: Optional[bool]
    _cached_supports_server_compiled_ael: Optional[bool]


class RoutingCapabilitiesMixin:
    """Connect-time cache for field ``43`` / ``44`` routing gates."""

    _cached_supports_query_selection: Optional[bool]
    _cached_supports_server_compiled_ael: Optional[bool]

    def _init_routing_capability_cache(self) -> None:
        """Initialize routing caches; call from client ``__init__``."""
        self._cached_supports_query_selection = None
        self._cached_supports_server_compiled_ael = None

    def _clear_routing_capability_cache(self) -> None:
        """Drop routing caches; call from client close paths."""
        self._cached_supports_query_selection = None
        self._cached_supports_server_compiled_ael = None

    def _cluster_versions_blocking(self: _RoutingCapabilitiesClient) -> List[Any]:
        """Blocking node versions for connect-time routing probes."""
        pac = self._client
        if pac is None:
            return []
        nodes_fn = getattr(pac, "nodes_blocking", None)
        if nodes_fn is None:
            return []
        return [node.version for node in nodes_fn()]

    def _apply_routing_capabilities_from_versions(
        self: _RoutingCapabilitiesClient,
        versions: List[Any],
    ) -> None:
        self._cached_supports_query_selection = capabilities.supports_query_selection(
            versions,
        )
        self._cached_supports_server_compiled_ael = capabilities.supports_ael(versions)

    def _warm_routing_capabilities_blocking(self: _RoutingCapabilitiesClient) -> None:
        """Fill routing caches from a live node list (blocking connect path)."""
        if not self._connected or self._client is None:
            return
        self._apply_routing_capabilities_from_versions(self._cluster_versions_blocking())

    @property
    def supports_query_selection(self: _RoutingCapabilitiesClient) -> bool:
        """``True`` when all cluster nodes support field ``44`` query selection."""
        if not self._connected or self._client is None:
            return False
        return bool(self._cached_supports_query_selection)

    @property
    def supports_server_compiled_ael(self: _RoutingCapabilitiesClient) -> bool:
        """``True`` when server-compiled AEL filters are usable on this connection."""
        if not self._connected or self._client is None:
            return False
        return bool(self._cached_supports_server_compiled_ael)
