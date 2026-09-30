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

"""Shared read-only view of one cluster node.

The async and sync :class:`Node` classes differ only in how they dispatch
:meth:`info`, the one call that reaches the server. Everything here is state
the client already tracks for the node, so reading it costs no network round
trip.
"""

from __future__ import annotations

from typing import Any, List, Tuple

from aerospike_async import Version


class NodeBase:
    """Properties common to the async and sync node views."""

    __slots__ = ("_pac",)

    def __init__(self, pac_node: Any) -> None:
        """Wrap a PAC node; obtained from ``Cluster.nodes`` or ``Cluster.get_node``."""
        self._pac = pac_node

    @property
    def name(self) -> str:
        """The server-assigned node ID, e.g. ``"BB9D4EB574A8DA6"``.

        This is the key :meth:`Cluster.get_node` looks up, and the key the
        ``*_per_node`` info helpers return results under.

        Example::

            names = [node.name for node in cluster.nodes()]

        Returns:
            The node's name.

        See Also:
            :attr:`address`: Where the client reaches this node.
        """
        return self._pac.name

    @property
    def address(self) -> str:
        """The ``host:port`` address the client connects to for this node.

        Example::

            print(f"{node.name} at {node.address}")   # BB9D4EB574A8DA6 at 10.0.0.12:3000

        Returns:
            The address as ``"host:port"``.

        See Also:
            :attr:`host`: The same address as a ``(host, port)`` tuple.
        """
        return self._pac.address

    @property
    def host(self) -> Tuple[str, int]:
        """The address the client connects to, as a ``(host, port)`` tuple.

        Example::

            hostname, port = node.host
            assert port != 4333, "still connected over TLS"

        Returns:
            ``(hostname, port)``.

        See Also:
            :attr:`address`: The same address as a string.
        """
        return self._pac.host

    @property
    def is_active(self) -> bool:
        """Whether the client still considers this node part of the cluster.

        A node object outlives its membership: one fetched before the node left
        keeps reporting its last-known state here as ``False``.

        Example::

            inactive = [node.name for node in cluster.nodes() if not node.is_active]

        Returns:
            ``True`` while the node is in the client's current node list.

        See Also:
            :meth:`Cluster.nodes`: The current, active node list.
        """
        return self._pac.is_active

    @property
    def version(self) -> Version:
        """The server version this node runs.

        Per-node, unlike :meth:`Cluster.server_version`, which reports the
        cluster minimum. Comparing them across nodes shows whether a rolling
        upgrade has finished.

        Example::

            versions = {node.name: str(node.version) for node in cluster.nodes()}
            if len(set(versions.values())) > 1:
                print(f"mixed-version cluster: {versions}")

        Returns:
            The node's :class:`~aerospike_async.Version`.

        See Also:
            :meth:`Cluster.server_version`: The minimum across all nodes.
        """
        return self._pac.version

    @property
    def failures(self) -> int:
        """Failures the client has counted for this node since its last successful refresh.

        Example::

            flaky = [node.name for node in cluster.nodes() if node.failures > 0]

        Returns:
            The failure count; ``0`` for a healthy node, and reset to ``0`` by the
            next successful cluster tend.

        See Also:
            :attr:`is_active`: Whether the node is still in the cluster.
        """
        return self._pac.failures

    @property
    def partition_generation(self) -> int:
        """The node's partition-map generation, bumped when its partition ownership changes.

        Example::

            generations = {node.name: node.partition_generation for node in cluster.nodes()}

        Returns:
            The generation the client last read from the node.

        See Also:
            :attr:`rebalance_generation`: The generation for rebalance events.
        """
        return self._pac.partition_generation

    @property
    def rebalance_generation(self) -> int:
        """The node's rebalance generation, bumped each time the cluster rebalances.

        Example::

            generations = {node.rebalance_generation for node in cluster.nodes()}
            settled = len(generations) == 1

        Returns:
            The generation the client last read from the node.

        See Also:
            :attr:`partition_generation`: The generation for partition-map changes.
        """
        return self._pac.rebalance_generation

    def aliases(self) -> List[Tuple[str, int]]:
        """Every ``(host, port)`` the client has seen this node answer on.

        A node reached through several addresses (seed hostname, peer-reported
        IP, alternate services address) is one node with several aliases.

        Example::

            for hostname, port in node.aliases():
                print(f"{node.name} reachable at {hostname}:{port}")

        Returns:
            The node's known addresses, including :attr:`host`.

        See Also:
            :attr:`host`: The address the client currently connects to.
        """
        return self._pac.aliases()

    def __repr__(self) -> str:
        return f"Node(name={self.name!r}, address={self.address!r}, active={self.is_active})"
