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

"""InfoCommands - High-level interface for Aerospike info commands."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Set

if TYPE_CHECKING:  # Not unused — avoids circular import; used in type annotations only.
    from aerospike_sdk.aio.session import Session

log = logging.getLogger("aerospike_sdk.info")


class InfoCommands:
    """
    Provides high-level methods to execute common Aerospike info commands.

    This class encapsulates the most commonly used Aerospike info commands and provides
    a convenient API for retrieving cluster information.

    Example::

            info = session.info()

            # Get all namespaces
            namespaces = await info.namespaces()

            # Get namespace details
            ns_detail = await info.namespace_details("test")

            # Get all secondary indexes
            indexes = await info.secondary_indexes()
    """

    def __init__(self, session: "Session") -> None:
        """
        Initialize InfoCommands.

        Args:
            session: The Session to use for info commands.
        """
        self._session = session

    async def build(self) -> Set[str]:
        """
        Get the build information from all nodes in the cluster.

        Returns:
            A set of build strings from all nodes.
        """
        # Get info from all nodes and merge the results
        all_responses = await self._session._client._client.info_on_all_nodes("build")

        # Extract build strings from all node responses
        build_set: Set[str] = set()
        for node_response in all_responses.values():
            # Response format is typically {"build": "8.1.0.1"}
            for value in node_response.values():
                if isinstance(value, str) and value:
                    build_set.add(value.strip())

        return build_set

    async def namespaces(self) -> Set[str]:
        """
        Get the list of namespaces from all nodes in the cluster.

        Returns:
            A set of namespace names from all nodes.
        """
        # Get info from all nodes and merge the results
        all_responses = await self._session._client._client.info_on_all_nodes("namespaces")

        # Extract namespace names from all node responses
        namespace_set: Set[str] = set()
        for node_response in all_responses.values():
            # Response format is typically {"namespaces": "ns1;ns2;ns3"}
            for value in node_response.values():
                if isinstance(value, str) and value:
                    # Split semicolon-separated namespace list (info protocol)
                    namespace_set.update([ns.strip() for ns in value.split(";") if ns.strip()])

        return namespace_set

    async def namespace_details(self, namespace: str) -> Optional[Dict[str, str]]:
        """
        Get detailed information about a specific namespace.

        Args:
            namespace: The name of the namespace.

        Returns:
            A dictionary containing namespace details, or None if not found.
        """
        try:
            response = await self._session._client._client.info(f"namespace/{namespace}")
            if not response:
                return None
            # Check if response indicates namespace doesn't exist
            # Response format for non-existent: {'namespace/name': 'type=unknown'}
            expected_key = f"namespace/{namespace}"
            if expected_key in response and str(response[expected_key]).strip() == "type=unknown":
                return None
            return response
        except Exception:
            log.debug("namespace_details(%s) failed", namespace, exc_info=True)
            return None

    async def sets(self, namespace: str) -> List[str]:
        """
        Get the list of sets in a specific namespace.

        Args:
            namespace: The name of the namespace.

        Returns:
            A list of set names in the namespace.
        """
        # Get info from all nodes and merge the results
        all_responses = await self._session._client._client.info_on_all_nodes(f"sets/{namespace}")

        # Extract set names from all node responses
        set_set: Set[str] = set()
        for node_response in all_responses.values():
            # Response format is typically {"sets": "set1,set2,set3"}
            for value in node_response.values():
                if isinstance(value, str) and value:
                    # Split comma-separated set list
                    set_set.update([s.strip() for s in value.split(",") if s.strip()])

        return sorted(list(set_set))

    async def secondary_indexes(self, namespace: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Get information about all secondary indexes.

        Args:
            namespace: Optional namespace filter. If provided, only returns
                      indexes for that namespace.

        Returns:
            A list of dictionaries containing secondary index information.
        """
        all_responses = await self._session._client._client.info_on_all_nodes("sindex-list")

        index_map: Dict[str, Dict[str, str]] = {}

        for node_response in all_responses.values():
            for value in node_response.values():
                if isinstance(value, str) and value:
                    for entry in value.split(";"):
                        entry = entry.strip()
                        if not entry:
                            continue

                        fields: Dict[str, str] = {}
                        for token in entry.split(":"):
                            if "=" in token:
                                k, v = token.split("=", 1)
                                fields[k] = v

                        index_name = fields.get("indexname", "")
                        ns = fields.get("ns", "")
                        if not index_name or not ns:
                            continue

                        if namespace and ns != namespace:
                            continue

                        if index_name not in index_map:
                            index_map[index_name] = {
                                "namespace": ns,
                                "set": fields.get("set", ""),
                                "bin": fields.get("bin", ""),
                                "name": index_name,
                            }
                            if "type" in fields:
                                index_map[index_name]["type"] = fields["type"]
                            if "state" in fields:
                                index_map[index_name]["state"] = fields["state"]

        return list(index_map.values())

    async def secondary_index_details(
        self, namespace: str, index_name: str
    ) -> Optional[Dict[str, str]]:
        """
        Get detailed information about a specific secondary index.

        Args:
            namespace: The namespace containing the index.
            index_name: The name of the index.

        Returns:
            A dictionary containing index details, or None if not found.
        """
        try:
            response = await self._session._client._client.info(f"sindex/{namespace}/{index_name}")
            if not response:
                return None
            # Check if response indicates index doesn't exist
            # Response format for non-existent: {'sindex/ns/name': 'ERROR:201:no index'}
            expected_key = f"sindex/{namespace}/{index_name}"
            if expected_key in response and "ERROR:201:no index" in str(response[expected_key]):
                return None
            return response
        except Exception:
            log.debug(
                "secondary_index_details(%s, %s) failed",
                namespace, index_name, exc_info=True,
            )
            return None

    async def is_cluster_stable(self) -> bool:
        """
        Check if all nodes agree on the current cluster state.

        Returns:
            True if the cluster is stable, False otherwise.
        """
        # Get cluster state from all nodes
        all_responses = await self._session._client._client.info_on_all_nodes("cluster-stable")

        if not all_responses:
            return False

        # Check if all nodes report "true" for cluster-stable
        for node_response in all_responses.values():
            for value in node_response.values():
                if isinstance(value, str):
                    # cluster-stable returns "true" or "false"
                    if value.lower() != "true":
                        return False

        return True

    async def get_cluster_size(self) -> int:
        """
        Get the number of nodes in the cluster.

        Returns:
            The number of nodes in the cluster.
        """
        node_names = await self._session._client._client.node_names()
        return len(node_names)

    async def info(self, command: str) -> Dict[str, str]:
        """
        Execute a raw info command against the cluster.

        Args:
            command: The info command to execute (e.g., "statistics", "build").

        Returns:
            A dictionary containing the info command response as key-value pairs.
        """
        return await self._session._client._client.info(command)

    async def info_on_all_nodes(self, command: str) -> Dict[str, Dict[str, str]]:
        """
        Execute a raw info command against all nodes in the cluster.

        Args:
            command: The info command to execute (e.g., "statistics", "build").

        Returns:
            A dictionary mapping node names to their response dictionaries.
        """
        return await self._session._client._client.info_on_all_nodes(command)

