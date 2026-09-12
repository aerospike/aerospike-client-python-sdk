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

from aerospike_sdk.index_list import parse_index_list
from aerospike_sdk.info_shared import InfoCommandsBase
from aerospike_sdk.info_types import (
    NamespaceDetail,
    SetDetail,
    Sindex,
    SindexDetail,
)
from aerospike_sdk.loggers import SdkLoggers

log = logging.getLogger(SdkLoggers.INFO)


class InfoCommands(InfoCommandsBase):
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
        all_responses = await self._session._client._client.info_on_all_nodes("build")
        return self._merge_scalar_set(all_responses)

    async def namespaces(self) -> Set[str]:
        """
        Get the list of namespaces from all nodes in the cluster.

        Returns:
            A set of namespace names from all nodes.
        """
        all_responses = await self._session._client._client.info_on_all_nodes("namespaces")
        return self._merge_delimited_set(all_responses, ";")

    async def namespace_details(self, namespace: str) -> Optional[NamespaceDetail]:
        """Get detailed configuration and statistics for one namespace.

        Args:
            namespace: The name of the namespace.

        Returns:
            A :class:`~aerospike_sdk.info_types.NamespaceDetail` -- a mapping of
            every key the server reported, with typed properties for the fields
            the SDK consults -- or ``None`` when the namespace is undefined.

        Example::

            detail = await session.info().namespace_details("customers")
            if detail is not None and detail.nsup_period == 0:
                print("record expiration is disabled on this namespace")

        See Also:
            :class:`~aerospike_sdk.info_types.NamespaceDetail`
        """
        try:
            response = await self._session._client._client.info(f"namespace/{namespace}")
        except Exception:
            log.debug("namespace_details(%s) failed", namespace, exc_info=True)
            return None
        return NamespaceDetail.from_response(response, namespace)

    async def sets(self, namespace: Optional[str] = None) -> List[SetDetail]:
        """
        Get every set with its counters, across the cluster.

        Args:
            namespace: Restrict to one namespace. Omit for every set in every
                namespace, which is what the bare ``sets`` info command answers.

        Returns:
            One :class:`~aerospike_sdk.info_types.SetDetail` per set, ordered by
            set name. Counters come from the first node reporting each set, so
            they describe that node's share rather than a cluster-wide total;
            :meth:`sets_per_node` keeps each node's own numbers.

        Example::

            # Every set in the cluster.
            for detail in await session.info().sets():
                print(detail.namespace, detail.name, detail.objects)

            # Or one namespace.
            for detail in await session.info().sets("customers"):
                print(detail.name, detail.data_used_bytes)

        See Also:
            :meth:`set`: One set by name.
            :meth:`sets_per_node`: Per-node counters instead of merged ones.
        """
        command = "sets" if namespace is None else f"sets/{namespace}"
        all_responses = await self._session._client._client.info_on_all_nodes(command)
        return self._merge_set_details(all_responses)

    async def set(self, namespace: str, name: str) -> Optional[SetDetail]:
        """
        Get one set's detail by name.

        Args:
            namespace: The name of the namespace.
            name: The set name.

        Returns:
            The :class:`~aerospike_sdk.info_types.SetDetail`, or ``None`` when
            the namespace reports no such set.

        Example::

            detail = await session.info().set("customers", "orders")
            if detail is not None and detail.truncating:
                print("a truncate is still running")
        """
        for detail in await self.sets(namespace):
            if detail.name == name:
                return detail
        return None

    async def secondary_indexes(self, namespace: Optional[str] = None) -> List[Sindex]:
        """
        Get information about all secondary indexes.

        Args:
            namespace: Optional namespace filter. If provided, only returns
                      indexes for that namespace.

        Returns:
            One :class:`~aerospike_sdk.info_types.Sindex` per index. These are
            mappings, so existing key access keeps working.
        """
        all_responses = await self._session._client._client.info_on_all_nodes("sindex-list")
        return [Sindex(entry) for entry in parse_index_list(all_responses, namespace=namespace)]

    async def secondary_index_details(
        self, namespace: str, index_name: str
    ) -> Optional[SindexDetail]:
        """
        Get detailed information about a specific secondary index.

        Args:
            namespace: The namespace containing the index.
            index_name: The name of the index.

        Returns:
            A :class:`~aerospike_sdk.info_types.SindexDetail`, or ``None`` when
            the index does not exist. This is a mapping of the parsed counters,
            not the raw ``{command: body}`` envelope.
        """
        try:
            response = await self._session._client._client.info(f"sindex/{namespace}/{index_name}")
        except Exception:
            log.debug(
                "secondary_index_details(%s, %s) failed",
                namespace, index_name, exc_info=True,
            )
            return None
        return SindexDetail.from_response(response, namespace, index_name)

    async def namespace_details_per_node(
        self, namespace: str
    ) -> Dict[str, NamespaceDetail]:
        """
        Get each node's own view of a namespace.

        The cluster-wide :meth:`namespace_details` asks one node. Config that
        has not finished propagating, or a node mid-restart, shows up only when
        every node is asked separately.

        Args:
            namespace: The name of the namespace.

        Returns:
            One :class:`~aerospike_sdk.info_types.NamespaceDetail` per node,
            keyed by node name. Nodes that do not host the namespace are
            omitted rather than mapped to an empty view.

        Example::

            per_node = await session.info().namespace_details_per_node("customers")
            modes = {node: d.strong_consistency for node, d in per_node.items()}
            if len(set(modes.values())) > 1:
                print(f"nodes disagree on consistency mode: {modes}")
        """
        responses = await self._session._client._client.info_on_all_nodes(
            f"namespace/{namespace}"
        )
        return self._per_node_namespace_details(responses, namespace)

    async def sets_per_node(self, namespace: str) -> Dict[str, List[SetDetail]]:
        """
        Get each node's own set counters for a namespace.

        Args:
            namespace: The name of the namespace.

        Returns:
            One list of :class:`~aerospike_sdk.info_types.SetDetail` per node,
            keyed by node name and ordered by set name.

        Example::

            per_node = await session.info().sets_per_node("customers")
            for node, details in per_node.items():
                total = sum(d.objects for d in details)
                print(f"{node}: {total} records")

        See Also:
            :meth:`sets`: One merged view across the cluster.
        """
        responses = await self._session._client._client.info_on_all_nodes(
            f"sets/{namespace}"
        )
        return self._per_node_set_details(responses)

    async def secondary_indexes_per_node(
        self, namespace: Optional[str] = None
    ) -> Dict[str, List[Sindex]]:
        """
        Get each node's own secondary-index list.

        Args:
            namespace: Optional namespace filter.

        Returns:
            One list of :class:`~aerospike_sdk.info_types.Sindex` per node,
            keyed by node name.

        Example::

            per_node = await session.info().secondary_indexes_per_node("customers")
            building = {n: [i.name for i in idx if not i.is_ready]
                        for n, idx in per_node.items()}

        See Also:
            :meth:`secondary_indexes`: One deduplicated list for the cluster.
        """
        responses = await self._session._client._client.info_on_all_nodes("sindex-list")
        return self._per_node_sindexes(responses, namespace)

    async def secondary_index_details_per_node(
        self, namespace: str, index_name: str
    ) -> Dict[str, SindexDetail]:
        """
        Get each node's own counters for one secondary index.

        Index build progress is per node, so this is what shows whether a
        rebuild has finished everywhere.

        Args:
            namespace: The namespace containing the index.
            index_name: The name of the index.

        Returns:
            One :class:`~aerospike_sdk.info_types.SindexDetail` per node, keyed
            by node name. Nodes that do not report the index are omitted.

        Example::

            per_node = await session.info().secondary_index_details_per_node(
                "customers", "by_age"
            )
            if not all(d.is_ready for d in per_node.values()):
                print("index is still building on some nodes")
        """
        responses = await self._session._client._client.info_on_all_nodes(
            f"sindex/{namespace}/{index_name}"
        )
        return self._per_node_sindex_details(responses, namespace, index_name)

    async def is_cluster_stable(self) -> bool:
        """
        Check if all nodes agree on the current cluster state.

        Returns:
            True if the cluster is stable, False otherwise.
        """
        all_responses = await self._session._client._client.info_on_all_nodes("cluster-stable")
        return self._all_nodes_stable(all_responses)

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

