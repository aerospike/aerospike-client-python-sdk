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

"""InfoCommands — synchronous info-command helpers using PAC ``_blocking``.

Never touches asyncio. Each call routes through PAC's ``info_blocking`` /
``info_on_all_nodes_blocking`` and parses the responses the same way the async
:class:`~aerospike_sdk.aio.info.InfoCommands` does.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

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
    """Synchronous high-level info-command helpers.

    Constructed by :meth:`~aerospike_sdk.sync.session.Session.info` (no args). Calls PAC's
    ``info_blocking`` / ``info_on_all_nodes_blocking`` directly — no
    asyncio loop is involved.
    """

    def __init__(self, pac_client: Any) -> None:
        """Pair with the PAC ``aerospike_async.Client`` from the session."""
        self._pac = pac_client

    def build(self) -> Set[str]:
        """Build strings from every node."""
        responses = self._pac.info_on_all_nodes_blocking("build")
        return self._merge_scalar_set(responses)

    def namespaces(self) -> Set[str]:
        """Namespace names across the cluster."""
        responses = self._pac.info_on_all_nodes_blocking("namespaces")
        return self._merge_delimited_set(responses, ";")

    def namespace_details(self, namespace: str) -> Optional[NamespaceDetail]:
        """Per-namespace info; ``None`` when the namespace is unknown.

        Returns a :class:`~aerospike_sdk.info_types.NamespaceDetail`: a mapping
        of every reported key, with typed properties for the fields the SDK
        consults.
        """
        try:
            response = self._pac.info_blocking(f"namespace/{namespace}")
        except Exception:
            log.debug("namespace_details(%s) failed", namespace, exc_info=True)
            return None
        return NamespaceDetail.from_response(response, namespace)

    def sets(self, namespace: Optional[str] = None) -> List[SetDetail]:
        """Every set with its counters, ordered by namespace then set name.

        Omit ``namespace`` for every set in every namespace, which is what the
        bare ``sets`` info command answers.

        Counters come from the first node reporting each set, so they describe
        that node's share rather than a cluster-wide total;
        :meth:`sets_per_node` keeps each node's own numbers.

        Args:
            namespace: Restrict to one namespace; omit for all of them.

        Returns:
            One :class:`~aerospike_sdk.info_types.SetDetail` per set.

        Example::

            for detail in session.info().sets("customers"):
                print(detail.name, detail.objects, detail.data_used_bytes)

        See Also:
            :meth:`set`: One set by name.
            :meth:`sets_per_node`: Per-node counters instead of merged ones.
        """
        command = "sets" if namespace is None else f"sets/{namespace}"
        responses = self._pac.info_on_all_nodes_blocking(command)
        return self._merge_set_details(responses)

    def set(self, namespace: str, name: str) -> Optional[SetDetail]:
        """One set's detail by name; ``None`` when the namespace has no such set.

        Args:
            namespace: The name of the namespace.
            name: The set name.

        Returns:
            The :class:`~aerospike_sdk.info_types.SetDetail`, or ``None``.
        """
        for detail in self.sets(namespace):
            if detail.name == name:
                return detail
        return None

    def secondary_indexes(self, namespace: Optional[str] = None) -> List[Sindex]:
        """All secondary indexes (optionally filtered by namespace).

        Returns one :class:`~aerospike_sdk.info_types.Sindex` per index. These
        are mappings, so existing key access keeps working.
        """
        responses = self._pac.info_on_all_nodes_blocking("sindex-list")
        return [Sindex(entry) for entry in parse_index_list(responses, namespace=namespace)]

    def secondary_index_details(
        self, namespace: str, index_name: str
    ) -> Optional[SindexDetail]:
        """Details for one secondary index; ``None`` when missing.

        Returns the parsed counters, not the raw ``{command: body}`` envelope.
        """
        try:
            response = self._pac.info_blocking(f"sindex/{namespace}/{index_name}")
        except Exception:
            log.debug(
                "secondary_index_details(%s, %s) failed",
                namespace, index_name, exc_info=True,
            )
            return None
        return SindexDetail.from_response(response, namespace, index_name)

    def namespace_details_per_node(self, namespace: str) -> Dict[str, NamespaceDetail]:
        """Each node's own view of ``namespace``, keyed by node name.

        The cluster-wide :meth:`namespace_details` asks one node; config that
        has not finished propagating shows up only when every node is asked.
        Nodes that do not host the namespace are omitted.

        Args:
            namespace: The name of the namespace.

        Returns:
            One :class:`~aerospike_sdk.info_types.NamespaceDetail` per node.

        Example::

            per_node = session.info().namespace_details_per_node("customers")
            modes = {node: d.strong_consistency for node, d in per_node.items()}
        """
        responses = self._pac.info_on_all_nodes_blocking(f"namespace/{namespace}")
        return self._per_node_namespace_details(responses, namespace)

    def sets_per_node(self, namespace: str) -> Dict[str, List[SetDetail]]:
        """Each node's own set counters for ``namespace``, keyed by node name.

        Args:
            namespace: The name of the namespace.

        Returns:
            One list of :class:`~aerospike_sdk.info_types.SetDetail` per node,
            ordered by set name.

        Example::

            for node, details in session.info().sets_per_node("customers").items():
                print(node, sum(d.objects for d in details))

        See Also:
            :meth:`sets`: One merged view across the cluster.
        """
        responses = self._pac.info_on_all_nodes_blocking(f"sets/{namespace}")
        return self._per_node_set_details(responses)

    def secondary_indexes_per_node(
        self, namespace: Optional[str] = None
    ) -> Dict[str, List[Sindex]]:
        """Each node's own secondary-index list, keyed by node name.

        Args:
            namespace: Optional namespace filter.

        Returns:
            One list of :class:`~aerospike_sdk.info_types.Sindex` per node.

        See Also:
            :meth:`secondary_indexes`: One deduplicated list for the cluster.
        """
        responses = self._pac.info_on_all_nodes_blocking("sindex-list")
        return self._per_node_sindexes(responses, namespace)

    def secondary_index_details_per_node(
        self, namespace: str, index_name: str
    ) -> Dict[str, SindexDetail]:
        """Each node's own counters for one index, keyed by node name.

        Index build progress is per node, so this is what shows whether a
        rebuild has finished everywhere. Nodes not reporting the index are
        omitted.

        Args:
            namespace: The namespace containing the index.
            index_name: The name of the index.

        Returns:
            One :class:`~aerospike_sdk.info_types.SindexDetail` per node.
        """
        responses = self._pac.info_on_all_nodes_blocking(
            f"sindex/{namespace}/{index_name}"
        )
        return self._per_node_sindex_details(responses, namespace, index_name)

    def is_cluster_stable(self) -> bool:
        """``True`` when every node reports ``cluster-stable=true``."""
        responses = self._pac.info_on_all_nodes_blocking("cluster-stable")
        return self._all_nodes_stable(responses)

    def get_cluster_size(self) -> int:
        """Number of cluster nodes."""
        return len(self._pac.node_names_blocking())

    def info(self, command: str) -> Dict[str, str]:
        """Raw info command against one random node."""
        return self._pac.info_blocking(command)

    def info_on_all_nodes(self, command: str) -> Dict[str, Dict[str, str]]:
        """Raw info command against every node."""
        return self._pac.info_on_all_nodes_blocking(command)


# Path-differentiated bare name is the committed convention (same as the aio
# class); the ``Sync``-prefixed alias stays importable for one deprecation
# cycle (removed at GA).
SyncInfoCommands = InfoCommands
