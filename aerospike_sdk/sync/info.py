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

from aerospike_sdk.info_shared import InfoCommandsBase
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

    def namespace_details(self, namespace: str) -> Optional[Dict[str, str]]:
        """Per-namespace info; ``None`` when the namespace is unknown."""
        try:
            response = self._pac.info_blocking(f"namespace/{namespace}")
        except Exception:
            log.debug("namespace_details(%s) failed", namespace, exc_info=True)
            return None
        return self._interpret_namespace_details(response, namespace)

    def sets(self, namespace: str) -> List[str]:
        """Set names in ``namespace``."""
        responses = self._pac.info_on_all_nodes_blocking(f"sets/{namespace}")
        return sorted(self._merge_delimited_set(responses, ","))

    def secondary_indexes(self, namespace: Optional[str] = None) -> List[Dict[str, str]]:
        """All secondary indexes (optionally filtered by namespace)."""
        responses = self._pac.info_on_all_nodes_blocking("sindex-list")
        return self._parse_sindex_list(responses, namespace)

    def secondary_index_details(self, namespace: str, index_name: str) -> Optional[Dict[str, str]]:
        """Details for one secondary index; ``None`` when missing."""
        try:
            response = self._pac.info_blocking(f"sindex/{namespace}/{index_name}")
        except Exception:
            log.debug(
                "secondary_index_details(%s, %s) failed",
                namespace, index_name, exc_info=True,
            )
            return None
        return self._interpret_sindex_details(response, namespace, index_name)

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
