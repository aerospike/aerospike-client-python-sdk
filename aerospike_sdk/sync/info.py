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

"""SyncInfoCommands — synchronous info-command helpers using PAC ``_blocking``.

Never touches asyncio. Each call routes through PAC's ``info_blocking`` /
``info_on_all_nodes_blocking`` and parses the responses the same way the async
:class:`~aerospike_sdk.aio.info.InfoCommands` does.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

log = logging.getLogger("aerospike_sdk.sync.info")


def _merge_set_values(responses: Dict[str, Dict[str, str]]) -> Set[str]:
    """Flatten ``{node: {key: 'a;b;c'}}`` responses into a set of items."""
    out: Set[str] = set()
    for node_response in responses.values():
        for value in node_response.values():
            if isinstance(value, str) and value:
                out.update(s.strip() for s in value.split(";") if s.strip())
    return out


class SyncInfoCommands:
    """Synchronous high-level info-command helpers.

    Constructed by :meth:`SyncSession.info` (no args). Calls PAC's
    ``info_blocking`` / ``info_on_all_nodes_blocking`` directly — no
    asyncio loop is involved.
    """

    def __init__(self, pac_client: Any) -> None:
        """Pair with the PAC ``aerospike_async.Client`` from the session."""
        self._pac = pac_client

    def build(self) -> Set[str]:
        """Build strings from every node."""
        responses = self._pac.info_on_all_nodes_blocking("build")
        out: Set[str] = set()
        for node_response in responses.values():
            for value in node_response.values():
                if isinstance(value, str) and value:
                    out.add(value.strip())
        return out

    def namespaces(self) -> Set[str]:
        """Namespace names across the cluster."""
        responses = self._pac.info_on_all_nodes_blocking("namespaces")
        return _merge_set_values(responses)

    def namespace_details(self, namespace: str) -> Optional[Dict[str, str]]:
        """Per-namespace info; ``None`` when the namespace is unknown."""
        try:
            response = self._pac.info_blocking(f"namespace/{namespace}")
        except Exception:
            log.debug("namespace_details(%s) failed", namespace, exc_info=True)
            return None
        if not response:
            return None
        expected_key = f"namespace/{namespace}"
        if expected_key in response and str(response[expected_key]).strip() == "type=unknown":
            return None
        return response

    def sets(self, namespace: str) -> List[str]:
        """Set names in ``namespace``."""
        responses = self._pac.info_on_all_nodes_blocking(f"sets/{namespace}")
        out: Set[str] = set()
        for node_response in responses.values():
            for value in node_response.values():
                if isinstance(value, str) and value:
                    out.update(s.strip() for s in value.split(",") if s.strip())
        return sorted(out)

    def secondary_indexes(self, namespace: Optional[str] = None) -> List[Dict[str, str]]:
        """All secondary indexes (optionally filtered by namespace)."""
        responses = self._pac.info_on_all_nodes_blocking("sindex-list")
        index_map: Dict[str, Dict[str, str]] = {}
        for node_response in responses.values():
            for value in node_response.values():
                if not isinstance(value, str) or not value:
                    continue
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
                        entry_map = {
                            "namespace": ns,
                            "set": fields.get("set", ""),
                            "bin": fields.get("bin", ""),
                            "name": index_name,
                        }
                        if "type" in fields:
                            entry_map["type"] = fields["type"]
                        if "state" in fields:
                            entry_map["state"] = fields["state"]
                        index_map[index_name] = entry_map
        return list(index_map.values())

    def secondary_index_details(
        self, namespace: str, index_name: str
    ) -> Optional[Dict[str, str]]:
        """Details for one secondary index; ``None`` when missing."""
        try:
            response = self._pac.info_blocking(f"sindex/{namespace}/{index_name}")
        except Exception:
            log.debug(
                "secondary_index_details(%s, %s) failed",
                namespace, index_name, exc_info=True,
            )
            return None
        if not response:
            return None
        expected_key = f"sindex/{namespace}/{index_name}"
        if expected_key in response and "ERROR:201:no index" in str(response[expected_key]):
            return None
        return response

    def is_cluster_stable(self) -> bool:
        """``True`` when every node reports ``cluster-stable=true``."""
        responses = self._pac.info_on_all_nodes_blocking("cluster-stable")
        if not responses:
            return False
        for node_response in responses.values():
            for value in node_response.values():
                if isinstance(value, str) and value.lower() != "true":
                    return False
        return True

    def get_cluster_size(self) -> int:
        """Number of cluster nodes."""
        return len(self._pac.node_names_blocking())

    def info(self, command: str) -> Dict[str, str]:
        """Raw info command against one random node."""
        return self._pac.info_blocking(command)

    def info_on_all_nodes(self, command: str) -> Dict[str, Dict[str, str]]:
        """Raw info command against every node."""
        return self._pac.info_on_all_nodes_blocking(command)
