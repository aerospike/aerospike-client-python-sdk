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

"""Shared response parsing for the info-command helpers.

The two info surfaces differ only in *dispatch* — one awaits the async PAC
client, the other calls the ``*_blocking`` PAC entries — but the info-protocol
responses they get back are parsed identically. That parsing lives here, once,
as stateless static helpers so the two trees cannot drift on how a response is
interpreted.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set


class InfoCommandsBase:
    """Stateless info-response parsers shared by the async and sync info helpers.

    Holds no state and constructs nothing: each leaf keeps its own constructor
    and its own dispatch terminal (async ``await`` vs blocking), then hands the
    raw response dict to one of these helpers. Defining the parsing once means a
    server-response format tweak lands in exactly one place.
    """

    @staticmethod
    def _merge_scalar_set(responses: Dict[str, Dict[str, str]]) -> Set[str]:
        """Collect every non-empty scalar value across all node responses.

        Shape: ``{node: {key: "value"}}`` -> ``{"value", ...}`` (stripped).
        """
        out: Set[str] = set()
        for node_response in responses.values():
            for value in node_response.values():
                if isinstance(value, str) and value:
                    out.add(value.strip())
        return out

    @staticmethod
    def _merge_delimited_set(
        responses: Dict[str, Dict[str, str]], sep: str
    ) -> Set[str]:
        """Split ``sep``-delimited values across all node responses into a set.

        Shape: ``{node: {key: "a<sep>b<sep>c"}}`` -> ``{"a", "b", "c"}`` (each
        item stripped, empties dropped).
        """
        out: Set[str] = set()
        for node_response in responses.values():
            for value in node_response.values():
                if isinstance(value, str) and value:
                    out.update(item.strip() for item in value.split(sep) if item.strip())
        return out

    @staticmethod
    def _parse_sindex_list(
        responses: Dict[str, Dict[str, str]], namespace: Optional[str]
    ) -> List[Dict[str, str]]:
        """Parse ``sindex-list`` responses into de-duplicated index records.

        Each entry is a ``:``-separated list of ``key=value`` tokens; records
        are keyed by ``indexname`` (first seen wins) and optionally filtered to
        *namespace*.
        """
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

    @staticmethod
    def _interpret_namespace_details(
        response: Optional[Dict[str, str]], namespace: str
    ) -> Optional[Dict[str, str]]:
        """Return the namespace-details response, or ``None`` when unknown.

        A non-existent namespace reports ``{"namespace/<name>": "type=unknown"}``.
        """
        if not response:
            return None
        expected_key = f"namespace/{namespace}"
        if expected_key in response and str(response[expected_key]).strip() == "type=unknown":
            return None
        return response

    @staticmethod
    def _interpret_sindex_details(
        response: Optional[Dict[str, str]], namespace: str, index_name: str
    ) -> Optional[Dict[str, str]]:
        """Return the sindex-details response, or ``None`` when missing.

        A non-existent index reports ``{"sindex/<ns>/<name>": "ERROR:201:no index"}``.
        """
        if not response:
            return None
        expected_key = f"sindex/{namespace}/{index_name}"
        if expected_key in response and "ERROR:201:no index" in str(response[expected_key]):
            return None
        return response

    @staticmethod
    def _all_nodes_stable(responses: Dict[str, Dict[str, str]]) -> bool:
        """Return whether every node reported ``cluster-stable=true``."""
        if not responses:
            return False
        for node_response in responses.values():
            for value in node_response.values():
                if isinstance(value, str) and value.lower() != "true":
                    return False
        return True
