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

"""Parse ``sindex-list`` info responses for :meth:`Session.list_indexes` and info helpers."""

from __future__ import annotations

from typing import Dict, List, Optional


def parse_index_list(
    raw_responses: Dict[str, Dict[str, str]],
    *,
    namespace: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Parse ``info_on_all_nodes("sindex-list")`` into deduplicated index dicts.

    Each record uses the public shape: ``namespace``, ``set``, ``bin``, ``name``,
    plus ``type``, ``index_type``, ``context``, and ``state`` when the server
    reports them. Entries missing ``indexname`` or ``ns`` are skipped; a missing
    ``bin`` is preserved as ``""`` rather than dropping the index.

    Args:
        raw_responses: ``{node: {info_key: value}}`` from PAC info-on-all-nodes.
        namespace: When set, keep only indexes in this namespace.
    """
    index_map: Dict[str, Dict[str, str]] = {}
    for node_response in raw_responses.values():
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
                if namespace is not None and ns != namespace:
                    continue
                if index_name in index_map:
                    continue
                rec: Dict[str, str] = {
                    "namespace": ns,
                    "set": fields.get("set", ""),
                    "bin": fields.get("bin", ""),
                    "name": index_name,
                }
                if "type" in fields:
                    rec["type"] = fields["type"]
                if "indextype" in fields:
                    rec["index_type"] = fields["indextype"]
                if "context" in fields:
                    rec["context"] = fields["context"]
                if "state" in fields:
                    rec["state"] = fields["state"]
                index_map[index_name] = rec
    return list(index_map.values())
