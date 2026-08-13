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

"""Parse ``sindex-list`` info responses for :meth:`Session.list_indexes`."""

from __future__ import annotations

from typing import Dict, List


def _parse_sindex_list(raw_responses: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    """Parse ``info_on_all_nodes("sindex-list")`` into deduplicated index dicts.

    Server response is semicolon-separated entries where each entry contains
    colon-separated ``key=value`` pairs, e.g.::

        ns=test:indexname=age_idx:set=users:bin=age:type=numeric:indextype=default:...
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
                index_name = fields.get("indexname")
                if not index_name or index_name in index_map:
                    continue
                ns = fields.get("ns")
                bin_name = fields.get("bin")
                if not ns or not bin_name:
                    continue
                rec: Dict[str, str] = {
                    "ns": ns,
                    "set": fields.get("set", ""),
                    "bin": bin_name,
                    "indexname": index_name,
                }
                if "type" in fields:
                    rec["type"] = fields["type"]
                if "indextype" in fields:
                    rec["indextype"] = fields["indextype"]
                if "context" in fields:
                    rec["context"] = fields["context"]
                index_map[index_name] = rec
    return list(index_map.values())


def parse_index_list(raw_responses: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    """Parse a raw ``sindex-list`` response into public index descriptors.

    Wraps :func:`_parse_sindex_list` and normalizes the server's field names
    to the public shape returned by ``list_indexes``: ``namespace``, ``set``,
    ``bin``, ``name``, plus ``type`` / ``index_type`` / ``context`` when the
    server reports them.
    """
    indexes: List[Dict[str, str]] = []
    for entry in _parse_sindex_list(raw_responses):
        rec: Dict[str, str] = {
            "namespace": entry.get("ns", ""),
            "set": entry.get("set", ""),
            "bin": entry.get("bin", ""),
            "name": entry.get("indexname", ""),
        }
        if "type" in entry:
            rec["type"] = entry["type"]
        if "indextype" in entry:
            rec["index_type"] = entry["indextype"]
        if "context" in entry:
            rec["context"] = entry["context"]
        indexes.append(rec)
    return indexes
