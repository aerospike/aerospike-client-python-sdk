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

from typing import Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:  # info_types imports this module; annotation only.
    from aerospike_sdk.info_types import (
        NamespaceDetail,
        SetDetail,
        Sindex,
        SindexDetail,
    )


def parse_info_body(body: str) -> Dict[str, str]:
    """Split one info-protocol body into its ``key=value`` pairs.

    A body is a flat ``;``-delimited document -- several hundred pairs for a
    namespace -- so this is the hot part of reading any info response. Keys and
    values are taken verbatim: the wire format does not pad around ``=`` or
    ``;``, and stripping every half of every pair costs more than the split
    itself. Values may contain ``=``, so only the first one separates.

    Args:
        body: One info response body, e.g. ``"type=device;nsup-period=120"``.

    Returns:
        Mapping of key to raw string value. Fragments without ``=`` are skipped.

    Example::

        pairs = parse_info_body("type=device;nsup-period=120")
        assert pairs["nsup-period"] == "120"
    """
    return dict(
        pair.split("=", 1) for pair in body.split(";") if "=" in pair
    )


def parse_info_records(body: str) -> List[Dict[str, str]]:
    """Split an info body that carries one record per entity.

    ``sets/<ns>`` and ``sindex-list`` answer with ``;``-separated records whose
    fields are ``:``-separated -- a different shape from the flat ``;``-separated
    document :func:`parse_info_body` handles. Splitting one with the other's
    delimiter silently yields a single element holding the whole body, which
    looks like a valid one-record answer.

    Args:
        body: One info response body, e.g. ``"ns=test:set=a;ns=test:set=b"``.

    Returns:
        One mapping per record, in the order the server reported them. Records
        with no ``key=value`` field at all are skipped.

    Example::

        records = parse_info_records("ns=test:set=users;ns=test:set=orders")
        assert [r["set"] for r in records] == ["users", "orders"]
    """
    records = []
    for entry in body.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        fields = dict(
            token.split("=", 1) for token in entry.split(":") if "=" in token
        )
        if fields:
            records.append(fields)
    return records


def single_info_body(response: Optional[Dict[str, str]], command: str) -> Optional[str]:
    """Pull the one body out of a single-command info response.

    ``info(cmd)`` answers ``{cmd: body}``, so the command itself is the key.
    Falls back to the sole value when the key does not match, which keeps this
    tolerant of a server that echoes the command differently than it was sent.

    Args:
        response: Raw response mapping, or ``None``/empty.
        command: Command that was sent, e.g. ``"namespace/test"``.

    Returns:
        The body string, or ``None`` when the response carried none.
    """
    if not response:
        return None
    body = response.get(command)
    if body is not None:
        return body
    return next(iter(response.values())) if len(response) == 1 else None


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
    def _per_node_namespace_details(
        responses: Dict[str, Dict[str, str]], namespace: str
    ) -> Dict[str, "NamespaceDetail"]:
        """One detail view per node, keyed by node name.

        Nodes the namespace is absent from are omitted rather than mapped to an
        empty view -- a node that does not host the namespace and one that
        reports nothing about it are different answers.
        """
        from aerospike_sdk.info_types import NamespaceDetail

        out: Dict[str, "NamespaceDetail"] = {}
        for node, node_response in responses.items():
            detail = NamespaceDetail.from_response(node_response, namespace)
            if detail is not None:
                out[node] = detail
        return out

    @staticmethod
    def _per_node_set_details(
        responses: Dict[str, Dict[str, str]]
    ) -> Dict[str, List["SetDetail"]]:
        """Per-node set detail, keyed by node name.

        Unlike the merged view, this keeps each node's own counters, which is
        the point: object counts differ per node and merging picks one.
        """
        from aerospike_sdk.info_types import SetDetail

        out: Dict[str, List["SetDetail"]] = {}
        for node, node_response in responses.items():
            details: List["SetDetail"] = []
            for value in node_response.values():
                if isinstance(value, str) and value:
                    details.extend(SetDetail.from_body(value))
            out[node] = sorted(details, key=lambda d: (d.namespace, d.name))
        return out

    @staticmethod
    def _per_node_sindexes(
        responses: Dict[str, Dict[str, str]], namespace: Optional[str] = None
    ) -> Dict[str, List["Sindex"]]:
        """Per-node secondary-index lists, keyed by node name.

        A node mid-rebuild reports an index in a different state than its peers,
        which the deduplicated cluster-wide list cannot show.
        """
        from aerospike_sdk.index_list import parse_index_list
        from aerospike_sdk.info_types import Sindex

        return {
            node: [Sindex(entry) for entry in parse_index_list({node: body}, namespace=namespace)]
            for node, body in responses.items()
        }

    @staticmethod
    def _per_node_sindex_details(
        responses: Dict[str, Dict[str, str]], namespace: str, index_name: str
    ) -> Dict[str, "SindexDetail"]:
        """Per-node index counters, keyed by node name; nodes without it omitted."""
        from aerospike_sdk.info_types import SindexDetail

        out: Dict[str, "SindexDetail"] = {}
        for node, node_response in responses.items():
            detail = SindexDetail.from_response(node_response, namespace, index_name)
            if detail is not None:
                out[node] = detail
        return out

    @staticmethod
    def _merge_set_details(responses: Dict[str, Dict[str, str]]) -> List["SetDetail"]:
        """Collect one ``SetDetail`` per set across every node's response.

        A set is reported by each node that holds part of it, so the same set
        arrives repeatedly; the first record for it wins. Counters are therefore
        that node's view rather than a cluster-wide total, which is the same
        contract the raw info command has.

        Keyed by namespace *and* name: set names are only unique within a
        namespace, and the unfiltered ``sets`` command spans all of them, so
        keying by name alone would silently drop same-named sets.
        """
        from aerospike_sdk.info_types import SetDetail

        by_set: Dict[tuple, "SetDetail"] = {}
        for node_response in responses.values():
            for value in node_response.values():
                if not isinstance(value, str) or not value:
                    continue
                for detail in SetDetail.from_body(value):
                    by_set.setdefault((detail.namespace, detail.name), detail)
        return [by_set[key] for key in sorted(by_set)]

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
