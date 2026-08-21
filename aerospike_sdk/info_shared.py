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

from typing import Dict, Optional, Set


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
