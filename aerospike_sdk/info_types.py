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

"""Typed views over info-command responses.

An info response is a flat ``key=value`` document with several hundred keys, of
which any one caller reads a handful. These views therefore subclass ``dict``
and coerce **on property access** rather than up front: the raw keys stay
addressable, lookups stay at C speed, and a caller that reads two fields does
not pay to convert the other four hundred.
"""

from __future__ import annotations

from typing import Dict, Optional

from aerospike_sdk.info_shared import parse_info_body, single_info_body


def _as_bool(raw: Dict[str, str], key: str, default: bool = False) -> bool:
    """Read a server boolean, which is spelled ``true`` / ``false`` on the wire."""
    value = raw.get(key)
    return default if value is None else value == "true"


def _as_int(raw: Dict[str, str], key: str, default: int = 0) -> int:
    """Read a server integer, falling back to *default* when absent or malformed."""
    value = raw.get(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


class NamespaceDetail(dict):
    """Parsed ``namespace/<name>`` info response for a single namespace.

    A ``dict`` of the raw wire keys, so any key the server reports is reachable
    by name even when this class has no property for it. The properties cover
    the fields the SDK itself consults and convert on access.

    Because this *is* a mapping, raw access and typed access mix freely::

        detail = await session.info().namespace_details("test")
        if detail.strong_consistency:
            ...
        replication = detail["replication-factor"]   # no property needed

    Example::

        detail = await session.info().namespace_details("customers")
        if detail is None:
            raise RuntimeError("namespace 'customers' is not configured")
        # A TTL is only honored when expiration is running, or when the
        # namespace explicitly permits a TTL without it.
        if detail.nsup_period == 0 and not detail.allow_ttl_without_nsup:
            print("record expiration is disabled; TTLs will be rejected")

    See Also:
        :meth:`~aerospike_sdk.aio.info.InfoCommands.namespace_details`
        :meth:`~aerospike_sdk.aio.session.Session.namespace_sc_status`
    """

    __slots__ = ()

    @classmethod
    def from_body(cls, body: str) -> "NamespaceDetail":
        """Build a detail view from one raw ``namespace/<name>`` body."""
        return cls(parse_info_body(body))

    @classmethod
    def from_response(
        cls, response: Optional[Dict[str, str]], namespace: str
    ) -> "Optional[NamespaceDetail]":
        """Build from a raw ``info`` response, or ``None`` when the namespace is absent.

        A namespace the server does not know answers ``type=unknown``, which is
        reported as ``None`` rather than an empty view -- absent and
        present-but-default are different answers.
        """
        body = single_info_body(response, f"namespace/{namespace}")
        if body is None:
            return None
        detail = cls.from_body(body)
        return detail if detail.exists else None

    @property
    def exists(self) -> bool:
        """False when the server reports ``type=unknown`` (no such namespace)."""
        return self.get("type") != "unknown"

    @property
    def strong_consistency(self) -> bool:
        """True when the namespace runs in strong-consistency (SC) mode.

        Accepts either spelling of the key: server builds have reported it both
        hyphenated and underscored, and a missing key must not read as SC.
        """
        value = self.get("strong-consistency")
        if value is None:
            value = self.get("strong_consistency")
        return value == "true"

    @property
    def strong_consistency_reported(self) -> bool:
        """True when the response carried a strong-consistency flag at all.

        Distinguishes "reported false" from "not reported", which
        :attr:`strong_consistency` alone cannot -- an absent flag reads as
        false there, and callers that explain *why* a namespace is treated as
        non-SC need the difference.
        """
        return "strong-consistency" in self or "strong_consistency" in self

    @property
    def strong_consistency_allow_expunge(self) -> bool:
        """True when an SC namespace permits expunging records (data loss)."""
        return _as_bool(self, "strong-consistency-allow-expunge")

    @property
    def disallow_expunge(self) -> bool:
        """True when an AP namespace refuses to expunge records."""
        return _as_bool(self, "disallow-expunge")

    @property
    def nsup_period(self) -> int:
        """Expiration/eviction cycle in seconds; ``0`` disables the cycle."""
        return _as_int(self, "nsup-period")

    @property
    def allow_ttl_without_nsup(self) -> bool:
        """True when records may carry a TTL even with the cycle disabled.

        Read this alongside :attr:`nsup_period`: a TTL is rejected only when the
        cycle is disabled *and* this is false.
        """
        return _as_bool(self, "allow-ttl-without-nsup")

    @property
    def default_ttl(self) -> int:
        """Namespace default TTL in seconds; ``0`` means never expire."""
        return _as_int(self, "default-ttl")

    def __repr__(self) -> str:
        # The full response runs to several hundred keys, so identify the view
        # by the fields that decide behavior rather than dumping the document.
        return (
            f"NamespaceDetail(keys={len(self)}, exists={self.exists}, "
            f"strong_consistency={self.strong_consistency}, "
            f"nsup_period={self.nsup_period})"
        )
