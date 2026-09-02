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

import re
from typing import Dict, Optional

from aerospike_sdk.info_shared import (
    parse_info_body,
    parse_info_records,
    single_info_body,
)


# ``file[0]`` / ``device[1]`` name a path; ``file[0].free_wblocks`` is a counter
# on that path. The optional third group is what tells them apart, and what
# regroups the flattened entries back into one view per location.
_STORAGE_FILE_KEY = re.compile(r"(file|device)\[(\d+)\](?:\.(.+))?")


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
    def storage_engine(self) -> "StorageEngine":
        """The namespace's ``storage-engine`` section as a typed view.

        Built on access from the keys already held, so reading it costs nothing
        for a caller that never asks.
        """
        return StorageEngine.from_namespace(self)

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


class SetDetail(dict):
    """One set's record from a ``sets/<namespace>`` info response.

    A ``dict`` of the raw wire keys, with properties over the fields worth
    converting. The wire spells the identity fields ``ns`` and ``set``; both
    are also reachable under the names used elsewhere in the SDK.

    Example::

        for detail in await session.info().sets("customers"):
            if detail.objects > 1_000_000:
                print(f"{detail.name}: {detail.objects} records")

    See Also:
        :meth:`~aerospike_sdk.aio.info.InfoCommands.sets`
    """

    __slots__ = ()

    @classmethod
    def from_body(cls, body: str) -> "list[SetDetail]":
        """Build one view per set from a raw ``sets/<namespace>`` body."""
        return [cls(record) for record in parse_info_records(body)]

    @property
    def namespace(self) -> str:
        """Namespace the set belongs to (wire key ``ns``)."""
        return self.get("ns", "")

    @property
    def name(self) -> str:
        """Set name (wire key ``set``)."""
        return self.get("set", "")

    @property
    def objects(self) -> int:
        """Records currently in the set."""
        return _as_int(self, "objects")

    @property
    def tombstones(self) -> int:
        """Tombstones left by durable deletes."""
        return _as_int(self, "tombstones")

    @property
    def data_used_bytes(self) -> int:
        """Bytes of record data the set occupies."""
        return _as_int(self, "data_used_bytes")

    @property
    def sindexes(self) -> int:
        """Secondary indexes defined on the set."""
        return _as_int(self, "sindexes")

    @property
    def default_ttl(self) -> int:
        """Per-set default TTL in seconds; ``0`` means the namespace default."""
        return _as_int(self, "default-ttl")

    @property
    def truncating(self) -> bool:
        """Whether a truncate is in progress."""
        return _as_bool(self, "truncating")

    @property
    def index_populating(self) -> bool:
        """Whether a secondary index on this set is still being built."""
        return _as_bool(self, "index_populating")

    @property
    def disable_eviction(self) -> bool:
        """Whether eviction is disabled for the set."""
        return _as_bool(self, "disable-eviction")

    @property
    def stop_writes_count(self) -> int:
        """Record count at which writes to the set are refused; ``0`` is unlimited."""
        return _as_int(self, "stop-writes-count")

    @property
    def stop_writes_size(self) -> int:
        """Byte size at which writes to the set are refused; ``0`` is unlimited."""
        return _as_int(self, "stop-writes-size")


class Sindex(dict):
    """One secondary index as reported by ``sindex-list``.

    Keys are the SDK's normalized names -- ``namespace``, ``set``, ``bin``,
    ``name`` -- not the wire's ``ns`` / ``indexname`` / ``indextype``.

    Example::

        for index in await session.info().secondary_indexes("customers"):
            if not index.is_ready:
                print(f"{index.name} is still building")

    See Also:
        :meth:`~aerospike_sdk.aio.info.InfoCommands.secondary_indexes`
    """

    __slots__ = ()

    @property
    def name(self) -> str:
        """Index name."""
        return self.get("name", "")

    @property
    def namespace(self) -> str:
        """Namespace the index is defined in."""
        return self.get("namespace", "")

    @property
    def set_name(self) -> str:
        """Set the index covers; empty when it spans the whole namespace."""
        return self.get("set", "")

    @property
    def bin_name(self) -> str:
        """Indexed bin. Empty for an expression index, which has no single bin."""
        return self.get("bin", "")

    @property
    def index_type(self) -> str:
        """Value type indexed: ``numeric``, ``string``, ``geo2dsphere``, ``blob``."""
        return self.get("type", "")

    @property
    def collection_type(self) -> str:
        """Collection the index walks: ``default``, ``list``, ``mapkeys``, ``mapvalues``."""
        return self.get("index_type", "")

    @property
    def state(self) -> str:
        """Server-reported state: ``RW`` once readable and writable, ``WO``
        while the index is still being written."""
        return self.get("state", "")

    @property
    def is_ready(self) -> bool:
        """Whether the index is built and serving queries."""
        return self.state == "RW"


class SindexDetail(dict):
    """Parsed ``sindex/<namespace>/<index>`` info response for one index.

    Counters describing a single index's storage and load progress.

    Example::

        detail = await session.info().secondary_index_details("customers", "by_age")
        if detail is not None and detail.load_pct < 100:
            print(f"{detail.load_pct}% built")

    See Also:
        :meth:`~aerospike_sdk.aio.info.InfoCommands.secondary_index_details`
    """

    __slots__ = ()

    @classmethod
    def from_response(
        cls, response: Optional[Dict[str, str]], namespace: str, index_name: str
    ) -> "Optional[SindexDetail]":
        """Build from a raw ``info`` response, or ``None`` when the index is absent.

        An index the server does not know answers ``ERROR:201:no index``, which
        is reported as ``None`` rather than an empty view.
        """
        command = f"sindex/{namespace}/{index_name}"
        body = single_info_body(response, command)
        if body is None or "ERROR:201:no index" in body:
            return None
        return cls(parse_info_body(body))

    @property
    def entries(self) -> int:
        """Index entries currently held."""
        return _as_int(self, "entries")

    @property
    def used_bytes(self) -> int:
        """Bytes of memory the index occupies."""
        return _as_int(self, "used_bytes")

    @property
    def entries_per_bval(self) -> int:
        """Average entries sharing one indexed value."""
        return _as_int(self, "entries_per_bval")

    @property
    def entries_per_rec(self) -> int:
        """Average entries contributed by one record."""
        return _as_int(self, "entries_per_rec")

    @property
    def load_pct(self) -> int:
        """Build progress as a percentage; ``100`` once fully populated."""
        return _as_int(self, "load_pct")

    @property
    def is_ready(self) -> bool:
        """Whether the index has finished building."""
        return self.load_pct >= 100


class StorageFileDetail(dict):
    """One file or device backing a namespace, with its own counters.

    The wire reports these indexed and flattened into the namespace response --
    ``storage-engine.file[0]`` names the path and
    ``storage-engine.file[0].free_wblocks`` counts against it -- so a caller
    reading them raw has to know the index and reassemble the group. This view
    is that group, with the path alongside its counters.

    Example::

        for storage_file in detail.storage_engine.files:
            if storage_file.read_errors:
                print(f"{storage_file.path}: {storage_file.read_errors} read errors")

    See Also:
        :attr:`StorageEngine.files`
    """

    __slots__ = ()

    @property
    def path(self) -> str:
        """Configured file or raw-device path."""
        return self.get("path", "")

    @property
    def used_bytes(self) -> int:
        """Bytes currently occupied on this location."""
        return _as_int(self, "used_bytes")

    @property
    def free_wblocks(self) -> int:
        """Write blocks available for new records."""
        return _as_int(self, "free_wblocks")

    @property
    def read_errors(self) -> int:
        """Reads that failed against this location."""
        return _as_int(self, "read_errors")

    @property
    def write_q(self) -> int:
        """Writes queued for this location."""
        return _as_int(self, "write_q")

    @property
    def writes(self) -> int:
        """Write blocks written."""
        return _as_int(self, "writes")

    @property
    def partial_writes(self) -> int:
        """Partially filled blocks written."""
        return _as_int(self, "partial_writes")

    @property
    def defrag_q(self) -> int:
        """Blocks queued for defragmentation."""
        return _as_int(self, "defrag_q")

    @property
    def defrag_reads(self) -> int:
        """Blocks read by the defragmenter."""
        return _as_int(self, "defrag_reads")

    @property
    def defrag_writes(self) -> int:
        """Blocks rewritten by the defragmenter."""
        return _as_int(self, "defrag_writes")

    @property
    def defrag_partial_writes(self) -> int:
        """Partially filled blocks written by the defragmenter."""
        return _as_int(self, "defrag_partial_writes")

    @property
    def age(self) -> int:
        """Age of the oldest record on this location, or ``-1`` when unreported."""
        return _as_int(self, "age", default=-1)


class StorageEngine(dict):
    """The ``storage-engine`` section of a namespace's info response.

    The wire reports this section flat, prefixed: ``storage-engine=device``
    names the engine and ``storage-engine.defrag-lwm-pct=50`` configures it.
    This view holds those keys with the prefix removed -- the engine name under
    ``engine``, everything else under its own name -- so the section reads as a
    unit rather than as a prefix search over the whole namespace response.

    Per-file counters stay addressable under their raw names, since they are
    indexed and open-ended::

        engine = detail.storage_engine
        for path in engine.files:
            print(path)
        free = engine["file[0].free_wblocks"]

    Example::

        detail = await session.info().namespace_details("customers")
        engine = detail.storage_engine
        if engine.is_memory:
            print("namespace is memory-backed; data is lost on restart")
        elif engine.compression != "none":
            print(f"compressed with {engine.compression}")

    See Also:
        :attr:`NamespaceDetail.storage_engine`
    """

    __slots__ = ()

    _PREFIX = "storage-engine"

    @classmethod
    def from_namespace(cls, detail: Dict[str, str]) -> "StorageEngine":
        """Extract the storage-engine section from a namespace response."""
        section = {}
        for key, value in detail.items():
            if key == cls._PREFIX:
                section["engine"] = value
            elif key.startswith(cls._PREFIX + "."):
                section[key[len(cls._PREFIX) + 1:]] = value
        return cls(section)

    @property
    def engine(self) -> str:
        """Configured engine: ``memory``, ``device``, or ``""`` when unreported."""
        return self.get("engine", "")

    @property
    def is_memory(self) -> bool:
        """Whether records live only in memory, and do not survive a restart."""
        return self.engine == "memory"

    @property
    def is_device(self) -> bool:
        """Whether records are persisted to files or raw devices."""
        return self.engine == "device"

    @property
    def compression(self) -> str:
        """Compression algorithm: ``none``, ``lz4``, ``snappy``, or ``zstd``."""
        return self.get("compression", "none")

    @property
    def commit_to_device(self) -> bool:
        """Whether a write is acknowledged only after reaching the device."""
        return _as_bool(self, "commit-to-device")

    @property
    def defrag_lwm_pct(self) -> int:
        """Block-fill percentage below which a write block is defragmented."""
        return _as_int(self, "defrag-lwm-pct")

    @property
    def files(self) -> "list[StorageFileDetail]":
        """The backing files or devices, in the order the server indexes them.

        Regroups the flattened ``file[N]`` / ``device[N]`` entries: the
        un-suffixed key names the path, and the suffixed ones are that path's
        counters. Paths and counters are otherwise interleaved in one flat
        mapping with the index as the only thing tying them together.
        """
        grouped: "dict[tuple, dict]" = {}
        for key, value in self.items():
            match = _STORAGE_FILE_KEY.fullmatch(key)
            if not match:
                continue
            kind, index, field = match.group(1), int(match.group(2)), match.group(3)
            entry = grouped.setdefault((kind, index), {})
            entry["path" if field is None else field] = value
        return [StorageFileDetail(grouped[k]) for k in sorted(grouped)]
