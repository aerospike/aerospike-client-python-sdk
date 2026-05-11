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

"""Background monitor that caches secondary index metadata for transparent
filter generation.

The :class:`IndexesMonitor` periodically queries the cluster via
``sindex-list`` and ``sindex-stat`` info commands, converts the responses into
:class:`~aerospike_sdk.ael.filter_gen.Index` objects, and stores them in
per-namespace :class:`~aerospike_sdk.ael.filter_gen.IndexContext` caches.

This module is intentionally at the SDK layer (not in PAC or core) — index
metadata is only needed by the AEL filter-generation pipeline.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Dict, List, Optional

from aerospike_async.exceptions import AerospikeError as PacError
from aerospike_sdk.ael.filter_gen import Index, IndexContext, IndexTypeEnum

if TYPE_CHECKING:  # Not unused — avoids circular import; used in type annotations only.
    from aerospike_async import Client

log = logging.getLogger("aerospike_sdk.index_monitor")

_DEFAULT_REFRESH_INTERVAL: float = 5.0
_DEFAULT_READY_TIMEOUT: float = 30.0

_SINDEX_TYPE_MAP: Dict[str, IndexTypeEnum] = {
    "numeric": IndexTypeEnum.NUMERIC,
    # Server 8.1.2+ reports integer indexes as ``type=integer``; older servers
    # report ``type=numeric``. Both collapse to the same internal enum.
    "integer": IndexTypeEnum.NUMERIC,
    "string": IndexTypeEnum.STRING,
    "geo2dsphere": IndexTypeEnum.GEO2D_SPHERE,
    "blob": IndexTypeEnum.BLOB,
}


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


def _parse_entries_per_bval(raw_response: Dict[str, str]) -> Optional[float]:
    """Extract ``entries_per_bval`` from an ``sindex-stat`` info response."""
    for value in raw_response.values():
        if not isinstance(value, str):
            continue
        for token in value.split(";"):
            token = token.strip()
            if token.startswith("entries_per_bval="):
                try:
                    return float(token.split("=", 1)[1])
                except (ValueError, IndexError):
                    pass
    return None


async def _stat_entries_per_bval(
    client: "Client", ns: str, indexname: str,
) -> Optional[float]:
    try:
        stat_resp = await client.info(
            f"sindex-stat:namespace={ns};indexname={indexname}",
        )
        return _parse_entries_per_bval(stat_resp)
    except PacError:
        log.debug(
            "Failed to fetch sindex-stat for %s.%s",
            ns, indexname, exc_info=True,
        )
        return None


async def _fetch_indexes(client: "Client") -> Dict[str, IndexContext]:
    """Fetch all secondary indexes and return per-namespace ``IndexContext`` caches."""
    raw = await client.info_on_all_nodes("sindex-list")
    entries = _parse_sindex_list(raw)

    bvals = await asyncio.gather(
        *(_stat_entries_per_bval(client, e["ns"], e["indexname"]) for e in entries),
    )

    indexes_by_ns: Dict[str, List[Index]] = {}
    for entry, bval in zip(entries, bvals):
        ns = entry["ns"]
        idx_type = _SINDEX_TYPE_MAP.get(
            entry.get("type", "").lower(), IndexTypeEnum.NUMERIC,
        )
        index = Index(
            bin=entry["bin"],
            index_type=idx_type,
            namespace=ns,
            name=entry["indexname"],
            bin_values_ratio=bval,
            set_name=entry.get("set") or None,
        )
        indexes_by_ns.setdefault(ns, []).append(index)

    return {
        ns: IndexContext.of(ns, idxs) for ns, idxs in indexes_by_ns.items()
    }


class IndexesMonitor:
    """Async background task that caches secondary index metadata.

    Start via :meth:`start` (typically called by ``Client.connect``).
    Retrieve cached data with :meth:`get_index_context`. Stop via
    :meth:`stop` (called by ``Client.close``).

        Example::

            monitor = IndexesMonitor()
            await monitor.start(client)
            await monitor.wait_until_ready()
            ctx = monitor.get_index_context("test")
            await monitor.stop()

    Args:
        refresh_interval: Seconds between cache refreshes (default 5.0).
    """

    def __init__(self, refresh_interval: float = _DEFAULT_REFRESH_INTERVAL) -> None:
        self._refresh_interval = refresh_interval
        self._cache: Dict[str, IndexContext] = {}
        self._task: Optional[asyncio.Task[None]] = None
        self._initial_ready = asyncio.Event()

    async def start(self, client: "Client") -> None:
        """Begin background refresh without blocking :meth:`Client.connect`.

        The first metadata fetch runs asynchronously. Callers that need index
        metadata immediately (for example AEL ``where()`` filter generation on
        a dataset query) should ``await`` :meth:`wait_until_ready` before
        relying on :meth:`get_index_context`.
        """
        if self._task is not None:
            return
        self._initial_ready.clear()
        self._task = asyncio.create_task(self._run(client))

    async def wait_until_ready(self, timeout: Optional[float] = None) -> None:
        """Block until the first index refresh attempt has finished.

        After this returns, :meth:`get_index_context` reflects the latest fetch
        (possibly empty if the cluster reports no secondary indexes).

        Args:
            timeout: Seconds to wait; ``None`` uses
                ``_DEFAULT_READY_TIMEOUT``.

        Raises:
            RuntimeError: If :meth:`start` was not called.
            asyncio.TimeoutError: If the first refresh does not complete in time.
        """
        if self._task is None:
            raise RuntimeError("IndexesMonitor.start must be called first")
        limit = _DEFAULT_READY_TIMEOUT if timeout is None else timeout
        await asyncio.wait_for(self._initial_ready.wait(), timeout=limit)

    async def stop(self) -> None:
        """Cancel the background refresh task."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def get_index_context(self, namespace: str) -> Optional[IndexContext]:
        """Return the cached ``IndexContext`` for *namespace*, or ``None``."""
        return self._cache.get(namespace)

    async def _run(self, client: "Client") -> None:
        """Periodic refresh loop."""
        while True:
            try:
                self._cache = await _fetch_indexes(client)
                total = sum(len(ctx.indexes) for ctx in self._cache.values())
                log.debug(
                    "Index cache refreshed: %d index(es) across %d namespace(s)",
                    total,
                    len(self._cache),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.debug("Error refreshing index cache", exc_info=True)
            finally:
                self._initial_ready.set()
            await asyncio.sleep(self._refresh_interval)
