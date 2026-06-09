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

The :class:`IndexesMonitor` runs a daemon thread that periodically queries
the cluster via ``sindex-list`` and ``sindex-stat`` info commands, converts
the responses into :class:`~aerospike_sdk.ael.filter_gen.Index` objects, and
stores them in per-namespace
:class:`~aerospike_sdk.ael.filter_gen.IndexContext` caches.

The thread uses PAC's blocking info APIs, so the monitor works identically
for the async :class:`~aerospike_sdk.aio.client.Client` and the synchronous
:class:`~aerospike_sdk.sync.client.SyncClient` — no event loop required.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Dict, List, Optional

from aerospike_async.exceptions import AerospikeError as PacError
from aerospike_sdk.ael.filter_gen import Index, IndexContext, IndexTypeEnum

if TYPE_CHECKING:  # avoids circular import; used in type annotations only.
    from aerospike_async import Client as PacClient

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


def _stat_entries_per_bval_blocking(
    pac_client: "PacClient", ns: str, indexname: str,
) -> Optional[float]:
    try:
        stat_resp = pac_client.info_blocking(
            f"sindex-stat:namespace={ns};indexname={indexname}",
        )
        return _parse_entries_per_bval(stat_resp)
    except PacError:
        log.debug(
            "Failed to fetch sindex-stat for %s.%s",
            ns, indexname, exc_info=True,
        )
        return None


def _fetch_indexes_blocking(pac_client: "PacClient") -> Dict[str, IndexContext]:
    """Fetch all secondary indexes and return per-namespace ``IndexContext`` caches."""
    raw = pac_client.info_on_all_nodes_blocking("sindex-list")
    entries = _parse_sindex_list(raw)

    indexes_by_ns: Dict[str, List[Index]] = {}
    for entry in entries:
        ns = entry["ns"]
        bval = _stat_entries_per_bval_blocking(pac_client, ns, entry["indexname"])
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
    """Daemon-thread background monitor that caches secondary index metadata.

    Matches the JSDK ``IndexesMonitor`` design: a single daemon thread polls
    the cluster's info APIs at a fixed interval and refreshes an in-memory
    cache. Readers (sync or async builders) consult the cache through
    :meth:`get_index_context`, which is non-blocking.

    The monitor starts lazily: :meth:`start` is invoked by the query
    builders on the first AEL ``where()`` query that needs cached metadata,
    not by ``Client.connect``. Callers that never use AEL filters pay zero
    daemon-thread cost. :meth:`stop` is called from the matching ``close``
    paths and is a no-op when the monitor never started.

        Example::

            monitor = IndexesMonitor()
            monitor.start(pac_client)   # idempotent; safe to call repeatedly
            monitor.wait_until_ready()
            ctx = monitor.get_index_context("test")
            monitor.stop()

    Args:
        refresh_interval: Seconds between cache refreshes (default 5.0).
    """

    def __init__(self, refresh_interval: float = _DEFAULT_REFRESH_INTERVAL) -> None:
        self._refresh_interval = refresh_interval
        self._cache: Dict[str, IndexContext] = {}
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._initial_ready = threading.Event()

    def start(self, pac_client: "PacClient") -> None:
        """Begin background refresh (idempotent; lazy-start friendly).

        Safe to call from multiple builders on every AEL query — if the
        daemon thread is already running, this is a no-op. The first
        metadata fetch runs asynchronously on the daemon thread; callers
        that need index metadata immediately (for example AEL ``where()``
        filter generation on a dataset query) should call
        :meth:`wait_until_ready` before relying on :meth:`get_index_context`.
        """
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._initial_ready.clear()
        self._thread = threading.Thread(
            target=self._run,
            args=(pac_client,),
            name="aerospike-sdk-index-monitor",
            daemon=True,
        )
        self._thread.start()

    def wait_until_ready(self, timeout: Optional[float] = None) -> None:
        """Block until the first index refresh attempt has finished.

        After this returns, :meth:`get_index_context` reflects the latest fetch
        (possibly empty if the cluster reports no secondary indexes).

        Args:
            timeout: Seconds to wait; ``None`` uses ``_DEFAULT_READY_TIMEOUT``.

        Raises:
            RuntimeError: If :meth:`start` was not called.
            TimeoutError: If the first refresh does not complete in time.
        """
        if self._thread is None:
            raise RuntimeError("IndexesMonitor.start must be called first")
        limit = _DEFAULT_READY_TIMEOUT if timeout is None else timeout
        if not self._initial_ready.wait(limit):
            raise TimeoutError(
                f"IndexesMonitor first refresh did not complete in {limit:.1f}s",
            )

    def stop(self) -> None:
        """Stop the background refresh thread.

        Idempotent: safe to call when not running. Joins the thread with a
        short timeout so a stuck refresh doesn't block shutdown.
        """
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=self._refresh_interval + 1.0)
            self._thread = None

    def get_index_context(self, namespace: str) -> Optional[IndexContext]:
        """Return the cached ``IndexContext`` for *namespace*, or ``None``."""
        return self._cache.get(namespace)

    def _run(self, pac_client: "PacClient") -> None:
        """Periodic refresh loop. Runs on the daemon thread."""
        while not self._stop_event.is_set():
            try:
                self._cache = _fetch_indexes_blocking(pac_client)
                total = sum(len(ctx.indexes) for ctx in self._cache.values())
                log.debug(
                    "Index cache refreshed: %d index(es) across %d namespace(s)",
                    total,
                    len(self._cache),
                )
            except Exception:
                log.debug("Error refreshing index cache", exc_info=True)
            finally:
                # Idempotent: only the first fetch completion signals readiness.
                self._initial_ready.set()
            # Sleep with stop awareness so shutdown is prompt.
            if self._stop_event.wait(self._refresh_interval):
                break
