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

"""Hot-reload for the SDK configuration file (``AEROSPIKE_SDK_CONFIG_URL``).

Watches the config file's modification time and, when it advances, re-runs the
full precedence pipeline (file profiles over programmatic settings over hard
defaults) and swaps the owner's settings holder wholesale. The holder is a
single attribute pointing at a frozen ``SystemSettings``, so readers on the
operation path always see a consistent snapshot with no locking.

A failed reload (file removed, unreadable, malformed) keeps the last-good
settings; it never reverts a running client to defaults.

Two monitor flavors share the poll logic: an ``asyncio.Task`` for the async
client and a daemon thread for the sync client. Lifecycle is tied to client
connect / close.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from dataclasses import dataclass, fields, replace
from typing import Callable, Optional

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.policy.sdk_config_loader import (
    apply_behaviors,
    fill_hard_defaults,
    merge_settings,
    named_profiles,
    parse_config_bytes,
    read_config_bytes,
    resolve_for_cluster,
)
from aerospike_sdk.policy.system_settings import SystemSettings

log = logging.getLogger(SdkLoggers.BEHAVIOR)

_POLL_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True)
class SdkConfigSource:
    """Inputs needed to recompute effective settings on each reload.

    ``initial_raw`` seeds the content-based change gate with the bytes
    read at connect, so the first poll after an mtime-only touch (deploy
    re-stamp, ``touch``) does no parse work.
    """

    path: str
    cluster_name: Optional[str]
    programmatic: Optional[SystemSettings]
    initial_raw: Optional[bytes] = None


# Everything in SystemSettings except these is read once, while building the
# ClientPolicy that opens the connection, and cannot change on a live client.
_LIVE_APPLIED_FIELDS = frozenset({"metrics"})


def adopt_discovered_cluster_name(
    discovered: Optional[str],
    settings: SystemSettings,
    source: SdkConfigSource,
) -> tuple[SystemSettings, SdkConfigSource]:
    """Re-resolve *settings* against the cluster name the server reports.

    Profile selection normally uses the name declared through
    ``validate_cluster_name_is()``. When nothing was declared, a
    ``system.<clusterName>`` block naming the real cluster is well-formed YAML
    that simply never matches, so the operator gets ``DEFAULT`` everywhere with
    nothing in the log to explain it. This closes that by selecting on the
    discovered name once the connection is up.

    Connect-time settings are already spent by then -- the pool sizes and tend
    interval in the adopted block cannot take effect on a running client -- so
    those are reported rather than applied, and only the live-appliable ones
    change. The returned source carries the discovered name so later reloads
    keep selecting the same block.

    Args:
        discovered: The server-reported cluster name, or ``None``.
        settings: The settings currently in force.
        source: The reload inputs, carrying the raw file and the code layer.

    Returns:
        ``(settings, source)`` -- re-resolved when a block matched, and the
        originals unchanged otherwise.
    """
    if not discovered or source.cluster_name is not None:
        return settings, source
    profiles = named_profiles(source.initial_raw, source.path)
    if discovered not in profiles:
        return settings, source

    loaded = parse_config_bytes(source.initial_raw, source.path)
    if loaded is None:
        return settings, source
    file_layer = resolve_for_cluster(loaded.profiles, discovered)
    adopted = fill_hard_defaults(merge_settings(file_layer, source.programmatic))

    spent = sorted(
        f.name
        for f in fields(SystemSettings)
        if f.name not in _LIVE_APPLIED_FIELDS
        and getattr(adopted, f.name) != getattr(settings, f.name)
    )
    if spent:
        log.warning(
            "SDK config: block system.%s was selected by the cluster's "
            "server-reported name, but %s in it cannot take effect on a "
            "connected client — declare the name with "
            "validate_cluster_name_is(%r) to have them applied at connect",
            discovered, ", ".join(spent), discovered,
        )
        # Those fields keep the values the connection was actually built with,
        # so what the object reports stays what the client is doing.
        adopted = replace(
            adopted,
            **{f: getattr(settings, f) for f in spent},
        )

    log.info("SDK config: applying block system.%s (server-reported name)", discovered)
    return adopted, replace(source, cluster_name=discovered)


class _SdkConfigPoller:
    """Shared mtime-poll / recompute / swap logic for both monitor flavors.

    ``apply`` is the owner's setter (a single attribute assignment on the
    client); it is only called when the recomputed settings differ from the
    current ones.
    """

    def __init__(
        self,
        source: SdkConfigSource,
        current: SystemSettings,
        apply: Callable[[SystemSettings], None],
    ) -> None:
        self._source = source
        self._current = current
        self._apply = apply
        self._last_mtime = self._stat_mtime()
        self._last_raw = source.initial_raw

    def _stat_mtime(self) -> Optional[float]:
        try:
            return os.stat(self._source.path).st_mtime
        except OSError:
            return None

    def poll_once(self) -> None:
        """Re-resolve and swap when the file actually changed.

        Three gates, each cheaper than the work it guards: mtime (no
        read), raw-content compare (no parse), and per-target change
        checks before apply (settings equality; per-behavior spec compare
        inside :func:`apply_behaviors`).
        """
        mtime = self._stat_mtime()
        if mtime is None or mtime == self._last_mtime:
            return
        self._last_mtime = mtime
        raw = read_config_bytes(self._source.path)
        if raw is None:
            log.warning(
                "SDK config: reload of %s failed; keeping last-good settings",
                self._source.path,
            )
            return
        if raw == self._last_raw:
            return
        # Connect-time loading is fail-soft (a broken file resolves to the
        # programmatic + default layers), but on reload that would silently
        # drop the last-good file layer — so a file that no longer parses
        # keeps the current settings instead. `_last_raw` is deliberately
        # not updated: reverting the file to its last-good content is then
        # caught by the content gate as "unchanged".
        loaded = parse_config_bytes(raw, self._source.path)
        if loaded is None:
            log.warning(
                "SDK config: reload of %s failed; keeping last-good settings",
                self._source.path,
            )
            return
        self._last_raw = raw
        if loaded.behaviors:
            apply_behaviors(loaded.behaviors)
        file_layer = resolve_for_cluster(loaded.profiles, self._source.cluster_name)
        resolved = fill_hard_defaults(merge_settings(file_layer, self._source.programmatic))
        if resolved == self._current:
            return
        self._current = resolved
        self._apply(resolved)
        log.info("SDK config reloaded from %s", self._source.path)


class AsyncSdkConfigMonitor:
    """Config hot-reload as an ``asyncio.Task`` on the client's loop."""

    def __init__(
        self,
        source: SdkConfigSource,
        current: SystemSettings,
        apply: Callable[[SystemSettings], None],
    ) -> None:
        self._poller = _SdkConfigPoller(source, current, apply)
        self._path = source.path
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        """Start polling; requires a running event loop."""
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())
            log.debug("SDK config: watching %s (~%.0fs)", self._path, _POLL_INTERVAL_SECONDS)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
            try:
                self._poller.poll_once()
            except Exception:
                log.warning("SDK config reload failed; keeping last-good", exc_info=True)

    async def stop(self) -> None:
        """Cancel the poll task and wait for it to finish."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None


class SyncSdkConfigMonitor:
    """Config hot-reload as a daemon thread for the sync client."""

    def __init__(
        self,
        source: SdkConfigSource,
        current: SystemSettings,
        apply: Callable[[SystemSettings], None],
    ) -> None:
        self._poller = _SdkConfigPoller(source, current, apply)
        self._path = source.path
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the daemon poll thread."""
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._run, name="aerospike-sdk-config", daemon=True,
            )
            self._thread.start()
            log.debug("SDK config: watching %s (~%.0fs)", self._path, _POLL_INTERVAL_SECONDS)

    def _run(self) -> None:
        while not self._stop.wait(_POLL_INTERVAL_SECONDS):
            try:
                self._poller.poll_once()
            except Exception:
                log.warning("SDK config reload failed; keeping last-good", exc_info=True)

    def stop(self) -> None:
        """Signal the poll thread to exit and join it briefly."""
        if self._thread is not None:
            self._stop.set()
            self._thread.join(timeout=2.0)
            self._thread = None
