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
from dataclasses import dataclass
from typing import Callable, Optional

from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.policy.sdk_config_loader import (
    apply_behaviors,
    fill_hard_defaults,
    merge_settings,
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
