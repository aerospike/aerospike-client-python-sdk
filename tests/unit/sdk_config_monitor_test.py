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

"""Tests for the SDK config hot-reload poll/swap logic (no server needed)."""

import os

from aerospike_sdk.policy.sdk_config_loader import load_and_resolve
from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.sdk_config_monitor import SdkConfigSource, _SdkConfigPoller

_IMPLICIT_TRUE = (
    "system:\n  DEFAULT:\n    transactions:\n      implicitBatchWriteTransactions: true\n"
)
_IMPLICIT_FALSE = (
    "system:\n  DEFAULT:\n    transactions:\n      implicitBatchWriteTransactions: false\n"
)


def _bump_mtime(path) -> None:
    """Advance mtime past filesystem timestamp granularity."""
    stat = os.stat(path)
    os.utime(path, (stat.st_atime, stat.st_mtime + 2))


def _make_poller(tmp_path, text=_IMPLICIT_TRUE):
    path = tmp_path / "sdk.yaml"
    path.write_text(text)
    source = SdkConfigSource(str(path), None, None)
    current = load_and_resolve(str(path), None, None)
    applied: list[SystemSettings] = []
    poller = _SdkConfigPoller(source, current, applied.append)
    return path, poller, applied


class TestSdkConfigPoller:
    """mtime-gated recompute and swap, with keep-last-good on failure."""

    def test_unchanged_file_no_swap(self, tmp_path):
        _, poller, applied = _make_poller(tmp_path)
        poller.poll_once()
        assert applied == []

    def test_changed_file_swaps(self, tmp_path):
        path, poller, applied = _make_poller(tmp_path)
        path.write_text(_IMPLICIT_FALSE)
        _bump_mtime(path)
        poller.poll_once()
        assert len(applied) == 1
        assert applied[0].transactions.implicit_batch_write_transactions is False

    def test_touch_without_content_change_no_swap(self, tmp_path):
        path, poller, applied = _make_poller(tmp_path)
        _bump_mtime(path)
        poller.poll_once()
        assert applied == []

    def test_broken_reload_keeps_last_good(self, tmp_path):
        path, poller, applied = _make_poller(tmp_path)
        path.write_text("system: [unbalanced : bracket\n")
        _bump_mtime(path)
        poller.poll_once()
        assert applied == []

    def test_recovery_after_broken_reload(self, tmp_path):
        path, poller, applied = _make_poller(tmp_path)
        path.write_text("system: [unbalanced : bracket\n")
        _bump_mtime(path)
        poller.poll_once()
        path.write_text(_IMPLICIT_FALSE)
        _bump_mtime(path)
        poller.poll_once()
        assert len(applied) == 1
        assert applied[0].transactions.implicit_batch_write_transactions is False

    def test_missing_file_keeps_last_good(self, tmp_path):
        path, poller, applied = _make_poller(tmp_path)
        path.unlink()
        poller.poll_once()
        assert applied == []
