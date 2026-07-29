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

"""AsyncPool ``use_uvloop`` free-threading gate.

Under free-threading the pool may use uvloop only when uvloop's libuv race
(#720) is mitigated: PAC's pipe-wake transport active, or a fixed uvloop
release. Construction-time decision only — no I/O, no cluster, no asyncio
loops (``AsyncPool.__init__`` performs no connection).

The pipe-wake check MUST mirror PAC's ``should_use_pipe`` exactly — ``1`` or
``auto``/unset activate it, any other value (``0``, empty, a typo) does not.
A looser check would let the pool enable uvloop while PAC stayed on the racy
``call_soon_threadsafe`` path — uvloop with no pipe, which wedges. These tests
pin that mapping.
"""

import types
from unittest.mock import patch

import pytest

from aerospike_sdk.aio.cluster_definition import ClusterDefinition, Host
from aerospike_sdk.aio.pool import (
    AsyncPool,
    _uvloop_has_721_fix,
    _uvloop_safe_under_ft,
)


@pytest.fixture(name="definition")
def _definition_fixture(aerospike_host) -> ClusterDefinition:
    """An unconnected definition; AsyncPool construction performs no I/O."""
    return ClusterDefinition(hosts=Host.parse_hosts(aerospike_host, 3000))


class TestUvloopSafeUnderFt:
    """Pipe-wake activation mapping — must match PAC ``should_use_pipe``."""

    @pytest.mark.parametrize("val", ["1", "auto"])
    def test_active_values_are_safe(self, val, monkeypatch):
        monkeypatch.setenv("AEROSPIKE_PIPE_WAKE", val)
        assert _uvloop_safe_under_ft() is True

    def test_unset_defaults_to_auto_and_is_safe(self, monkeypatch):
        monkeypatch.delenv("AEROSPIKE_PIPE_WAKE", raising=False)
        assert _uvloop_safe_under_ft() is True

    @pytest.mark.parametrize("val", ["0", "", "yes", "true", "on", "Auto"])
    def test_inactive_values_defer_to_version_check(self, val, monkeypatch):
        # Anything other than 1/auto leaves PAC on the racy path, so safety
        # hinges solely on a fixed uvloop — never on the pipe transport.
        monkeypatch.setenv("AEROSPIKE_PIPE_WAKE", val)
        with patch("aerospike_sdk.aio.pool._uvloop_has_721_fix", return_value=False):
            assert _uvloop_safe_under_ft() is False
        with patch("aerospike_sdk.aio.pool._uvloop_has_721_fix", return_value=True):
            assert _uvloop_safe_under_ft() is True


class TestUvloopHas721Fix:
    """Version heuristic: #721 shipped after 0.22.1, so only > 0.22.1 has it."""

    @pytest.mark.parametrize(
        "version,expected",
        [
            ("0.22.0", False),
            ("0.22.1", False),  # the release the race lives in; also master's self-report
            ("0.22.2", True),
            ("0.23.0", True),
            ("1.0.0", True),
        ],
    )
    def test_version_threshold(self, version, expected):
        fake = types.SimpleNamespace(__version__=version)
        with patch.dict("sys.modules", {"uvloop": fake}):
            assert _uvloop_has_721_fix() is expected

    def test_unparseable_version_is_false(self):
        fake = types.SimpleNamespace(__version__="not-a-version")
        with patch.dict("sys.modules", {"uvloop": fake}):
            assert _uvloop_has_721_fix() is False

    def test_uvloop_absent_is_false(self):
        with patch.dict("sys.modules", {"uvloop": None}):
            assert _uvloop_has_721_fix() is False


@patch("aerospike_sdk.aio.pool.os.cpu_count", return_value=8)
class TestGateWiring:
    """The ``use_uvloop is None`` default resolves through the gate helpers."""

    def test_ft_pipe_active_enables_uvloop(self, _cpu, definition, monkeypatch):
        monkeypatch.delenv("AEROSPIKE_PIPE_WAKE", raising=False)  # auto
        with patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False):
            pool = AsyncPool(definition, loop_count=4)
        assert pool._use_uvloop is True

    def test_ft_pipe_off_disables_uvloop(self, _cpu, definition, monkeypatch):
        monkeypatch.setenv("AEROSPIKE_PIPE_WAKE", "0")
        with patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False), \
                patch("aerospike_sdk.aio.pool._uvloop_has_721_fix", return_value=False):
            pool = AsyncPool(definition, loop_count=4)
        assert pool._use_uvloop is False

    def test_gil_on_always_enables_uvloop(self, _cpu, definition, monkeypatch):
        # Race can't fire with the GIL on, so uvloop is allowed regardless.
        monkeypatch.setenv("AEROSPIKE_PIPE_WAKE", "0")
        with patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=True):
            pool = AsyncPool(definition, loop_count=4)
        assert pool._use_uvloop is True

    def test_explicit_kwarg_overrides_gate(self, _cpu, definition, monkeypatch):
        monkeypatch.setenv("AEROSPIKE_PIPE_WAKE", "0")
        with patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False):
            pool = AsyncPool(definition, loop_count=4, use_uvloop=True)
        assert pool._use_uvloop is True
