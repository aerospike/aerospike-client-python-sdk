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

"""AsyncPool auto-enable logic for per-Client Tokio runtime.

Exercises only the construction-time decisions (threshold + GIL gate +
worker count derivation) — no I/O, no real cluster, no asyncio loops.
The actual policy field is set on each Client in :meth:`AsyncPool.start`,
but the values computed in ``__init__`` are what get applied; testing
them in isolation is enough.
"""

import warnings
from unittest.mock import patch

import pytest

from aerospike_sdk.aio.pool import AsyncPool


def _factory():
    """A no-op factory; AsyncPool construction never calls it (only start does)."""
    raise AssertionError("client_factory must not be called at construction time")


@patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False)
@patch("aerospike_sdk.aio.pool.os.cpu_count", return_value=8)
class TestAutoDecideThreshold:
    """At ``loop_count < 4`` the auto path leaves per-Client runtime off.

    GIL is mocked off so the threshold (not the GIL gate) is what's tested.
    """

    def test_loops_1_auto_off(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=1)
        assert pool._per_client_runtime is False

    def test_loops_2_auto_off(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=2)
        assert pool._per_client_runtime is False

    def test_loops_3_auto_off(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=3)
        assert pool._per_client_runtime is False

    def test_loops_4_auto_on(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=4)
        assert pool._per_client_runtime is True

    def test_loops_8_auto_on(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=8)
        assert pool._per_client_runtime is True


@patch("aerospike_sdk.aio.pool.os.cpu_count", return_value=8)
class TestGilGate:
    """Auto-decide must leave per-Client runtime OFF when the GIL is on.

    Rationale: per-Client Tokio workers serialize on one GIL when
    delivering completions to asyncio, which deadlocks under load (16+
    worker threads all in futex_do_wait while the asyncio main loop
    blocks on epoll). Observed on bench-asd, 2026-05-22.
    """

    @patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=True)
    def test_gil_on_at_threshold_auto_off(self, _gil, _cpu):
        pool = AsyncPool(client_factory=_factory, loop_count=4)
        assert pool._per_client_runtime is False

    @patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=True)
    def test_gil_on_high_loops_auto_off(self, _gil, _cpu):
        pool = AsyncPool(client_factory=_factory, loop_count=8)
        assert pool._per_client_runtime is False

    @patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=True)
    def test_gil_on_explicit_true_warns_but_honors(self, _gil, _cpu):
        """Explicit ``per_client_runtime=True`` on GIL-on emits a warning
        but honors the user's choice — known footgun, their problem."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            pool = AsyncPool(
                client_factory=_factory, loop_count=4, per_client_runtime=True
            )
        assert pool._per_client_runtime is True
        gil_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert len(gil_warnings) == 1
        assert "GIL is enabled" in str(gil_warnings[0].message)
        assert "deadlock" in str(gil_warnings[0].message)

    @patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=True)
    def test_gil_on_explicit_false_no_warning(self, _gil, _cpu):
        """Explicit ``per_client_runtime=False`` on GIL-on: no warning, off."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            pool = AsyncPool(
                client_factory=_factory, loop_count=8, per_client_runtime=False
            )
        assert pool._per_client_runtime is False
        assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []


@patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False)
@patch("aerospike_sdk.aio.pool.os.cpu_count", return_value=8)
class TestExplicitOverride:
    """``per_client_runtime=True/False`` forces the decision regardless of loop count.

    GIL is mocked off so the override (not the GIL gate) is what's tested.
    """

    def test_force_on_below_threshold(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=2, per_client_runtime=True)
        assert pool._per_client_runtime is True

    def test_force_on_at_threshold(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=4, per_client_runtime=True)
        assert pool._per_client_runtime is True

    def test_force_off_above_threshold(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=8, per_client_runtime=False)
        assert pool._per_client_runtime is False

    def test_force_off_at_threshold(self, _cpu, _gil):
        pool = AsyncPool(client_factory=_factory, loop_count=4, per_client_runtime=False)
        assert pool._per_client_runtime is False


@patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False)
@patch("aerospike_sdk.aio.pool.os.cpu_count")
class TestWorkerCount:
    """Worker count = ``max(2, cpu_count // loop_count)``.

    The ``max(2, ...)`` floor protects against degenerate single-worker
    runtimes when ``cpu_count`` and ``loop_count`` happen to be equal or
    when one CPU is divided across many loops.
    """

    def test_8cpu_4loops_2workers(self, mock_cpu, _gil):
        mock_cpu.return_value = 8
        pool = AsyncPool(client_factory=_factory, loop_count=4)
        assert pool._per_client_runtime_workers == 2

    def test_8cpu_8loops_floor(self, mock_cpu, _gil):
        # 8/8 = 1 would be too few; floor at 2
        mock_cpu.return_value = 8
        pool = AsyncPool(client_factory=_factory, loop_count=8)
        assert pool._per_client_runtime_workers == 2

    def test_16cpu_4loops_4workers(self, mock_cpu, _gil):
        mock_cpu.return_value = 16
        pool = AsyncPool(client_factory=_factory, loop_count=4)
        assert pool._per_client_runtime_workers == 4

    def test_32cpu_8loops_4workers(self, mock_cpu, _gil):
        mock_cpu.return_value = 32
        pool = AsyncPool(client_factory=_factory, loop_count=8)
        assert pool._per_client_runtime_workers == 4

    def test_4cpu_2loops_2workers(self, mock_cpu, _gil):
        # below threshold, but worker count still computes
        mock_cpu.return_value = 4
        pool = AsyncPool(client_factory=_factory, loop_count=2)
        assert pool._per_client_runtime_workers == 2

    def test_cpu_count_none_defaults_to_4(self, mock_cpu, _gil):
        # If os.cpu_count() returns None (unusual containers), the pool
        # uses 4 as the fallback for both loop count and cpu count.
        mock_cpu.return_value = None
        pool = AsyncPool(client_factory=_factory, loop_count=4)
        assert pool._per_client_runtime_workers == 2  # max(2, 4 // 4) = 2


@patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False)
@patch("aerospike_sdk.aio.pool.os.cpu_count", return_value=8)
def test_default_loop_count_uses_cpu_count(_cpu, _gil):
    """When ``loop_count`` is omitted, the pool falls back to ``os.cpu_count()``."""
    pool = AsyncPool(client_factory=_factory)
    assert pool._n == 8
    # 8 loops on 8 CPUs, GIL off → above threshold, auto on
    assert pool._per_client_runtime is True
    assert pool._per_client_runtime_workers == 2  # max(2, 8 // 8)


@patch("aerospike_sdk.aio.pool.os.cpu_count", return_value=8)
class TestUvloopGate:
    """``use_uvloop`` auto-decide tracks the GIL: OFF under free-threading.

    uvloop's libuv free-threading race on ``loop._ready_len`` (MagicStack/
    uvloop #720/#721) stalls a multi-loop pool when the GIL is disabled, so
    the auto path must pick the stdlib selector loop there. Under GIL-on the
    race can't fire, so uvloop is left on to preserve prior behavior.
    """

    @patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False)
    def test_auto_off_under_free_threading(self, _gil, _cpu):
        pool = AsyncPool(client_factory=_factory, loop_count=4)
        assert pool._use_uvloop is False

    @patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=True)
    def test_auto_on_under_gil(self, _gil, _cpu):
        pool = AsyncPool(client_factory=_factory, loop_count=4)
        assert pool._use_uvloop is True

    @patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=False)
    def test_explicit_true_overrides_ft_default(self, _gil, _cpu):
        # Opt-in footgun: honored even under FT where it can stall.
        pool = AsyncPool(client_factory=_factory, loop_count=4, use_uvloop=True)
        assert pool._use_uvloop is True

    @patch("aerospike_sdk.aio.pool._gil_is_enabled", return_value=True)
    def test_explicit_false_overrides_gil_default(self, _gil, _cpu):
        pool = AsyncPool(client_factory=_factory, loop_count=4, use_uvloop=False)
        assert pool._use_uvloop is False
