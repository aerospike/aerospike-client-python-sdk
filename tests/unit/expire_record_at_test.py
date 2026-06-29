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
# License for the specific language governing permissions and limitations
# under the License.

"""Unit tests for expire_record_after(timedelta) and expire_record_at(datetime)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from aerospike_sdk.aio.background import BackgroundOperationBuilder, _OpType
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.operations_shared import _seconds_from_timedelta, _seconds_until
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.policy.behavior_settings import Mode


def _session_mock() -> MagicMock:
    s = MagicMock()
    s.behavior = Behavior.DEFAULT
    fc = MagicMock()
    fc._client = MagicMock()
    s.client = fc
    s._resolve_namespace_mode = AsyncMock(return_value=Mode.AP)
    return s


def test_seconds_from_timedelta_basic():
    assert _seconds_from_timedelta(timedelta(seconds=30)) == 30
    assert _seconds_from_timedelta(timedelta(days=1)) == 86400
    assert _seconds_from_timedelta(timedelta(hours=2, minutes=30)) == 9000


def test_seconds_from_timedelta_truncates_fractional_seconds():
    assert _seconds_from_timedelta(timedelta(seconds=1.9)) == 1


def test_seconds_from_timedelta_rejects_zero():
    with pytest.raises(ValueError, match="must be positive"):
        _seconds_from_timedelta(timedelta(seconds=0))


def test_seconds_from_timedelta_rejects_negative():
    with pytest.raises(ValueError, match="must be positive"):
        _seconds_from_timedelta(timedelta(seconds=-1))


def test_seconds_until_naive_future():
    target = datetime.now() + timedelta(minutes=5)
    seconds = _seconds_until(target)
    # Allow 1s slack for compute time between datetime.now() calls.
    assert 298 <= seconds <= 300


def test_seconds_until_aware_future_utc():
    target = datetime.now(timezone.utc) + timedelta(minutes=5)
    seconds = _seconds_until(target)
    assert 298 <= seconds <= 300


def test_seconds_until_aware_future_other_tz():
    # A timezone-aware datetime in any tz should still resolve correctly.
    tz_plus_3 = timezone(timedelta(hours=3))
    target = datetime.now(tz_plus_3) + timedelta(minutes=5)
    seconds = _seconds_until(target)
    assert 298 <= seconds <= 300


def test_seconds_until_rejects_past_naive():
    with pytest.raises(ValueError, match="must be in the future"):
        _seconds_until(datetime.now() - timedelta(seconds=1))


def test_seconds_until_rejects_past_aware():
    with pytest.raises(ValueError, match="must be in the future"):
        _seconds_until(datetime.now(timezone.utc) - timedelta(seconds=1))


def test_seconds_until_rejects_exactly_now():
    # Pre-compute and pass the same instant — the small drift between
    # the two clock reads should not save us, both round down to 0.
    target = datetime.now()
    with pytest.raises(ValueError, match="must be in the future"):
        _seconds_until(target)


# --- builder wiring ---


def _builder() -> BackgroundOperationBuilder:
    return BackgroundOperationBuilder(_session_mock(), DataSet.of("test", "bgset"), _OpType.UPDATE)


def test_background_builder_expire_record_after_sets_ttl():
    b = _builder()
    b.expire_record_after(timedelta(hours=1))
    assert b._ttl_seconds == 3600


def test_background_builder_expire_record_at_sets_ttl():
    b = _builder()
    b.expire_record_at(datetime.now() + timedelta(minutes=10))
    assert b._ttl_seconds is not None
    assert 595 <= b._ttl_seconds <= 600


def test_background_builder_expire_record_at_rejects_past():
    b = _builder()
    with pytest.raises(ValueError, match="must be in the future"):
        b.expire_record_at(datetime.now() - timedelta(minutes=1))


def test_background_builder_expire_record_after_chains():
    b = _builder()
    assert b.expire_record_after(timedelta(seconds=42)) is b
    assert b.expire_record_at(datetime.now() + timedelta(seconds=42)) is b
