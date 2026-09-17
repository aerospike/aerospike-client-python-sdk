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

"""Exception conversion and in-doubt propagation on the session fast paths.

:meth:`Session.get` / :meth:`Session.put` reach the underlying client in one
``await``, and :meth:`Session.get_many` / :meth:`Session.put_many` in one
window submission. All four convert a *raised* client exception at the
boundary, so ``except AerospikeError`` (and its typed subclasses) works on
every path; the original client exception is preserved as ``__cause__`` and
the typed ``in_doubt`` arrives intact. Per-key exception *instances* inside
window result lists are currently delivered unconverted — converting them
unconditionally would scan every successful window on the happy path. That is
being closed once the window submission reports whether the window held any
failure, so the scan runs only when there is something to convert; both
behaviors are pinned below, the target one as ``xfail``. The sync session's
``get``/``put`` carry the same contract.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from aerospike_sdk import Key, ResultCode
from aerospike_sdk.aio.session import Session
from aerospike_sdk.exceptions import (
    AerospikeError,
    PacServerError,
    PacTimeoutError,
    RecordNotFoundError,
    TimeoutError,
)
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.session import Session as SyncSession


def _make_session(pac_error: Exception) -> Session:
    pac = MagicMock()
    pac.get = AsyncMock(side_effect=pac_error)
    pac.put = AsyncMock(side_effect=pac_error)
    pac._submit_many_read = AsyncMock(side_effect=pac_error)
    pac._submit_many_write = AsyncMock(side_effect=pac_error)

    client = MagicMock()
    client._async_client = pac
    return Session(client=client, behavior=Behavior.DEFAULT)


def _make_sync_session(pac_error: Exception) -> SyncSession:
    pac = MagicMock()
    pac.get_blocking = MagicMock(side_effect=pac_error)
    pac.put_blocking = MagicMock(side_effect=pac_error)

    client = MagicMock()
    client.underlying_client = pac
    return SyncSession(client=client, behavior=Behavior.DEFAULT)


def _pac_timeout(message: str) -> PacTimeoutError:
    err = PacTimeoutError(message)
    err.in_doubt = True
    return err


class TestFastPathRaisesSdkTypes:
    """Raised client failures surface as SDK exception types."""

    async def test_put_raises_sdk_timeout_with_in_doubt(self):
        pac_err = _pac_timeout("timed out")
        session = _make_session(pac_err)

        with pytest.raises(TimeoutError) as exc_info:
            await session.put(Key("test", "unit", "k1"), {"b": 1})
        assert isinstance(exc_info.value, AerospikeError)
        assert exc_info.value.in_doubt is True
        assert exc_info.value.__cause__ is pac_err

    async def test_get_raises_sdk_timeout_with_in_doubt(self):
        pac_err = _pac_timeout("timed out")
        session = _make_session(pac_err)

        with pytest.raises(TimeoutError) as exc_info:
            await session.get(Key("test", "unit", "k1"))
        assert isinstance(exc_info.value, AerospikeError)
        assert exc_info.value.in_doubt is True
        assert exc_info.value.__cause__ is pac_err

    async def test_get_converts_server_error_to_typed_subclass(self):
        pac_err = PacServerError(
            "not found", ResultCode.KEY_NOT_FOUND_ERROR, False, None, None, None,
        )
        session = _make_session(pac_err)

        with pytest.raises(RecordNotFoundError) as exc_info:
            await session.get(Key("test", "unit", "k1"))
        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR
        assert exc_info.value.__cause__ is pac_err

    async def test_put_many_whole_window_failure_raises_sdk_type(self):
        pac_err = _pac_timeout("window submit failed")
        session = _make_session(pac_err)

        with pytest.raises(TimeoutError) as exc_info:
            await session.put_many(
                [Key("test", "unit", "k1"), Key("test", "unit", "k2")], {"b": 1},
            )
        assert exc_info.value.in_doubt is True
        assert exc_info.value.__cause__ is pac_err

    async def test_get_many_whole_window_failure_raises_sdk_type(self):
        pac_err = _pac_timeout("window submit failed")
        session = _make_session(pac_err)

        with pytest.raises(TimeoutError) as exc_info:
            await session.get_many([Key("test", "unit", "k1")])
        assert exc_info.value.in_doubt is True
        assert exc_info.value.__cause__ is pac_err

    async def test_catch_all_aerospike_error_matches_fast_path(self):
        """The headline contract: a bare ``except AerospikeError`` catches
        fast-path failures — the exact hole this conversion closes."""
        session = _make_session(_pac_timeout("timed out"))

        with pytest.raises(AerospikeError):
            await session.get(Key("test", "unit", "k1"))


class TestFastPathWindowSlots:
    """Per-key window slots stay unconverted — today's shipped behavior.

    Retired together with the ``xfail`` markers below once slot conversion
    lands; kept meanwhile so the current contract is not changed silently.
    """

    async def test_put_many_per_key_slots_carry_in_doubt(self):
        err = _pac_timeout("timed out")
        session = _make_session(PacTimeoutError("unused"))
        session._pac_client._submit_many_write = AsyncMock(return_value=[None, err])

        outcomes = await session.put_many(
            [Key("test", "unit", "k1"), Key("test", "unit", "k2")], {"b": 1},
        )
        assert outcomes[0] is None
        assert isinstance(outcomes[1], PacTimeoutError)
        assert outcomes[1].in_doubt is True


class TestWindowSlotConversionContract:
    """Target contract: window slots carry SDK exception types like every other path.

    The mocks return the ``(slots, failure_count)`` shape the window submission
    grows so the scan can be skipped on failure-free windows. Strict ``xfail``
    means these report as failures the moment conversion lands, which is the
    signal to drop the markers and the class above.
    """

    _PENDING = "slot conversion is gated on the window submission reporting failure presence"

    @pytest.mark.xfail(strict=True, reason=_PENDING)
    async def test_get_many_slots_are_sdk_typed(self):
        record = MagicMock(name="record")
        err = PacServerError(
            "not found", ResultCode.KEY_NOT_FOUND_ERROR, False, None, None, None,
        )
        session = _make_session(PacTimeoutError("unused"))
        session._pac_client._submit_many_read = AsyncMock(
            return_value=([record, err], 1))

        slots = await session.get_many(
            [Key("test", "unit", "k1"), Key("test", "unit", "k2")])
        assert slots[0] is record, "successful slots must pass through untouched"
        assert isinstance(slots[1], RecordNotFoundError)
        assert isinstance(slots[1], AerospikeError)

    @pytest.mark.xfail(strict=True, reason=_PENDING)
    async def test_put_many_slots_are_sdk_typed(self):
        err = _pac_timeout("timed out")
        session = _make_session(PacTimeoutError("unused"))
        session._pac_client._submit_many_write = AsyncMock(
            return_value=([None, err], 1))

        outcomes = await session.put_many(
            [Key("test", "unit", "k1"), Key("test", "unit", "k2")], {"b": 1},
        )
        assert outcomes[0] is None
        assert isinstance(outcomes[1], TimeoutError)
        assert isinstance(outcomes[1], AerospikeError)
        assert outcomes[1].in_doubt is True, "in_doubt must survive conversion"

    @pytest.mark.xfail(strict=True, reason=_PENDING)
    async def test_failure_free_window_is_not_scanned(self):
        """A zero-failure window must return the submission's list unchanged —
        this is the whole point of the flag, so pin it, not just the typing."""
        slots = [MagicMock(name="r1"), MagicMock(name="r2")]
        session = _make_session(PacTimeoutError("unused"))
        session._pac_client._submit_many_read = AsyncMock(return_value=(slots, 0))

        returned = await session.get_many(
            [Key("test", "unit", "k1"), Key("test", "unit", "k2")])
        assert returned is slots


class TestSyncFastPathRaisesSdkTypes:
    """The sync session's fast paths carry the same conversion contract."""

    def test_get_raises_sdk_timeout_with_in_doubt(self):
        pac_err = _pac_timeout("timed out")
        session = _make_sync_session(pac_err)

        with pytest.raises(TimeoutError) as exc_info:
            session.get(Key("test", "unit", "k1"))
        assert isinstance(exc_info.value, AerospikeError)
        assert exc_info.value.in_doubt is True
        assert exc_info.value.__cause__ is pac_err

    def test_put_raises_sdk_timeout_with_in_doubt(self):
        pac_err = _pac_timeout("timed out")
        session = _make_sync_session(pac_err)

        with pytest.raises(TimeoutError) as exc_info:
            session.put(Key("test", "unit", "k1"), {"b": 1})
        assert isinstance(exc_info.value, AerospikeError)
        assert exc_info.value.in_doubt is True
        assert exc_info.value.__cause__ is pac_err

    def test_get_converts_server_error_to_typed_subclass(self):
        pac_err = PacServerError(
            "not found", ResultCode.KEY_NOT_FOUND_ERROR, False, None, None, None,
        )
        session = _make_sync_session(pac_err)

        with pytest.raises(RecordNotFoundError) as exc_info:
            session.get(Key("test", "unit", "k1"))
        assert exc_info.value.result_code == ResultCode.KEY_NOT_FOUND_ERROR
        assert exc_info.value.__cause__ is pac_err
