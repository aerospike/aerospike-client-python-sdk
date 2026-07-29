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

"""In-doubt propagation on the session fast path.

:meth:`Session.get` / :meth:`Session.put` raise the underlying client's
exceptions directly — one ``await`` reaches the client, with no boundary
conversion — and :meth:`Session.get_many` / :meth:`Session.put_many` both
raise on whole-call failure and deliver per-key exception *instances* in
their result lists. On every surface the typed ``in_doubt`` must arrive
intact. The builder path converts at the boundary instead (covered in
``exceptions_test.py``). These tests fail loudly if a future fast-path
wrapper drops the flag.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from aerospike_sdk import Key
from aerospike_sdk.aio.session import Session
from aerospike_sdk.exceptions import PacTimeoutError
from aerospike_sdk.policy.behavior import Behavior


def _make_session(pac_error: Exception) -> Session:
    pac = MagicMock()
    pac.get = AsyncMock(side_effect=pac_error)
    pac.put = AsyncMock(side_effect=pac_error)
    pac._submit_many_read = AsyncMock(side_effect=pac_error)
    pac._submit_many_write = AsyncMock(side_effect=pac_error)

    client = MagicMock()
    client._async_client = pac
    return Session(client=client, behavior=Behavior.DEFAULT)


async def test_fast_path_put_propagates_in_doubt():
    err = PacTimeoutError("timed out")
    err.in_doubt = True
    session = _make_session(err)

    with pytest.raises(PacTimeoutError) as exc_info:
        await session.put(Key("test", "unit", "k1"), {"b": 1})
    assert exc_info.value.in_doubt is True


async def test_fast_path_get_propagates_in_doubt():
    err = PacTimeoutError("timed out")
    err.in_doubt = True
    session = _make_session(err)

    with pytest.raises(PacTimeoutError) as exc_info:
        await session.get(Key("test", "unit", "k1"))
    assert exc_info.value.in_doubt is True


async def test_fast_path_put_many_raise_propagates_in_doubt():
    err = PacTimeoutError("window submit failed")
    err.in_doubt = True
    session = _make_session(err)

    with pytest.raises(PacTimeoutError) as exc_info:
        await session.put_many(
            [Key("test", "unit", "k1"), Key("test", "unit", "k2")], {"b": 1},
        )
    assert exc_info.value.in_doubt is True


async def test_fast_path_get_many_raise_propagates_in_doubt():
    err = PacTimeoutError("window submit failed")
    err.in_doubt = True
    session = _make_session(err)

    with pytest.raises(PacTimeoutError) as exc_info:
        await session.get_many([Key("test", "unit", "k1")])
    assert exc_info.value.in_doubt is True


async def test_fast_path_put_many_per_key_slots_carry_in_doubt():
    """Per-key exception instances in the result list keep the flag —
    guards against a future wrapper that maps or copies slots lossily."""
    err = PacTimeoutError("timed out")
    err.in_doubt = True
    session = _make_session(PacTimeoutError("unused"))
    session._pac_client._submit_many_write = AsyncMock(return_value=[None, err])

    outcomes = await session.put_many(
        [Key("test", "unit", "k1"), Key("test", "unit", "k2")], {"b": 1},
    )
    assert outcomes[0] is None
    assert isinstance(outcomes[1], PacTimeoutError)
    assert outcomes[1].in_doubt is True
