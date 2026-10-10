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

"""A single-key query with read operations sends the Behavior's read settings.

PNC's ``operate`` takes only a ``WritePolicy``, so the read settings ride on
one. Every route a single-key read-operate takes (the default fast path, an
``on_error`` strategy, a ``where()`` filter, an SC namespace) must resolve
them from the read point scope, never from the write scope or PNC defaults.
"""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from aerospike_native import ReadModeAP, ReadModeSC

from aerospike_sdk import ErrorStrategy, Key
from aerospike_sdk.aio.session import Session
from aerospike_sdk.policy import Behavior, Mode, Settings
from aerospike_sdk.sync.session import Session as SyncSession

_KEY = Key("test", "read_operate", 1)

# Reads and writes differ in every asserted field, so a policy built from the
# write scope or from PNC defaults cannot pass.
_BEHAVIOR = Behavior.DEFAULT.derive_with_changes(
    "read-operate-policy",
    reads=Settings(
        total_timeout=timedelta(milliseconds=1234), max_retries=7,
        read_mode_ap=ReadModeAP.ALL,
    ),
    reads_sc=Settings(read_mode_sc=ReadModeSC.LINEARIZE),
    writes=Settings(total_timeout=timedelta(milliseconds=9876), max_retries=1),
)

_ROUTES = pytest.mark.parametrize("route", ["default", "on_error", "where"])


def _sdk_client(pnc: MagicMock) -> MagicMock:
    client = MagicMock()
    client._async_client = pnc
    client.underlying_client = pnc
    client._usage_on = False
    client._record_on = False
    client.supports_server_compiled_ael = True
    client.supports_query_selection = False
    return client


def _async_session(mode: Mode) -> tuple[Session, MagicMock]:
    pnc = MagicMock()
    pnc.operate = AsyncMock(return_value=MagicMock())
    session = Session(client=_sdk_client(pnc), behavior=_BEHAVIOR)
    session._resolve_namespace_mode = AsyncMock(return_value=mode)
    return session, pnc.operate


def _sync_session(mode: Mode) -> tuple[SyncSession, MagicMock]:
    pnc = MagicMock()
    session = SyncSession(client=_sdk_client(pnc), behavior=_BEHAVIOR)
    session._resolve_namespace_mode_blocking = lambda namespace: mode
    return session, pnc.operate_blocking


def _read_op(session, route: str):
    builder = session.query(_KEY).bin("m").map_size()
    if route == "where":
        builder = builder.where("$.m.a == 1")
    kwargs = {"on_error": ErrorStrategy.IN_STREAM} if route == "on_error" else {}
    return builder, kwargs


def _assert_read_settings(operate: MagicMock) -> None:
    policy = operate.call_args.kwargs["policy"]
    assert policy.total_timeout == 1234
    assert policy.max_retries == 7
    assert policy.read_mode_ap == ReadModeAP.ALL


class TestAsyncReadOperatePolicy:
    """Async single-key read-operate policies come from the read scope."""

    @_ROUTES
    async def test_read_settings_reach_operate(self, route):
        session, operate = _async_session(Mode.AP)
        builder, kwargs = _read_op(session, route)
        await (await builder.execute(**kwargs)).collect()
        _assert_read_settings(operate)

    async def test_sc_namespace_uses_sc_read_settings(self):
        session, operate = _async_session(Mode.SC)
        await (await session.query(_KEY).bin("m").map_size().execute()).collect()
        assert operate.call_args.kwargs["policy"].read_mode_sc == ReadModeSC.LINEARIZE


class TestSyncReadOperatePolicy:
    """Sync single-key read-operate policies come from the read scope."""

    @_ROUTES
    def test_read_settings_reach_operate(self, route):
        session, operate = _sync_session(Mode.AP)
        builder, kwargs = _read_op(session, route)
        list(builder.execute(**kwargs))
        _assert_read_settings(operate)

    def test_sc_namespace_uses_sc_read_settings(self):
        session, operate = _sync_session(Mode.SC)
        list(session.query(_KEY).bin("m").map_size().execute())
        assert operate.call_args.kwargs["policy"].read_mode_sc == ReadModeSC.LINEARIZE
