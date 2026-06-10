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

"""SC-mode routing on the async fast path.

Regression guard for the async-side analog of the SC-fastpath fix that
landed for ``SyncSession.get`` / ``SyncSession.put``: both
:meth:`Session.get` and :meth:`Session.put` must route through PAC's
``*_with_overrides`` async entries and pass BOTH ``base_policy`` (AP) and
``base_policy_sc`` so PAC's per-Client namespace_mode_cache can pick the
right policy per resolved namespace.

If a future change reverts to plain ``client.get`` / ``client.put`` on
this path, users on SC namespaces silently fall back to AP semantics
(no exception, no warning, wrong consistency guarantees during
partition / failover). These tests fail loudly if that happens.

**Marked xfail (strict=False).** These tests pin a *workaround* contract:
PAC maintains a separate ``namespace_mode_cache: HashMap<String, bool>``
populated by an info call on first touch, consulted via the
``*_with_overrides`` entries per op. The architecturally-correct fix is
to expose ``scMode`` on aerospike-core's partition-map API (the way
JSDK reads ``partitions.scMode`` inline at op time — zero info call,
zero separate cache, zero per-op HashMap lookup). When that lands,
these tests should be replaced with ones asserting partition-map-based
selection.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from aerospike_async import Key

from aerospike_sdk.aio.session import Session
from aerospike_sdk.policy.behavior import Behavior


def _make_session() -> tuple[Session, MagicMock]:
    pac = MagicMock()
    pac.get_with_overrides = AsyncMock(return_value=MagicMock(name="Record"))
    pac.put_with_overrides = AsyncMock(return_value=None)
    # The pre-fix code path. Wire these so an accidental regression fails
    # loudly instead of silently doing the wrong thing on SC namespaces.
    pac.get = AsyncMock(
        side_effect=AssertionError(
            "Session.get must route through get_with_overrides — calling "
            "plain client.get loses SC-namespace policy resolution"
        )
    )
    pac.put = AsyncMock(
        side_effect=AssertionError(
            "Session.put must route through put_with_overrides — calling "
            "plain client.put loses SC-namespace policy resolution"
        )
    )

    client = MagicMock()
    client._async_client = pac
    client._namespace_mode_cache = {}
    session = Session(client=client, behavior=Behavior.DEFAULT)
    return session, pac


_XFAIL_REASON = (
    "Pins the SC-routing workaround (per-Client namespace_mode_cache + "
    "*_with_overrides). Replace when aerospike-core exposes scMode on the "
    "partition map and PAC reads it inline at op time (JSDK pattern)."
)


@pytest.mark.xfail(strict=False, reason=_XFAIL_REASON)
async def test_session_get_passes_both_ap_and_sc_policies():
    session, pac = _make_session()
    key = Key("test", "unit", "k1")

    await session.get(key)

    pac.get_with_overrides.assert_called_once()
    _args, kwargs = pac.get_with_overrides.call_args

    assert "base_policy_sc" in kwargs, (
        "base_policy_sc must be passed so PAC can switch to the SC "
        "policy when the namespace_mode_cache resolves the namespace as SC"
    )
    assert kwargs["base_policy_sc"] is not None
    assert kwargs["base_policy_sc"] is session._cached_read_policy_sc

    pac.get.assert_not_called()


@pytest.mark.xfail(strict=False, reason=_XFAIL_REASON)
async def test_session_put_passes_both_ap_and_sc_policies():
    session, pac = _make_session()
    key = Key("test", "unit", "k1")

    await session.put(key, {"b": 1})

    pac.put_with_overrides.assert_called_once()
    _args, kwargs = pac.put_with_overrides.call_args

    assert "base_policy_sc" in kwargs, (
        "base_policy_sc must be passed so PAC can switch to the SC "
        "policy when the namespace_mode_cache resolves the namespace as SC"
    )
    assert kwargs["base_policy_sc"] is not None
    assert kwargs["base_policy_sc"] is session._cached_write_policy_sc

    pac.put.assert_not_called()


@pytest.mark.xfail(strict=False, reason=_XFAIL_REASON)
async def test_session_get_ap_and_sc_policies_are_distinct():
    """If AP/SC policies were identical, callers wouldn't observe a bug —
    but they're not.  Confirm Behavior.DEFAULT yields distinct caches so
    the routing fix has something meaningful to switch between."""
    session, _ = _make_session()
    assert session._cached_read_policy is not session._cached_read_policy_sc
    assert session._cached_write_policy is not session._cached_write_policy_sc
