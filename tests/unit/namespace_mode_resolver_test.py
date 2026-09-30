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

"""SC/AP namespace-mode resolution for policy routing.

The mode comes from PAC's partition map, which answers without I/O. Only a
namespace the map does not list goes to the server, and that answer is never
cached: a failed or premature probe must not pin a namespace's mode for the
life of the client.
"""

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from aerospike_sdk.aio.session import Session
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.sync.client import SyncClient

NS = "test"


def _async_resolver(pac, cache, server_says_sc=False):
    owner = SimpleNamespace(
        _client=SimpleNamespace(_namespace_mode_cache=cache),
        _pac_client=pac,
        is_namespace_sc=AsyncMock(return_value=server_says_sc),
    )
    return lambda ns: Session._resolve_namespace_mode(owner, ns), owner.is_namespace_sc


def _async_session_blocking_resolver(pac, cache, server_says_sc=False):
    owner = SimpleNamespace(_client=SimpleNamespace(_namespace_mode_cache=cache), _pac_client=pac)
    return lambda ns: Session._resolve_namespace_mode_blocking(owner, ns), pac.info_blocking


def _sync_client_resolver(pac, cache, server_says_sc=False):
    owner = SimpleNamespace(_namespace_mode_cache=cache, underlying_client=pac)
    return lambda ns: SyncClient._resolve_namespace_mode_blocking(owner, ns), pac.info_blocking


RESOLVERS = pytest.mark.parametrize(
    "make_resolver",
    [_async_resolver, _async_session_blocking_resolver, _sync_client_resolver],
    ids=["async", "async-session-blocking", "sync-client"],
)


async def _resolve(resolve, namespace):
    result = resolve(namespace)
    return await result if inspect.isawaitable(result) else result


def _pac(is_sc):
    pac = MagicMock()
    pac.is_strong_consistency.return_value = is_sc
    return pac


@RESOLVERS
@pytest.mark.parametrize("is_sc, mode", [(True, Mode.SC), (False, Mode.AP)])
async def test_the_partition_map_answers_without_asking_the_server(make_resolver, is_sc, mode):
    cache = {}
    resolve, server_probe = make_resolver(_pac(is_sc), cache)
    assert await _resolve(resolve, NS) == mode
    assert cache == {NS: mode}
    server_probe.assert_not_called()


@RESOLVERS
async def test_a_cached_mode_skips_the_partition_map(make_resolver):
    pac = _pac(False)
    resolve, _ = make_resolver(pac, {NS: Mode.SC})
    assert await _resolve(resolve, NS) == Mode.SC
    pac.is_strong_consistency.assert_not_called()


@RESOLVERS
async def test_a_namespace_missing_from_the_map_asks_the_server_without_caching(make_resolver):
    pac = _pac(None)
    pac.info_blocking.return_value = {"namespace/test": "strong-consistency=true"}
    cache = {}
    resolve, server_probe = make_resolver(pac, cache, server_says_sc=True)
    assert await _resolve(resolve, NS) == Mode.SC
    server_probe.assert_called_once()
    assert cache == {}


@pytest.mark.parametrize(
    "make_resolver",
    [_async_session_blocking_resolver, _sync_client_resolver],
    ids=["async-session-blocking", "sync-client"],
)
async def test_a_failed_blocking_probe_does_not_pin_the_namespace_as_ap(make_resolver):
    pac = _pac(None)
    pac.info_blocking.side_effect = RuntimeError("node unreachable")
    cache = {}
    resolve, _ = make_resolver(pac, cache)
    assert await _resolve(resolve, NS) == Mode.AP
    assert cache == {}

    pac.is_strong_consistency.return_value = True
    assert await _resolve(resolve, NS) == Mode.SC
