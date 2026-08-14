# Copyright 2025-2026 Aerospike, Inc.
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

"""Unit tests for the connect-time routing capability cache."""

import asyncio
from datetime import timedelta

import pytest

from aerospike_sdk.policy.system_settings import SystemSettings
from aerospike_sdk.routing_capabilities_shared import (
    _PAC_DEFAULT_TEND_INTERVAL_SECONDS,
    RoutingCapabilitiesMixin,
)


class _FakeVersion:
    def __init__(self, *, ael: bool = True, query_selection: bool = True) -> None:
        self._ael = ael
        self._query_selection = query_selection

    def supports_server_compiled_ael(self) -> bool:
        return self._ael

    def supports_query_selection(self) -> bool:
        return self._query_selection


class _FakeNode:
    def __init__(self, version: _FakeVersion) -> None:
        self.version = version


class _FakePacClient:
    """Stands in for PAC ``Client``: exposes ``nodes_blocking`` on the class."""

    def __init__(self, *versions: _FakeVersion) -> None:
        self._nodes = [_FakeNode(v) for v in versions]
        self.list_calls = 0
        self.blocking_calls = 0

    def nodes_blocking(self):
        self.list_calls += 1
        self.blocking_calls += 1
        return list(self._nodes)

    async def nodes(self):
        self.list_calls += 1
        return list(self._nodes)

    def join(self, version: _FakeVersion) -> None:
        """A node appears in the list PAC's tend loop publishes."""
        self._nodes.append(_FakeNode(version))

    def leave(self) -> None:
        self._nodes.pop()


class _FakeThreadLocalProxy:
    """Stands in for ``_ThreadLocalLocalClient``.

    No ``nodes_blocking`` on the class, and any *instance* attribute miss falls
    through ``__getattr__`` — which in the real proxy builds a per-thread PAC
    client. Misses are recorded so a test can assert none happened.
    """

    def __init__(self) -> None:
        self.attribute_misses: list[str] = []

    def __getattr__(self, name: str):
        self.attribute_misses.append(name)
        raise AttributeError(name)


class _Client(RoutingCapabilitiesMixin):
    def __init__(self, pac, tend_interval: timedelta | None = None) -> None:
        self._client = pac
        self._connected = True
        self._sdk_settings = SystemSettings(tend_interval=tend_interval)
        self._init_routing_capability_cache()


@pytest.fixture
def clock(monkeypatch):
    """Hand-advanced replacement for the mixin's ``time.monotonic``."""

    class _Clock:
        def __init__(self) -> None:
            self.now = 1000.0

        def advance(self, seconds: float) -> None:
            self.now += seconds

    fake = _Clock()
    monkeypatch.setattr(
        "aerospike_sdk.routing_capabilities_shared.time.monotonic",
        lambda: fake.now,
    )
    return fake


def test_capable_nodes_open_both_gates():
    client = _Client(_FakePacClient(_FakeVersion(), _FakeVersion()))
    client._warm_routing_capabilities_blocking()
    assert client.supports_server_compiled_ael is True
    assert client.supports_query_selection is True


def test_one_lagging_node_closes_both_gates():
    client = _Client(
        _FakePacClient(
            _FakeVersion(),
            _FakeVersion(ael=False, query_selection=False),
        ),
    )
    client._warm_routing_capabilities_blocking()
    assert client.supports_server_compiled_ael is False
    assert client.supports_query_selection is False


def test_unlistable_nodes_keep_string_ael_open():
    """Field 43 is the only string-AEL encoding, so an unverifiable cluster gets it.

    Regression: the thread-local proxy has no node list, which previously read as
    "cluster below 8.1.3" and rejected string AEL against a capable cluster.
    """
    client = _Client(_FakeThreadLocalProxy())
    client._warm_routing_capabilities_blocking()
    assert client.supports_server_compiled_ael is True


def test_unlistable_nodes_close_query_selection():
    """Field 44 has a working field-43 fallback, so it stays conservative."""
    client = _Client(_FakeThreadLocalProxy())
    client._warm_routing_capabilities_blocking()
    assert client.supports_query_selection is False


def test_probe_never_touches_the_proxy_instance():
    """The node-listing probe must not build a per-thread client to answer."""
    proxy = _FakeThreadLocalProxy()
    client = _Client(proxy)
    client._warm_routing_capabilities_blocking()
    assert proxy.attribute_misses == []


def test_disconnected_client_reports_no_capabilities():
    client = _Client(_FakePacClient(_FakeVersion()))
    client._warm_routing_capabilities_blocking()
    client._connected = False
    assert client.supports_server_compiled_ael is False
    assert client.supports_query_selection is False


class TestTendIntervalRefresh:
    """Gates track the node list PAC tends, within one tend interval."""

    def test_lagging_node_joining_after_connect_closes_the_gates(self, clock):
        pac = _FakePacClient(_FakeVersion())
        client = _Client(pac, tend_interval=timedelta(seconds=1))
        client._warm_routing_capabilities_blocking()
        assert client.supports_server_compiled_ael is True

        pac.join(_FakeVersion(ael=False, query_selection=False))
        clock.advance(1.0)

        assert client.supports_server_compiled_ael is False
        assert client.supports_query_selection is False

    def test_gates_reopen_when_the_lagging_node_leaves(self, clock):
        """Unlike the Java SDK's one-way ratchet, recovery needs no reconnect."""
        pac = _FakePacClient(
            _FakeVersion(),
            _FakeVersion(ael=False, query_selection=False),
        )
        client = _Client(pac, tend_interval=timedelta(seconds=1))
        client._warm_routing_capabilities_blocking()
        assert client.supports_server_compiled_ael is False

        pac.leave()
        clock.advance(1.0)

        assert client.supports_server_compiled_ael is True
        assert client.supports_query_selection is True

    def test_within_the_interval_the_node_list_is_not_rewalked(self, clock):
        """Hot-path reads must stay a cached-boolean lookup between tends."""
        pac = _FakePacClient(_FakeVersion())
        client = _Client(pac, tend_interval=timedelta(seconds=1))
        client._warm_routing_capabilities_blocking()
        calls_after_warm = pac.list_calls

        clock.advance(0.9)
        for _ in range(100):
            assert client.supports_server_compiled_ael is True
            assert client.supports_query_selection is True

        assert pac.list_calls == calls_after_warm

    def test_a_configured_interval_sets_the_refresh_window(self, clock):
        pac = _FakePacClient(_FakeVersion())
        client = _Client(pac, tend_interval=timedelta(seconds=30))
        client._warm_routing_capabilities_blocking()
        pac.join(_FakeVersion(ael=False, query_selection=False))

        clock.advance(29.0)
        assert client.supports_server_compiled_ael is True

        clock.advance(1.0)
        assert client.supports_server_compiled_ael is False

    def test_unset_interval_falls_back_to_the_pac_default(self):
        client = _Client(_FakePacClient(_FakeVersion()))
        assert client._routing_capability_ttl_seconds() == pytest.approx(
            _PAC_DEFAULT_TEND_INTERVAL_SECONDS,
        )

    def test_unlistable_client_refresh_never_touches_the_proxy_instance(self, clock):
        """The periodic re-derive must not build a per-thread client either."""
        proxy = _FakeThreadLocalProxy()
        client = _Client(proxy, tend_interval=timedelta(seconds=1))
        client._warm_routing_capabilities_blocking()

        clock.advance(5.0)
        assert client.supports_server_compiled_ael is True
        assert client.supports_query_selection is False
        assert proxy.attribute_misses == []

    def test_a_reader_refreshes_when_connect_never_warmed(self):
        """A never-stamped cache resolves on first read rather than reading ``None``."""
        client = _Client(_FakePacClient(_FakeVersion()))
        assert client.supports_server_compiled_ael is True
        assert client.supports_query_selection is True


class TestRefreshUnderARunningLoop:
    """PAC rejects a blocking node read inside a loop, so the async refresh defers."""

    @pytest.mark.asyncio
    async def test_a_stale_read_never_blocks_on_the_node_list(self, clock):
        pac = _FakePacClient(_FakeVersion())
        client = _Client(pac, tend_interval=timedelta(seconds=1))
        await client._warm_routing_capabilities()
        blocking_calls_after_warm = pac.blocking_calls

        pac.join(_FakeVersion(ael=False, query_selection=False))
        clock.advance(1.0)

        # The stale read returns the value it has and schedules the re-derive.
        assert client.supports_server_compiled_ael is True
        assert pac.blocking_calls == blocking_calls_after_warm

        await asyncio.sleep(0)
        assert client.supports_server_compiled_ael is False

    @pytest.mark.asyncio
    async def test_a_burst_of_stale_reads_schedules_one_refresh(self, clock):
        pac = _FakePacClient(_FakeVersion())
        client = _Client(pac, tend_interval=timedelta(seconds=1))
        await client._warm_routing_capabilities()
        calls_after_warm = pac.list_calls

        clock.advance(1.0)
        for _ in range(50):
            client.supports_server_compiled_ael  # noqa: B018
            client.supports_query_selection  # noqa: B018
        await asyncio.sleep(0)

        assert pac.list_calls == calls_after_warm + 1

    @pytest.mark.asyncio
    async def test_a_failed_refresh_keeps_the_previous_gates(self, clock):
        pac = _FakePacClient(_FakeVersion())
        client = _Client(pac, tend_interval=timedelta(seconds=1))
        await client._warm_routing_capabilities()

        async def _unreachable():
            raise ConnectionError("cluster unreachable")

        pac.nodes = _unreachable
        clock.advance(1.0)
        client.supports_server_compiled_ael  # noqa: B018
        await asyncio.sleep(0)

        assert client.supports_server_compiled_ael is True
        assert client._routing_capability_refresh is None

    @pytest.mark.asyncio
    async def test_close_cancels_an_in_flight_refresh(self, clock):
        pac = _FakePacClient(_FakeVersion())
        client = _Client(pac, tend_interval=timedelta(seconds=1))
        await client._warm_routing_capabilities()

        clock.advance(1.0)
        client.supports_server_compiled_ael  # noqa: B018
        in_flight = client._routing_capability_refresh
        assert in_flight is not None

        client._clear_routing_capability_cache()
        assert in_flight.cancelled() or in_flight.cancelling()
        assert client._routing_capability_refresh is None
