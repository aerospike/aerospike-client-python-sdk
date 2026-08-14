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

from aerospike_sdk.routing_capabilities_shared import RoutingCapabilitiesMixin


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

    def nodes_blocking(self):
        return list(self._nodes)


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
    def __init__(self, pac) -> None:
        self._client = pac
        self._connected = True
        self._init_routing_capability_cache()


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
