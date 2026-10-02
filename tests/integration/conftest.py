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

"""Integration-test-only pytest hooks and fixtures."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pytest

from tests.pac_compat import (
    skip_if_lacks_query_selection,
    skip_if_lacks_server_compiled_ael,
)

_CAPABILITY_MARKERS = (
    ("requires_server_compiled_ael", skip_if_lacks_server_compiled_ael),
    ("requires_query_selection", skip_if_lacks_query_selection),
)

# Seed fixtures a capability marker may name with ``host=``, mapped to whether
# connecting to that seed needs ``AEROSPIKE_AUTH_*``.
_PROBE_SEEDS = {"aerospike_host": False, "aerospike_host_sc": True}


@dataclass(frozen=True)
class _ClusterCapabilities:
    supports_server_compiled_ael: bool
    supports_query_selection: bool


@pytest.fixture(scope="session")
def cluster_capabilities(make_cluster_definition) -> Callable[[str, bool], _ClusterCapabilities]:
    """Return a probe that reports a seed's capability flags, connecting once per seed."""
    cache: dict[str, _ClusterCapabilities] = {}

    def _probe(seed: str, auth: bool) -> _ClusterCapabilities:
        capabilities = cache.get(seed)
        if capabilities is None:
            with make_cluster_definition(seed, auth=auth, sync=True).connect() as cluster:
                capabilities = cache[seed] = _ClusterCapabilities(
                    supports_server_compiled_ael=cluster.supports_ael(),
                    supports_query_selection=cluster.supports_query_selection(),
                )
        return capabilities

    return _probe


@pytest.fixture(autouse=True)
def _enforce_capability_markers(request):
    """Skip a capability-marked test when its cluster lacks the capability.

    Marks on a ``pytest.param`` land on the collected item, so one variant can
    skip while its twin runs. Unmarked tests never pay the probe.

    The probe asks the seed named by the marker's ``host=`` (default
    ``aerospike_host``), not the test's own connection. A marked test that
    connects to a different seed must name that seed's fixture, e.g.
    ``@requires_server_compiled_ael(host="aerospike_host_sc")``.
    """
    marked = [
        (marker, skip)
        for name, skip in _CAPABILITY_MARKERS
        if (marker := request.node.get_closest_marker(name)) is not None
    ]
    if not marked:
        return
    probe = request.getfixturevalue("cluster_capabilities")
    for marker, skip in marked:
        host_fixture = marker.kwargs.get("host", "aerospike_host")
        if host_fixture not in _PROBE_SEEDS:
            pytest.fail(
                f"@{marker.name}(host={host_fixture!r}): unknown seed fixture; "
                f"add it to _PROBE_SEEDS in tests/integration/conftest.py",
                pytrace=False,
            )
        skip(probe(request.getfixturevalue(host_fixture), _PROBE_SEEDS[host_fixture]))
