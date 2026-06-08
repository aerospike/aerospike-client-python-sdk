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

"""Runtime :func:`pytest.mark.xfail` ``condition`` helpers tied to live cluster version.

Pytest evaluates ``condition`` for ``xfail`` at import/collection time for plain
booleans. Aerospike version is only known after the ``client`` fixture connects,
so these objects always compare **false** at import time and integrate with
``tests/integration/async/exp_test.py`` (module autouse), which evaluates them
before each **async** integration test in that module and calls
:func:`pytest.xfail` when the bound applies.

Use :func:`server_version_lt` when a feature or bug applies only **below** a build
(for example ``server_version_lt(\"8.1.4\")`` once a fix ships in 8.1.4). Use
:func:`server_version_gte` when behaviour is wrong on a build **and newer** (for
example server-side AEL regressions first present at 8.1.3).
"""

from __future__ import annotations

from tests.cluster_version import parse_version_spec, version_tuple_lt


class ServerVersionLt:
    """``xfail`` when the cluster's **minimum** active build is **strictly less** than *spec*."""

    __slots__ = ("_spec", "_bound")

    def __init__(self, spec: str) -> None:
        self._spec = spec
        self._bound = parse_version_spec(spec)

    @property
    def bound(self) -> tuple[int, ...]:
        return self._bound

    def __bool__(self) -> bool:
        # Never true at collection/import; real check is in integration async conftest.
        return False

    def __repr__(self) -> str:
        return f"ServerVersionLt({self._spec!r})"

    def should_xfail(self, cluster_min: tuple[int, ...]) -> bool:
        return version_tuple_lt(cluster_min, self._bound)


def server_version_lt(spec: str) -> ServerVersionLt:
    """Return ``condition=...`` for :func:`pytest.mark.xfail` (see module docstring)."""
    return ServerVersionLt(spec)


class ServerVersionGte:
    """``xfail`` when the cluster's **minimum** active build is **>=** *spec*."""

    __slots__ = ("_spec", "_bound")

    def __init__(self, spec: str) -> None:
        self._spec = spec
        self._bound = parse_version_spec(spec)

    @property
    def bound(self) -> tuple[int, ...]:
        return self._bound

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return f"ServerVersionGte({self._spec!r})"

    def should_xfail(self, cluster_min: tuple[int, ...]) -> bool:
        return not version_tuple_lt(cluster_min, self._bound)


def server_version_gte(spec: str) -> ServerVersionGte:
    """Return ``condition=...`` for :func:`pytest.mark.xfail` (see :class:`ServerVersionGte`)."""
    return ServerVersionGte(spec)
