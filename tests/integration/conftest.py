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

import pytest

from tests.pac_compat import (
    SupportsPacCapabilities,
    skip_if_lacks_query_selection,
    skip_if_lacks_server_compiled_ael,
)

_CAPABILITY_MARKERS: tuple[tuple[str, object], ...] = (
    ("requires_server_compiled_ael", skip_if_lacks_server_compiled_ael),
    ("requires_query_selection", skip_if_lacks_query_selection),
)


def pytest_runtest_call(item: pytest.Item) -> None:
    """Honor PAC capability markers once the test's fixtures are materialized."""
    skip_checks = [
        skip_fn
        for marker_name, skip_fn in _CAPABILITY_MARKERS
        if item.get_closest_marker(marker_name) is not None
    ]
    if not skip_checks:
        return

    client = resolve_sdk_client_from_funcargs(item.funcargs)
    if client is None:
        pytest.fail(
            "PAC capability marker present but no SDK client fixture found — "
            "name the fixture client / cluster* / session / session_with_* / "
            "qsel_client (or extend resolve_sdk_client_from_funcargs); skipping "
            "here would silently drop coverage.",
            pytrace=False,
        )

    for skip_fn in skip_checks:
        skip_fn(client)  # type: ignore[operator]


def _is_sdk_capability_client(candidate: object) -> bool:
    """True when *candidate* exposes the public SDK ``supports_*`` properties."""
    return (
        hasattr(candidate, "supports_server_compiled_ael")
        and hasattr(candidate, "supports_query_selection")
    )


def _unwrap_sdk_client(value: object) -> SupportsPacCapabilities | None:
    """Return an SDK client exposing ``supports_*`` flags, unwrapping facades."""
    if value is None:
        return None

    sdk = getattr(value, "_sdk_client", None)
    if sdk is not None and _is_sdk_capability_client(sdk):
        return sdk  # type: ignore[return-value]

    for candidate in (
        value,
        getattr(value, "_client", None),
        getattr(getattr(value, "client", None), "_client", None),
        getattr(value, "client", None),
    ):
        if candidate is not None and _is_sdk_capability_client(candidate):
            return candidate  # type: ignore[return-value]

    return None


def resolve_sdk_client_from_funcargs(
    funcargs: dict[str, object],
) -> SupportsPacCapabilities | None:
    """Return a connected SDK client from a test's resolved fixture dict."""
    if "client" in funcargs:
        client = _unwrap_sdk_client(funcargs["client"])
        if client is not None:
            return client

    for name, value in funcargs.items():
        if name.endswith("_client"):
            client = _unwrap_sdk_client(value)
            if client is not None:
                return client

    for name, value in funcargs.items():
        if name == "cluster" or name.startswith("cluster_"):
            client = _unwrap_sdk_client(value)
            if client is not None:
                return client

    for name, value in funcargs.items():
        if name == "session" or name.startswith("session_with_"):
            client = _unwrap_sdk_client(value)
            if client is not None:
                return client

    return None
