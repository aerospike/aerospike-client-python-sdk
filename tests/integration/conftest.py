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
    SupportsServerCompiledAel,
    skip_if_lacks_server_compiled_ael,
    skip_if_server_compiled_ael_available,
)


def pytest_runtest_call(item: pytest.Item) -> None:
    """Honor AEL path markers once the test's fixtures are materialized."""
    need_server = item.get_closest_marker("requires_server_compiled_ael") is not None
    need_client = item.get_closest_marker("requires_client_side_ael") is not None
    if not (need_server or need_client):
        return
    client = resolve_ael_client_from_funcargs(item.funcargs)
    if client is None:
        pytest.fail(
            "AEL path marker present but no client/cluster/session fixture "
            "found — the test's mode cannot be determined. Name the fixture "
            "client / cluster* / session / session_with_* (or extend "
            "resolve_ael_client_from_funcargs); skipping here would silently "
            "drop coverage for both modes.",
            pytrace=False,
        )
    if need_server:
        skip_if_lacks_server_compiled_ael(client)
    if need_client:
        skip_if_server_compiled_ael_available(client)


def resolve_ael_client_from_funcargs(
    funcargs: dict[str, object],
) -> SupportsServerCompiledAel | None:
    """Return a connected SDK client from a test's resolved fixture dict."""
    if "client" in funcargs:
        client = funcargs["client"]
        if getattr(client, "supports_server_compiled_ael", None) is not None:
            return client  # type: ignore[return-value]

    for name, value in funcargs.items():
        if name == "cluster" or name.startswith("cluster_"):
            sdk_client = getattr(value, "_sdk_client", None)
            if sdk_client is not None:
                return sdk_client  # type: ignore[return-value]

    for name, value in funcargs.items():
        if name == "session" or name.startswith("session_with_"):
            client = getattr(value, "client", None)
            if getattr(client, "supports_server_compiled_ael", None) is not None:
                return client  # type: ignore[return-value]

    return None
