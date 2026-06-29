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

"""PAC capability checks shared by unit and integration tests.

Integration tests that need server-compiled AEL on the wire can use
:data:`requires_server_compiled_ael`; tests that assume the **client-side**
string-AEL path (no server compilation for ``where(str)``) can use
:data:`requires_client_side_ael` (see ``tests/integration/conftest.py``).

Runtime :func:`xfail_if_server_compiled_ael_wire_active` /
:func:`xfail_if_server_compiled_ael_factory_exposed` mark known-broken cases
when the server-compiled AEL path is active (see in-repo xfail call sites).
"""

from __future__ import annotations

from typing import Protocol

import pytest
from aerospike_async import FilterExpression


class SupportsServerCompiledAel(Protocol):
    """Connected client (or stand-in) that reports server-compiled AEL availability."""

    @property
    def supports_server_compiled_ael(self) -> bool:
        ...


def skip_if_lacks_server_compiled_ael(client: SupportsServerCompiledAel) -> None:
    """Skip when server-compiled AEL is not available for this connection/cluster.

    Mirrors :attr:`aerospike_sdk.aio.client.Client.supports_server_compiled_ael`:
    PAC must expose ``FilterExpression.from_server_compiled_ael``, and the
    **first active** node's ``Version`` must report server-compiled AEL support
    (homogeneous cluster: all nodes same build).
    """
    if client.supports_server_compiled_ael:
        return
    pytest.skip(
        "Requires server-compiled AEL: PAC FilterExpression.from_server_compiled_ael "
        "and first active node Version.supports_server_compiled_ael "
        "(Client.supports_server_compiled_ael; homogeneous cluster assumption)."
    )


_XFAIL_SERVER_COMPILED_AEL_MSG = (
    "Known breakage when server-compiled AEL wire path is active "
    "(tracked; revisit when chain / operate + [128, AEL] is fixed)."
)


def xfail_if_server_compiled_ael_wire_active(client: SupportsServerCompiledAel) -> None:
    """Call at the start of an integration test that fails only under server-compiled AEL."""
    if client.supports_server_compiled_ael:
        pytest.xfail(_XFAIL_SERVER_COMPILED_AEL_MSG)


def xfail_if_server_compiled_ael_factory_exposed() -> None:
    """Call at the start of a unit test without a connected ``Client``.

    When PAC exposes ``FilterExpression.from_server_compiled_ael``, string AEL
    helpers may touch code paths that expect a full QueryBuilder (e.g.
    ``_supports_server_compiled_ael`` on the parent collector).
    """
    if callable(getattr(FilterExpression, "from_server_compiled_ael", None)):
        pytest.xfail(_XFAIL_SERVER_COMPILED_AEL_MSG)


def skip_if_server_compiled_ael_available(client: SupportsServerCompiledAel) -> None:
    """Skip when the SDK would use server-compiled AEL for string ``where()`` predicates.

    Use for integration tests that only apply to the client-side
    :func:`~aerospike_sdk.ael.parser.parse_ael` path (``Client.supports_server_compiled_ael``
    is false: missing PAC API, old server build, or pre-connect client).
    """
    if not client.supports_server_compiled_ael:
        return
    pytest.skip(
        "Requires client-side AEL parsing for string predicates: "
        "Client.supports_server_compiled_ael is true (server-compiled path in use)."
    )


# Integration tests: use with tests/integration/conftest.py autouse gate (resolves ``client``).
requires_server_compiled_ael = pytest.mark.requires_server_compiled_ael
requires_client_side_ael = pytest.mark.requires_client_side_ael
