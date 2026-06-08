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
:data:`requires_server_compiled_ael` (see ``tests/integration/conftest.py``).
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
    PAC must expose ``FilterExpression.from_server_compiled_ael``, and every active
    node must report server-compiled AEL support via PAC's ``Version`` API.
    """
    if client.supports_server_compiled_ael:
        return
    pytest.skip(
        "Requires server-compiled AEL: PAC FilterExpression.from_server_compiled_ael "
        "and every active node Version.supports_server_compiled_ael "
        "(Client.supports_server_compiled_ael)."
    )


# Integration tests: use with tests/integration/conftest.py autouse gate (resolves ``client``).
requires_server_compiled_ael = pytest.mark.requires_server_compiled_ael


def skip_if_pac_lacks_from_server_compiled_ael() -> None:
    """Skip when the installed ``aerospike_async`` predates ``from_server_compiled_ael``."""
    import aerospike_async

    factory = getattr(FilterExpression, "from_server_compiled_ael", None)
    if callable(factory):
        return
    loc = getattr(aerospike_async, "__file__", "?")
    pytest.skip(
        "PAC lacks FilterExpression.from_server_compiled_ael "
        f"(imported aerospike_async from {loc}). "
        "Rebuild PAC from your checkout: "
        "`cd ../aerospike-client-python-async && maturin develop --features tls`. "
        "Then reinstall this SDK without overwriting PAC: "
        "`pip install -r requirements-local.txt && pip install -e . --no-deps`. "
        "See README.md \"Local PAC and Rust core\"."
    )
