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

from collections.abc import Awaitable
from typing import Any, Protocol

import pytest
from aerospike_async.exceptions import InvalidRequest, ResultCode
from aerospike_sdk.feature_gates import PSDK_ENABLE_SERVER_COMPILED_AEL


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
    if not PSDK_ENABLE_SERVER_COMPILED_AEL:
        pytest.skip(
            "server-compiled AEL feature gate disabled "
            "(PSDK_ENABLE_SERVER_COMPILED_AEL)"
        )
    if client.supports_server_compiled_ael:
        return
    pytest.skip(
        "Requires server-compiled AEL: PAC FilterExpression.from_server_compiled_ael "
        "and first active node Version.supports_server_compiled_ael "
        "(Client.supports_server_compiled_ael; homogeneous cluster assumption)."
    )

requires_server_compiled_ael = pytest.mark.requires_server_compiled_ael
