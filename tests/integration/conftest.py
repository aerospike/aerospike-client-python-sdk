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
    skip_if_lacks_server_compiled_ael,
    skip_if_server_compiled_ael_available,
)


@pytest.fixture(autouse=True)
def _honor_ael_path_markers(request: pytest.FixtureRequest) -> None:
    """Honor AEL path markers using the real ``client`` fixture (see ``tests/pac_compat``)."""
    need_server = request.node.get_closest_marker("requires_server_compiled_ael") is not None
    need_client = request.node.get_closest_marker("requires_client_side_ael") is not None
    if not (need_server or need_client):
        return
    client = request.getfixturevalue("client")
    if need_server:
        skip_if_lacks_server_compiled_ael(client)
    if need_client:
        skip_if_server_compiled_ael_available(client)
