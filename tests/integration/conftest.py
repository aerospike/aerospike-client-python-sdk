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

from tests.pac_compat import skip_if_lacks_server_compiled_ael


@pytest.fixture(autouse=True)
def _skip_unless_server_compiled_ael(request: pytest.FixtureRequest) -> None:
    """Honor ``@pytest.mark.requires_server_compiled_ael`` using the real ``client`` fixture."""
    if request.node.get_closest_marker("requires_server_compiled_ael") is None:
        return
    client = request.getfixturevalue("client")
    skip_if_lacks_server_compiled_ael(client)
