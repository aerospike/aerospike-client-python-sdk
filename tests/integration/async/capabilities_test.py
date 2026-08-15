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

"""Live wiring tests for server-capability probes on a connected cluster.

The all-nodes fold logic is unit-tested in ``tests/unit/capabilities_test.py``
against fakes. This module adds two live checks: probe wiring (delegation to
``capabilities.supports_*`` over node versions) and version-floor alignment
against the cluster's reported minimum version (catches a wrong mapping on CI).
"""

import pytest
import pytest_asyncio

from aerospike_sdk import Version, capabilities

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def cluster(aerospike_host, make_cluster_definition):
    async with await make_cluster_definition(aerospike_host).connect() as c:
        yield c


class TestCapabilityProbes:

    async def test_server_version_is_reported(self, cluster):
        v = await cluster.server_version()
        assert isinstance(v, Version)
        # A real cluster reports a positive major version.
        assert v.major >= 1

    async def test_probes_return_bools(self, cluster):
        for probe in (cluster.supports_ael, cluster.supports_query_operations,
                      cluster.supports_string_operations,
                      cluster.supports_query_selection):
            assert isinstance(await probe(), bool)

    async def test_probes_agree_with_pac_version_predicates(self, cluster):
        """Wiring: cluster probe → ``capabilities.supports_*`` over live node versions.

        This alone is tautological (same fold on both sides); pair with
        :meth:`test_probes_match_reported_version_floors` to catch a bad mapping.
        """
        versions = await cluster._sdk_client._cluster_versions()
        assert versions, "connected cluster should report at least one node"
        assert await cluster.supports_query_operations() == (
            capabilities.supports_query_operations(versions)
        )
        assert await cluster.supports_string_operations() == (
            capabilities.supports_string_operations(versions)
        )
        assert await cluster.supports_ael() == capabilities.supports_ael(versions)
        assert await cluster.supports_query_selection() == (
            capabilities.supports_query_selection(versions)
        )

    async def test_probes_match_reported_version_floors(self, cluster):
        """Each probe matches the version floor it gates on (relative to ``server_version()``).

        Catches a wrong ``capabilities.supports_*`` mapping that the delegation
        check above would miss on a homogeneous CI cluster.
        """
        v = await cluster.server_version()
        assert v is not None
        vt = (v.major, v.minor, v.patch)
        assert await cluster.supports_query_operations() == (vt >= (8, 1, 2))
        assert await cluster.supports_string_operations() == (vt >= (8, 1, 3))
        assert await cluster.supports_ael() == (vt >= (8, 1, 3))
        assert await cluster.supports_query_selection() == (vt >= (8, 1, 3))
