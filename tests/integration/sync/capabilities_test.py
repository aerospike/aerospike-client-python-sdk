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

"""Live server-capability probes on a connected sync cluster.

Sync counterpart of the async capabilities suite; the sync client has its own
node accessor (`_cluster_versions_blocking`), so it needs its own live check.
"""

import pytest

from aerospike_sdk import Version


@pytest.fixture(scope="module")
def cluster(aerospike_host, make_cluster_definition):
    with make_cluster_definition(aerospike_host, sync=True).connect() as c:
        yield c


class TestSyncCapabilityProbes:

    def test_server_version_is_reported(self, cluster):
        v = cluster.server_version()
        assert isinstance(v, Version)
        assert v.major >= 1

    def test_probes_return_bools(self, cluster):
        for probe in (cluster.supports_ael, cluster.supports_query_operations,
                      cluster.supports_string_operations,
                      cluster.supports_query_selection):
            assert isinstance(probe(), bool)

    def test_probes_agree_with_reported_version(self, cluster):
        v = cluster.server_version()
        vt = (v.major, v.minor, v.patch)
        assert cluster.supports_query_operations() == (vt >= (8, 1, 2))
        assert cluster.supports_string_operations() == (vt >= (8, 1, 3))
        assert cluster.supports_ael() == (vt >= (8, 1, 3))
        assert cluster.supports_query_selection() == (vt >= (8, 1, 3))
