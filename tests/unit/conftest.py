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

"""Unit-suite fixtures. Unit tests never connect; seeds are inert config."""

import pytest

from aerospike_sdk.aio.cluster_definition import ClusterDefinition, Host


@pytest.fixture
def unit_cluster_definition(aerospike_host) -> ClusterDefinition:
    """Unconnected ClusterDefinition on the env-configured seed.

    For construction-time tests only — nothing here performs I/O, so the
    seed value is inert; it rides :func:`aerospike_host` so the unit suite
    stays consistent with the integration seed configuration.
    """
    return ClusterDefinition(hosts=Host.parse_hosts(aerospike_host, 3000))
