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

"""Shared async integration fixtures.

``Client.connect`` + auth against a real cluster is typically ~1s per connection.
Using one module-scoped client per test file removes that overhead from every
test function. Tests must keep keys isolated (unique names / sets) or clean up.
"""

import pytest_asyncio

from aerospike_sdk.aio.client import Client


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(aerospike_host, client_policy):
    """One connected async :class:`~aerospike_sdk.aio.client.Client` per module."""
    async with Client(seeds=aerospike_host, policy=client_policy) as c:
        yield c


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client_sc(aerospike_host_sc, client_policy_sc):
    """One Client per module against ``AEROSPIKE_HOST_SC`` (MRT / SC-only suites)."""
    async with Client(seeds=aerospike_host_sc, policy=client_policy_sc) as c:
        yield c
