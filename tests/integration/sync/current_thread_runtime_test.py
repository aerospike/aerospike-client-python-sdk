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
# License for the specific language governing permissions and limitations
# under the License.

"""Boundary coverage for the experimental per-thread-runtime sync path.

This mode swaps the shared client for a thread-local proxy that builds a
separate underlying client per calling OS thread, so it is a genuinely
distinct code path that no other sync test touches — and it had no coverage
at all. It is reachable only through the deprecated ``SyncClient`` and is
deliberately *not* exposed on ``ClusterDefinition``, because the per-thread
client implements only part of the operation surface.

The point of this file is to pin that boundary. The key-value surface must
keep working, and the unsupported operations must keep failing loudly rather
than silently degrading — if a future dependency bump fills those gaps, these
tests fail and prompt a fresh look at promoting the mode to the public
builder. Throughput claims belong in the benchmark suite, not here.
"""

from concurrent.futures import ThreadPoolExecutor

import pytest

from aerospike_sdk import Behavior, DataSet
# Imported from the defining module rather than the package namespace, whose
# deprecation shim would fire on import and add noise unrelated to this file.
from aerospike_sdk.sync.client import SyncClient
from aerospike_sdk.sync._threadlocal_client import _ThreadLocalLocalClient
from tests.integration.namespace import general_namespace

SET_NAME = "sync_ct_runtime"


@pytest.fixture
def ct_client(aerospike_host, client_policy):
    """A client whose sync ops use a per-thread runtime.

    Built directly rather than through ``ClusterDefinition``: the opt-in has
    no public builder equivalent, by design. ``client_policy`` carries auth
    when the general leg targets an auth-required seed.
    """
    with SyncClient(aerospike_host, policy=client_policy, current_thread_runtime=True) as client:
        yield client


def test_installs_the_thread_local_proxy(ct_client):
    """Opting in must actually swap the client, not silently no-op."""
    assert isinstance(ct_client._client, _ThreadLocalLocalClient)


def test_key_value_surface_round_trips_across_threads(ct_client):
    """Records written on worker threads are readable from another thread.

    Each thread lazily builds its own underlying client, so this is a real
    cross-connection round-trip rather than a single-client echo.
    """
    session = ct_client.create_session(Behavior.DEFAULT)
    ds = DataSet.of(general_namespace(), SET_NAME)
    keys = [ds.id(f"t{i}") for i in range(4)]

    def _write(index: int) -> None:
        session.upsert(keys[index]).put({"tid": index}).execute()

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(_write, range(4)))

    for index, key in enumerate(keys):
        assert session.get(key).bins["tid"] == index
        assert session.exists(key).execute() is not None

    # Single-key and multi-key builder chains route through the same
    # per-thread ops, so they work where a dataset scan does not.
    assert session.query(keys[0]).execute().first_or_raise().record.bins["tid"] == 0
    assert len(list(session.query(keys).execute())) == len(keys)

    for key in keys:
        session.delete(key).execute()


def test_dataset_query_is_unsupported(ct_client):
    """Scans have no per-thread equivalent — must fail, not silently misbehave."""
    session = ct_client.create_session(Behavior.DEFAULT)
    with pytest.raises(Exception, match="query_blocking"):
        list(session.query(DataSet.of(general_namespace(), SET_NAME)).execute())


def test_cluster_wide_info_is_unsupported(ct_client):
    """Fan-out info (index/UDF listing) has no per-thread equivalent."""
    with pytest.raises(AttributeError, match="info_on_all_nodes_blocking"):
        ct_client._list_indexes()
