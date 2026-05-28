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

"""Shared-policy invariant guard for ``AsyncPool``.

When ``per_client_runtime`` is enabled, ``AsyncPool.start()`` applies a
one-shot mutation to ``clients[0]._policy``. The assumption is that all
N Clients share that same ``ClientPolicy`` PyO3 object (the documented
factory shape: ``lambda: Client(seeds, policy=shared_policy)``). If a
factory builds a fresh ``ClientPolicy`` per call, the one-shot mutation
would land only on client 0 and silently break the per-Client-runtime
promise for clients 1..N-1 — these tests catch that case explicitly.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from aerospike_sdk.aio.pool import AsyncPool


def _make_pool_with_clients(per_client_runtime: bool, clients_policies: list[object]) -> AsyncPool:
    """Construct an AsyncPool and pre-populate the data start() would see.

    The assertion helper only inspects ``client._policy`` identity, so we
    can fake the Clients with plain objects carrying a ``_policy`` field.
    """
    pool = AsyncPool(
        client_factory=lambda: (_ for _ in ()).throw(AssertionError("not called")),
        loop_count=len(clients_policies),
        per_client_runtime=per_client_runtime,
    )
    fake_clients = [SimpleNamespace(_policy=p) for p in clients_policies]
    return pool, fake_clients


def test_shared_policy_passes():
    """All clients share one policy → assertion passes silently."""
    shared = object()
    pool, clients = _make_pool_with_clients(
        per_client_runtime=True,
        clients_policies=[shared] * 4,
    )
    pool._assert_shared_policy_invariant(clients)  # must not raise


def test_distinct_policies_raises():
    """Each client has its own policy → assertion raises with diagnostic."""
    pool, clients = _make_pool_with_clients(
        per_client_runtime=True,
        clients_policies=[object(), object(), object(), object()],
    )
    with pytest.raises(RuntimeError, match="single ClientPolicy object"):
        pool._assert_shared_policy_invariant(clients)


def test_mostly_shared_but_one_drifted_raises():
    """N-1 clients share one policy; client at index 2 has its own → raises."""
    shared = object()
    odd_one_out = object()
    pool, clients = _make_pool_with_clients(
        per_client_runtime=True,
        clients_policies=[shared, shared, odd_one_out, shared],
    )
    with pytest.raises(RuntimeError, match=r"client 2's policy"):
        pool._assert_shared_policy_invariant(clients)


def test_single_client_passes():
    """N=1 → no pairwise comparison needed; assertion is a no-op."""
    pool, clients = _make_pool_with_clients(
        per_client_runtime=True,
        clients_policies=[object()],
    )
    pool._assert_shared_policy_invariant(clients)  # must not raise
