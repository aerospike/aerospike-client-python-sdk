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

"""Thread-local PAC `LocalClient` proxy.

Each Python OS thread that touches this proxy gets its own
:class:`aerospike_async.LocalClient` — a sync-only client that owns a
``current_thread`` Tokio runtime on the calling thread. Eliminates the
cross-thread worker hop that the shared multi-thread runtime imposes on
the standard ``Client.*_blocking`` methods.

The proxy duck-types as a PAC ``Client`` for the ``*_blocking`` /
``*_blocking_with_overrides`` surface PSDK's sync hot path uses, so
:class:`SyncSession` and its builder/segment children can use either the
shared client or this thread-local proxy without code changes.

Documented caveats:

- **Lifetime**: per-thread Clients live until the thread exits. Use a
  long-lived thread pool, not short-lived threads, to avoid construction
  cost / connection-pool churn.
- **Cluster tend**: each per-thread Client runs its own cluster tend
  loop. At high thread counts (32+) this multiplies cluster info-call
  load — file a server-side ticket if it becomes an issue. Set
  ``tend_interval`` higher (e.g. 10s) for high-thread workloads.
- **Connection multiplication**: N threads × ``conn_pools_per_node`` =
  total connections per node. Set ``conn_pools_per_node = 1`` in your
  ClientPolicy when using this mode (the proxy doesn't override it for
  you — it respects whatever you passed).
"""

from __future__ import annotations

import threading
from typing import Any

from aerospike_async import ClientPolicy
from aerospike_async import _LocalClient as LocalClient


class _ThreadLocalLocalClient:
    """Proxy returning a per-thread experimental ``_LocalClient`` from PAC."""

    __slots__ = ("_policy", "_seeds", "_tls", "_closed")

    def __init__(self, policy: ClientPolicy, seeds: str) -> None:
        self._policy = policy
        self._seeds = seeds
        self._tls = threading.local()
        self._closed = False

    def _get(self) -> LocalClient:
        c = getattr(self._tls, "client", None)
        if c is None:
            if self._closed:
                raise RuntimeError("SyncClient is closed")
            c = LocalClient(self._policy, self._seeds)
            self._tls.client = c
        return c

    # -- Hot-path methods forwarded explicitly (every µs counts) ----------

    def get_blocking(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().get_blocking(*args, **kwargs)

    def put_blocking(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().put_blocking(*args, **kwargs)

    def operate_blocking(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().operate_blocking(*args, **kwargs)

    def delete_blocking(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().delete_blocking(*args, **kwargs)

    def touch_blocking(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().touch_blocking(*args, **kwargs)

    def exists_blocking(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().exists_blocking(*args, **kwargs)

    def get_blocking_with_overrides(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().get_blocking_with_overrides(*args, **kwargs)

    def operate_blocking_with_overrides(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().operate_blocking_with_overrides(*args, **kwargs)

    def info_blocking(self, *args: Any, **kwargs: Any) -> Any:
        return self._get().info_blocking(*args, **kwargs)

    # -- Lifecycle ---------------------------------------------------------

    def close_blocking(self) -> None:
        # Mark the proxy as closed; per-thread Clients drop when their
        # owning thread's `threading.local` goes out of scope (thread exit
        # + Python GC).  Future `_get()` calls from any thread error out.
        self._closed = True

    # -- Fallback for any uncommon PAC method ------------------------------

    def __getattr__(self, name: str) -> Any:
        # Only fires for attrs missing on this proxy; explicit methods above
        # are preferred.  This catches `truncate_blocking`,
        # `register_udf_blocking`, etc. that the standard SyncClient
        # delegates through `underlying_client`.
        return getattr(self._get(), name)
