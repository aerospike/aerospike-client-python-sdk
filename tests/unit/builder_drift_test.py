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

"""Drift guard for the paired sync/async builder + stream leaves.

Under the shared-base design each concurrency variant is a thin leaf over a
runtime-agnostic base, adding only its own runtime-bound terminals. The two
leaves in a pair must therefore expose the *same* public surface — the only
sanctioned differences are the iterator/context-manager dunders on the stream
pair (async ``__aiter__``/``__anext__``/``__aenter__``/``__aexit__`` vs the
sync ``__iter__``/``__next__``/``__enter__``/``__exit__``), which are excluded
from the public-name comparison anyway.

Two independent checks mechanically catch every drift class that motivated the
shared-base consolidation:

1. **Missing / renamed public member** — the symmetric difference of the two
   leaves' public attribute names must be empty (a method dropped on one side,
   or a rename applied to only one side, both surface here).
2. **Signature skew** — for every shared public method, the parameter shape
   (names, kinds, and which are required) must match across the pair, so a
   drifted keyword (``keys_list=`` vs ``keys=``) or arity is caught even when
   both sides still expose the same method name. Annotations are ignored on
   purpose: return types and per-tree parameter types legitimately differ
   (``Client`` vs ``SyncClient`` on the ``client`` property, for instance).

A third, related check guards the *positional* constructor contract the
session hot paths depend on — see ``_POSITIONAL_CTOR_ORDER`` below.
"""

import inspect

import pytest

from aerospike_sdk.aio.cluster import Cluster as AsyncCluster
from aerospike_sdk.aio.cluster_definition import (
    ClusterDefinition as AsyncClusterDefinition,
    Host as AsyncHost,
)
from aerospike_sdk.aio.info import InfoCommands as AsyncInfoCommands
from aerospike_sdk.aio.background import (
    BackgroundOperationBuilder as AsyncBackgroundOperationBuilder,
    BackgroundTaskSession as AsyncBackgroundTaskSession,
    BackgroundUdfBuilder as AsyncBackgroundUdfBuilder,
    BackgroundUdfFunctionBuilder as AsyncBackgroundUdfFunctionBuilder,
    BackgroundWriteBinBuilder as AsyncBackgroundWriteBinBuilder,
)
from aerospike_sdk.aio.operations.index import IndexBuilder as AsyncIndexBuilder
from aerospike_sdk.aio.operations.query import (
    QueryBuilder as AsyncQueryBuilder,
    WriteSegmentBuilder as AsyncWriteSegmentBuilder,
    _SingleKeyWriteSegment as AsyncSingleKeyWriteSegment,
)
from aerospike_sdk.aio.operations.udf import (
    UdfBuilder as AsyncUdfBuilder,
    UdfFunctionBuilder as AsyncUdfFunctionBuilder,
)
from aerospike_sdk.aio.session import Session as AsyncSession
from aerospike_sdk.aio.transactional_session import (
    TransactionalSession as AsyncTransactionalSession,
)
from aerospike_sdk.operations_shared import _SingleKeyWriteSegmentBase
from aerospike_sdk.query_shared import _QueryBuilderBase
from aerospike_sdk.record_stream import RecordStream
from aerospike_sdk.sync.background import (
    SyncBackgroundOperationBuilder,
    SyncBackgroundTaskSession,
    SyncBackgroundUdfBuilder,
    SyncBackgroundUdfFunctionBuilder,
    SyncBackgroundWriteBinBuilder,
)
from aerospike_sdk.sync.cluster import Cluster as SyncCluster
from aerospike_sdk.sync.cluster_definition import (
    ClusterDefinition as SyncClusterDefinition,
    Host as SyncHost,
)
from aerospike_sdk.sync.info import InfoCommands as SyncInfoCommands
from aerospike_sdk.sync.operations.index import IndexBuilder as SyncIndexBuilder
from aerospike_sdk.sync.operations.query import (
    QueryBuilder as SyncQueryBuilder,
    WriteSegmentBuilder as SyncWriteSegmentBuilder,
    _SingleKeyWriteSegment as SyncSingleKeyWriteSegment,
)
from aerospike_sdk.sync.operations.udf import (
    UdfBuilder as SyncUdfBuilder,
    UdfFunctionBuilder as SyncUdfFunctionBuilder,
)
from aerospike_sdk.sync.record_stream import RecordStream as SyncRecordStream
from aerospike_sdk.sync.session import Session as SyncSession
from aerospike_sdk.sync.transactional_session import (
    TransactionalSession as SyncTransactionalSession,
)

# (async_leaf, sync_leaf, label, allowed_diff). Every pair must have an
# identical public surface apart from ``allowed_diff``; the stream pair's
# runtime dunders are private and excluded automatically. The stream pair's
# producer *source adapters* (``_from_pac_recordset`` /
# ``_from_chunked_pac_recordset`` / ``_from_pac_batch_stream``) are private
# plumbing on both trees, so they drop out of the public-surface comparison —
# no allowlist needed.
_PAIRS = [
    (AsyncQueryBuilder, SyncQueryBuilder, "QueryBuilder", set()),
    (AsyncWriteSegmentBuilder, SyncWriteSegmentBuilder, "WriteSegmentBuilder", set()),
    (AsyncSingleKeyWriteSegment, SyncSingleKeyWriteSegment, "_SingleKeyWriteSegment", set()),
    (AsyncIndexBuilder, SyncIndexBuilder, "IndexBuilder", set()),
    (AsyncUdfFunctionBuilder, SyncUdfFunctionBuilder, "UdfFunctionBuilder", set()),
    (AsyncUdfBuilder, SyncUdfBuilder, "UdfBuilder", set()),
    (RecordStream, SyncRecordStream, "RecordStream", set()),
    # Top-layer pairs (Phase 3). These are still standalone duplicates today; the
    # guard locks their surface *before* the shared-base hoist so every later step
    # is a mechanical, verifiable move. Same-name-across-trees classes (Cluster,
    # ClusterDefinition, Host) are aliased Async*/Sync* at import.
    (AsyncSession, SyncSession, "Session", set()),
    (AsyncCluster, SyncCluster, "Cluster", set()),
    (AsyncClusterDefinition, SyncClusterDefinition, "ClusterDefinition", set()),
    (AsyncHost, SyncHost, "Host", set()),
    (AsyncTransactionalSession, SyncTransactionalSession, "TransactionalSession", set()),
    (AsyncInfoCommands, SyncInfoCommands, "InfoCommands", set()),
    # Background-task family. The sync side is a wrapper over the async builders
    # rather than a shared-base leaf, which is exactly why it drifted unnoticed:
    # the four durable-delete verbs existed only on the async tree until these
    # pairs were guarded.
    (
        AsyncBackgroundTaskSession, SyncBackgroundTaskSession,
        "BackgroundTaskSession", set(),
    ),
    (
        AsyncBackgroundOperationBuilder, SyncBackgroundOperationBuilder,
        "BackgroundOperationBuilder", set(),
    ),
    (
        AsyncBackgroundUdfFunctionBuilder, SyncBackgroundUdfFunctionBuilder,
        "BackgroundUdfFunctionBuilder", set(),
    ),
    (
        AsyncBackgroundUdfBuilder, SyncBackgroundUdfBuilder,
        "BackgroundUdfBuilder", set(),
    ),
    (
        AsyncBackgroundWriteBinBuilder, SyncBackgroundWriteBinBuilder,
        "BackgroundWriteBinBuilder", set(),
    ),
]

_PAIR_IDS = [label for _, _, label, _ in _PAIRS]


def _public_names(cls: type) -> set:
    """Public attribute names (methods, classmethods, properties, constants)."""
    return {name for name in dir(cls) if not name.startswith("_")}


def _param_shape(func) -> tuple:
    """Normalized parameter shape: (name, kind, required) per param, minus self.

    Annotations and return type are intentionally excluded so tree-specific
    types (``RecordStream`` vs ``SyncRecordStream``) don't read as drift; only
    parameter names, kinds, and required-ness are compared.
    """
    shape = []
    for name, param in inspect.signature(func).parameters.items():
        if name == "self":
            continue
        required = param.default is inspect.Parameter.empty
        shape.append((name, param.kind, required))
    return tuple(shape)


@pytest.mark.parametrize("async_cls, sync_cls, label, allowed_diff", _PAIRS, ids=_PAIR_IDS)
def test_pair_public_surface_matches(async_cls, sync_cls, label, allowed_diff):
    """The two leaves in each pair expose the same public names (minus allowed)."""
    diff = (_public_names(async_cls) ^ _public_names(sync_cls)) - allowed_diff
    assert diff == set(), (
        f"{label}: sync/async public surface drifted; symmetric difference "
        f"should be empty but was {sorted(diff)}"
    )


@pytest.mark.parametrize("async_cls, sync_cls, label, allowed_diff", _PAIRS, ids=_PAIR_IDS)
def test_pair_shared_method_signatures_match(async_cls, sync_cls, label, allowed_diff):
    """Shared public methods have the same parameter shape across the pair."""
    shared = _public_names(async_cls) & _public_names(sync_cls)
    skew = {}
    for name in sorted(shared):
        a_attr = inspect.getattr_static(async_cls, name)
        s_attr = inspect.getattr_static(sync_cls, name)
        # Only compare things that carry a signature (methods / classmethods /
        # staticmethods). Data attributes and properties have no param list.
        a_func = a_attr.__func__ if isinstance(a_attr, (classmethod, staticmethod)) else a_attr
        s_func = s_attr.__func__ if isinstance(s_attr, (classmethod, staticmethod)) else s_attr
        if not inspect.isfunction(a_func) or not inspect.isfunction(s_func):
            continue
        a_shape = _param_shape(a_func)
        s_shape = _param_shape(s_func)
        if a_shape != s_shape:
            skew[name] = (a_shape, s_shape)
    assert not skew, (
        f"{label}: shared methods have divergent parameter shapes "
        f"(async vs sync): {skew}"
    )


# The session hot paths construct these two bases **positionally** to avoid
# materializing a kwargs dict per operation, so their parameter order is a
# load-bearing contract rather than an implementation detail. Reordering a base
# ``__init__`` would silently mis-wire every argument after the change point
# (a ``txn`` landing in ``cached_read_policy_sc``, say) with no type error and
# no import failure. Pin the order here so that refactor fails loudly instead.
_POSITIONAL_CTOR_ORDER = {
    _QueryBuilderBase: (
        "aerospike_sdk.aio.session.Session._fast_query_builder",
        (
            "client", "namespace", "set_name", "behavior", "indexes_monitor",
            "cached_read_policy", "cached_write_policy",
            "cached_read_policy_sc", "cached_write_policy_sc",
            "txn", "namespace_mode_resolver",
            "namespace_mode_resolver_blocking", "sdk_client",
        ),
    ),
    _SingleKeyWriteSegmentBase: (
        "aerospike_sdk.aio.session.Session._fast_write_segment",
        (
            "client", "key", "op_type", "behavior",
            "write_policy", "read_policy", "txn",
            "namespace_mode_resolver", "namespace_mode_resolver_blocking",
            "write_policy_sc", "read_policy_sc", "sdk_client",
        ),
    ),
}


@pytest.mark.parametrize(
    "base_cls", list(_POSITIONAL_CTOR_ORDER), ids=lambda c: c.__name__,
)
def test_positional_ctor_order_is_pinned(base_cls):
    """Bases built positionally on the hot path keep their parameter order."""
    call_site, expected = _POSITIONAL_CTOR_ORDER[base_cls]
    actual = tuple(inspect.signature(base_cls.__init__).parameters)[1:]
    assert actual == expected, (
        f"{base_cls.__name__}.__init__ parameter order changed. "
        f"{call_site} passes these positionally — update that call site (and "
        f"any sibling fast path) to match before updating this expectation.\n"
        f"  expected: {expected}\n  actual:   {actual}"
    )
