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
   (``RecordStream`` vs ``SyncRecordStream``).
"""

import inspect

import pytest

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
from aerospike_sdk.record_stream import RecordStream
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
from aerospike_sdk.sync.record_stream import SyncRecordStream

# The stream pair legitimately differs on its runtime-bound *source adapters* —
# the classmethods that wrap a per-runtime producer. Async wraps a PAC async
# recordset (``from_pac_recordset`` / ``from_chunked_pac_recordset``); sync
# wraps a blocking recordset (``from_recordset`` / ``from_chunked_recordset``).
# Like the iterator/context-manager dunders, these are runtime-bound by nature,
# so they're sanctioned rather than treated as drift.
_STREAM_SOURCE_ADAPTERS = {
    "from_pac_recordset",
    "from_chunked_pac_recordset",
    "from_recordset",
    "from_chunked_recordset",
}

# (async_leaf, sync_leaf, label, allowed_diff). Every pair must have an
# identical public surface apart from ``allowed_diff``; the stream pair's
# runtime dunders are private and excluded automatically.
_PAIRS = [
    (AsyncQueryBuilder, SyncQueryBuilder, "QueryBuilder", set()),
    (AsyncWriteSegmentBuilder, SyncWriteSegmentBuilder, "WriteSegmentBuilder", set()),
    (AsyncSingleKeyWriteSegment, SyncSingleKeyWriteSegment, "_SingleKeyWriteSegment", set()),
    (AsyncIndexBuilder, SyncIndexBuilder, "IndexBuilder", set()),
    (AsyncUdfFunctionBuilder, SyncUdfFunctionBuilder, "UdfFunctionBuilder", set()),
    (AsyncUdfBuilder, SyncUdfBuilder, "UdfBuilder", set()),
    (RecordStream, SyncRecordStream, "RecordStream", _STREAM_SOURCE_ADAPTERS),
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
