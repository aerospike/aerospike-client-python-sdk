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

"""Cluster server-capability resolution (minimum version across all nodes).

A cluster supports a feature only when *every* connected node does, so each
predicate folds the per-node :class:`~aerospike_async.Version` with ``all``;
:func:`min_version` returns the minimum version across the nodes — the
least-capable node a caller must guard against. These are pure functions over
a list of PAC ``Version`` objects — the async and sync clients supply the list
from their respective node accessors, keeping one implementation for both.

All predicates delegate to PAC's own ``Version.supports_*`` methods (PAC owns
the authoritative version→capability mapping). The pinned ``aerospike-async``
dependency is assumed to expose those predicates and
``FilterExpression.from_server_compiled_ael``; older PAC builds are unsupported.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple


def version_key(version: Any) -> Tuple[int, int, int, int]:
    """Sortable ``(major, minor, patch, build)`` tuple for a PAC ``Version``."""
    return (version.major, version.minor, version.patch, version.build)


def min_version(versions: List[Any]) -> Optional[Any]:
    """The minimum version across the nodes (least-capable), or ``None`` if empty."""
    if not versions:
        return None
    return min(versions, key=version_key)


def supports_ael(versions: List[Any]) -> bool:
    """Whether every node parses server-compiled AEL (filters/exp reads+writes)."""
    return bool(versions) and all(v.supports_server_compiled_ael() for v in versions)


def supports_query_operations(versions: List[Any]) -> bool:
    """Whether every node supports read operations inside an index query."""
    return bool(versions) and all(v.supports_query_ops_projection_ext() for v in versions)


def supports_string_operations(versions: List[Any]) -> bool:
    """Whether every node supports the server-side string operations."""
    return bool(versions) and all(v.supports_string_operations() for v in versions)


def supports_query_selection(versions: List[Any]) -> bool:
    """Whether every node supports server-led index selection (field ``44``)."""
    return bool(versions) and all(v.supports_query_selection() for v in versions)
