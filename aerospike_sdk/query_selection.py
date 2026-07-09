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

"""Server-led query selection capability helpers (field ``44`` explain → execute)."""

from __future__ import annotations

from typing import Any


def _version_supports_query_selection(version_obj: object) -> bool:
    """Call PAC ``Version.supports_query_selection()`` when present."""
    fn = getattr(version_obj, "supports_query_selection", None)
    if not callable(fn):
        return False
    return bool(fn())


async def compute_query_selection_support(pac: Any) -> bool:
    """``True`` when every connected node reports query-selection support.

    Mirrors Rust ``Cluster::supports_query_selection()`` (all nodes >= 8.1.3).
    """
    nodes_fn = getattr(pac, "nodes", None)
    if not callable(nodes_fn):
        return False
    nodes = await nodes_fn()
    if not nodes:
        return False
    return all(_version_supports_query_selection(n.version) for n in nodes)


def compute_query_selection_support_blocking(pac: Any) -> bool:
    """Blocking counterpart of :func:`compute_query_selection_support`."""
    nodes_fn = getattr(pac, "nodes_blocking", None)
    if not callable(nodes_fn):
        return False
    nodes = nodes_fn()
    if not nodes:
        return False
    return all(_version_supports_query_selection(n.version) for n in nodes)
