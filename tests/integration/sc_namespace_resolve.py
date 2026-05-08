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

"""Pick the strong-consistency namespace name for integration tests."""

from __future__ import annotations

import os
from typing import Any


class MultipleScNamespacesError(Exception):
    """More than one SC namespace exists and ``AEROSPIKE_SC_NAMESPACE`` is unset."""

    def __init__(self, names: list[str]) -> None:
        self.names = names
        super().__init__(", ".join(sorted(names)))


class NoStrongConsistencyNamespace(Exception):
    """Env unset and no namespace on the cluster reports strong consistency."""

    def __init__(self, namespace_names: list[str]) -> None:
        self.namespace_names = namespace_names
        super().__init__(", ".join(sorted(namespace_names)))


def skip_reason_no_sc_namespace(namespace_names: list[str]) -> str:
    """Human-readable pytest skip text when no SC namespace is available."""
    listed = ", ".join(sorted(namespace_names)) if namespace_names else "(could not list namespaces)"
    msg = (
        "No namespace has strong-consistency enabled "
        f"(namespaces seen on cluster: {listed}). "
        "These tests require Aerospike Enterprise with strong-consistency on at least "
        "one namespace; AP-only clusters skip."
    )
    if not os.environ.get("AEROSPIKE_HOST_SC", "").strip():
        msg += (
            " If SC is on another seed (different host/port), set AEROSPIKE_HOST_SC."
        )
    return msg


def pinned_namespace_env_hint() -> str:
    """Suffix for skip messages when ``AEROSPIKE_SC_NAMESPACE`` pins a name."""
    if not os.environ.get("AEROSPIKE_SC_NAMESPACE", "").strip():
        return ""
    return (
        " Remove or fix AEROSPIKE_SC_NAMESPACE (e.g. in aerospike.env); "
        "unset enables auto-select when exactly one SC namespace exists."
    )


async def resolve_sc_namespace(session: Any) -> str:
    """Resolve namespace for SC-gated tests.

    1. If ``AEROSPIKE_SC_NAMESPACE`` is set and non-empty, return it.
    2. If the cluster has exactly one namespace with strong consistency enabled,
       return that name (no env var needed).
    3. If several namespaces are SC and env is unset, raise
       :class:`MultipleScNamespacesError`.
    4. If env is unset and no namespace is SC, raise
       :class:`NoStrongConsistencyNamespace` (cluster is AP-only or Enterprise without SC).
    """
    explicit = os.environ.get("AEROSPIKE_SC_NAMESPACE", "").strip()
    if explicit:
        return explicit
    try:
        names = sorted(await session.info().namespaces())
    except Exception:
        names = []
    sc_names: list[str] = []
    for ns in names:
        st = await session.namespace_sc_status(ns)
        if st.is_sc:
            sc_names.append(ns)
    if len(sc_names) == 1:
        return sc_names[0]
    if len(sc_names) > 1:
        raise MultipleScNamespacesError(sc_names)
    raise NoStrongConsistencyNamespace(names)


def resolve_sc_namespace_sync(session: Any) -> str:
    """Sync variant of :func:`resolve_sc_namespace`."""
    explicit = os.environ.get("AEROSPIKE_SC_NAMESPACE", "").strip()
    if explicit:
        return explicit
    try:
        names = sorted(session.info().namespaces())
    except Exception:
        names = []
    sc_names: list[str] = []
    for ns in names:
        st = session.namespace_sc_status(ns)
        if st.is_sc:
            sc_names.append(ns)
    if len(sc_names) == 1:
        return sc_names[0]
    if len(sc_names) > 1:
        raise MultipleScNamespacesError(sc_names)
    raise NoStrongConsistencyNamespace(names)
