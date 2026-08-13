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

"""Server-compiled AEL filter capability helpers (field **43** ``[128, "<utf-8>"]``)."""

from __future__ import annotations

from typing import Any, List

from aerospike_async import FilterExpression

from aerospike_sdk import capabilities


def _pac_exposes_server_compiled_factory() -> bool:
    return callable(getattr(FilterExpression, "from_server_compiled_ael", None))


def supports_server_compiled_ael_routing(versions: List[Any]) -> bool:
    """Whether string AEL may route through field **43** on this cluster.

    Requires every node's ``Version.supports_server_compiled_ael()`` and PAC
    ``FilterExpression.from_server_compiled_ael``.
    """
    if not _pac_exposes_server_compiled_factory():
        return False
    return capabilities.supports_ael(versions)


def compute_server_compiled_ael_support_blocking(pac: Any) -> bool:
    """``True`` when field **43** routing is usable on the connected cluster."""
    nodes_fn = getattr(pac, "nodes_blocking", None)
    if not callable(nodes_fn):
        return False
    nodes = nodes_fn()
    if not nodes:
        return False
    return supports_server_compiled_ael_routing([node.version for node in nodes])
