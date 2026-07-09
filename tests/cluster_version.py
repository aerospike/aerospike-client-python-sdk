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

"""Parse Aerospike server build versions from PAC node metadata (integration tests)."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aerospike_sdk.aio.client import Client as SdkClient


def parse_version_spec(spec: str) -> tuple[int, ...]:
    """Parse ``\"8.1.3\"`` or ``\"8.1.3.0\"`` into a tuple of ints."""
    parts = spec.strip().split(".")
    if not parts or any(p == "" for p in parts):
        raise ValueError(f"invalid version spec: {spec!r}")
    return tuple(int(p) for p in parts)


def _normalize(t: tuple[int, ...], width: int = 8) -> tuple[int, ...]:
    t = t + (0,) * width
    return t[:width]


def version_tuple_lt(a: tuple[int, ...], b: tuple[int, ...]) -> bool:
    """Lexicographic compare on zero-padded tuples (Aerospike build semantics)."""
    return _normalize(a) < _normalize(b)


def version_tuple_from_pac(version_obj: object) -> tuple[int, ...]:
    """Best-effort parse of PAC node :attr:`version` into numeric tuple."""
    text = str(version_obj)
    m = re.search(r"\b(\d+)\.(\d+)\.(\d+)(?:\.(\d+))?\b", text)
    if not m:
        return (0,)
    return tuple(int(m.group(i)) for i in range(1, 5) if m.group(i) is not None)


async def min_active_server_version_tuple(client: SdkClient) -> tuple[int, ...]:
    """Minimum version tuple among active nodes (conservative for mixed clusters)."""
    pac = client.underlying_client
    nodes = await pac.nodes()
    active = [n for n in nodes if n.is_active]
    if not active:
        return (0,)
    tuples = [version_tuple_from_pac(n.version) for n in active]
    return min(_normalize(t) for t in tuples)
