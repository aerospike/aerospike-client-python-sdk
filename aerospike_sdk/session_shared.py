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

"""Neutral session-level types shared by async + sync session implementations.

No asyncio anywhere. Lives at the package root so neither
:mod:`aerospike_sdk.aio.session` nor :mod:`aerospike_sdk.sync.session` has
to reach across tiers for these.
"""

from __future__ import annotations

from typing import NamedTuple


class NamespaceScStatus(NamedTuple):
    """Result of :meth:`aerospike_sdk.aio.session.Session.namespace_sc_status` /
    :meth:`aerospike_sdk.sync.session.SyncSession.namespace_sc_status`."""

    is_sc: bool
    """True when the namespace exists and ``strong-consistency`` is enabled."""
    detail: str
    """Empty when ``is_sc`` is true; otherwise a short explanation for logging or skips."""
