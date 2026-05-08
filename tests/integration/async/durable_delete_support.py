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

"""Helpers for durable-delete integration scenarios."""

from __future__ import annotations

from typing import Iterable

from aerospike_async import Key


async def delete_keys_durable(session, keys: Iterable[Key]) -> None:
    """Best-effort cleanup using durable delete when supported, then plain delete.

    Strong-consistency namespaces expect durable deletes for tests that rely on
    tombstones; this mirrors the cleanup pattern used across durable-delete
    suites.
    """
    for k in keys:
        try:
            await session.delete(k).durably_delete().execute()
        except Exception:
            try:
                await session.delete(k).execute()
            except Exception:
                pass
