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

"""Shared helpers for UDF admin operations (used by the aio and sync clients)."""

from __future__ import annotations


def parse_udf_list(raw: str) -> list[dict[str, str]]:
    """Parse a server ``udf-list`` info response into module descriptors.

    The server returns modules as ``filename=<name>,hash=<sha>,type=<lang>``
    entries joined by ``;`` (empty when nothing is registered). Each entry
    becomes a dict with ``name`` / ``hash`` / ``type`` keys, matching the
    shape the legacy Python client's ``udf_list`` returns.
    """
    modules: list[dict[str, str]] = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        fields: dict[str, str] = {}
        for kv in entry.split(","):
            key, sep, value = kv.partition("=")
            if sep:
                fields[key.strip()] = value.strip()
        modules.append(
            {
                # server uses `filename`; `module` kept as a defensive fallback
                "name": fields.get("filename") or fields.get("module", ""),
                "hash": fields.get("hash", ""),
                "type": fields.get("type", ""),
            }
        )
    return modules
