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

"""Invariant: ``aerospike_sdk/sync/`` must not import ``asyncio``.

The whole point of :class:`~aerospike_sdk.sync.client.SyncClient` is that it
runs without an asyncio event loop. Every IO path routes through PAC's
``_blocking`` entries. If a regression accidentally pulls ``asyncio`` into
the sync tree, this test fails — catching the issue at unit-test time
rather than at runtime in production code that may never have seen a loop.
"""

import re
from pathlib import Path


_SYNC_PKG_ROOT = Path(__file__).parent.parent.parent / "aerospike_sdk" / "sync"

# Match `import asyncio` and `from asyncio import ...` at module scope or
# function scope. We grep on raw text so even deferred / inline imports
# trip the invariant.
_ASYNCIO_IMPORT = re.compile(r"^\s*(import\s+asyncio|from\s+asyncio\s+import)", re.MULTILINE)


def test_no_asyncio_imports_in_sync_tree():
    """Every file under ``aerospike_sdk/sync/`` must be asyncio-free."""
    offenders: list[tuple[Path, list[int]]] = []
    for py in _SYNC_PKG_ROOT.rglob("*.py"):
        text = py.read_text()
        matches = [
            text[:m.start()].count("\n") + 1
            for m in _ASYNCIO_IMPORT.finditer(text)
        ]
        if matches:
            offenders.append((py.relative_to(_SYNC_PKG_ROOT.parent.parent), matches))

    if offenders:
        report = "\n".join(
            f"  {path}: line(s) {lines}" for path, lines in offenders
        )
        raise AssertionError(
            "asyncio import(s) found in aerospike_sdk/sync/ tree:\n"
            + report
            + "\n\nSync code must route IO through PAC's `_blocking` entries; "
            "no asyncio loop is constructed or required.",
        )
