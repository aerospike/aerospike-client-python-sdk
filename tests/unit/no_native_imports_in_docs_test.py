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

"""The docs and examples never import ``aerospike_native``.

Everything SDK code needs is re-exported from ``aerospike_sdk``, and what the
docs show is what callers copy. An import of the native package there means a
type is missing from the SDK's exports, or the example is out of date. The
package's name as a logger name is not an import and does not match.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NATIVE_IMPORT = re.compile(r"^\s*(from aerospike_native\b|import aerospike_native\b)")


def _files():
    yield ROOT / "README.md"
    for pattern in ("docs/guide/**/*.md", "docs/api/**/*.md", "examples/**/*.py"):
        yield from sorted(ROOT.glob(pattern))


def test_docs_and_examples_do_not_import_the_native_package():
    hits = [
        f"{path.relative_to(ROOT)}:{number}: {line.strip()}"
        for path in _files()
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if NATIVE_IMPORT.match(line)
    ]
    assert not hits, "import from aerospike_sdk instead:\n" + "\n".join(hits)


def test_the_scan_covers_the_docs_and_examples():
    """Guards the test above against a moved directory matching nothing."""
    scanned = [path.relative_to(ROOT).as_posix() for path in _files()]
    for prefix in ("README.md", "docs/guide/", "docs/api/", "examples/"):
        assert any(name.startswith(prefix) for name in scanned), prefix
