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

"""Guard against installed-vs-pinned PAC (``aerospike-async``) drift.

The dev-channel PAC is resolved from an internal index out-of-band, so the
installed version can silently fall behind the exact pin in ``pyproject.toml``
(and a bare ``--pre`` install quietly resolves the newest *public* pre-release
instead). This is cheap insurance: it fails loudly the moment an index-resolved
install no longer matches the pin, instead of the mismatch surfacing weeks later.

An **editable** PAC checkout is exempt: its source tree declares a base version
with no dev-channel ``.devN`` suffix (that suffix comes from the publish run, not
the tree), so an exact match is neither expected nor meaningful there. The drift
this guards against is index-resolved wheel installs (CI and fresh dev envs).
"""

from __future__ import annotations

import json
import re
import tomllib
from importlib import metadata
from pathlib import Path

import pytest

_PAC_DIST = "aerospike-async"
_PAC_VECTOR_URL = "https://github.com/aerospike/aerospike-client-python-async.git"
_PAC_VECTOR_BRANCH_REQUIREMENT = f"{_PAC_DIST} @ git+{_PAC_VECTOR_URL}@aie/vector"


def _repo_pyproject() -> Path:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "pyproject.toml"
        if candidate.exists():
            return candidate
    raise AssertionError("could not locate pyproject.toml above this test")


def _pac_requirement() -> str | None:
    pyproject = tomllib.loads(_repo_pyproject().read_text())
    return next(
        (
            requirement
            for requirement in pyproject["project"]["dependencies"]
            if requirement.startswith(_PAC_DIST)
        ),
        None,
    )


def _pinned_pac_version(requirement: str | None) -> str | None:
    match = re.fullmatch(rf"{re.escape(_PAC_DIST)}==([A-Za-z0-9._-]+)", requirement or "")
    return match.group(1) if match else None


def _pac_is_editable() -> bool:
    try:
        raw = metadata.distribution(_PAC_DIST).read_text("direct_url.json")
    except Exception:
        return False
    if not raw:
        return False
    return bool(json.loads(raw).get("dir_info", {}).get("editable"))


def _versions_equal(a: str, b: str) -> bool:
    try:
        from packaging.version import Version

        return Version(a) == Version(b)
    except Exception:
        return a == b


def test_pac_dependency_source_is_explicit():
    """PAC must use either the release pin or the vector feature branch."""
    requirement = _pac_requirement()
    pinned = _pinned_pac_version(requirement)
    assert pinned is not None or requirement == _PAC_VECTOR_BRANCH_REQUIREMENT, (
        f"pyproject.toml must carry an exact '{_PAC_DIST}==<version>' pin or "
        f"the explicit vector-branch requirement {_PAC_VECTOR_BRANCH_REQUIREMENT!r}; "
        f"found {requirement!r}"
    )


def test_installed_pac_matches_pin():
    if _pac_is_editable():
        pytest.skip(
            f"{_PAC_DIST} installed editable; pin-match applies to index-resolved installs",
        )

    requirement = _pac_requirement()
    pinned = _pinned_pac_version(requirement)
    if requirement == _PAC_VECTOR_BRANCH_REQUIREMENT:
        direct_url = json.loads(metadata.distribution(_PAC_DIST).read_text("direct_url.json"))
        assert direct_url["url"] == _PAC_VECTOR_URL
        assert direct_url["vcs_info"]["vcs"] == "git"
        assert direct_url["vcs_info"]["requested_revision"] == "aie/vector"
        return

    assert pinned is not None, f"unexpected PAC dependency requirement: {requirement!r}"
    installed = metadata.version(_PAC_DIST)

    assert _versions_equal(installed, pinned), (
        f"PAC drift: installed {_PAC_DIST} {installed!r} != pinned {pinned!r}. "
        f"Reinstall (make dev) so the pinned wheel resolves, or bump the pyproject pin."
    )
