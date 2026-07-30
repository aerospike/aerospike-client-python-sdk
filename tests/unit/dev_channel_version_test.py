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

"""Tests for the dev-channel version computation and stamping used by CI.

A wrong answer here publishes packages that sort incorrectly against real
releases, which is close to unrecoverable once consumers have resolved them,
so the ordering guarantee and the PEP 440 mapping are both pinned.
"""

import importlib.util
import pathlib

import pytest

from packaging.version import Version

_SCRIPTS = pathlib.Path(__file__).resolve().parents[2] / ".github" / "scripts"


def _load(name: str):
    """Import a CI script, which lives outside any importable package."""
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


next_dev_version = _load("next_dev_version").next_dev_version
stamp = _load("stamp_version").stamp


@pytest.mark.parametrize(
    "base,expected_stamped,expected_pep440",
    [
        ("0.9.0-alpha.5", "0.9.0-alpha.6.dev.7", "0.9.0a6.dev7"),
        ("0.9.0-beta.2", "0.9.0-beta.3.dev.7", "0.9.0b3.dev7"),
        ("0.9.0-rc.1", "0.9.0-rc.2.dev.7", "0.9.0rc2.dev7"),
        ("0.9.0", "0.9.1-dev.7", "0.9.1.dev7"),
    ],
)
def test_version_shapes(base, expected_stamped, expected_pep440):
    assert next_dev_version(base, 7) == (expected_stamped, expected_pep440)


def test_stamped_version_normalizes_to_the_advertised_pep440_version():
    """The stamped form is what setuptools sees; the PEP 440 form is what CI
    addresses the published artifact by. They must agree."""
    for base in ("0.9.0-alpha.5", "0.9.0-beta.2", "0.9.0-rc.1", "0.9.0"):
        stamped, pep440 = next_dev_version(base, 123)
        assert str(Version(stamped)) == pep440


def test_dev_builds_sort_between_the_released_and_next_versions():
    stamped_first, _ = next_dev_version("0.9.0-alpha.5", 1)
    stamped_second, _ = next_dev_version("0.9.0-alpha.5", 2)
    versions = [
        Version("0.9.0-alpha.5"),
        Version(stamped_first),
        Version(stamped_second),
        Version("0.9.0-alpha.6"),
    ]
    assert sorted(versions) == versions


def test_rejects_a_base_that_already_carries_a_dev_segment():
    with pytest.raises(ValueError, match="already carries a dev segment"):
        next_dev_version("0.9.0-alpha.6.dev.3", 4)


def test_rejects_an_unrecognized_shape():
    with pytest.raises(ValueError, match="unrecognized version shape"):
        next_dev_version("0.9.0-alpha", 1)


def test_stamping_writes_exactly_the_computed_version(tmp_path, monkeypatch):
    """Stamping once ran through a shell escape that appended a stray
    character, so the build failed on an unparseable version. Pin the exact
    file contents, not just that a write happened.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "VERSION").write_text("0.9.0-alpha.5\n")

    stamped, pep440 = next_dev_version("0.9.0-alpha.5", 1)
    stamp(stamped)

    written = (tmp_path / "VERSION").read_text()
    assert written == f"{stamped}\n"
    assert str(Version(written.strip())) == pep440


def test_stamping_outside_the_repository_root_fails_loudly(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit, match="VERSION not found"):
        stamp("0.9.0-alpha.6.dev.1")
