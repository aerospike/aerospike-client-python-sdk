# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0

"""Newest release/RC selection for the nightly floating canary."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / ".github" / "resolve-server-tag.py"
_spec = importlib.util.spec_from_file_location("resolve_server_tag", _SCRIPT)
assert _spec and _spec.loader
_resolve = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_resolve)


def test_release_beats_rc_of_the_same_build():
    assert _resolve.choose_tag(["8.2.0.1-rc2", "8.2.0.1", "8.1.2.5"]) == "8.2.0.1"


def test_newer_rc_beats_older_release():
    assert _resolve.choose_tag(["8.2.0.0", "8.2.0.2-rc.1", "latest", "8.2"]) == "8.2.0.2-rc.1"


def test_rc_separators_and_nightlies_are_filtered():
    assert _resolve.choose_tag(
        ["8.2.0.3_nightly", "8.2.0.1_rc1", "8.2.0.1rc2", "8.2.0.0-1-gabcd"]
    ) == "8.2.0.1rc2"


def test_nothing_at_the_floor_is_an_error():
    with pytest.raises(ValueError, match="8.2.0.0"):
        _resolve.choose_tag(["8.1.2.5", "latest", "8.2"])
