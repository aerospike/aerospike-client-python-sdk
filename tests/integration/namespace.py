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

"""Namespace resolution for the general integration op-suites (Mode axis, Leg A).

The general suites historically hard-coded ``DataSet.of("test", …)``, pinning
every op to the AP ``test`` namespace with no way to aim them at a strong-consistency
namespace. :func:`ds` reads the target namespace from the ``AEROSPIKE_NAMESPACE``
environment variable (default ``"test"``) at call time, so the *same* suite can run
against either mode by selecting the namespace at launch:

- ``AEROSPIKE_NAMESPACE`` unset / ``test`` -> AP (default, unchanged behavior).
- ``AEROSPIKE_NAMESPACE=test_sc`` (with ``AEROSPIKE_HOST`` on a cluster that hosts an
  SC namespace) -> the general suites exercise strong consistency.

This is the single knob replacing the hard-coded literals. Call sites change the
namespace *argument* only — ``DataSet.of("test", set_name)`` becomes
``DataSet.of(general_namespace(), set_name)`` (and a module-level ``NS = "test"``
becomes ``NS = general_namespace()``). A callable is used rather than a bare helper
like ``ds()`` on purpose: ``ds`` is already the ubiquitous local-variable / fixture
name for datasets across the suite, so a ``ds()`` helper collides (``ds = ds(...)``
shadows to an ``UnboundLocalError``; ``def ds(): return ds(...)`` self-recurses).
``general_namespace`` is only ever *called*, never assigned, so it never collides.

Companion helper: :mod:`tests.integration.sc_namespace_resolve` resolves the *SC-specific*
namespace name (``AEROSPIKE_SC_NAMESPACE`` / auto-select) for the dedicated SC suites, and
``conftest.is_namespace_sc`` is the authoritative per-namespace SC verdict (use it for
``@requires_mode``, not a string check on ``AEROSPIKE_NAMESPACE``). This module is only the
general-suite namespace *argument* knob — keep the three concerns in these two places rather than
adding a third helper.
"""

from __future__ import annotations

import os

import pytest

_DEFAULT_NAMESPACE = "test"


def general_namespace() -> str:
    """The namespace the general op-suites target (``AEROSPIKE_NAMESPACE`` or ``test``)."""
    return os.environ.get("AEROSPIKE_NAMESPACE", "").strip() or _DEFAULT_NAMESPACE


def requires_mode(mode: str):
    """Mark a test as valid only in the given namespace mode — ``"ap"`` or ``"sc"``.

    Enforced by conftest's ``_enforce_requires_mode`` against the **server-derived** mode of
    the general namespace (``general_namespace_is_sc``), not the ``AEROSPIKE_NAMESPACE`` string.
    Reserve this for tests whose *assertion* is mode-specific (e.g. non-durable delete, valid
    only on AP). For teardown that merely differs by mode, use the ``sc_aware_delete`` fixture
    instead — don't surrender SC coverage over a cleanup artifact.
    """
    if mode not in ("ap", "sc"):
        raise ValueError(f"mode must be 'ap' or 'sc', got {mode!r}")
    return pytest.mark.requires_mode(mode)


def requires_mode_skip_reason(want: str, is_sc: bool) -> str | None:
    """The skip reason for a ``requires_mode(want)`` test given the server-derived mode.

    Returns ``None`` when the test should run (``want`` matches the actual mode), otherwise
    a human-readable skip reason. Pure decision logic behind conftest's
    ``_enforce_requires_mode`` — extracted so the skip behavior (whose bugs would silently
    *remove* coverage) is unit-testable without a live cluster.
    """
    have = "sc" if is_sc else "ap"
    if want == have:
        return None
    return f"requires {want!r} namespace; general namespace is {have!r} mode"
