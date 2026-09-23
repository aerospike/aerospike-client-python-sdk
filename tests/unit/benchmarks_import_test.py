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

"""The benchmark harness still resolves against the package it measures.

Nothing else in the suite imports ``benchmarks``, so a rename in the SDK can
leave it referring to a name that no longer exists and nobody finds out until
someone tries to run a benchmark -- which is how
``aerospike_sdk.sync.session.SyncSession`` survived being renamed to
``Session``.

Importing is the whole test. It is deliberately not a benchmark: no cluster,
no traffic, no timing. It only asks whether every module in the package can
still be loaded, which is the failure this is meant to catch. The harness also
reaches past the public API in places (``aerospike_sdk.sync.client``,
``aerospike_sdk.aio.client``), so internal renames break it the same way and
with less warning.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest

import benchmarks

_MODULES = sorted(module.name for module in pkgutil.iter_modules(benchmarks.__path__))


def test_the_package_has_modules_to_check():
    """Guards the parametrization itself: an empty list would pass vacuously."""
    assert len(_MODULES) >= 5, _MODULES


@pytest.mark.parametrize("name", _MODULES)
def test_benchmark_module_imports(name):
    importlib.import_module(f"benchmarks.{name}")
