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

"""Deprecated ``Client`` / ``SyncClient`` exports: warn, resolve, stay off __all__.

``ClusterDefinition -> Cluster -> Session`` is the supported entry;
the connection primitives stay importable for one deprecation cycle
behind package-level ``__getattr__`` shims.
"""

import warnings

import pytest

import aerospike_sdk
import aerospike_sdk.aio
import aerospike_sdk.sync


@pytest.mark.parametrize(
    ("module", "name"),
    [
        (aerospike_sdk, "Client"),
        (aerospike_sdk, "SyncClient"),
        (aerospike_sdk.aio, "Client"),
        (aerospike_sdk.sync, "SyncClient"),
    ],
    ids=lambda v: getattr(v, "__name__", v),
)
def test_deprecated_access_warns_and_resolves(module, name):
    with pytest.warns(DeprecationWarning, match="deprecated"):
        cls = getattr(module, name)
    assert cls.__name__ == name


def test_deprecated_names_resolve_to_real_classes():
    from aerospike_sdk.aio.client import Client as RealClient
    from aerospike_sdk.sync.client import SyncClient as RealSyncClient

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert aerospike_sdk.Client is RealClient
        assert aerospike_sdk.SyncClient is RealSyncClient


def test_deprecated_names_not_in_all():
    assert "Client" not in aerospike_sdk.__all__
    assert "SyncClient" not in aerospike_sdk.__all__
    assert "Client" not in aerospike_sdk.aio.__all__
    assert "SyncClient" not in aerospike_sdk.sync.__all__


def test_internal_module_paths_stay_silent():
    """Library-internal imports must not pay a warning: the class modules
    themselves are not deprecated, only the package-level exports."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        from aerospike_sdk.aio.client import Client  # noqa: F401
        from aerospike_sdk.sync.client import SyncClient  # noqa: F401
    assert [w for w in caught if issubclass(w.category, DeprecationWarning)] == []


def test_unknown_attribute_still_raises():
    with pytest.raises(AttributeError, match="no attribute"):
        aerospike_sdk.DoesNotExist
    with pytest.raises(AttributeError, match="no attribute"):
        aerospike_sdk.aio.DoesNotExist
    with pytest.raises(AttributeError, match="no attribute"):
        aerospike_sdk.sync.DoesNotExist
