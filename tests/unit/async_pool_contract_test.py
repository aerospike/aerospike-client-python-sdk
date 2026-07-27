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

"""AsyncPool construction contract: ClusterDefinition-first, deprecated factory adapter.

Construction-time only — no I/O, no loops. The pool is defined by a
:class:`ClusterDefinition`; the ``client_factory`` kwarg (and the old
positional-callable shape) is a one-cycle deprecation adapter.
"""

import warnings

import pytest

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.pool import AsyncPool


@pytest.fixture
def factory(aerospike_host):
    def _factory() -> Client:
        return Client(seeds=aerospike_host)

    return _factory


class TestDefinitionContract:
    """The supported shape: AsyncPool(ClusterDefinition(...), loop_count=N)."""

    def test_definition_positional_no_warning(self, unit_cluster_definition):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            pool = AsyncPool(unit_cluster_definition, loop_count=2)
        assert pool._definition is not None
        assert pool._factory is None
        assert [w for w in caught if issubclass(w.category, DeprecationWarning)] == []

    def test_neither_definition_nor_factory_raises(self):
        with pytest.raises(ValueError, match="cluster_definition is required"):
            AsyncPool(loop_count=2)

    def test_non_definition_non_callable_raises_type_error(self):
        with pytest.raises(TypeError, match="must be a ClusterDefinition"):
            AsyncPool("127.0.0.1:3000", loop_count=2)  # type: ignore[arg-type]

    def test_monitor_interval_defaults_from_definition(self, unit_cluster_definition):
        cluster_def = unit_cluster_definition.with_index_refresh_interval(2.5)
        pool = AsyncPool(cluster_def, loop_count=2)
        assert pool._shared_monitor._refresh_interval == 2.5

    def test_monitor_interval_kwarg_overrides_definition(self, unit_cluster_definition):
        cluster_def = unit_cluster_definition.with_index_refresh_interval(2.5)
        pool = AsyncPool(cluster_def, loop_count=2, index_refresh_interval=0.5)
        assert pool._shared_monitor._refresh_interval == 0.5


class TestDeprecatedFactoryAdapter:
    """client_factory= still works for one deprecation cycle, with a warning."""

    def test_factory_kwarg_warns_and_adapts(self, factory):
        with pytest.warns(DeprecationWarning, match="client_factory"):
            pool = AsyncPool(client_factory=factory, loop_count=2)
        assert pool._definition is None
        assert pool._factory is factory
        # Legacy default for the shared monitor interval is preserved.
        assert pool._shared_monitor._refresh_interval == 5.0

    def test_factory_positional_warns_and_adapts(self, factory):
        """The old positional shape AsyncPool(factory, N) shifts into the
        deprecated kwarg path rather than failing the isinstance check."""
        with pytest.warns(DeprecationWarning, match="client_factory"):
            pool = AsyncPool(factory, loop_count=2)  # type: ignore[arg-type]
        assert pool._definition is None
        assert pool._factory is factory

    def test_both_definition_and_factory_raises(self, unit_cluster_definition, factory):
        with pytest.raises(ValueError, match="not both"):
            AsyncPool(unit_cluster_definition, loop_count=2, client_factory=factory)
