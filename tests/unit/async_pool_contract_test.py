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

"""AsyncPool construction contract.

Construction-time only — no I/O, no loops. A pool is defined by a
:class:`ClusterDefinition`; nothing else is accepted.
"""

import warnings

import pytest

from aerospike_sdk.aio.pool import AsyncPool


class TestDefinitionContract:
    """The supported shape: AsyncPool(ClusterDefinition(...), loop_count=N)."""

    def test_definition_positional_no_warning(self, unit_cluster_definition):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            pool = AsyncPool(unit_cluster_definition, loop_count=2)
        assert pool._definition is not None
        assert [w for w in caught if issubclass(w.category, DeprecationWarning)] == []


class TestRejectedConstruction:
    """What the constructor refuses, now that a definition is the only shape."""

    def test_non_definition_raises_type_error(self):
        with pytest.raises(TypeError, match="must be a ClusterDefinition"):
            AsyncPool("127.0.0.1:3000", loop_count=2)  # type: ignore[arg-type]

    def test_callable_raises_type_error(self):
        """A callable used to be accepted positionally as a client factory."""
        with pytest.raises(TypeError, match="must be a ClusterDefinition"):
            AsyncPool(lambda: None, loop_count=2)  # type: ignore[arg-type]

    def test_no_definition_raises(self):
        with pytest.raises(ValueError, match="cluster_definition is required"):
            AsyncPool(loop_count=2)
