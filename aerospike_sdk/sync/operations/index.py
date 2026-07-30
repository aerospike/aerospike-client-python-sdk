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

"""Synchronous secondary-index builder (blocking terminals).

Chain state and chaining methods live on the shared, runtime-agnostic
:class:`~aerospike_sdk.index_shared._IndexBuilderBase`; this module adds
``create()`` / ``drop()`` terminals that dispatch through PAC's blocking
entries — no asyncio loop is involved.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aerospike_sdk.exceptions import _convert_pac_exception
from aerospike_sdk.index_shared import _IndexBuilderBase

if TYPE_CHECKING:
    from aerospike_sdk.sync.client import SyncClient


class IndexBuilder(_IndexBuilderBase):
    """Synchronous secondary-index builder.

    Chain :meth:`on_bin` (or :meth:`on_expression`), :meth:`named`,
    :meth:`numeric` / :meth:`string` / :meth:`geo2dsphere`, optional
    :meth:`collection` or :meth:`context` (inherited from the shared base),
    then :meth:`create` or :meth:`drop`.

    See Also:
        :class:`~aerospike_sdk.aio.operations.index.IndexBuilder`: Async equivalent.
    """

    def __init__(
        self,
        async_client: SyncClient,
        namespace: str,
        set_name: str,
    ) -> None:
        """Pair with ``namespace``/``set`` from the parent Session."""
        super().__init__(namespace, set_name)
        self._async_client = async_client

    def create(self) -> None:
        """Create the index (blocks until the admin call completes).

        Raises:
            ValueError: Same validation as async :meth:`~aerospike_sdk.aio.operations.index.IndexBuilder.create`.
            AerospikeError: On failure from the cluster (typed when mapped).
        """
        if self._expression is not None:
            index_name, index_type, expression = self._validate_expression_create()
            try:
                self._async_client._async_client.create_index_using_expression_blocking(
                    self._namespace,
                    self._set_name,
                    index_name,
                    index_type,
                    expression,
                    self._collection_index_type,
                )
            except Exception as e:
                raise _convert_pac_exception(e) from e
            return
        if not self._bin_name:
            raise ValueError("bin_name is required. Call on_bin() first.")
        if not self._index_name:
            raise ValueError("index_name is required. Call named() first.")
        if not self._index_type:
            raise ValueError(
                "index_type is required. "
                "Call numeric(), string(), blob(), or geo2dsphere() first.")
        try:
            self._async_client._async_client.create_index_blocking(
                self._namespace,
                self._set_name,
                self._bin_name,
                self._index_name,
                self._index_type,
                self._collection_index_type,
                self._ctx,
            )
        except Exception as e:
            raise _convert_pac_exception(e) from e

    def drop(self) -> None:
        """Drop the index (blocks until the admin call completes).

        Raises:
            ValueError: If the index name was not set via :meth:`named`.
            AerospikeError: On failure from the cluster.
        """
        if not self._index_name:
            raise ValueError("index_name is required. Call named() first.")
        try:
            self._async_client._async_client.drop_index_blocking(
                self._namespace, self._set_name, self._index_name,
            )
        except Exception as e:
            raise _convert_pac_exception(e) from e


# Deprecated alias, kept importable for one release cycle.
SyncIndexBuilder = IndexBuilder
