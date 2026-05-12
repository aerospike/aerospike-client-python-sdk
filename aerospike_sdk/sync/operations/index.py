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

"""SyncIndexBuilder - Synchronous wrapper for index operations."""

from __future__ import annotations

from typing import List, Optional

from aerospike_async import CTX, CollectionIndexType, IndexType

from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.operations.index import IndexBuilder
from aerospike_sdk.sync.client import _EventLoopManager


class SyncIndexBuilder:
    """Synchronous façade over :class:`~aerospike_sdk.aio.operations.index.IndexBuilder`.

    Chain :meth:`on_bin`, :meth:`named`, :meth:`numeric` / :meth:`string`, optional
    :meth:`collection`, then :meth:`create` or :meth:`drop`; each mutating step
    is stored locally and replayed onto a fresh async builder when executing.

    See Also:
        :class:`~aerospike_sdk.aio.operations.index.IndexBuilder`
    """

    def __init__(
        self,
        async_client: Client,
        namespace: str,
        set_name: str,
        loop_manager: _EventLoopManager,
    ) -> None:
        """Pair with ``namespace``/``set`` and the parent's loop manager."""
        self._async_client = async_client
        self._namespace = namespace
        self._set_name = set_name
        self._loop_manager = loop_manager
        self._bin_name: Optional[str] = None
        self._index_name: Optional[str] = None
        self._index_type: Optional[IndexType] = None
        self._collection_index_type: Optional[CollectionIndexType] = None
        self._ctx: Optional[List[CTX]] = None

    def _get_async_builder(self) -> IndexBuilder:
        """Get the underlying async index builder."""
        builder = IndexBuilder(
            client=self._async_client._async_client,
            namespace=self._namespace,
            set_name=self._set_name,
        )
        if self._bin_name:
            builder.on_bin(self._bin_name)
        if self._index_name:
            builder.named(self._index_name)
        if self._index_type:
            if self._index_type == IndexType.NUMERIC:
                builder.numeric()
            elif self._index_type == IndexType.STRING:
                builder.string()
        if self._collection_index_type:
            builder.collection(self._collection_index_type)
        if self._ctx:
            builder.context(self._ctx)
        return builder

    def on_bin(self, bin_name: str) -> SyncIndexBuilder:
        """Set which bin this secondary index covers."""
        self._bin_name = bin_name
        return self

    def named(self, index_name: str) -> SyncIndexBuilder:
        """Set the secondary index name the cluster uses (required for create and drop)."""
        self._index_name = index_name
        return self

    def numeric(self) -> SyncIndexBuilder:
        """Set the secondary index type to numeric; use :meth:`string` for string bins."""
        self._index_type = IndexType.NUMERIC
        return self

    def string(self) -> SyncIndexBuilder:
        """Set the secondary index type to string; use :meth:`numeric` for numeric bins."""
        self._index_type = IndexType.STRING
        return self

    def geo2dsphere(self) -> SyncIndexBuilder:
        """Set the secondary index type to GEO2DSPHERE (for GeoJSON bin values)."""
        self._index_type = IndexType.GEO2D_SPHERE
        return self

    def collection(
        self, collection_index_type: CollectionIndexType
    ) -> SyncIndexBuilder:
        """Set the collection index variant for map or list bins.

        Args:
            collection_index_type: Same as
                :meth:`~aerospike_sdk.aio.operations.index.IndexBuilder.collection`.
        """
        self._collection_index_type = collection_index_type
        return self

    def context(self, ctx: List[CTX]) -> SyncIndexBuilder:
        """Set a CDT context path for indexing a nested list or map element.

        Args:
            ctx: CDT path entries (e.g., ``[CTX.map_key("outer")]``).

        Returns:
            ``self`` for method chaining.
        """
        self._ctx = ctx
        return self

    def create(self) -> None:
        """Create the index (blocks until the admin call completes).

        Raises:
            ValueError: Same validation as async :meth:`~aerospike_sdk.aio.operations.index.IndexBuilder.create`.
            AerospikeError: On failure from the cluster (typed when mapped).
        """
        builder = self._get_async_builder()
        self._loop_manager.run_async(builder.create())

    def drop(self) -> None:
        """Drop the index (blocks until the admin call completes).

        Raises:
            ValueError: If the index name was not set via :meth:`named`.
            AerospikeError: On failure from the cluster.
        """
        builder = self._get_async_builder()
        self._loop_manager.run_async(builder.drop())

