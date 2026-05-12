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

"""Builder for creating and dropping secondary indexes."""

from __future__ import annotations

from typing import List, Optional

from aerospike_async import (
    CTX,
    Client,
    CollectionIndexType,
    IndexType,
)

from aerospike_sdk.exceptions import _convert_pac_exception


class IndexBuilder:
    """Configure a secondary index, then :meth:`create` or :meth:`drop` it.

    Typical chain for a new index: :meth:`on_bin` → :meth:`named` →
    :meth:`numeric` or :meth:`string` → optional :meth:`collection` or
    :meth:`context` → ``await`` :meth:`create`.

    For removal, only :meth:`named` (and namespace/set from construction) is
    required before ``await`` :meth:`drop`.

    Example::

            await (
                client.index(namespace="test", set_name="users")
                .on_bin("email")
                .named("email_idx")
                .string()
                .create()
            )

    See Also:
        :meth:`~aerospike_sdk.aio.client.Client.index`
    """

    def __init__(
        self,
        client: Client,
        namespace: str,
        set_name: str,
    ) -> None:
        """
        Args:
            client: Connected async cluster client used for admin calls.
            namespace: Namespace containing the set to index.
            set_name: Set name within the namespace.
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._bin_name: Optional[str] = None
        self._index_name: Optional[str] = None
        self._index_type: Optional[IndexType] = None
        self._collection_index_type: Optional[CollectionIndexType] = None
        self._ctx: Optional[List[CTX]] = None

    def on_bin(self, bin_name: str) -> IndexBuilder:
        """Set which bin this secondary index covers (required before :meth:`create`).

        Args:
            bin_name: Name of the bin to index.

        Returns:
            ``self`` for method chaining.
        """
        self._bin_name = bin_name
        return self

    def named(self, index_name: str) -> IndexBuilder:
        """Set the secondary index name the cluster stores (required for create and drop).

        Args:
            index_name: Name passed to create/drop admin calls; must match when dropping.

        Returns:
            ``self`` for method chaining.
        """
        self._index_name = index_name
        return self

    def numeric(self) -> IndexBuilder:
        """Set the secondary index type to numeric (for numeric bin values).

        Call this or :meth:`string` before :meth:`create`, matching how the bin is
        stored. If both are called on the same builder, the last call wins.

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.NUMERIC
        return self

    def string(self) -> IndexBuilder:
        """Set the secondary index type to string (for string bin values).

        Call this or :meth:`numeric` before :meth:`create`. If both are called,
        the last call wins (see :meth:`numeric`).

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.STRING
        return self

    def geo2dsphere(self) -> IndexBuilder:
        """Set the secondary index type to GEO2DSPHERE (for GeoJSON bin values).

        Call this before :meth:`create` to index a bin containing GeoJSON Points,
        Polygons, or AeroCircles for spatial query via ``geoCompare(...)``.

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.GEO2D_SPHERE
        return self

    def collection(
        self, collection_index_type: CollectionIndexType
    ) -> IndexBuilder:
        """Set the collection index variant for map or list bins (optional).

        Use together with :meth:`numeric` or :meth:`string` when indexing into
        collection data types.

        Args:
            collection_index_type: ``CollectionIndexType`` constant from the
                ``aerospike_async`` package (map- vs list-style collection indexing).

        Returns:
            ``self`` for method chaining.
        """
        self._collection_index_type = collection_index_type
        return self

    def context(self, ctx: List[CTX]) -> IndexBuilder:
        """Set a CDT context path for indexing a nested list or map element.

        Args:
            ctx: One or more ``CTX`` entries describing the path to the
                nested element (e.g., ``[CTX.map_key("outer"), CTX.list_index(0)]``).

        Returns:
            ``self`` for method chaining.

        Example::

            await (
                client.index("test", "events")
                .on_bin("payload")
                .named("nested_ts_idx")
                .numeric()
                .context([CTX.map_key("meta"), CTX.map_key("timestamp")])
                .create()
            )

        See Also:
            :meth:`~aerospike_async.Filter.context`: Attach the same path when querying.
        """
        self._ctx = ctx
        return self

    async def create(self) -> None:
        """Create the index on the cluster.

        Example::

            await (
                client.index(namespace="test", set_name="users")
                .on_bin("email")
                .named("email_idx")
                .string()
                .create()
            )

        Raises:
            ValueError: If ``on_bin``, ``named``, or index type was not set.
            AerospikeError: On server or transport failure (typed subclass when
                the driver maps a result code).

        See Also:
            :meth:`drop`
        """
        if not self._bin_name:
            raise ValueError("bin_name is required. Call on_bin() first.")
        if not self._index_name:
            raise ValueError("index_name is required. Call named() first.")
        if not self._index_type:
            raise ValueError("index_type is required. Call numeric() or string() first.")

        try:
            await self._client.create_index(
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

    async def drop(self) -> None:
        """Drop a previously created index by name.

        Example::

            await (
                client.index(namespace="test", set_name="users")
                .named("email_idx")
                .drop()
            )

        Raises:
            ValueError: If :meth:`named` was not called.
            AerospikeError: On server or transport failure.

        Note:
            Namespace and set come from the builder constructor, not from
            :meth:`on_bin`.
        """
        if not self._index_name:
            raise ValueError("index_name is required. Call named() first.")

        try:
            await self._client.drop_index(
                self._namespace, self._set_name, self._index_name
            )
        except Exception as e:
            raise _convert_pac_exception(e) from e
