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

"""Builder for creating and dropping secondary indexes (async terminals).

Chain state and chaining methods live on the shared, runtime-agnostic
:class:`~aerospike_sdk.index_shared._IndexBuilderBase`; this module adds
the async ``create()`` / ``drop()`` dispatchers.
"""

from __future__ import annotations

from aerospike_async import Client

from aerospike_sdk.exceptions import _convert_pac_exception
from aerospike_sdk.index_shared import _IndexBuilderBase

# Re-exported for callers that historically imported the base from this
# module (the shared definition now lives in index_shared).
__all__ = ["IndexBuilder", "_IndexBuilderBase"]


class IndexBuilder(_IndexBuilderBase):
    """Configure a secondary index, then :meth:`create` or :meth:`drop` it.

    Typical chain for a new index: :meth:`on_bin` → :meth:`named` →
    :meth:`numeric` or :meth:`string` → optional :meth:`collection` or
    :meth:`context` → ``await`` :meth:`create`. Expression-based indexes
    (server 8.1.2+) replace :meth:`on_bin` with :meth:`on_expression`.

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
        super().__init__(namespace, set_name)
        self._client = client

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
            ValueError: If ``on_bin`` (or ``on_expression``), ``named``, or
                index type was not set, or if mutually exclusive chain methods
                were combined.
            AerospikeError: On server or transport failure (typed subclass when
                the driver maps a result code).

        See Also:
            :meth:`drop`
        """
        if self._expression is not None:
            index_name, index_type, expression = self._validate_expression_create()
            try:
                await self._client.create_index_using_expression(
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
            await self._client.drop_index(self._namespace, self._set_name, self._index_name)
        except Exception as e:
            raise _convert_pac_exception(e) from e
