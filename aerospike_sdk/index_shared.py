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

"""Runtime-agnostic secondary-index builder base shared by the async and sync leaves.

Holds the chain state (bin, index name/type, collection variant, CDT
context) and the chaining methods — no I/O. Terminal ``create()`` /
``drop()`` dispatchers are runtime-bound and live on the leaves:
:class:`aerospike_sdk.aio.operations.index.IndexBuilder` (async) and
:class:`aerospike_sdk.sync.operations.index.IndexBuilder` (blocking).
"""

from __future__ import annotations

from typing import List, Optional, Union

from typing import Self

from aerospike_async import (
    CTX,
    CollectionIndexType,
    FilterExpression,
    IndexType,
)

from aerospike_sdk.server_filter import filter_expression_from_ael_string


class _IndexBuilderBase:
    """State + chaining shared by the async and sync index builders."""

    # Class-level default: only expression-based builders ever assign it,
    # so bin-based chains skip the per-instance write entirely.
    _expression: Optional[Union[str, FilterExpression]] = None

    def __init__(
        self,
        namespace: str,
        set_name: str,
    ) -> None:
        """
        Args:
            namespace: Namespace containing the set to index.
            set_name: Set name within the namespace.
        """
        self._namespace = namespace
        self._set_name = set_name
        self._bin_name: Optional[str] = None
        self._index_name: Optional[str] = None
        self._index_type: Optional[IndexType] = None
        self._collection_index_type: Optional[CollectionIndexType] = None
        self._ctx: Optional[List[CTX]] = None

    def on_bin(self, bin_name: str) -> Self:
        """Set which bin this secondary index covers (required before :meth:`create`).

        Args:
            bin_name: Name of the bin to index.

        Returns:
            ``self`` for method chaining.
        """
        self._bin_name = bin_name
        return self

    def on_expression(self, expression: Union[str, FilterExpression]) -> Self:
        """Index the value an expression computes per record, instead of a bin.

        The expression is evaluated server-side for every record in the set;
        its result becomes the indexed value. The expression's *result type*
        must match the index type set via :meth:`numeric`, :meth:`string`,
        or :meth:`geo2dsphere` — a boolean predicate is rejected by the
        server, so build a value-producing expression (e.g. via
        ``FilterExpression.cond``). Requires server 8.1.2 or newer.

        An AEL string may be passed instead of a prebuilt expression; the
        server parses and compiles it when the index is created, so the
        cluster must support server-compiled AEL (server 8.1.3 or newer on
        every node) or :meth:`create` raises with result code
        ``OP_NOT_APPLICABLE``.

        Mutually exclusive with :meth:`on_bin` — an index covers either a
        bin or an expression, never both. Not combinable with
        :meth:`context` (encode CDT navigation inside the expression
        instead).

        Args:
            expression: A prebuilt ``FilterExpression`` whose result is the
                value to index, or an AEL string for the server to compile.

        Returns:
            ``self`` for method chaining.

        Raises:
            TypeError: If *expression* is neither an AEL string nor a
                ``FilterExpression``.
            ValueError: If :meth:`on_bin` was already called on this builder.

        Example::

            from aerospike_sdk.exp import Exp

            adult_flag = Exp.cond([
                Exp.ge(Exp.int_bin("age"), Exp.int_val(18)),
                Exp.int_val(1),
                Exp.unknown(),
            ])
            await (
                client.index("test", "users")
                .on_expression(adult_flag)
                .named("users_adult_idx")
                .numeric()
                .create()
            )

            # Or let the server compile an AEL string (server 8.1.3+):
            await (
                client.index("test", "users")
                .on_expression("$.age + 1")
                .named("users_age_ael_idx")
                .numeric()
                .create()
            )

        See Also:
            :meth:`on_bin`: Index a plain bin value.
        """
        if not isinstance(expression, (str, FilterExpression)):
            raise TypeError(
                "expression must be an AEL string or a FilterExpression, got "
                f"{type(expression).__name__}",
            )
        if self._bin_name is not None:
            raise ValueError(
                "on_bin() and on_expression() are mutually exclusive; "
                "an index covers either a bin or an expression",
            )
        self._expression = expression
        return self

    def _validate_expression_create(
        self, sdk_client,
    ) -> tuple[str, IndexType, FilterExpression]:
        """Validate chain state for an expression-based ``create()``.

        Shared by the async and sync leaf terminals so the two runtimes
        cannot drift on what a valid expression-index chain looks like.
        Returns the narrowed ``(index_name, index_type, expression)``
        triple the terminals hand to the client. An AEL string set via
        :meth:`on_expression` is resolved here to its server-compiled
        wire form, reading *sdk_client*'s capability gate only on the
        string path so prebuilt-expression chains never pay for it.
        """
        if self._bin_name:
            raise ValueError(
                "on_bin() and on_expression() are mutually exclusive; "
                "an index covers either a bin or an expression",
            )
        if self._ctx:
            raise ValueError(
                "context() cannot be combined with on_expression(); "
                "encode CDT navigation inside the expression instead",
            )
        if not self._index_name:
            raise ValueError("index_name is required. Call named() first.")
        if not self._index_type:
            raise ValueError(
                "index_type is required. "
                "Call numeric(), string(), blob(), or geo2dsphere() first.",
            )
        expression = self._expression
        assert expression is not None
        if isinstance(expression, str):
            expression = filter_expression_from_ael_string(
                expression,
                supports_server_compiled_ael=sdk_client.supports_server_compiled_ael,
            )
        return self._index_name, self._index_type, expression

    def named(self, index_name: str) -> Self:
        """Set the secondary index name the cluster stores (required for create and drop).

        Args:
            index_name: Name passed to create/drop admin calls; must match when dropping.

        Returns:
            ``self`` for method chaining.
        """
        self._index_name = index_name
        return self

    def numeric(self) -> Self:
        """Set the secondary index type to numeric (for numeric bin values).

        Call this or :meth:`string` before :meth:`create`, matching how the bin is
        stored. If both are called on the same builder, the last call wins.

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.NUMERIC
        return self

    def string(self) -> Self:
        """Set the secondary index type to string (for string bin values).

        Call this or :meth:`numeric` before :meth:`create`. If both are called,
        the last call wins (see :meth:`numeric`).

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.STRING
        return self

    def geo2dsphere(self) -> Self:
        """Set the secondary index type to GEO2DSPHERE (for GeoJSON bin values).

        Call this before :meth:`create` to index a bin containing GeoJSON Points,
        Polygons, or AeroCircles for spatial query via ``geoCompare(...)``.

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.GEO2D_SPHERE
        return self

    def blob(self) -> Self:
        """Set the secondary index type to blob (for bytes bin values).

        Call this before :meth:`create` to index a bin containing raw ``bytes``
        values for exact-match query. Requires server 7.0 or newer.

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.BLOB
        return self

    def collection(self, collection_index_type: CollectionIndexType) -> Self:
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

    def context(self, ctx: List[CTX]) -> Self:
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
