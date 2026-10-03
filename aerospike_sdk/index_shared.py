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

    # Class-level defaults: only expression-based and set-index builders ever
    # assign these, so bin-based chains skip the per-instance writes entirely.
    _expression: Optional[Union[str, FilterExpression]] = None
    _on_set: bool = False

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

        Raises:
            ValueError: If :meth:`on_set` was already called on this builder.
        """
        if self._on_set:
            raise ValueError(
                "on_set() is mutually exclusive with on_bin() and on_expression(); "
                "a set index covers records, not values",
            )
        self._bin_name = bin_name
        return self

    def on_expression(self, expression: Union[str, FilterExpression]) -> Self:
        """Index the value an expression computes per record, instead of a bin.

        The expression is evaluated server-side for every record in the set;
        its result becomes the indexed value. The expression's *result type*
        must match the index type set via :meth:`integer`, :meth:`string`,
        or :meth:`geo2dsphere` — a boolean predicate is rejected by the
        server, so build a value-producing expression (e.g. via
        ``FilterExpression.cond``).

        An AEL string may be passed instead of a prebuilt expression; the
        server parses and compiles it when the index is created, so the
        cluster must support server-compiled AEL (server 8.2.0 or newer on
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
                .integer()
                .create()
            )

            # Or let the server compile an AEL string (server 8.2.0+):
            await (
                client.index("test", "users")
                .on_expression("$.age + 1")
                .named("users_age_ael_idx")
                .integer()
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
        if self._on_set:
            raise ValueError(
                "on_set() is mutually exclusive with on_bin() and on_expression(); "
                "a set index covers records, not values",
            )
        self._expression = expression
        return self

    def on_set(self) -> Self:
        """Index record presence in the set itself, rather than a bin or expression.

        A set index covers every record in the set and nothing about their
        contents, so it takes no bin, index type, collection variant, or CDT
        context: the chain is :meth:`on_set` → :meth:`named` → :meth:`create`.
        Creating one needs only the ``sindex-admin`` privilege. The builder
        must name a set; a namespace-wide set index is rejected.

        Mutually exclusive with :meth:`on_bin` and :meth:`on_expression`.

        Returns:
            ``self`` for method chaining.

        Raises:
            ValueError: If :meth:`on_bin` or :meth:`on_expression` was already
                called on this builder.

        Example::

            task = await (
                session.index("test", "users")
                .on_set()
                .named("users_set_idx")
                .create()
            )
            await task.wait_till_complete()

        See Also:
            :meth:`on_bin`: Index a bin value.
            :meth:`on_expression`: Index a value an expression computes.
        """
        if self._bin_name is not None or self._expression is not None:
            raise ValueError(
                "on_set() is mutually exclusive with on_bin() and on_expression(); "
                "a set index covers records, not values",
            )
        self._on_set = True
        return self

    def _configure(
        self,
        index_name: str,
        bin_name: Optional[str],
        index_type: Optional[IndexType],
        collection_type: Optional[CollectionIndexType],
        ctx: Optional[List[CTX]],
        expression: Optional[Union[str, FilterExpression]],
    ) -> Self:
        """Apply the flat ``create_index`` arguments as the equivalent chain.

        Shared by both sessions' ``create_index`` verbs so the two runtimes map
        arguments to chain calls identically. Nothing is validated here: the
        chain methods and ``create()`` enforce the same rules they do for a
        hand-built chain. Only a name, with no bin or expression, is a set
        index.
        """
        self.named(index_name)
        if bin_name is not None:
            self.on_bin(bin_name)
        if expression is not None:
            self.on_expression(expression)
        if bin_name is None and expression is None:
            self.on_set()
        if index_type is not None:
            self._index_type = index_type
        if collection_type is not None:
            self.collection(collection_type)
        if ctx is not None:
            self.context(ctx)
        return self

    def _validate_set_create(self) -> str:
        """Validate chain state for a set-index ``create()``.

        Shared by the async and sync leaf terminals so the two runtimes
        cannot drift on what a valid set-index chain looks like. Returns the
        narrowed index name the terminals hand to the client.
        """
        if self._bin_name is not None or self._expression is not None:
            raise ValueError(
                "on_set() is mutually exclusive with on_bin() and on_expression(); "
                "a set index covers records, not values",
            )
        if self._index_type is not None or self._collection_index_type is not None:
            raise ValueError(
                "a set index has no index type or collection type; "
                "drop the integer()/string()/blob()/geo2dsphere()/collection() call",
            )
        if self._ctx:
            raise ValueError("context() cannot be combined with on_set()")
        if not self._set_name:
            raise ValueError("a set index requires a set; build it from a DataSet with a set name")
        return self._require_index_name()

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
        index_name, index_type = self._require_name_and_type()
        expression = self._expression
        assert expression is not None
        if isinstance(expression, str):
            expression = filter_expression_from_ael_string(
                expression,
                supports_server_compiled_ael=sdk_client.supports_server_compiled_ael,
            )
        return index_name, index_type, expression

    def _validate_bin_create(self) -> tuple[str, str, IndexType]:
        """Validate chain state for a bin-based ``create()``.

        Shared by the async and sync leaf terminals so the two runtimes
        cannot drift on what a valid bin-index chain looks like. Returns the
        narrowed ``(bin_name, index_name, index_type)`` triple the terminals
        hand to the client.
        """
        if not self._bin_name:
            raise ValueError("bin_name is required. Call on_bin() first.")
        index_name, index_type = self._require_name_and_type()
        return self._bin_name, index_name, index_type

    def _require_name_and_type(self) -> tuple[str, IndexType]:
        """Return the narrowed ``(index_name, index_type)`` every ``create()`` needs."""
        index_name = self._require_index_name()
        if not self._index_type:
            raise ValueError(
                "index_type is required. "
                "Call integer(), string(), blob(), or geo2dsphere() first.",
            )
        return index_name, self._index_type

    def _require_index_name(self) -> str:
        """Return the narrowed index name that both ``create()`` and ``drop()`` need."""
        if not self._index_name:
            raise ValueError("index_name is required. Call named() first.")
        return self._index_name

    def named(self, index_name: str) -> Self:
        """Set the secondary index name the cluster stores (required for create and drop).

        Args:
            index_name: Name passed to create/drop admin calls; must match when dropping.

        Returns:
            ``self`` for method chaining.
        """
        self._index_name = index_name
        return self

    def integer(self) -> Self:
        """Set the secondary index type to integer (for integer bin values).

        Call this or :meth:`string` before :meth:`create`, matching how the bin is
        stored. If both are called on the same builder, the last call wins. The
        server has called this index type ``integer`` since 8.1.3; the SDK sends
        that name.

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.INTEGER
        return self

    def string(self) -> Self:
        """Set the secondary index type to string (for string bin values).

        Call this or :meth:`integer` before :meth:`create`. If both are called,
        the last call wins (see :meth:`integer`).

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
        values for exact-match query.

        Returns:
            ``self`` for method chaining.
        """
        self._index_type = IndexType.BLOB
        return self

    def collection(self, collection_index_type: CollectionIndexType) -> Self:
        """Set the collection index variant for map or list bins (optional).

        Use together with :meth:`integer` or :meth:`string` when indexing into
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
                .integer()
                .context([CTX.map_key("meta"), CTX.map_key("timestamp")])
                .create()
            )

        See Also:
            :meth:`~aerospike_async.Filter.context`: Attach the same path when querying.
        """
        self._ctx = ctx
        return self
