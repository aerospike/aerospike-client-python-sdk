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

"""Read-only CDT action builders for query bin operations.

Two builder classes provide terminal read methods on a CDT path:

- ``CdtReadBuilder``  -- non-invertable terminals (get_values, count, …),
  string reads on a navigated string leaf (``str_strlen``, ``str_substr``, …)
  plus singular CDT navigation for deeper nesting.
- ``CdtReadInvertableBuilder`` -- adds inverted "all others" terminals.
  Singular value selectors may support further navigation when ``to_ctx``
  is set; range and list multi-selectors are terminal for nesting when the
  runtime does not expose a matching context step.

Both are generic on the parent builder type ``T`` so terminal methods
return the parent for continued chaining.  The actual CDT operation is
produced by an ``op_factory`` callable injected by the navigation method
that created the builder.

Nested navigation accumulates ``CTX`` entries.  Each navigation step
pushes the current selector into the context and creates new factories
for the next selector with ``.set_context()`` applied.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Callable, Generic, Optional, Sequence, TypeVar, Union

from aerospike_native import (
    CTX,
    CdtOperation,
    ExpOperation,
    ExpReadFlags,
    ExpType,
    FilterExpression,
    ListOperation,
    ListOrderType,
    ListReturnType,
    MapOperation,
    MapOrder,
    MapReturnType,
    ModifyFlags,
    SelectFlags,
    StringNumericType,
    StringOperation,
    StringRegexFlags,
)

T = TypeVar("T")

_ReturnTypeCls = Union[type[MapReturnType], type[ListReturnType]]


def _map_item_pairs(items: Mapping[Any, Any] | Sequence[tuple[Any, Any]]) -> list[tuple[Any, Any]]:
    """Normalize mapping or sequence of pairs for ``MapOperation.put_items``."""
    if isinstance(items, Mapping):
        return list(items.items())
    return list(items)


def _value_list(values: Iterable[Any], param: str) -> list[Any]:
    """Return *values* as a list of keys or values, refusing a bare string or bytes.

    Both are iterable, so taken as a collection a ``str`` would select one
    entry per character and ``bytes`` one small-integer key per byte. A
    ``bytes`` *element* of the collection is a single blob key.
    """
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError(
            f"{param} must be a collection, not a single {type(values).__name__}; "
            f"wrap one value in a list: [{values!r}]"
        )
    return values if isinstance(values, list) else list(values)


class CdtReadBuilder(Generic[T]):
    """Terminal read actions and singular navigation for a CDT path.

    The parent (type ``T``) must expose ``add_operation(op)`` so the
    builder can register the produced operation before returning it.

    Navigation fields (``bin_name``, ``ctx``, ``to_ctx``) are optional;
    when not provided, further navigation is not available (terminal-only).

    Example::

        Read values from a map key within a query::

            stream = await (
                session.query(key)
                    .bin("settings").on_map_key("theme").get_values()
                    .execute()
            )
    """

    __slots__ = (
        "_parent", "_op_factory", "_rt", "_is_map",
        "_bin_name", "_ctx", "_to_ctx", "_filterable",
    )

    def __init__(
        self,
        parent: T,
        op_factory: Callable[[Any], Any],
        return_type_cls: _ReturnTypeCls,
        *,
        is_map: bool,
        bin_name: str = "",
        ctx: Sequence[Any] = (),
        to_ctx: Callable[[], Any] | None = None,
        filterable: bool = True,
    ) -> None:
        self._parent = parent
        self._op_factory = op_factory
        self._rt = return_type_cls
        self._is_map = is_map
        self._bin_name = bin_name
        self._ctx: tuple[Any, ...] = tuple(ctx)
        self._to_ctx = to_ctx
        # The server returns a corrupted value for an and-filter after a map
        # index or rank context, so those steps pass False.
        self._filterable = filterable

    def on_each_child(self) -> "CdtPathBuilder[T]":
        """Continue from this selection as a path over every child.

        Returns:
            A :class:`CdtPathBuilder` one level below the current selection.

        Example::

            .bin("catalog").on_map_key("book").on_each_child().collect_values()
        """
        bin_name, new_ctx, _ = self._push_ctx()
        return CdtPathBuilder(self._parent, bin_name, new_ctx + (CTX.all_children(),))

    def on_each_child_where(self, predicate: Any) -> "CdtPathBuilder[T]":
        """Continue as a path over the children matching *predicate*.

        Args:
            predicate: An :class:`~aerospike_sdk.Exp` over the element's loop
                variable.

        Returns:
            A :class:`CdtPathBuilder` over the matching children.
        """
        bin_name, new_ctx, _ = self._push_ctx()
        return CdtPathBuilder(
            self._parent, bin_name, new_ctx + (CTX.all_children_with_filter(predicate),),
        )

    def on_map_keys_in(self, keys: Iterable[Any]) -> "CdtPathBuilder[T]":
        """Continue as a path over the map entries whose key is in *keys*.

        Args:
            keys: The map keys to select.

        Returns:
            A :class:`CdtPathBuilder` over the selected entries.

        Raises:
            TypeError: If *keys* is a bare ``str``, ``bytes`` or ``bytearray``
                rather than a collection of keys.

        Example::

            .bin("catalog").on_map_key("prices").on_map_keys_in(["a", "c"]).collect_values()
        """
        bin_name, new_ctx, _ = self._push_ctx()
        return CdtPathBuilder(
            self._parent, bin_name,
            new_ctx + (CTX.map_keys_in(_value_list(keys, "keys")),), filterable=True,
        )

    def and_filter(self, predicate: Any) -> "CdtPathBuilder[T]":
        """Continue as a path that keeps this selection only if it matches *predicate*.

        The server evaluates *predicate* against the element this step
        selected: a match continues the path with it, a miss selects nothing.

        Example::

            over_10 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10))
            stream = await (
                session.query(key)
                    .bin("prices").on_map_key("book").and_filter(over_10).collect_values()
                    .execute()
            )

        Args:
            predicate: An :class:`~aerospike_sdk.Exp` over the element's loop
                variable.

        Returns:
            A :class:`CdtPathBuilder` over the filtered selection.

        Raises:
            TypeError: If this step is a range or multi-element selection,
                which has no single element to filter, or a map index or rank step,
                where the server does not return the filtered value correctly.

        See Also:
            :meth:`CdtPathBuilder.and_filter`: the same filter after
            :meth:`on_map_keys_in` or a step inside a path.
        """
        if self._to_ctx is None:
            raise TypeError(
                "and_filter() must follow a single-element step such as "
                "on_map_key(); a range or multi-element selection has no single "
                "element to filter"
            )
        if not self._filterable:
            raise TypeError(
                "and_filter() is not supported after on_map_index() or "
                "on_map_rank(); select by key with on_map_key() or on_map_keys_in()"
            )
        return CdtPathBuilder(
            self._parent, self._bin_name,
            self._ctx + (self._to_ctx(), CTX.and_filter(predicate)),
        )

    # -- Internal helpers -----------------------------------------------------

    def _emit(self, return_type: Any) -> T:
        op = self._op_factory(return_type)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def _require_map(self, method: str) -> None:
        if not self._is_map:
            raise TypeError(f"{method}() is only supported for map operations")

    def _push_ctx(self) -> tuple[str, tuple[Any, ...], list[Any]]:
        """Extend context by pushing the current selector.

        Returns ``(bin_name, new_ctx_tuple, new_ctx_list)`` for use by
        navigation methods.  Raises if navigation is not supported.
        """
        if self._to_ctx is None:
            raise TypeError("This builder does not support further navigation")
        new_ctx = self._ctx + (self._to_ctx(),)
        return self._bin_name, new_ctx, list(new_ctx)

    def _context_list_for_nested_ops(self) -> list[Any]:
        """CDT context for collection-level ops at the current selection.

        First-hop builders from a bin store only ``_to_ctx`` until a deeper
        navigation pushes it into ``_ctx``. Terminals like ``map_clear`` or
        ``map_size`` must include both.
        """
        out = list(self._ctx)
        if self._to_ctx is not None:
            out.append(self._to_ctx())
        return out

    def _build_navigated(
        self, *, op_factory: Callable[[Any], Any],
        rt_cls: _ReturnTypeCls, is_map: bool,
        ctx: Sequence[Any], to_ctx: Callable[[], Any],
        filterable: bool = True,
        **_extra: Any,
    ) -> CdtReadBuilder[T]:
        """Create a navigated builder of the same flavor.

        Subclasses override to preserve their builder type and extra
        fields (e.g. ``remove_factory``, ``set_to_factory``).
        """
        return CdtReadBuilder(
            self._parent, op_factory, rt_cls, is_map=is_map,
            bin_name=self._bin_name, ctx=ctx, to_ctx=to_ctx, filterable=filterable,
        )

    def _build_invertable(
        self,
        op_factory: Callable[[Any], Any],
        rt_cls: _ReturnTypeCls,
        *,
        is_map: bool,
        ctx: Sequence[Any],
        to_ctx: Callable[[], Any] | None = None,
        remove_factory: Callable[[Any], Any] | None = None,
    ) -> CdtReadInvertableBuilder[T]:
        return CdtReadInvertableBuilder(
            self._parent, op_factory, rt_cls, is_map=is_map,
            bin_name=self._bin_name, ctx=ctx, to_ctx=to_ctx,
        )

    # -- Singular CDT navigation (deeper nesting) ----------------------------

    def on_map_key(
        self, key: Any, *, create_type: Optional[MapOrder] = None,
    ) -> CdtReadBuilder[T]:
        """Navigate into a map element by key.

        Args:
            key: Map key to target.
            create_type: If set, use a create-on-missing context for this key
                with the given map key order.

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element.

        Example::
            .bin("m").on_map_key("x").get_values()
        """
        b, new_ctx, ctx_l = self._push_ctx()
        if create_type is not None:
            to_ctx = lambda: CTX.map_key_create(key, create_type)
        else:
            to_ctx = lambda: CTX.map_key(key)
        return self._build_navigated(
            op_factory=lambda rt: MapOperation.get_by_key(b, key, rt).set_context(ctx_l),
            remove_factory=lambda rt: MapOperation.remove_by_key(b, key, rt).set_context(ctx_l),
            rt_cls=MapReturnType, is_map=True,
            ctx=new_ctx, to_ctx=to_ctx,
        )

    def on_map_index(self, index: int) -> CdtReadBuilder[T]:
        """Navigate into a map element by index.

        Args:
            index: Map index to target.

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element.
        """
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: MapOperation.get_by_index(b, index, rt).set_context(ctx_l),
            remove_factory=lambda rt: MapOperation.remove_by_index(b, index, rt).set_context(ctx_l),
            rt_cls=MapReturnType, is_map=True,
            ctx=new_ctx, to_ctx=lambda: CTX.map_index(index), filterable=False,
        )

    def on_map_rank(self, rank: int) -> CdtReadBuilder[T]:
        """Navigate into a map element by rank (0 = lowest value).

        Args:
            rank: Rank position (0 = lowest value).

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element.
        """
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: MapOperation.get_by_rank(b, rank, rt).set_context(ctx_l),
            remove_factory=lambda rt: MapOperation.remove_by_rank(b, rank, rt).set_context(ctx_l),
            rt_cls=MapReturnType, is_map=True,
            ctx=new_ctx, to_ctx=lambda: CTX.map_rank(rank), filterable=False,
        )

    def on_list_index(
        self, index: int,
        *,
        order: Optional[ListOrderType] = None,
        pad: bool = False,
    ) -> CdtReadBuilder[T]:
        """Navigate into a list element by index.

        Args:
            index: List index (0-based, negative counts from end).
            order: If set (or if *pad* is ``True``), use create-on-missing
                list context with this order; when only *pad* is ``True``,
                defaults to :data:`~aerospike_native.ListOrderType.UNORDERED`.
            pad: When using create-on-missing context, allow sparse indexes.

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element.

        Example::
            .bin("items").on_list_index(0).get_values()
        """
        b, new_ctx, ctx_l = self._push_ctx()
        use_create = order is not None or pad
        if use_create:
            eff_order = order if order is not None else ListOrderType.UNORDERED
            to_ctx = lambda: CTX.list_index_create(index, eff_order, pad)
        else:
            to_ctx = lambda: CTX.list_index(index)
        return self._build_navigated(
            op_factory=lambda rt: ListOperation.get_by_index(b, index, rt).set_context(ctx_l),
            remove_factory=lambda rt: ListOperation.remove_by_index(b, index, rt).set_context(ctx_l),
            rt_cls=ListReturnType, is_map=False,
            ctx=new_ctx, to_ctx=to_ctx,
        )

    def on_list_rank(self, rank: int) -> CdtReadBuilder[T]:
        """Navigate into a list element by rank (0 = lowest value).

        Args:
            rank: Rank position (0 = lowest value).

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element.
        """
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_navigated(
            op_factory=lambda rt: ListOperation.get_by_rank(b, rank, rt).set_context(ctx_l),
            remove_factory=lambda rt: ListOperation.remove_by_rank(b, rank, rt).set_context(ctx_l),
            rt_cls=ListReturnType, is_map=False,
            ctx=new_ctx, to_ctx=lambda: CTX.list_rank(rank),
        )

    # -- Invertable CDT navigation (range / value / list selectors) -----------

    def on_map_value(self, value: Any) -> CdtReadInvertableBuilder[T]:
        """Navigate into map elements matching a value (may match multiple).

        Args:
            value: Value to match.

        Returns:
            :class:`CdtReadInvertableBuilder` for reading the selection;
            further singular navigation is supported when this builder was
            produced from a nested path.
        """
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_value(b, value, rt).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx,
            to_ctx=lambda: CTX.map_value(value),
        )

    def on_map_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into map elements by index range."""
        b, new_ctx, ctx_l = self._push_ctx()
        if count is None:
            op_f = lambda rt: MapOperation.get_by_index_range_from(
                b, index, rt,
            ).set_context(ctx_l)
        else:
            op_f = lambda rt: MapOperation.get_by_index_range(
                b, index, count, rt,
            ).set_context(ctx_l)
        return self._build_invertable(
            op_f, MapReturnType, is_map=True, ctx=new_ctx, to_ctx=None,
        )

    def on_map_key_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into map elements by key range ``[start, end)``."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_key_range(
                b, start, end, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx, to_ctx=None,
        )

    def on_map_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into map elements by rank range."""
        b, new_ctx, ctx_l = self._push_ctx()
        if count is None:
            op_f = lambda rt: MapOperation.get_by_rank_range_from(
                b, rank, rt,
            ).set_context(ctx_l)
        else:
            op_f = lambda rt: MapOperation.get_by_rank_range(
                b, rank, count, rt,
            ).set_context(ctx_l)
        return self._build_invertable(
            op_f, MapReturnType, is_map=True, ctx=new_ctx, to_ctx=None,
        )

    def on_map_value_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into map elements by value range ``[start, end)``."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_value_range(
                b, start, end, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx, to_ctx=None,
        )

    def on_map_key_relative_index_range(
        self, key: Any, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into map entries by index range relative to an anchor key."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_key_relative_index_range(
                b, key, index, count, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx, to_ctx=None,
        )

    def on_map_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into map entries by value rank range relative to an anchor."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx, to_ctx=None,
        )

    def on_map_key_list(self, keys: Sequence[Any]) -> CdtReadInvertableBuilder[T]:
        """Navigate into map elements matching a list of keys.

        Args:
            keys: Map keys to match.

        Returns:
            :class:`CdtReadInvertableBuilder` for reading the matched elements.

        Raises:
            TypeError: If *keys* is a bare ``str``, ``bytes`` or ``bytearray``
                rather than a collection of keys.
        """
        keys = _value_list(keys, "keys")
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_key_list(b, keys, rt).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx, to_ctx=None,
        )

    def on_map_value_list(
        self, values: Sequence[Any],
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into map elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtReadInvertableBuilder` for reading the matched elements.

        Raises:
            TypeError: If *values* is a bare ``str``, ``bytes`` or
                ``bytearray`` rather than a collection of values.
        """
        values = _value_list(values, "values")
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_value_list(
                b, values, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx, to_ctx=None,
        )

    def on_list_value(self, value: Any) -> CdtReadInvertableBuilder[T]:
        """Navigate into list elements matching a value."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_value(b, value, rt).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx,
            to_ctx=lambda: CTX.list_value(value),
        )

    def on_list_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into list elements by index range."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_index_range(
                b, index, count, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx, to_ctx=None,
        )

    def on_list_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into list elements by rank range."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_rank_range(
                b, rank, count, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx, to_ctx=None,
        )

    def on_list_value_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into list elements by value range ``[start, end)``."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_value_range(
                b, start, end, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx, to_ctx=None,
        )

    def on_list_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into list elements by value rank range relative to anchor."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx, to_ctx=None,
        )

    def on_list_value_list(
        self, values: Sequence[Any],
    ) -> CdtReadInvertableBuilder[T]:
        """Navigate into list elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtReadInvertableBuilder` for reading the matched elements.

        Raises:
            TypeError: If *values* is a bare ``str``, ``bytes`` or
                ``bytearray`` rather than a collection of values.
        """
        values = _value_list(values, "values")
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_value_list(
                b, values, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx, to_ctx=None,
        )

    # -- Terminal read methods ------------------------------------------------

    def get_values(self) -> T:
        """Return value(s) at the current CDT selection.

        Returns:
            The parent builder for chaining.

        Example::
            .bin("m").on_map_key("x").get_values()
        """
        return self._emit(self._rt.VALUE)

    def get_keys(self) -> T:
        """Return map keys at the current CDT selection.

        Returns:
            The parent builder for chaining.
        """
        self._require_map("get_keys")
        return self._emit(self._rt.KEY)

    def get_keys_and_values(self) -> T:
        """Return map key-value pairs at the current CDT selection.

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`get_as_map`: The same entries as an unordered ``dict``.
            :meth:`get_as_ordered_map`: The same entries as a key-ordered ``SortedMap``.
        """
        self._require_map("get_keys_and_values")
        return self._emit(self._rt.KEY_VALUE)

    def get_as_map(self) -> T:
        """Return the selected map entries as an unordered ``dict``.

        The server returns the entries as an unordered map rather than as
        key-value pairs, so the result makes no promise about order; use
        :meth:`get_keys_and_values` to keep the server's selection order or
        :meth:`get_as_ordered_map` for key order.

        Example::

            stream = await (
                session.query(players.id("p-1001"))
                .bin("scores").on_map_key_range("a", "m").get_as_map()
                .execute()
            )
            early = (await stream.first_or_raise()).record.bins["scores"]

        Returns:
            The parent builder for chaining.

        Raises:
            TypeError: If the selection is on a list rather than a map.

        See Also:
            :meth:`get_keys_and_values`: Entries in selection order, as a ``dict``.
            :meth:`get_as_ordered_map`: Entries in key order, as a ``SortedMap``.
        """
        self._require_map("get_as_map")
        return self._emit(self._rt.UNORDERED_MAP)

    def get_as_ordered_map(self) -> T:
        """Return the selected map entries as a key-ordered :class:`~aerospike_sdk.SortedMap`.

        Unlike :meth:`get_keys_and_values`, which keeps the order the server
        selected the entries in (rank order for a rank range), the result is
        ordered by key whatever the bin's own map order. Writing it back to a
        bin stores that map key-ordered.

        Example::

            stream = await (
                session.query(players.id("p-1001"))
                .bin("scores").on_map_rank_range(-3, 3).get_as_ordered_map()
                .execute()
            )
            top_three = (await stream.first_or_raise()).record.bins["scores"]
            # SortedMap({"amy": 870, "raj": 910, "zoe": 955})

        Returns:
            The parent builder for chaining.

        Raises:
            TypeError: If the selection is on a list rather than a map.

        See Also:
            :meth:`get_keys_and_values`: Entries in selection order, as a ``dict``.
        """
        self._require_map("get_as_ordered_map")
        return self._emit(self._rt.ORDERED_MAP)

    def count(self) -> T:
        """Return the count of elements at the current CDT selection.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.COUNT)

    def get_indexes(self) -> T:
        """Return indexes of the selected CDT elements.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.INDEX)

    def get_reverse_indexes(self) -> T:
        """Return reverse indexes of the selected CDT elements.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.REVERSE_INDEX)

    def get_ranks(self) -> T:
        """Return ranks of the selected CDT elements.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.RANK)

    def get_reverse_ranks(self) -> T:
        """Return reverse ranks of the selected CDT elements.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.REVERSE_RANK)

    def exists(self) -> T:
        """Check whether the selected CDT element(s) exist.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.EXISTS)

    def map_size(self) -> T:
        """Return the element count of the map at the current CDT path.

        Returns:
            The parent builder for chaining.
        """
        op = MapOperation.size(self._bin_name).set_context(
            self._context_list_for_nested_ops(),
        )
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_size(self) -> T:
        """Return the element count of the list at the current CDT path.

        Returns:
            The parent builder for chaining.
        """
        op = ListOperation.size(self._bin_name).set_context(
            self._context_list_for_nested_ops(),
        )
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_join(self, separator: Optional[str] = None) -> T:
        """Concatenate the string items of the list at the current CDT path.

        The list must hold only strings; any other element type fails with
        ``PARAMETER_ERROR``. An empty list joins to an empty string.

        Args:
            separator: Inserted between consecutive items. ``None`` = no
                separator.

        Returns:
            The parent builder for chaining.
        """
        op = ListOperation.join(self._bin_name, separator).set_context(
            self._context_list_for_nested_ops(),
        )
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_get(self, index: int) -> T:
        """Read the list element at *index* at the current CDT path."""
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.get(self._bin_name, index).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_get_range(self, index: int, count: Optional[int] = None) -> T:
        """Read a slice of the list starting at *index* (through end if *count* is ``None``)."""
        ctx = self._context_list_for_nested_ops()
        if count is None:
            op = ListOperation.get_range_from(self._bin_name, index).set_context(
                ctx,
            )
        else:
            op = ListOperation.get_range(
                self._bin_name, index, count,
            ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    # -- String reads on a navigated string leaf (server 8.2.0+) --------------
    #
    # Mirrors the ``str_*`` read family on the flat bin builders; the CDT
    # path accumulated by the navigation methods becomes the operation's
    # ``ctx``. Only read ops live here — the modify half sits on the write
    # builder so a query path cannot mutate.

    def _emit_op(self, op: Any) -> T:
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def str_strlen(self) -> T:
        """Return the Unicode codepoint count of the string at this CDT path.

        Example::

            stream = await (
                session.query(key)
                    .bin("tags").on_list_index(1).str_strlen()
                    .execute()
            )

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`str_byte_length`: UTF-8 byte length instead.
        """
        return self._emit_op(StringOperation.strlen(
            self._bin_name, ctx=self._context_list_for_nested_ops(),
        ))

    def str_substr(self, start: int, end: Optional[int] = None) -> T:
        """Return the codepoint range ``[start, end)`` of the string at this CDT path.

        ``end`` omitted runs to the end of the string; negative ``start``
        counts from the end. Out-of-bounds indexes clamp.

        Args:
            start: Codepoint index to start at.
            end: End-exclusive codepoint index. ``None`` means run to end.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.substr(
            self._bin_name, start, end, ctx=self._context_list_for_nested_ops(),
        ))

    def str_char_at(self, index: int) -> T:
        """Return the codepoint at ``index`` as a one-codepoint string.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.char_at(
            self._bin_name, index, ctx=self._context_list_for_nested_ops(),
        ))

    def str_find(self, needle: str, occurrence: Optional[int] = None) -> T:
        """Return the codepoint index of ``needle`` (``-1`` when absent).

        Args:
            needle: Substring to locate.
            occurrence: 1-based match index; ``-1`` selects the last match.
                ``None`` means first occurrence.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.find(
            self._bin_name, needle, occurrence, ctx=self._context_list_for_nested_ops(),
        ))

    def str_contains(self, needle: str) -> T:
        """Return ``True`` iff the string at this CDT path contains ``needle``.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.contains(
            self._bin_name, needle, ctx=self._context_list_for_nested_ops(),
        ))

    def str_starts_with(self, prefix: str) -> T:
        """Return ``True`` iff the string at this CDT path starts with ``prefix``.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.starts_with(
            self._bin_name, prefix, ctx=self._context_list_for_nested_ops(),
        ))

    def str_ends_with(self, suffix: str) -> T:
        """Return ``True`` iff the string at this CDT path ends with ``suffix``.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.ends_with(
            self._bin_name, suffix, ctx=self._context_list_for_nested_ops(),
        ))

    def str_to_integer(self) -> T:
        """Parse the string at this CDT path as ``int64``.

        Server returns ``OP_NOT_APPLICABLE`` (subcode
        ``SubCode.OPNOT_STRING_CONVERSION_FAILED``) if it is not numeric.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.to_integer(
            self._bin_name, ctx=self._context_list_for_nested_ops(),
        ))

    def str_to_double(self) -> T:
        """Parse the string at this CDT path as ``float64``.

        Server returns ``OP_NOT_APPLICABLE`` (subcode
        ``SubCode.OPNOT_STRING_CONVERSION_FAILED``) if it is not numeric.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.to_double(
            self._bin_name, ctx=self._context_list_for_nested_ops(),
        ))

    def str_byte_length(self) -> T:
        """Return the UTF-8 byte count of the string at this CDT path.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.byte_length(
            self._bin_name, ctx=self._context_list_for_nested_ops(),
        ))

    def str_is_numeric(self, numeric_type: Optional[StringNumericType] = None) -> T:
        """Return ``True`` iff the string at this CDT path parses as a number.

        Args:
            numeric_type: Restrict to :attr:`~aerospike_sdk.StringNumericType.INT`
                or :attr:`~aerospike_sdk.StringNumericType.FLOAT`. ``None`` = either.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.is_numeric(
            self._bin_name, numeric_type, ctx=self._context_list_for_nested_ops(),
        ))

    def str_is_upper(self) -> T:
        """Return ``True`` iff every cased codepoint at this CDT path is uppercase.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.is_upper(
            self._bin_name, ctx=self._context_list_for_nested_ops(),
        ))

    def str_is_lower(self) -> T:
        """Return ``True`` iff every cased codepoint at this CDT path is lowercase.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.is_lower(
            self._bin_name, ctx=self._context_list_for_nested_ops(),
        ))

    def str_to_blob(self) -> T:
        """Return the UTF-8 bytes of the string at this CDT path as a blob.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.to_blob(
            self._bin_name, ctx=self._context_list_for_nested_ops(),
        ))

    def str_split(self, separator: Optional[str] = None) -> T:
        """Split the string at this CDT path into a list.

        Args:
            separator: Substring to split on. ``None`` splits per codepoint.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.split(
            self._bin_name, separator, ctx=self._context_list_for_nested_ops(),
        ))

    def str_b64_decode(self) -> T:
        """Base64-decode the string at this CDT path, returning bytes.

        Server returns ``OP_NOT_APPLICABLE`` (subcode
        ``SubCode.OPNOT_STRING_B64_INVALID``) if it is not valid base64.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.b64_decode(
            self._bin_name, ctx=self._context_list_for_nested_ops(),
        ))

    def str_regex_compare(self, pattern: str, flags: int | StringRegexFlags = 0) -> T:
        """Return ``True`` iff the ICU regex ``pattern`` matches the string at this CDT path.

        Server returns ``OP_NOT_APPLICABLE`` (subcode
        ``SubCode.OPNOT_STRING_REGEX_LIMIT_EXCEEDED``) if matching exceeds
        the server's regex resource budget.

        Args:
            pattern: ICU regex pattern.
            flags: OR-combined :class:`~aerospike_sdk.StringRegexFlags` bitmask.

        Returns:
            The parent builder for chaining.
        """
        return self._emit_op(StringOperation.regex_compare(
            self._bin_name, pattern, int(flags), ctx=self._context_list_for_nested_ops(),
        ))


class CdtReadInvertableBuilder(CdtReadBuilder[T]):
    """Terminal read actions with inverted (all-others) variants.

    Used for range and list selectors where INVERTED makes semantic sense
    (e.g. "all keys *except* those in this range").  When constructed with
    ``to_ctx`` set (e.g. after a singular value selector on a nested path),
    singular navigation methods continue the CDT path.

    Example::

        Get all map values *except* those in a key range::

            .bin("m").on_map_key_range("a", "d").get_all_other_values()
    """

    # -- Inverted terminal methods --------------------------------------------

    def get_all_other_values(self) -> T:
        """Return all values *except* those matching the selection (INVERTED).

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.VALUE | self._rt.INVERTED)

    def get_all_other_keys(self) -> T:
        """Return all map keys *except* those matching the selection.

        Returns:
            The parent builder for chaining.
        """
        self._require_map("get_all_other_keys")
        return self._emit(self._rt.KEY | self._rt.INVERTED)

    def get_all_other_keys_and_values(self) -> T:
        """Return all map key-value pairs *except* those matching the selection.

        Returns:
            The parent builder for chaining.
        """
        self._require_map("get_all_other_keys_and_values")
        return self._emit(self._rt.KEY_VALUE | self._rt.INVERTED)

    def count_all_others(self) -> T:
        """Return the count of elements *except* those matching the selection.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.COUNT | self._rt.INVERTED)

    def get_all_other_indexes(self) -> T:
        """Return indexes of all elements *except* those matching the selection.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.INDEX | self._rt.INVERTED)

    def get_all_other_reverse_indexes(self) -> T:
        """Return reverse indexes of all elements *except* those matching the selection.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.REVERSE_INDEX | self._rt.INVERTED)

    def get_all_other_ranks(self) -> T:
        """Return ranks of all elements *except* those matching the selection.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.RANK | self._rt.INVERTED)

    def get_all_other_reverse_ranks(self) -> T:
        """Return reverse ranks of all elements *except* those matching the selection.

        Returns:
            The parent builder for chaining.
        """
        return self._emit(self._rt.REVERSE_RANK | self._rt.INVERTED)


class CdtPathBuilder(Generic[T]):
    """Fluent builder for CDT *path* selects and modifies.

    The other CDT builders navigate to one element and read or write it. A path
    walks a whole level -- every child, or every child matching a predicate --
    and applies one server operation across the selection. That is a different
    family of server ops (``CdtOperation.select_* / modify_* / remove``), hence
    a separate builder rather than more terminals on
    :class:`CdtReadBuilder`.

    Reached from ``on_each_child()`` / ``on_each_child_where()`` /
    ``on_map_keys_in()``; not constructed directly. Navigation accumulates
    ``CTX`` steps, so paths nest: ``.on_each_child().on_each_child()`` walks
    two levels down.
    """

    __slots__ = ("_parent", "_bin_name", "_ctx", "_filterable")

    def __init__(
        self, parent: T, bin_name: str, ctx: Sequence[Any], *, filterable: bool = False,
    ) -> None:
        self._parent = parent
        self._bin_name = bin_name
        self._ctx: tuple[Any, ...] = tuple(ctx)
        # The server takes and_filter() only after a non-expression step and
        # once per level; the builder refuses the other positions up front.
        self._filterable = filterable

    # -- Navigation -----------------------------------------------------------

    def on_each_child(self) -> CdtPathBuilder[T]:
        """Descend to every child of the current selection.

        Returns:
            A :class:`CdtPathBuilder` one level deeper.

        Example::

            .bin("m").on_each_child().on_each_child().collect_values()
        """
        return CdtPathBuilder(
            self._parent, self._bin_name, self._ctx + (CTX.all_children(),),
        )

    def on_each_child_where(self, predicate: Any) -> CdtPathBuilder[T]:
        """Descend to the children matching *predicate*.

        Args:
            predicate: An :class:`~aerospike_sdk.Exp` over the current
                element's loop variable, e.g.
                ``Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(5))``.

        Returns:
            A :class:`CdtPathBuilder` over the matching children.

        Example::

            .bin("nums").on_each_child_where(over_5).remove_matches()
        """
        return CdtPathBuilder(
            self._parent,
            self._bin_name,
            self._ctx + (CTX.all_children_with_filter(predicate),),
        )

    def on_map_key(self, key: Any) -> CdtPathBuilder[T]:
        """Descend into one map key, continuing the path.

        Args:
            key: Map key to step into.

        Returns:
            A :class:`CdtPathBuilder` at that key.

        Example::

            .bin("catalog").on_map_key("book").on_each_child().collect_values()
        """
        return CdtPathBuilder(
            self._parent, self._bin_name, self._ctx + (CTX.map_key(key),), filterable=True,
        )

    def on_list_index(self, index: int) -> CdtPathBuilder[T]:
        """Descend into one list index, continuing the path.

        Args:
            index: List index to step into.

        Returns:
            A :class:`CdtPathBuilder` at that index.
        """
        return CdtPathBuilder(
            self._parent, self._bin_name, self._ctx + (CTX.list_index(index),), filterable=True,
        )

    def on_map_value(self, value: Any) -> CdtPathBuilder[T]:
        """Descend into map elements matching *value*, continuing the path.

        Returns:
            A :class:`CdtPathBuilder` at the matching elements.
        """
        return CdtPathBuilder(
            self._parent, self._bin_name, self._ctx + (CTX.map_value(value),), filterable=True,
        )

    def on_list_value(self, value: Any) -> CdtPathBuilder[T]:
        """Descend into list elements matching *value*, continuing the path.

        Returns:
            A :class:`CdtPathBuilder` at the matching elements.
        """
        return CdtPathBuilder(
            self._parent, self._bin_name, self._ctx + (CTX.list_value(value),), filterable=True,
        )

    # -- Read terminals -------------------------------------------------------

    def on_map_keys_in(self, keys: Iterable[Any]) -> CdtPathBuilder[T]:
        """Descend to the map entries whose key is in *keys*.

        Args:
            keys: The map keys to select.

        Returns:
            A :class:`CdtPathBuilder` over the selected entries. Follow it
            with :meth:`and_filter` to narrow them by a predicate.

        Raises:
            TypeError: If *keys* is a bare ``str``, ``bytes`` or ``bytearray``
                rather than a collection of keys.

        Example::

            .bin("rows").on_each_child().on_map_keys_in(["id", "total"]).collect_values()
        """
        return CdtPathBuilder(
            self._parent,
            self._bin_name,
            self._ctx + (CTX.map_keys_in(_value_list(keys, "keys")),),
            filterable=True,
        )

    def and_filter(self, predicate: Any) -> CdtPathBuilder[T]:
        """Keep only the elements of the preceding selection matching *predicate*.

        Args:
            predicate: An :class:`~aerospike_sdk.Exp` over the entry's loop
                variable, e.g.
                ``Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10))``.

        Returns:
            A :class:`CdtPathBuilder` over the narrowed selection.

        Raises:
            TypeError: If there is no step to refine: the filter cannot open
                a path, follow ``on_each_child()`` / ``on_each_child_where()``
                (the predicate belongs there instead), or follow another
                ``and_filter()`` (combine conditions with ``Exp.and_`` in one
                call). It follows :meth:`on_map_keys_in` or a single-element
                step such as :meth:`on_map_key`.

        Example::

            over_10 = Exp.gt(Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10))
            .bin("m").on_map_keys_in(["a", "b", "c"]).and_filter(over_10).collect_map_entries()
        """
        if not self._filterable:
            raise TypeError(
                "and_filter() must follow a selection step such as on_map_keys_in(); "
                "combine conditions with Exp.and_, or filter children with "
                "on_each_child_where()"
            )
        return CdtPathBuilder(
            self._parent, self._bin_name, self._ctx + (CTX.and_filter(predicate),),
        )

    # -- Terminals ------------------------------------------------------------

    def _emit(self, op: Any) -> T:
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def collect_values(self, *, no_fail: bool = False) -> T:
        """Read the value of every element the path selects.

        Args:
            no_fail: When true, a path filter that cannot evaluate an element
                -- such as an integer comparison against a string -- skips
                that element instead of failing the operation. Useful when
                element types vary. An empty or absent collection is never an
                error; it yields an empty list.

        Returns:
            The parent builder for chaining.

        Example::

            .bin("nums").on_each_child().collect_values()
        """
        if no_fail:
            return self._emit(
                CdtOperation.select_by_path(
                    self._bin_name, SelectFlags.VALUE | SelectFlags.NO_FAIL,
                    list(self._ctx),
                )
            )
        return self._emit(
            CdtOperation.select_values(self._bin_name, list(self._ctx))
        )

    def collect_values_as_expression_read(
        self,
        bin_type: ExpType,
        *,
        no_fail: bool = False,
        ignore_eval_failure: bool = False,
    ) -> T:
        """Read the same selection as :meth:`collect_values` through an expression read.

        The path is evaluated as a select expression over the bin, and the
        resulting list is returned under this bin's name; the stored record is
        unchanged. Use it for expression-read semantics, or alongside other
        :meth:`~aerospike_sdk.aio.operations.query.QueryBinBuilder.select_from`
        projections; otherwise prefer :meth:`collect_values`.

        Args:
            bin_type: The type of the bin the path starts from,
                ``ExpType.MAP`` or ``ExpType.LIST``. The expression reads the
                bin as that type.
            no_fail: When true, a path filter that cannot evaluate an element
                -- such as an integer comparison against a string -- skips
                that element instead of failing the operation.
            ignore_eval_failure: When true, a failed evaluation -- such as a
                bin that is not of ``bin_type`` -- leaves the bin out of the
                result instead of failing the operation.

        Returns:
            The parent builder for chaining.

        Raises:
            ValueError: If ``bin_type`` is not ``ExpType.MAP`` or
                ``ExpType.LIST``; a path can only select from a collection.

        Example::

            from aerospike_sdk import ExpType

            stream = await (
                session.query(key)
                .bin("catalog").on_map_key("book").on_each_child().on_map_key("title")
                .collect_values_as_expression_read(ExpType.MAP)
                .execute()
            )
            titles = (await stream.first_or_raise()).record.bins["catalog"]

        See Also:
            :meth:`collect_values`: the same selection as a CDT operation.
            :meth:`~aerospike_sdk.aio.operations.query.QueryBinBuilder.select_from`:
            for other select flags, pass ``Exp.exp_select_by_path`` to it.
        """
        if bin_type == ExpType.MAP:
            bin_exp = FilterExpression.map_bin(self._bin_name)
        elif bin_type == ExpType.LIST:
            bin_exp = FilterExpression.list_bin(self._bin_name)
        else:
            raise ValueError(
                f"bin_type must be ExpType.MAP or ExpType.LIST, got {bin_type!r}"
            )
        select_flags = SelectFlags.VALUE | SelectFlags.NO_FAIL if no_fail else SelectFlags.VALUE
        read_flags = ExpReadFlags.EVAL_NO_FAIL if ignore_eval_failure else ExpReadFlags.DEFAULT
        expression = FilterExpression.exp_select_by_path(
            ExpType.LIST, int(select_flags), bin_exp, list(self._ctx),
        )
        return self._emit(ExpOperation.read(self._bin_name, expression, read_flags))

    def collect_map_keys(self, *, no_fail: bool = False) -> T:
        """Read the key of every map entry the path selects.

        Args:
            no_fail: When true, a path filter that cannot evaluate an element
                -- such as an integer comparison against a string -- skips
                that element instead of failing the operation. An empty or
                absent collection is never an error; it yields an empty list.

        Returns:
            The parent builder for chaining.

        Example::

            .bin("prices").on_each_child_where(over_10).collect_map_keys()

        See Also:
            :meth:`collect_map_entries`: the keys together with their values.
        """
        if no_fail:
            return self._emit(
                CdtOperation.select_by_path(
                    self._bin_name, SelectFlags.MAP_KEY | SelectFlags.NO_FAIL,
                    list(self._ctx),
                )
            )
        return self._emit(
            CdtOperation.select_map_keys(self._bin_name, list(self._ctx))
        )

    def collect_map_entries(self, *, no_fail: bool = False) -> T:
        """Read every selected map entry as a key/value pair.

        The result is one flat list of alternating keys and values.

        Args:
            no_fail: When true, a path filter that cannot evaluate an element
                -- such as an integer comparison against a string -- skips
                that element instead of failing the operation. An empty or
                absent collection is never an error; it yields an empty list.

        Returns:
            The parent builder for chaining.

        Example::

            .bin("prices").on_map_keys_in(["apple", "pear"]).collect_map_entries()

        See Also:
            :meth:`collect_map_keys`: the keys alone.
        """
        if no_fail:
            return self._emit(
                CdtOperation.select_by_path(
                    self._bin_name, SelectFlags.MAP_KEY_VALUE | SelectFlags.NO_FAIL,
                    list(self._ctx),
                )
            )
        return self._emit(
            CdtOperation.select_map_entries(self._bin_name, list(self._ctx))
        )

    def collect_matching_tree(self, *, no_fail: bool = False) -> T:
        """Read the selection as a tree, preserving the nesting it was found in.

        Args:
            no_fail: When true, a path filter that cannot evaluate an element
                -- such as an integer comparison against a string -- skips
                that element instead of failing the operation. An empty or
                absent collection is never an error; it yields an empty tree.

        Returns:
            The parent builder for chaining.

        Example::

            .bin("catalog").on_map_key("items").on_each_child_where(in_stock)
            .collect_matching_tree()

        See Also:
            :meth:`collect_values`: the selected values as a flat list.
        """
        if no_fail:
            return self._emit(
                CdtOperation.select_by_path(
                    self._bin_name, SelectFlags.MATCHING_TREE | SelectFlags.NO_FAIL,
                    list(self._ctx),
                )
            )
        return self._emit(
            CdtOperation.select_matching_tree(self._bin_name, list(self._ctx))
        )

    def collect_by_path(self, flags: Any) -> T:
        """Read the selection with explicit :class:`SelectFlags`.

        Args:
            flags: The :class:`SelectFlags` controlling what is returned.

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`collect_values`: the common ``SelectFlags.VALUE`` case.
        """
        return self._emit(
            CdtOperation.select_by_path(self._bin_name, flags, list(self._ctx))
        )

    # -- Write terminals ------------------------------------------------------

    def modify_by(self, expression: Any, flags: Any = None) -> T:
        """Rewrite every selected element to *expression*.

        Args:
            expression: An :class:`~aerospike_sdk.Exp` computing the new value,
                typically over the element's loop variable.
            flags: Optional :class:`ModifyFlags`; the server default applies
                when omitted.

        Returns:
            The parent builder for chaining.

        Example::

            .bin("nums").on_each_child().modify_by(
                Exp.num_add([Exp.int_loop_var(LoopVarPart.VALUE), Exp.val(10)])
            )
        """
        flag = ModifyFlags.DEFAULT if flags is None else flags
        return self._emit(
            CdtOperation.modify_by_path(
                self._bin_name, flag, expression, list(self._ctx),
            )
        )

    def modify_no_fail(self, expression: Any) -> T:
        """Rewrite every selected element, tolerating elements that cannot be.

        Args:
            expression: An :class:`~aerospike_sdk.Exp` computing the new value.

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`modify_by`: fails the operation instead of skipping.
        """
        return self._emit(
            CdtOperation.modify_no_fail(self._bin_name, expression, list(self._ctx))
        )

    def remove_matches(self) -> T:
        """Remove every element the path selects.

        Returns:
            The parent builder for chaining.

        Example::

            .bin("nums").on_each_child_where(over_5).remove_matches()
        """
        return self._emit(CdtOperation.remove(self._bin_name, list(self._ctx)))
