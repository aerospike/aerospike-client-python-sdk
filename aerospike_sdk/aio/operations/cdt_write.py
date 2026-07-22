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

"""CDT action builders for write contexts.

Extends the read-only builders from ``cdt_read`` with:

- ``remove()`` / ``remove_all_others()`` terminals
- ``set_to(value)`` / ``add(value)`` write terminals (map key navigation)
- Collection-level map/list terminals (``map_clear``, ``map_upsert_items``, …)
- Nested navigation that preserves write capability through the chain
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Sequence

from aerospike_async import (
    CTX,
    ListOperation,
    ListOrderType,
    ListPolicy,
    ListReturnType,
    ListSortFlags,
    ListWriteFlags,
    MapOperation,
    MapOrder,
    MapPolicy,
    MapReturnType,
    MapWriteFlags,
)

from aerospike_sdk.aio.operations.cdt_read import (
    CdtReadBuilder,
    CdtReadInvertableBuilder,
    T,
    _ReturnTypeCls,
    _map_item_pairs,
)

_DEFAULT_MAP_POLICY = MapPolicy(None, None)
_UNORDERED_LIST_POLICY = ListPolicy(None, None)
_ORDERED_LIST_POLICY = ListPolicy(ListOrderType.ORDERED, None)


def _resolve_list_policy(
    order: ListOrderType | None,
    *,
    unique: bool = False,
    bounded: bool = False,
    no_fail: bool = False,
    partial: bool = False,
) -> ListPolicy:
    """Build a ``ListPolicy`` from caller-supplied write-flag booleans.

    Flags are combined as a bitmask (same semantics as the server's list
    write flags). When none are set, returns a pre-built constant for the
    given *order* when possible (zero allocation).
    """
    flags = 0
    if unique:
        flags |= ListWriteFlags.ADD_UNIQUE
    if bounded:
        flags |= ListWriteFlags.INSERT_BOUNDED
    if no_fail:
        flags |= ListWriteFlags.NO_FAIL
    if partial:
        flags |= ListWriteFlags.PARTIAL
    if not flags:
        if order is None:
            return _UNORDERED_LIST_POLICY
        if order is ListOrderType.ORDERED:
            return _ORDERED_LIST_POLICY
        return ListPolicy(order, None)
    return ListPolicy(order, flags)


def _resolve_map_policy(
    base_flags: int,
    *,
    order: MapOrder | None = None,
    persist_index: bool = False,
    no_fail: bool = False,
    partial: bool = False,
) -> MapPolicy:
    """Build a ``MapPolicy`` from base flags plus caller-supplied options."""
    flags = base_flags
    if no_fail:
        flags |= MapWriteFlags.NO_FAIL
    if partial:
        flags |= MapWriteFlags.PARTIAL
    if persist_index:
        return MapPolicy.new_with_flags_and_persisted_index(order, flags)
    if flags:
        return MapPolicy.new_with_flags(order, flags)
    return MapPolicy(order, None)


class _RemoveMixin:
    """Shared remove logic for CDT write builders."""

    _remove_factory: Callable[[Any], Any]
    _parent: Any
    _rt: Any

    def _emit_remove(self, return_type: Any) -> Any:
        op = self._remove_factory(return_type)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def remove(self, *, return_type: Any = None) -> Any:
        """Remove the selected CDT element(s).

        Example::

            await session.update(key).bin("m").on_map_key("x").remove().execute()

            stream = await (
                session.update(key)
                    .bin("scores")
                    .on_map_value_range(80, 100)
                    .remove(return_type=MapReturnType.VALUE)
                    .execute()
            )

        Args:
            return_type: What the server should return about the removed
                elements. Use :class:`~aerospike_async.MapReturnType` or
                :class:`~aerospike_async.ListReturnType` (for example
                ``MapReturnType.VALUE``, ``ListReturnType.COUNT``). When
                omitted, the server returns no removal metadata (``NONE``).

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`CdtWriteInvertableBuilder.remove_all_others`: Remove
            everything except the current selection (inverted).
        """
        rt = self._rt.NONE if return_type is None else return_type
        return self._emit_remove(rt)


class CdtWriteBuilder(_RemoveMixin, CdtReadBuilder[T]):
    """CDT action builder with read, remove, and write terminals.

    Inherits all read terminals and navigation from ``CdtReadBuilder``.
    Adds ``remove()``, and on map-key paths ``set_to()`` / ``add()``.
    Navigation returns ``CdtWriteBuilder`` to preserve write capability.

    Example::

        Set a nested map value and remove another key::

            await (
                session.upsert(key)
                    .bin("settings").on_map_key("theme").set_to("dark")
                    .bin("settings").on_map_key("old_key").remove()
                    .execute()
            )
    """

    __slots__ = ("_remove_factory", "_set_to_factory", "_add_factory")

    def __init__(
        self,
        parent: T,
        op_factory: Callable[[Any], Any],
        remove_factory: Callable[[Any], Any],
        return_type_cls: _ReturnTypeCls,
        *,
        is_map: bool,
        bin_name: str = "",
        ctx: Sequence[Any] = (),
        to_ctx: Callable[[], Any] | None = None,
        set_to_factory: Callable[[Any], Any] | None = None,
        add_factory: Callable[[Any], Any] | None = None,
    ) -> None:
        super().__init__(
            parent, op_factory, return_type_cls, is_map=is_map,
            bin_name=bin_name, ctx=ctx, to_ctx=to_ctx,
        )
        self._remove_factory = remove_factory
        self._set_to_factory = set_to_factory
        self._add_factory = add_factory

    def _build_navigated(
        self, *, op_factory: Callable[[Any], Any],
        rt_cls: _ReturnTypeCls, is_map: bool,
        ctx: Sequence[Any], to_ctx: Callable[[], Any],
        **extra: Any,
    ) -> CdtWriteBuilder[T]:
        return CdtWriteBuilder(
            self._parent, op_factory,
            extra.get("remove_factory", self._remove_factory),
            rt_cls, is_map=is_map,
            bin_name=self._bin_name, ctx=ctx, to_ctx=to_ctx,
            set_to_factory=extra.get("set_to_factory"),
            add_factory=extra.get("add_factory"),
        )

    def on_map_key(
        self, key: Any, *, create_type: Optional[MapOrder] = None,
    ) -> CdtWriteBuilder[T]:
        """Navigate into a map element by key (supports set_to / add).

        Args:
            key: Map key to target.
            create_type: If set, use a create-on-missing context for this key
                with the given map key order.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element.
        """
        b, new_ctx, ctx_l = self._push_ctx()
        if create_type is not None:
            to_ctx = lambda: CTX.map_key_create(key, create_type)
        else:
            to_ctx = lambda: CTX.map_key(key)
        return self._build_navigated(
            op_factory=lambda rt: MapOperation.get_by_key(b, key, rt).set_context(ctx_l),
            remove_factory=lambda rt: MapOperation.remove_by_key(b, key, rt).set_context(ctx_l),
            set_to_factory=lambda v: MapOperation.put(b, key, v, _DEFAULT_MAP_POLICY).set_context(ctx_l),
            add_factory=lambda v: MapOperation.increment_value(b, key, v, _DEFAULT_MAP_POLICY).set_context(ctx_l),
            rt_cls=MapReturnType, is_map=True,
            ctx=new_ctx, to_ctx=to_ctx,
        )

    def on_map_index(self, index: int) -> CdtWriteBuilder[T]:  # type: ignore[override]
        """Navigate into a map element by index."""
        return super().on_map_index(index)  # type: ignore[return-value]

    def on_map_rank(self, rank: int) -> CdtWriteBuilder[T]:  # type: ignore[override]
        """Navigate into a map element by rank (0 = lowest value)."""
        return super().on_map_rank(rank)  # type: ignore[return-value]

    def on_list_index(
        self, index: int,
        *,
        order: Optional[ListOrderType] = None,
        pad: bool = False,
    ) -> CdtWriteBuilder[T]:  # type: ignore[override]
        """Navigate into a list element by index."""
        return super().on_list_index(index, order=order, pad=pad)  # type: ignore[return-value]

    def on_list_rank(self, rank: int) -> CdtWriteBuilder[T]:  # type: ignore[override]
        """Navigate into a list element by rank (0 = lowest value)."""
        return super().on_list_rank(rank)  # type: ignore[return-value]

    def _build_invertable(
        self,
        op_factory: Callable[[Any], Any],
        rt_cls: _ReturnTypeCls,
        *,
        is_map: bool,
        ctx: Sequence[Any],
        to_ctx: Callable[[], Any] | None = None,
        remove_factory: Callable[[Any], Any] | None = None,
    ) -> CdtWriteInvertableBuilder[T]:
        assert remove_factory is not None
        return CdtWriteInvertableBuilder(
            self._parent, op_factory, remove_factory, rt_cls, is_map=is_map,
            bin_name=self._bin_name, ctx=ctx, to_ctx=to_ctx,
        )

    def on_map_value(self, value: Any) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map elements matching a value (may match multiple)."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_value(b, value, rt).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx,
            to_ctx=lambda: CTX.map_value(value),
            remove_factory=lambda rt: MapOperation.remove_by_value(b, value, rt).set_context(ctx_l),
        )

    def on_map_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map elements by index range."""
        b, new_ctx, ctx_l = self._push_ctx()
        if count is None:
            get_f = lambda rt: MapOperation.get_by_index_range_from(
                b, index, rt,
            ).set_context(ctx_l)
            rm_f = lambda rt: MapOperation.remove_by_index_range_from(
                b, index, rt,
            ).set_context(ctx_l)
        else:
            get_f = lambda rt: MapOperation.get_by_index_range(
                b, index, count, rt,
            ).set_context(ctx_l)
            rm_f = lambda rt: MapOperation.remove_by_index_range(
                b, index, count, rt,
            ).set_context(ctx_l)
        return self._build_invertable(
            get_f, MapReturnType, is_map=True, ctx=new_ctx,
            remove_factory=rm_f,
        )

    def on_map_key_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map elements by key range ``[start, end)``."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_key_range(
                b, start, end, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx,
            remove_factory=lambda rt: MapOperation.remove_by_key_range(
                b, start, end, rt,
            ).set_context(ctx_l),
        )

    def on_map_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map elements by rank range."""
        b, new_ctx, ctx_l = self._push_ctx()
        if count is None:
            get_f = lambda rt: MapOperation.get_by_rank_range_from(
                b, rank, rt,
            ).set_context(ctx_l)
            rm_f = lambda rt: MapOperation.remove_by_rank_range_from(
                b, rank, rt,
            ).set_context(ctx_l)
        else:
            get_f = lambda rt: MapOperation.get_by_rank_range(
                b, rank, count, rt,
            ).set_context(ctx_l)
            rm_f = lambda rt: MapOperation.remove_by_rank_range(
                b, rank, count, rt,
            ).set_context(ctx_l)
        return self._build_invertable(
            get_f, MapReturnType, is_map=True, ctx=new_ctx,
            remove_factory=rm_f,
        )

    def on_map_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map elements by value range ``[start, end)``."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_value_range(
                b, start, end, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx,
            remove_factory=lambda rt: MapOperation.remove_by_value_range(
                b, start, end, rt,
            ).set_context(ctx_l),
        )

    def on_map_key_relative_index_range(
        self, key: Any, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map entries by index range relative to an anchor key."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_key_relative_index_range(
                b, key, index, count, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx,
            remove_factory=lambda rt: MapOperation.remove_by_key_relative_index_range(
                b, key, index, count, rt,
            ).set_context(ctx_l),
        )

    def on_map_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map entries by value rank range relative to an anchor."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx,
            remove_factory=lambda rt: MapOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ).set_context(ctx_l),
        )

    def on_map_key_list(self, keys: Sequence[Any]) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map elements matching a list of keys."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_key_list(b, keys, rt).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx,
            remove_factory=lambda rt: MapOperation.remove_by_key_list(b, keys, rt).set_context(ctx_l),
        )

    def on_map_value_list(
        self, values: Sequence[Any],
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into map elements matching a list of values."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: MapOperation.get_by_value_list(
                b, values, rt,
            ).set_context(ctx_l),
            MapReturnType, is_map=True, ctx=new_ctx,
            remove_factory=lambda rt: MapOperation.remove_by_value_list(
                b, values, rt,
            ).set_context(ctx_l),
        )

    def on_list_value(self, value: Any) -> CdtWriteInvertableBuilder[T]:
        """Navigate into list elements matching a value."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_value(b, value, rt).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx,
            to_ctx=lambda: CTX.list_value(value),
            remove_factory=lambda rt: ListOperation.remove_by_value(b, value, rt).set_context(ctx_l),
        )

    def on_list_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into list elements by index range."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_index_range(
                b, index, count, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx,
            remove_factory=lambda rt: ListOperation.remove_by_index_range(
                b, index, count, rt,
            ).set_context(ctx_l),
        )

    def on_list_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into list elements by rank range."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_rank_range(
                b, rank, count, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx,
            remove_factory=lambda rt: ListOperation.remove_by_rank_range(
                b, rank, count, rt,
            ).set_context(ctx_l),
        )

    def on_list_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into list elements by value range ``[start, end)``."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_value_range(
                b, start, end, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx,
            remove_factory=lambda rt: ListOperation.remove_by_value_range(
                b, start, end, rt,
            ).set_context(ctx_l),
        )

    def on_list_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into list elements by value rank range relative to anchor."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx,
            remove_factory=lambda rt: ListOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ).set_context(ctx_l),
        )

    def on_list_value_list(
        self, values: Sequence[Any],
    ) -> CdtWriteInvertableBuilder[T]:
        """Navigate into list elements matching a list of values."""
        b, new_ctx, ctx_l = self._push_ctx()
        return self._build_invertable(
            lambda rt: ListOperation.get_by_value_list(
                b, values, rt,
            ).set_context(ctx_l),
            ListReturnType, is_map=False, ctx=new_ctx,
            remove_factory=lambda rt: ListOperation.remove_by_value_list(
                b, values, rt,
            ).set_context(ctx_l),
        )

    # -- Write terminals ------------------------------------------------------

    def set_to(self, value: Any) -> T:
        """Set the value at the current CDT path.

        Requires prior ``on_map_key()`` navigation.

        Args:
            value: New value to store.

        Example::

            .bin("m").on_map_key("color").set_to("blue")

        Raises:
            TypeError: If not preceded by ``on_map_key()`` navigation.
        """
        if self._set_to_factory is None:
            raise TypeError("set_to() requires on_map_key() navigation")
        op = self._set_to_factory(value)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def add(self, value: Any) -> T:
        """Increment the value at the current CDT path.

        Args:
            value: Numeric value to add.

        Returns:
            The parent builder for chaining.

        Raises:
            TypeError: If not preceded by ``on_map_key()`` navigation.
        """
        if self._add_factory is None:
            raise TypeError("add() requires on_map_key() navigation")
        op = self._add_factory(value)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    # -- Collection-level map -------------------------------------------------

    def map_clear(self) -> T:
        """Remove all entries from the map at the current CDT path.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = MapOperation.clear(self._bin_name).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_upsert_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> T:
        """Put multiple map entries (create or update each key).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.DEFAULT,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        op = MapOperation.put_items(
            self._bin_name, pairs, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_insert_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> T:
        """Put map entries only for keys that do not yet exist.

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.CREATE_ONLY,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        op = MapOperation.put_items(
            self._bin_name, pairs, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_update_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> T:
        """Update existing map entries only (no new keys).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.UPDATE_ONLY,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        op = MapOperation.put_items(
            self._bin_name, pairs, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_create(self, order: MapOrder) -> T:
        """Create an empty map with the given key order.

        Args:
            order: Key sort order for the map.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = MapOperation.create(self._bin_name, order).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def map_set_policy(self, order: MapOrder) -> T:
        """Set map sort order policy without changing entries.

        Args:
            order: Key sort order to apply.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = MapOperation.set_map_policy(
            self._bin_name, MapPolicy(order, None),
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    # -- Collection-level list ------------------------------------------------

    def list_clear(self) -> T:
        """Remove all elements from the list at the current CDT path.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.clear(self._bin_name).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_sort(self, flags: ListSortFlags = ListSortFlags.DEFAULT) -> T:
        """Sort the list at the current path.

        Args:
            flags: Sort behaviour flags (default ``DEFAULT``).

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.sort(self._bin_name, flags).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_append_items(
        self, items: Sequence[Any],
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> T:
        """Append values to an unordered list.

        Args:
            items: Values to append.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent builder for chaining.

        Example::

            .bin("tags").on_map_key("items").list_append_items([10, 20])
        """
        ctx = self._context_list_for_nested_ops()
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        op = ListOperation.append_items(
            self._bin_name, items, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_add_items(
        self, items: Sequence[Any],
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> T:
        """Insert values into an ordered list (sorted positions).

        Args:
            items: Values to insert.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        policy = _resolve_list_policy(
            ListOrderType.ORDERED, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        op = ListOperation.append_items(
            self._bin_name, items, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_create(
        self, order: ListOrderType, *, pad: bool = False, persist_index: bool = False,
    ) -> T:
        """Create an empty list with the given order.

        Args:
            order: Element ordering for the list.
            pad: If ``True``, allow sparse indexes.
            persist_index: If ``True``, maintain a persistent index.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.create(
            self._bin_name, order, pad, persist_index,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_set_order(self, order: ListOrderType) -> T:
        """Set list sort order without changing elements.

        Args:
            order: Sort order to apply.

        Returns:
            The parent builder for chaining.
        """
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.set_order(self._bin_name, order).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    # -- Index-based list (nested CDT) ---------------------------------------

    def list_insert(
        self, index: int, value: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
    ) -> T:
        """Insert *value* at *index* in the list at the current CDT path.

        Args:
            index: List index (0-based; negative counts from the end).
            value: Element to insert.
            unique: Reject if the value already exists in the list.
            bounded: Reject if index is beyond the current list bounds.
            no_fail: Do not raise on write failures.
        """
        ctx = self._context_list_for_nested_ops()
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded, no_fail=no_fail,
        )
        op = ListOperation.insert(
            self._bin_name, index, value, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_insert_items(
        self, index: int, items: Sequence[Any],
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> T:
        """Insert *items* starting at *index* in the list at the current path.

        Args:
            index: List index at which to insert the first element.
            items: Values to insert in order.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.
        """
        ctx = self._context_list_for_nested_ops()
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        op = ListOperation.insert_items(
            self._bin_name, index, items, policy,
        ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_set(self, index: int, value: Any) -> T:
        """Replace the list element at *index* with *value*."""
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.set(self._bin_name, index, value).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_increment(self, index: int, value: int = 1) -> T:
        """Add *value* to the numeric element at *index* (default ``1``)."""
        ctx = self._context_list_for_nested_ops()
        if value == 1:
            op = ListOperation.increment_by_one(self._bin_name, index).set_context(
                ctx,
            )
        else:
            op = ListOperation.increment(
                self._bin_name, index, value, _UNORDERED_LIST_POLICY,
            ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_remove(self, index: int) -> T:
        """Remove the element at *index* from the list at the current path."""
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.remove(self._bin_name, index).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_remove_range(
        self, index: int, count: Optional[int] = None,
    ) -> T:
        """Remove *count* elements from *index*, or through the end if *count* is ``None``."""
        ctx = self._context_list_for_nested_ops()
        if count is None:
            op = ListOperation.remove_range_from(self._bin_name, index).set_context(
                ctx,
            )
        else:
            op = ListOperation.remove_range(
                self._bin_name, index, count,
            ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_pop(self, index: int) -> T:
        """Pop the element at *index* (returned in the operate result)."""
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.pop(self._bin_name, index).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_pop_range(
        self, index: int, count: Optional[int] = None,
    ) -> T:
        """Pop *count* elements from *index*, or through the end if *count* is ``None``."""
        ctx = self._context_list_for_nested_ops()
        if count is None:
            op = ListOperation.pop_range_from(self._bin_name, index).set_context(
                ctx,
            )
        else:
            op = ListOperation.pop_range(
                self._bin_name, index, count,
            ).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    def list_trim(self, index: int, count: int) -> T:
        """Keep *count* elements starting at *index*; remove the rest."""
        ctx = self._context_list_for_nested_ops()
        op = ListOperation.trim(self._bin_name, index, count).set_context(ctx)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent


class CdtWriteInvertableBuilder(CdtWriteBuilder[T], CdtReadInvertableBuilder[T]):
    """CDT action builder with read, inverted-read, remove, and nested write paths.

    Combines :class:`CdtWriteBuilder` navigation (including invertable selectors)
    with inverted read terminals from :class:`CdtReadInvertableBuilder`.

    Example::

        Remove everything except the selected range::

            .bin("m").on_map_value_range(0, 100).remove_all_others()
    """

    __slots__ = ()

    def __init__(
        self,
        parent: T,
        op_factory: Callable[[Any], Any],
        remove_factory: Callable[[Any], Any],
        return_type_cls: _ReturnTypeCls,
        *,
        is_map: bool,
        bin_name: str = "",
        ctx: Sequence[Any] = (),
        to_ctx: Callable[[], Any] | None = None,
    ) -> None:
        CdtWriteBuilder.__init__(
            self, parent, op_factory, remove_factory, return_type_cls,
            is_map=is_map, bin_name=bin_name, ctx=ctx, to_ctx=to_ctx,
            set_to_factory=None, add_factory=None,
        )

    def remove_all_others(self, *, return_type: Any = None) -> T:
        """Remove all CDT elements *except* the selected ones.

        The inverted selection flag is applied automatically; pass only the
        base return type (for example ``MapReturnType.COUNT``), not OR'd with
        ``INVERTED``.

        Example::

            await session.update(key).bin("m").on_map_key_range("b", "d").remove_all_others().execute()

            stream = await (
                session.update(key)
                    .bin("m")
                    .on_map_key_range("b", "d")
                    .remove_all_others(return_type=MapReturnType.VALUE)
                    .execute()
            )

        Args:
            return_type: What the server should return about the removed
                elements. When omitted, the server returns no removal metadata
                (``NONE``).

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`remove`: Remove only the current selection.
        """
        base = self._rt.NONE if return_type is None else return_type
        return self._emit_remove(base | self._rt.INVERTED)
