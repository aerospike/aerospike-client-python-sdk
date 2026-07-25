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

"""Neutral session-level types shared by async + sync session implementations.

No asyncio anywhere. Lives at the package root so neither
:mod:`aerospike_sdk.aio.session` nor :mod:`aerospike_sdk.sync.session` has
to reach across tiers for these.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    List,
    NamedTuple,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from typing_extensions import deprecated

from aerospike_async import Key

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.policy.behavior import OpKind, OpShape
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.policy_mapper import to_read_policy, to_write_policy

if TYPE_CHECKING:  # Forward-reference only; the concrete builders live per-tree.
    from aerospike_async import Txn

    from aerospike_sdk.operations_shared import _WriteSegmentBuilderBase
    from aerospike_sdk.policy.behavior import Behavior
    from aerospike_sdk.query_shared import _QueryBuilderBase

# Each session leaf binds these to its tree's concrete builders (async or sync),
# so the factories inherited from :class:`SessionBase` return the
# runtime-appropriate builder type instead of a single hard-coded tree's.
_WSB = TypeVar("_WSB", bound="_WriteSegmentBuilderBase")
_QB = TypeVar("_QB", bound="_QueryBuilderBase")
# The tree's transactional-session type. Each leaf binds this to its own class
# (via a forward reference, so the runtime never has to close the
# ``session -> transactional_session -> session`` import cycle), keeping the
# shared :meth:`SessionBase.transaction` return type precise per tree.
_TS = TypeVar("_TS")


class NamespaceScStatus(NamedTuple):
    """Result of :meth:`aerospike_sdk.aio.session.Session.namespace_sc_status` /
    :meth:`aerospike_sdk.sync.session.Session.namespace_sc_status`."""

    is_sc: bool
    """True when the namespace exists and ``strong-consistency`` is enabled."""
    detail: str
    """Empty when ``is_sc`` is true; otherwise a short explanation for logging or skips."""


class SessionBase(Generic[_WSB, _QB, _TS]):
    """Runtime-agnostic session behavior shared by the async and sync sessions.

    Holds the parts of a session that never touch the event loop: argument
    normalization and the write-verb / query builder factories. The pieces that
    *do* differ by runtime — building the tree-appropriate builder and wiring the
    namespace-mode resolver — stay on the leaves behind the
    :meth:`_fast_write_segment` / :meth:`_build_write_segment` /
    :meth:`_fast_query_builder` / :meth:`_build_query_builder` hooks, which each
    leaf overrides.

    Neither leaf subclasses the other; both subclass this base directly.
    """

    # Set by each leaf's ``__init__``; declared here so the shared factories can
    # read them without the type-checker flagging a missing attribute. ``_client``
    # is deliberately loose on the base (the leaves narrow it to their concrete
    # ``Client`` / ``SyncClient``); the base only reads it to seed a txn session.
    _txn: "Optional[Txn]"
    _behavior: "Behavior"
    _client: Any

    # -- Shared lifecycle / state ---------------------------------------------
    # ``__init__`` stays per-leaf (it differs only by the source of the raw PAC
    # client handle, and hoisting it would loosen the ``_client`` / ``_pac_client``
    # types the hot paths rely on). The substantive construction logic lives here.

    def _refresh_cached_policies(self) -> None:
        """(Re)build the cached base policies from the current behavior.

        Called at construction and by config hot-reload when the behavior
        changes. Each attribute swap is a single atomic assignment, so in-flight
        operations observe either the old or the new policy snapshot — never a
        half-updated set. Both AP and SC variants are cached so the mode-resolved
        fast paths pick the right policy without rebuilding.
        """
        behavior = self._behavior
        self._cached_read_policy = to_read_policy(
            behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.AP))
        self._cached_write_policy = to_write_policy(
            behavior.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP))
        self._cached_read_policy_sc = to_read_policy(
            behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.SC))
        self._cached_write_policy_sc = to_write_policy(
            behavior.get_settings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.SC))

    @property
    def behavior(self) -> "Behavior":
        """Policy bundle applied to operations created from this session.

        Returns:
            The :class:`~aerospike_sdk.policy.behavior.Behavior` bound to this
            session at creation.

        Example::

            session = client.create_session(Behavior.DEFAULT)
            assert session.behavior is Behavior.DEFAULT

        See Also:
            :meth:`get_current_transaction`: The session's active transaction, if any.
        """
        return self._behavior

    def get_current_transaction(self) -> "Optional[Txn]":
        """Return the active transaction for this session, or ``None``.

        Regular sessions always return ``None``; only a transactional session
        inside its active block returns a live :class:`~aerospike_async.Txn`.
        Builders spawned from the session read this and thread the result
        through every policy they hand to the PAC, so operations started inside
        a transaction auto-participate.

        Returns:
            The active :class:`~aerospike_async.Txn`, or ``None`` outside a
            transaction.

        Example::

            session = client.create_session()
            assert session.get_current_transaction() is None

        See Also:
            :attr:`behavior`: The session's policy bundle.
        """
        return self._txn

    def _txn_session_cls(self) -> "type[_TS]":
        """Return the tree's transactional-session class. Overridden per leaf.

        Kept as a per-leaf hook rather than a direct import both to bind the
        tree-appropriate class and to defer the import that would otherwise close
        the ``session -> transactional_session -> session`` cycle at module load.
        """
        raise NotImplementedError

    def transaction(self) -> _TS:
        """Start a multi-record transaction (MRT) using this session's behavior.

        Returns a context manager that allocates a fresh
        :class:`~aerospike_async.Txn`. Every operation run on the returned
        session auto-participates — builders stamp ``policy.txn`` under the hood,
        so user code never touches a policy object. On clean exit the
        transaction is committed; if an exception propagates out of the block it
        is aborted.

        Defined once here so this session's :attr:`behavior` is *always* threaded
        into the transactional session — the two trees cannot re-drift into
        dropping it on one side.

        Returns:
            The tree's transactional session (async or sync), bound to this
            session's client and behavior.

        Example::

            async with session.transaction() as tx:
                await tx.upsert(accounts.id("A")).bin("balance").set_to(100).execute()
                await tx.upsert(accounts.id("B")).bin("balance").set_to(200).execute()

        See Also:
            :meth:`get_current_transaction`: The active transaction, if any.
        """
        return self._txn_session_cls()(self._client, self._behavior)

    @deprecated("Renamed to transaction(); begin_transaction() will be removed after preview.")
    def begin_transaction(self) -> _TS:
        """Deprecated alias for :meth:`transaction` (preview back-compat).

        :meta private:
        """
        return self.transaction()

    # -- Per-leaf hooks (overridden by each session leaf) ---------------------
    # These construct the tree-appropriate builder. They live on the leaves
    # because the builder class and the namespace-mode resolver are
    # runtime-bound; the base only routes to them.

    def _fast_write_segment(self, op_type: str, key: Key) -> _WSB:
        """Single-key write shortcut; overridden per leaf. Not called on the base."""
        raise NotImplementedError

    def _build_write_segment(
        self,
        op_type: str,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *more_keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Multi-key / dataset write segment; overridden per leaf. Not called on the base."""
        raise NotImplementedError

    def _fast_query_builder(self, key: Key, behavior: "Behavior") -> _QB:
        """Single-key query shortcut; overridden per leaf. Not called on the base."""
        raise NotImplementedError

    def _build_query_builder(
        self,
        *,
        dataset: Optional[DataSet],
        key: Optional[Key],
        keys: Optional[List[Key]],
        namespace: Optional[str],
        set_name: Optional[str],
        behavior: "Behavior",
    ) -> _QB:
        """Multi-key / dataset / namespace query; overridden per leaf. Not called on the base."""
        raise NotImplementedError

    # -- Shared transaction binding -------------------------------------------

    def _bind_txn(self, builder: _QB) -> _QB:
        """Stamp the session's active txn onto a builder, if any.

        Used by the builder factories so operations started inside a
        transactional session auto-participate; a no-op outside a transaction.
        Returns the builder for fluent use.
        """
        if self._txn is not None:
            builder.with_txn(self._txn)
        return builder

    # -- Shared argument normalization ----------------------------------------

    def _is_single_key(
        self,
        arg1: object,
        arg2: object,
        keys: Tuple[Key, ...],
        key: object,
        dataset: object,
        namespace: object,
        key_value: object,
    ) -> bool:
        """True when a write verb was called with exactly one positional key.

        This is the hot single-key shape (``session.upsert(users.id(1))``); it
        routes to the direct :meth:`_fast_write_segment` path instead of the
        general key-resolution path.
        """
        return (
            isinstance(arg1, Key) and arg2 is None and not keys
            and key is None and dataset is None
            and namespace is None and key_value is None
        )

    # -- Write-verb builder factories -----------------------------------------
    # One shared body per verb: the single-key fast shape short-circuits to
    # `_fast_write_segment`; every other shape flows through
    # `_build_write_segment`. The chained terminal (`.execute()`) is awaited on
    # async sessions and blocking on sync sessions.

    def upsert(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Start a create-or-replace write for one or more keys.

        If the record exists, bins are merged according to the chained
        operations; if it does not exist, it is created. Use :meth:`insert` when
        the record must not already exist.

        Args:
            arg1: A single :class:`~aerospike_async.Key`, a list of keys, or omit
                and pass ``key`` / ``dataset`` + ``key_value`` / ``namespace`` +
                ``set_name`` + ``key_value``.
            arg2: Optional second key when passing multiple keys positionally.
            *keys: Additional keys when the first positional is a key.
            key: Single key (keyword form).
            dataset: Dataset used with ``key_value`` to build a key.
            namespace: Namespace used with ``set_name`` and ``key_value``.
            set_name: Set name used with ``namespace`` and ``key_value``.
            key_value: User key value with ``dataset`` or ``namespace``/``set_name``.

        Returns:
            A write-segment builder for ``put``, ``bin``, ``where``, ``execute``, etc.

        Raises:
            ValueError: If no keys are resolved or lists are empty.
            TypeError: If positional arguments are not keys or lists of keys.

        Example::

            users = DataSet.of("test", "users")
            session.upsert(users.id(1)).put({"name": "Tim", "age": 30}).execute()

        See Also:
            :meth:`insert`: Fails if the record already exists.
            :meth:`update`: Fails if the record does not exist.
            :meth:`replace`: Replace-entire-record semantics when configured.
        """
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("upsert", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "upsert", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def insert(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Start a create-only write; fails on execute if the record already exists.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example::

            users = DataSet.of("test", "users")
            session.insert(users.id(99)).put({"name": "new"}).execute()

        See Also:
            :meth:`upsert`: Create or update.
        """
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("insert", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "insert", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def update(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Start an update-only write; fails on execute if the record does not exist.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example::

            users = DataSet.of("test", "users")
            session.update(users.id(1)).bin("age").set_to(31).execute()

        See Also:
            :meth:`upsert`: Create or update.
            :meth:`replace_if_exists`: Replace-entire-record only if present.
        """
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("update", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "update", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def replace(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Start a replace-entire-record write (create or replace).

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example::

            users = DataSet.of("test", "users")
            session.replace(users.id(1)).put({"name": "Tim"}).execute()

        See Also:
            :meth:`replace_if_exists`: Replace only when the record exists.
            :meth:`upsert`: Merge instead of replace.
        """
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("replace", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "replace", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def replace_if_exists(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Start a replace-entire-record write that fails if the record is absent.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example::

            users = DataSet.of("test", "users")
            session.replace_if_exists(users.id(1)).put({"name": "Tim"}).execute()

        See Also:
            :meth:`replace`: Create or replace.
            :meth:`update`: Merge only when the record exists.
        """
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("replace_if_exists", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "replace_if_exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def delete(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Start a delete for one or more keys.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example::

            users = DataSet.of("test", "users")
            session.delete(users.id(1)).execute()

        See Also:
            :meth:`upsert`: Create or update the same keys.
        """
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("delete", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "delete", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def touch(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Start a touch (bump generation / reset TTL) for one or more keys.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example::

            users = DataSet.of("test", "users")
            session.touch(users.id(1)).execute()

        See Also:
            :meth:`update`: Modify bins as well as metadata.
        """
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("touch", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "touch", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    def exists(
        self,
        arg1: Optional[Union[Key, List[Key]]] = None,
        arg2: Optional[Key] = None,
        *keys: Key,
        key: Optional[Key] = None,
        dataset: Optional[DataSet] = None,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key_value: Optional[Union[str, int, bytes]] = None,
    ) -> _WSB:
        """Start an existence check for one or more keys.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder whose result reports presence per key.

        Raises:
            ValueError: If no keys are resolved.
            TypeError: If positional arguments are invalid.

        Example::

            users = DataSet.of("test", "users")
            present = session.exists(users.id(1)).execute().first().as_bool()

        See Also:
            :meth:`query`: Fetch the record instead of just presence.
        """
        if self._is_single_key(arg1, arg2, keys, key, dataset, namespace, key_value):
            return self._fast_write_segment("exists", arg1)  # type: ignore[arg-type]
        return self._build_write_segment(
            "exists", arg1, arg2, *keys,
            key=key, dataset=dataset, namespace=namespace,
            set_name=set_name, key_value=key_value,
        )

    # -- Query factory --------------------------------------------------------

    def query(
        self,
        arg1: Optional[Union[DataSet, Key, List[Key], str]] = None,
        arg2: Optional[Union[str, Key]] = None,
        *keys: Key,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        dataset: Optional[DataSet] = None,
        key: Optional[Key] = None,
        keys_list: Optional[List[Key]] = None,
        behavior: Optional["Behavior"] = None,
    ) -> _QB:
        """Start a read or secondary-index query for keys or a whole set.

        This session's behavior is applied to the underlying query builder.
        Supported shapes: a :class:`~aerospike_sdk.dataset.DataSet` (set-wide
        query), a single :class:`~aerospike_async.Key`, multiple keys (list or
        varargs), or explicit ``namespace`` / ``set_name`` for index scans.

        Args:
            arg1: Positional dataset, key, list of keys, or namespace string
                (when paired with ``arg2`` as set name).
            arg2: When ``arg1`` is a namespace, the set name; otherwise may be a
                second key when passing multiple keys positionally.
            *keys: Additional keys when the first positional argument is a key.
            namespace: Keyword namespace (with ``set_name``) when not using a dataset.
            set_name: Keyword set name (with ``namespace``).
            dataset: Keyword :class:`~aerospike_sdk.dataset.DataSet`.
            key: Keyword single key.
            keys_list: Keyword list of keys when not using ``arg1`` or varargs.
            behavior: Optional override for this query; defaults to the session's
                behavior.

        Returns:
            A query builder to chain ``where``, ``bins``, ``execute``, etc. The
            terminal ``execute()`` is awaited on async sessions and blocking on
            sync sessions.

        Raises:
            TypeError: If positional types do not match the supported shapes.
            ValueError: If a key list is empty or arguments are inconsistent.

        Example::

            users = DataSet.of("test", "users")
            rs = await session.query(users.id(1)).bins(["name"]).execute()
            row = await rs.first_or_raise()

        See Also:
            :meth:`upsert`: Writes for the same keys.
        """
        # Ultra-fast entry for the most common shape: ``session.query(key)`` with
        # no other args. Skip the isinstance chain and kwarg routing entirely and
        # go straight to the single-key builder. This is the bench / typical-app
        # read pattern; the extra normalization below is pure cold-path cost.
        if (
            arg1.__class__ is Key
            and arg2 is None
            and not keys
            and namespace is None
            and set_name is None
            and dataset is None
            and key is None
            and keys_list is None
            and behavior is None
        ):
            return self._fast_query_builder(arg1, self._behavior)  # type: ignore[arg-type]

        b = self._behavior if behavior is None else behavior

        if arg1 is not None:
            if isinstance(arg1, DataSet):
                dataset = arg1
            elif isinstance(arg1, Key):
                all_keys = [arg1]
                if isinstance(arg2, Key):
                    all_keys.append(arg2)
                    all_keys.extend(keys)
                elif keys:
                    all_keys.extend(keys)
                if len(all_keys) == 1:
                    key = arg1
                else:
                    keys_list = all_keys
            elif isinstance(arg1, list):
                if not arg1:
                    raise ValueError("keys list cannot be empty")
                if not isinstance(arg1[0], Key):
                    raise TypeError(
                        f"Expected List[Key], got first element {type(arg1[0])}",
                    )
                keys_list = arg1
            elif isinstance(arg1, str) and arg2 is not None and isinstance(arg2, str):
                namespace = arg1
                set_name = arg2
            else:
                raise TypeError(f"Unsupported arg1 type: {type(arg1)}")

        if key is not None and keys_list is None and dataset is None and namespace is None:
            builder = self._fast_query_builder(key, b)
        else:
            builder = self._build_query_builder(
                dataset=dataset, key=key, keys=keys_list,
                namespace=namespace, set_name=set_name, behavior=b,
            )
        return self._bind_txn(builder)
