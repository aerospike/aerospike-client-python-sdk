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
    Any,
    Dict,
    Generic,
    List,
    NamedTuple,
    Optional,
    overload,
    TYPE_CHECKING,
    TypeVar,
    Union,
)


from aerospike_native import Key

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.info_types import NamespaceDetail
from aerospike_sdk.policy.behavior import OpKind, OpShape
from aerospike_sdk.policy.behavior_settings import Mode
from aerospike_sdk.policy.policy_mapper import (
    to_read_operate_policy,
    to_read_policy,
    to_write_policy,
)

if TYPE_CHECKING:  # Forward-reference only; the concrete builders live per-tree.
    from aerospike_native import Txn

    from aerospike_sdk.operations_shared import (
        _DataSetWriteBuilderBase,
        _WriteSegmentBuilderBase,
    )
    from aerospike_sdk.policy.behavior import Behavior
    from aerospike_sdk.query_shared import _QueryBuilderBase

# Each session leaf binds these to its tree's concrete builders (async or sync),
# so the factories inherited from :class:`SessionBase` return the
# runtime-appropriate builder type instead of a single hard-coded tree's.
_WSB = TypeVar("_WSB", bound="_WriteSegmentBuilderBase")
_DSWB = TypeVar("_DSWB", bound="_DataSetWriteBuilderBase")
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


def _namespace_sc_status(
    namespace: str, detail: Optional[NamespaceDetail]
) -> NamespaceScStatus:
    """Build a :class:`NamespaceScStatus` from a parsed namespace detail.

    Shared so the async and blocking sessions cannot drift in the text they
    hand back to callers -- the reason is user-facing, and two copies of it
    are two things to keep correct.

    Args:
        namespace: Namespace that was inspected.
        detail: Parsed detail, or ``None`` when the namespace is undefined.

    Returns:
        :class:`NamespaceScStatus` with ``is_sc`` and a reason when false.
    """
    if detail is None:
        return NamespaceScStatus(
            False,
            f"Namespace {namespace!r} is not defined on this cluster "
            "(info reports type=unknown). Check the namespace name, or add the "
            "namespace to the server configuration.",
        )
    if not detail.strong_consistency_reported:
        return NamespaceScStatus(
            False,
            f"Namespace {namespace!r} info did not report strong-consistency; "
            "treating as non-SC.",
        )
    if detail.strong_consistency:
        return NamespaceScStatus(True, "")
    return NamespaceScStatus(
        False,
        f"Namespace {namespace!r} exists but strong-consistency is false "
        "(AP mode). Strong consistency is a server-side namespace setting; "
        "it cannot be enabled from the client.",
    )


def _namespace_mode_from_partition_map(
    pnc_client: Any, cache: Dict[str, Mode], namespace: str,
) -> Optional[Mode]:
    """Resolve SC or AP from PNC's partition map, caching the answer.

    The partition map answers without I/O. ``None`` means the map does not list
    the namespace (unknown to the cluster, or not yet tended); callers then ask
    the server and do not cache, so a failed or premature lookup never pins a
    namespace's mode for the life of the client. A namespace cannot change mode
    without a server restart, so a definite answer is safe to keep.
    """
    is_sc = pnc_client.is_strong_consistency(namespace)
    if is_sc is None:
        return None
    mode = cache[namespace] = Mode.SC if is_sc else Mode.AP
    return mode


def _probe_namespace_mode_blocking(pnc_client: Any, namespace: str) -> Mode:
    """Ask the server for *namespace*'s mode; AP when the probe fails.

    Only reached for a namespace the partition map does not list, where the
    operation about to run will surface the server's own error.
    """
    try:
        result = pnc_client.info_blocking(f"namespace/{namespace}")
    except Exception:
        return Mode.AP
    detail = NamespaceDetail.from_response(result, namespace)
    return Mode.SC if detail is not None and detail.strong_consistency else Mode.AP


def _dataset_not_strings(verb: str) -> str:
    """Message for a namespace/set string passed where a DataSet belongs."""
    return (
        f"{verb}() takes a DataSet, not namespace and set strings; "
        "use DataSet.of(namespace, set_name)"
    )


def _keys_need_key_first(verb: str, arg1: object) -> str:
    """Message for extra positional keys after a non-Key first argument."""
    return f"{verb}() takes extra positional keys only after a Key, got {type(arg1).__name__}"


class SessionBase(Generic[_WSB, _QB, _TS, _DSWB]):
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
    # ``__init__`` stays per-leaf (it differs only by the source of the raw PNC
    # client handle, and hoisting it would loosen the ``_client`` / ``_pnc_client``
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
        self._cached_read_operate_policy = to_read_operate_policy(
            behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.AP))
        self._cached_read_operate_policy_sc = to_read_operate_policy(
            behavior.get_settings(OpKind.READ, OpShape.POINT, Mode.SC))

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
            :attr:`current_transaction`: The session's active transaction, if any.
        """
        return self._behavior

    @property
    def current_transaction(self) -> "Optional[Txn]":
        """The active transaction for this session, or ``None``.

        Regular sessions always hold ``None``; only a transactional session
        inside its active block exposes a live :class:`~aerospike_native.Txn`.
        Builders spawned from the session read this and thread the result
        through every policy they hand to the PNC, so operations started inside
        a transaction auto-participate.

        Returns:
            The active :class:`~aerospike_native.Txn`, or ``None`` outside a
            transaction.

        Example::

            session = client.create_session()
            assert session.current_transaction is None

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
        :class:`~aerospike_native.Txn`. Every operation run on the returned
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
            :attr:`current_transaction`: The active transaction, if any.
        """
        return self._txn_session_cls()(self._client, self._behavior)

    # -- Per-leaf hooks (overridden by each session leaf) ---------------------
    # These construct the tree-appropriate builder. They live on the leaves
    # because the builder class and the namespace-mode resolver are
    # runtime-bound; the base only routes to them.

    def _fast_write_segment(self, op_type: str, key: Key) -> _WSB:
        """Single-key write shortcut; overridden per leaf. Not called on the base."""
        raise NotImplementedError

    def _dataset_write_builder(self, op_type: str, dataset: DataSet) -> _DSWB:
        """Dataset-scoped tabular write; overridden per leaf. Not called on the base."""
        raise NotImplementedError

    def _build_write_segment(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> _WSB:
        """Multi-key write segment; overridden per leaf. Not called on the base."""
        raise NotImplementedError

    def _fast_query_builder(self, key: Key) -> _QB:
        """Single-key query shortcut; overridden per leaf. Not called on the base."""
        raise NotImplementedError

    def _build_query_builder(
        self, *, dataset: Optional[DataSet], keys: Optional[List[Key]],
    ) -> _QB:
        """Multi-key / dataset query; overridden per leaf. Not called on the base."""
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

    # -- Write-verb builder factories -----------------------------------------
    # One shared body per verb: the single-key fast shape short-circuits to
    # `_fast_write_segment`; every other shape flows through
    # `_build_write_segment`. The chained terminal (`.execute()`) is awaited on
    # async sessions and blocking on sync sessions.

    @overload
    def upsert(self, arg1: DataSet, /) -> _DSWB: ...

    @overload
    def upsert(
        self,
        arg1: Union[Key, List[Key]],
        /,
        *keys: Key,
    ) -> _WSB: ...

    def upsert(  # type: ignore[misc]
        self,
        arg1: Union[DataSet, Key, List[Key]],
        /,
        *keys: Key,
    ) -> Union[_WSB, _DSWB]:
        """Start a create-or-replace write for one or more keys.

        If the record exists, bins are merged according to the chained
        operations; if it does not exist, it is created. Use :meth:`insert` when
        the record must not already exist.

        Args:
            arg1: A single :class:`~aerospike_native.Key`, a list of keys, or a
                :class:`~aerospike_sdk.dataset.DataSet` for a tabular write
                (rows follow via ``bins(...)``).
            *keys: Additional keys when ``arg1`` is a key.

        Returns:
            A write-segment builder for ``put``, ``bin``, ``where``, ``execute``, etc.

        Raises:
            ValueError: If a key list is empty.
            TypeError: If the arguments are not keys, a list of keys, or a dataset.

        Example::

            users = DataSet.of("test", "users")
            session.upsert(users.id(1)).put({"name": "Tim", "age": 30}).execute()

            # Several records with the same bins, as rows under a column schema:
            session.upsert(users).bins("name", "age").row(1, "Tim", 30).row(2, "Bob", 25).execute()

        See Also:
            :meth:`insert`: Fails if the record already exists.
            :meth:`update`: Fails if the record does not exist.
            :meth:`replace`: Replace-entire-record semantics when configured.
        """
        if arg1.__class__ is Key and not keys:
            return self._fast_write_segment("upsert", arg1)  # type: ignore[arg-type]
        if isinstance(arg1, DataSet):
            if keys:
                raise TypeError(_keys_need_key_first("upsert", arg1))
            return self._dataset_write_builder("upsert", arg1)
        return self._build_write_segment("upsert", arg1, *keys)  # type: ignore[arg-type]

    @overload
    def insert(self, arg1: DataSet, /) -> _DSWB: ...

    @overload
    def insert(
        self,
        arg1: Union[Key, List[Key]],
        /,
        *keys: Key,
    ) -> _WSB: ...

    def insert(  # type: ignore[misc]
        self,
        arg1: Union[DataSet, Key, List[Key]],
        /,
        *keys: Key,
    ) -> Union[_WSB, _DSWB]:
        """Start a create-only write; fails on execute if the record already exists.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If a key list is empty.
            TypeError: If the arguments are not keys or a list of keys.

        Example::

            users = DataSet.of("test", "users")
            session.insert(users.id(99)).put({"name": "new"}).execute()

        See Also:
            :meth:`upsert`: Create or update.
        """
        if arg1.__class__ is Key and not keys:
            return self._fast_write_segment("insert", arg1)  # type: ignore[arg-type]
        if isinstance(arg1, DataSet):
            if keys:
                raise TypeError(_keys_need_key_first("insert", arg1))
            return self._dataset_write_builder("insert", arg1)
        return self._build_write_segment("insert", arg1, *keys)  # type: ignore[arg-type]

    @overload
    def update(self, arg1: DataSet, /) -> _DSWB: ...

    @overload
    def update(
        self,
        arg1: Union[Key, List[Key]],
        /,
        *keys: Key,
    ) -> _WSB: ...

    def update(  # type: ignore[misc]
        self,
        arg1: Union[DataSet, Key, List[Key]],
        /,
        *keys: Key,
    ) -> Union[_WSB, _DSWB]:
        """Start an update-only write; fails on execute if the record does not exist.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If a key list is empty.
            TypeError: If the arguments are not keys or a list of keys.

        Example::

            users = DataSet.of("test", "users")
            session.update(users.id(1)).bin("age").set_to(31).execute()

        See Also:
            :meth:`upsert`: Create or update.
            :meth:`replace_if_exists`: Replace-entire-record only if present.
        """
        if arg1.__class__ is Key and not keys:
            return self._fast_write_segment("update", arg1)  # type: ignore[arg-type]
        if isinstance(arg1, DataSet):
            if keys:
                raise TypeError(_keys_need_key_first("update", arg1))
            return self._dataset_write_builder("update", arg1)
        return self._build_write_segment("update", arg1, *keys)  # type: ignore[arg-type]

    @overload
    def replace(self, arg1: DataSet, /) -> _DSWB: ...

    @overload
    def replace(
        self,
        arg1: Union[Key, List[Key]],
        /,
        *keys: Key,
    ) -> _WSB: ...

    def replace(  # type: ignore[misc]
        self,
        arg1: Union[DataSet, Key, List[Key]],
        /,
        *keys: Key,
    ) -> Union[_WSB, _DSWB]:
        """Start a replace-entire-record write (create or replace).

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If a key list is empty.
            TypeError: If the arguments are not keys or a list of keys.

        Example::

            users = DataSet.of("test", "users")
            session.replace(users.id(1)).put({"name": "Tim"}).execute()

        See Also:
            :meth:`replace_if_exists`: Replace only when the record exists.
            :meth:`upsert`: Merge instead of replace.
        """
        if arg1.__class__ is Key and not keys:
            return self._fast_write_segment("replace", arg1)  # type: ignore[arg-type]
        if isinstance(arg1, DataSet):
            if keys:
                raise TypeError(_keys_need_key_first("replace", arg1))
            return self._dataset_write_builder("replace", arg1)
        return self._build_write_segment("replace", arg1, *keys)  # type: ignore[arg-type]

    def replace_if_exists(
        self,
        arg1: Union[Key, List[Key]],
        /,
        *keys: Key,
    ) -> _WSB:
        """Start a replace-entire-record write that fails if the record is absent.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If a key list is empty.
            TypeError: If the arguments are not keys or a list of keys.

        Example::

            users = DataSet.of("test", "users")
            session.replace_if_exists(users.id(1)).put({"name": "Tim"}).execute()

        See Also:
            :meth:`replace`: Create or replace.
            :meth:`update`: Merge only when the record exists.
        """
        if arg1.__class__ is Key and not keys:
            return self._fast_write_segment("replace_if_exists", arg1)  # type: ignore[arg-type]
        return self._build_write_segment("replace_if_exists", arg1, *keys)

    def delete(
        self,
        arg1: Union[Key, List[Key]],
        /,
        *keys: Key,
    ) -> _WSB:
        """Start a delete for one or more keys.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If a key list is empty.
            TypeError: If the arguments are not keys or a list of keys.

        Example::

            users = DataSet.of("test", "users")
            session.delete(users.id(1)).execute()

        See Also:
            :meth:`upsert`: Create or update the same keys.
        """
        if arg1.__class__ is Key and not keys:
            return self._fast_write_segment("delete", arg1)  # type: ignore[arg-type]
        return self._build_write_segment("delete", arg1, *keys)

    def touch(
        self,
        arg1: Union[Key, List[Key]],
        /,
        *keys: Key,
    ) -> _WSB:
        """Start a touch (bump generation / reset TTL) for one or more keys.

        Key resolution matches :meth:`upsert`.

        Returns:
            A write-segment builder.

        Raises:
            ValueError: If a key list is empty.
            TypeError: If the arguments are not keys or a list of keys.

        Example::

            users = DataSet.of("test", "users")
            session.touch(users.id(1)).execute()

        See Also:
            :meth:`update`: Modify bins as well as metadata.
        """
        if arg1.__class__ is Key and not keys:
            return self._fast_write_segment("touch", arg1)  # type: ignore[arg-type]
        return self._build_write_segment("touch", arg1, *keys)

    def exists(
        self,
        arg1: Union[Key, List[Key]],
        /,
        *keys: Key,
    ) -> _WSB:
        """Start an existence check for one or more keys.

        Key resolution matches :meth:`upsert`. A single key always yields one
        row, reading ``False`` when the record is absent; a batch omits absent
        keys unless ``include_missing_keys()`` is set on the builder.

        Returns:
            A write-segment builder whose result reports presence per key.

        Raises:
            ValueError: If a key list is empty.
            TypeError: If the arguments are not keys or a list of keys.

        Example::

            users = DataSet.of("test", "users")
            present = session.exists(users.id(1)).execute().first().as_bool()

        See Also:
            :meth:`query`: Fetch the record instead of just presence.
        """
        if arg1.__class__ is Key and not keys:
            return self._fast_write_segment("exists", arg1)  # type: ignore[arg-type]
        return self._build_write_segment("exists", arg1, *keys)

    # -- Query factory --------------------------------------------------------

    def query(self, arg1: Union[DataSet, Key, List[Key]], /, *keys: Key) -> _QB:
        """Start a read or secondary-index query for keys or a whole set.

        This session's behavior is applied to the underlying query builder; use
        :meth:`session_for` to run a query under a different one. Supported
        shapes: a :class:`~aerospike_sdk.dataset.DataSet` (set-wide query), a
        single :class:`~aerospike_native.Key`, or multiple keys (varargs or a
        list). Multi-key queries are split into per-node sub-batches, and a node
        whose sub-batch holds a single key is sent a regular single-record
        command automatically — size-1 batches need no special-casing by the
        caller.

        Args:
            arg1: A dataset, a key, or a list of keys.
            *keys: Additional keys when ``arg1`` is a key.

        Returns:
            A query builder to chain ``where``, ``bins``, ``execute``, etc. The
            terminal ``execute()`` is awaited on async sessions and blocking on
            sync sessions.

        Raises:
            TypeError: If the arguments are not a dataset, keys, or a list of keys.
            ValueError: If a key list is empty.

        Example::

            users = DataSet.of("test", "users")
            rs = await session.query(users.id(1)).bins(["name"]).execute()
            row = await rs.first_or_raise()

            # A whole set, or a whole namespace with DataSet.of("test"):
            rs = await session.query(users).where("$.age > 30").execute()

        See Also:
            :meth:`upsert`: Writes for the same keys.
        """
        # The bench / typical-app read shape: skip the isinstance chain below.
        if arg1.__class__ is Key and not keys:
            return self._fast_query_builder(arg1)  # type: ignore[arg-type]

        if isinstance(arg1, Key):
            builder = self._build_query_builder(dataset=None, keys=[arg1, *keys])
        elif isinstance(arg1, str):
            raise TypeError(_dataset_not_strings("query"))
        elif keys:
            raise TypeError(_keys_need_key_first("query", arg1))
        elif isinstance(arg1, DataSet):
            builder = self._build_query_builder(dataset=arg1, keys=None)
        elif isinstance(arg1, list):
            if not arg1:
                raise ValueError("keys list cannot be empty")
            if not isinstance(arg1[0], Key):
                raise TypeError(f"Expected List[Key], got first element {type(arg1[0])}")
            builder = self._build_query_builder(dataset=None, keys=arg1)
        else:
            raise TypeError(f"Expected a DataSet, Key, or List[Key], got {type(arg1)}")
        return self._bind_txn(builder)
