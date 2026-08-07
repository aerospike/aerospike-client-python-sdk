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

"""Runtime-agnostic query chain layer shared by the async and sync builders.

Holds the value types (:class:`QueryHint`, operation specs), the
:class:`_QueryBuilderBase` chain core (state + chaining methods + spec
machinery — no I/O), and the tier-neutral per-bin builders
(:class:`WriteBinBuilder`, :class:`QueryBinBuilder`). Terminal dispatchers
are runtime-bound and live on the leaves: async terminals in
:mod:`aerospike_sdk.aio.operations.query`, blocking dispatch in
:mod:`aerospike_sdk.sync.operations.query_dispatch`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import (
    Protocol,
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
)

from typing_extensions import Self, deprecated

from aerospike_async import (
    BasePolicy,
    BatchDeleteOp,
    BatchDeletePolicy,
    BatchPolicy,
    BatchReadOp,
    BatchReadPolicy,
    BatchUDFPolicy,
    BatchWriteOp,
    BatchWritePolicy,
    BitOperation,
    BitPolicy,
    BitwiseResizeFlags,
    BitWriteFlags,
    CTX,
    Client,
    ExpOperation,
    ExpReadFlags,
    Filter,
    FilterExpression,
    GenerationPolicy,
    GeoJSON,
    HLLWriteFlags,
    HllOperation,
    Key,
    ListOperation,
    ListOrderType,
    ListReturnType,
    ListSortFlags,
    MapOperation,
    MapOrder,
    MapPolicy,
    MapReturnType,
    MapWriteFlags,
    Operation,
    PartitionFilter,
    QueryDuration,
    QueryPolicy,
    ReadPolicy,
    Replica,
    Statement,
    StringNumericType,
    StringOperation,
    StringRegexFlags,
    StringWriteFlags,
    Txn,
    WritePolicy,
)
from aerospike_async.exceptions import ResultCode

try:
    from aerospike_async import QueryWhereFlags
except ImportError:  # pragma: no cover - older PAC without Tier-D flags
    QueryWhereFlags = None  # type: ignore[misc, assignment]

from aerospike_sdk.aio.operations.cdt_read import (
    CdtReadBuilder,
    CdtReadInvertableBuilder,
    _map_item_pairs,
)
from aerospike_sdk.aio.operations.cdt_write import (
    CdtWriteBuilder,
    CdtWriteInvertableBuilder,
    _UNORDERED_LIST_POLICY,
    _resolve_list_policy,
    _resolve_map_policy,
)
from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.operations_shared import (
    NamespaceModeResolver,
    _OP_TYPE_TO_REA,
    _cmd_failed,
    _TTL_DONT_UPDATE,
    _TTL_NEVER_EXPIRE,
    _TTL_SERVER_DEFAULT,
    _WriteSegmentBuilderBase,
    _seconds_from_timedelta,
    _seconds_until,
    _to_expiration,
    _WriteVerbs,
)
from aerospike_sdk.policy.policy_mapper import (
    resolve_durable_delete,
    to_batch_policy,
    to_read_policy,
    to_write_policy,
)
from aerospike_sdk.background_shared import (
    make_background_write_policy,
    reject_unsupported_background_write_ops,
)
from aerospike_sdk.ael.parser import parse_ael_with_index
from aerospike_sdk.ael.server_filter import filter_expression_from_ael_string
from aerospike_sdk.error_strategy import (
    ErrorHandler,
    OnError,
    _ErrorDisposition,
)
from aerospike_sdk.hll_config import HllConfig
from aerospike_sdk.implicit_txn import (
    implicit_txn_enabled,
)
from aerospike_sdk.exceptions import (
    _convert_pac_exception,
    _result_code_to_exception,
)
from aerospike_sdk.policy.behavior_settings import Mode, OpKind, OpShape, Settings
from aerospike_sdk.record_result import RecordResult, batch_records_to_results
from aerospike_sdk.record_stream import RecordStream

if TYPE_CHECKING:
    # Leaf classes, referenced here only in annotations (safe circular
    # type-time import; the leaves import this module at runtime).
    from aerospike_sdk.aio.operations.query import (  # noqa: F401
        QueryBuilder,
        WriteSegmentBuilder,
    )
    from aerospike_sdk.aio.operations.udf import UdfFunctionBuilder  # noqa: F401

log = logging.getLogger(SdkLoggers.QUERY)

_bitwise_and = BitOperation.and_
_bitwise_not = BitOperation.not_
_bitwise_or = BitOperation.or_


def _bit_policy_or_default(policy: Optional[Any]) -> Any:
    if policy is None:
        return BitPolicy(BitWriteFlags.DEFAULT)
    return policy


def _resize_flags_or_default(resize_flags: Optional[Any]) -> Any:
    if resize_flags is None:
        return BitwiseResizeFlags.DEFAULT
    return resize_flags


def _resolve_hll_flags(
    *,
    create_only: bool = False,
    update_only: bool = False,
    no_fail: bool = False,
    allow_fold: bool = False,
) -> int:
    """Compose HLL write flags from individual keyword booleans.

    ``create_only`` and ``update_only`` are mutually exclusive; passing both
    raises :class:`ValueError`. Returns an int bitmask suitable as the
    ``flags`` argument to PAC ``HllOperation.init`` / ``add`` / ``set_union``.
    """
    if create_only and update_only:
        raise ValueError(
            "create_only and update_only are mutually exclusive",
        )
    flags = int(HLLWriteFlags.DEFAULT)
    if create_only:
        flags |= int(HLLWriteFlags.CREATE_ONLY)
    if update_only:
        flags |= int(HLLWriteFlags.UPDATE_ONLY)
    if no_fail:
        flags |= int(HLLWriteFlags.NO_FAIL)
    if allow_fold:
        flags |= int(HLLWriteFlags.ALLOW_FOLD)
    return flags


@dataclass(frozen=True)
class QueryHint:
    """Hint for influencing secondary index selection and query scheduling.

    Provide ``index_name`` as a soft explain hint on the server-led path, or
    ``bin_name`` to skip explain and use legacy client-side index selection.
    ``index_name`` and ``bin_name`` are mutually exclusive.

    On clusters that support field ``44`` query selection (>= 8.1.3),
    ``require_index`` and ``hard_hint`` set Tier-D WHERE flags on explain.

    Example::

        hint = QueryHint(
            index_name="age_idx",
            query_duration=QueryDuration.SHORT,
        )
        stream = await (
            session.query(dataset)
                .where("$.age > 30")
                .with_hint(hint)
                .execute()
        )

    Args:
        index_name: Soft index name hint (field ``21`` on explain).
        bin_name: Legacy path — skip server explain; client picks index by bin.
        query_duration: Override ``expected_duration`` on the query policy.
        require_index: Explain flag — reject primary-index fallback.
        hard_hint: Explain flag — require ``index_name`` to be selected.

    Raises:
        ValueError: If both ``index_name`` and ``bin_name`` are provided, or
            ``hard_hint`` without ``index_name``.

    See Also:
        :meth:`QueryBuilder.with_hint`
    """

    index_name: Optional[str] = None
    bin_name: Optional[str] = None
    query_duration: Optional[QueryDuration] = None
    require_index: bool = False
    hard_hint: bool = False

    def __post_init__(self) -> None:
        if self.index_name is not None and self.bin_name is not None:
            raise ValueError(
                "index_name and bin_name are mutually exclusive; "
                "provide one or neither, not both"
            )
        if self.hard_hint and not self.index_name:
            raise ValueError("hard_hint requires index_name")


@dataclass
class _FilterRecord:
    """Internal: wraps a Filter with optional creation metadata for hint reconstruction."""

    filter: Filter
    method: Optional[str] = None
    identifier: Optional[str] = None
    args: Optional[tuple] = None
    ctx: Optional[List[CTX]] = None

    def rebuild_for_hint(self, hint: QueryHint) -> Filter:
        """Reconstruct this filter with the hint's index_name or bin_name override."""
        if self.method is None or self.args is None:
            raise ValueError(
                "Cannot apply index_name/bin_name hint to a pre-built Filter. "
                "Use Filter.*_by_index() directly or let the PSDK generate the "
                "filter via parse_ael_with_index()."
            )
        if hint.index_name is not None:
            factory = getattr(Filter, f"{self.method}_by_index")
            f = factory(hint.index_name, *self.args)
        elif hint.bin_name is not None:
            factory = getattr(Filter, self.method)
            f = factory(hint.bin_name, *self.args)
        else:
            return self.filter
        if self.ctx:
            f = f.context(self.ctx)
        return f


if TYPE_CHECKING:
    from aerospike_sdk.ael.filter_gen import IndexContext
    from aerospike_sdk.index_monitor import IndexesMonitor
    from aerospike_sdk.policy.behavior import Behavior

@dataclass(slots=True)
class _OperationSpec:
    """A single operation segment in a chained builder.

    Each spec captures the keys,
    accumulated bin operations, projected bins, optional filter
    expression, and the operation type for one segment in a chain.

    ``op_type`` is ``None`` for read/query segments.  For write
    segments it is one of ``"upsert"``, ``"insert"``, ``"update"``,
    ``"replace"``, ``"replace_if_exists"``, ``"delete"``, ``"touch"``,
    or ``"exists"``.
    """

    keys: List[Key]
    operations: List[Any] = field(default_factory=list)
    bins: Optional[List[str]] = None
    filter_expression: Optional[FilterExpression] = None
    op_type: Optional[str] = None
    generation: Optional[int] = None
    ttl_seconds: Optional[int] = None
    durable_delete: Optional[bool] = None
    durable_delete_command_default: Optional[bool] = None
    contains_record_delete_op: bool = False
    udf_package: Optional[str] = None
    udf_function: Optional[str] = None
    udf_args: Optional[List[Any]] = None


class _SupportsAddOperation(Protocol):
    """Structural type for builders that accept arbitrary CDT/op additions
    and can spawn write-segment chains.

    Used as the bound for :data:`_T` in :class:`QueryBinBuilder` /
    :class:`WriteBinBuilder` so mypyc can resolve ``parent.add_operation(...)``
    and ``parent._start_write_verb(...)`` statically. Concrete implementers:
    ``_QueryBuilderBase``, ``_WriteSegmentBuilderBase``.
    """

    def add_operation(self, op: Any) -> Self: ...
    def _start_write_verb(
        self, op_type: str, arg1: Any, *more_keys: Any,
    ) -> "WriteSegmentBuilder": ...


_T = TypeVar("_T", bound=_SupportsAddOperation)


class _QueryBuilderBase:
    """State + chaining + policy factories + blocking IO shared by query builders.

    Holds all the configuration state (filters, bins, partitions, write specs,
    durable-delete flags, namespace/MRT plumbing), the policy factories that
    convert that state into PAC policy objects, and the blocking IO
    dispatchers (``execute_blocking_*``, ``_execute_*_blocking``) — they are
    sync code paths that both concrete subclasses can route to.

    Subclasses:
        - :class:`QueryBuilder` (this file): async ``execute()`` returning
          an awaitable :class:`~aerospike_sdk.record_stream.RecordStream`.
        - :class:`~aerospike_sdk.sync.operations.query.SyncQueryBuilder`:
          sync ``execute()`` dispatching through the inherited blocking
          methods and returning
          :class:`~aerospike_sdk.sync.record_stream.RecordStream`.

    End users never construct this base directly; they get a concrete
    subclass via :meth:`~aerospike_sdk.aio.session.Session.query` or
    :meth:`~aerospike_sdk.sync.session.Session.query`.
    """

    # Op types whose single-key/batch writes require the record to exist
    # (no insert/upsert semantics). The dispatch + error-handling paths
    # consult this to decide whether ``KEY_NOT_FOUND`` is a fatal error or
    # a soft "no-op" outcome.
    _WRITES_REQUIRING_EXISTING_KEY = frozenset({"update", "replace_if_exists"})

    # Class-level defaults for always-virgin-at-init fields. Reads fall
    # through here until a chained method sets the instance attribute,
    # eliminating ~17 attribute writes per `session.query(key)` /
    # `session.upsert(key)` op. The bypass-check chain at the top of
    # `execute()` relies on these defaults being readable; chained
    # methods like `.bins(...)` simply assign instance attrs that
    # shadow the class default.
    _bins: Optional[List[str]] = None
    _with_no_bins: bool = False
    _filter_expression: Optional[FilterExpression] = None
    _query_hint: Optional[QueryHint] = None
    _where_ael: Optional[str] = None
    _index_context: Optional["IndexContext"] = None
    _policy: Optional[QueryPolicy] = None
    _partition_filter: Optional[PartitionFilter] = None
    _chunk_size: Optional[int] = None
    _fail_on_filtered_out: bool = False
    _respond_all_keys: bool = False
    _read_policy: Optional[ReadPolicy] = None
    _op_type: Optional[str] = None
    _generation: Optional[int] = None
    _ttl_seconds: Optional[int] = None
    _durable_delete: Optional[bool] = None
    _durable_delete_command_default: Optional[bool] = None
    _record_delete_in_operations: bool = False
    _default_filter_expression: Optional[FilterExpression] = None
    _default_ttl_seconds: Optional[int] = None
    _udf_package: Optional[str] = None
    _udf_function: Optional[str] = None
    _udf_args: Optional[List[Any]] = None
    _op_projection: Optional[List[Any]] = None
    # Set by with_txn(None): the caller explicitly opted out of any
    # transaction, so the implicit batch-write wrap must not fire either.
    _txn_opted_out: bool = False
    _default_where_ael: Optional[str] = None
    _supports_server_compiled_ael: bool = False
    _supports_query_selection: bool = False
    # Mixed-mode batch state: modes for key namespaces beyond the builder's
    # own (populated only when a batch actually spans namespaces), and
    # whether any key in the batch lands in an SC namespace.
    _batch_namespace_modes: Optional[Dict[str, Mode]] = None
    _batch_any_sc: bool = False

    def __init__(
        self,
        client: Client,
        namespace: str,
        set_name: str,
        behavior: Optional[Behavior] = None,
        indexes_monitor: Optional["IndexesMonitor"] = None,
        cached_read_policy: Optional[ReadPolicy] = None,
        cached_write_policy: Optional[WritePolicy] = None,
        cached_read_policy_sc: Optional[ReadPolicy] = None,
        cached_write_policy_sc: Optional[WritePolicy] = None,
        txn: Optional[Txn] = None,
        namespace_mode_resolver: NamespaceModeResolver = None,
        namespace_mode_resolver_blocking: Optional[Callable[[str], "Mode"]] = None,
        sdk_client: Optional[Any] = None,
        supports_server_compiled_ael: Optional[bool] = None,
        supports_query_selection: Optional[bool] = None,
    ) -> None:
        """
        Initialize a QueryBuilder.

        Args:
            client: The underlying async client.
            namespace: The namespace name.
            set_name: The set name.
            behavior: Optional Behavior for deriving policies.
            indexes_monitor: Optional monitor providing cached index metadata
                for transparent filter generation from AEL expressions.
            cached_read_policy: Pre-computed read policy from the session.
            cached_write_policy: Pre-computed write policy from the session.
            txn: Optional active :class:`~aerospike_async.Txn` captured from
                a transactional session at construction; every policy this
                builder hands to the PAC gets stamped with it. ``None``
                means no transaction participation. Callers rarely pass
                this directly — transactional sessions thread it through
                automatically.
            namespace_mode_resolver: Optional async callable ``namespace -> Mode``
                used to apply AP vs SC behavior scopes before policies are built.
                Session-scoped builders supply this; client-only builders omit it.
            namespace_mode_resolver_blocking: Sync counterpart used by
                :meth:`execute_blocking` (the sync path bypassing asyncio).
            sdk_client: Optional owning SDK client, consulted at execute time
                for SDK-level settings (implicit batch-write transactions)
                and cluster MRT capability. ``None`` disables implicit
                transactions for this builder.
        """
        self._client = client
        self._namespace = namespace
        self._set_name = set_name
        self._behavior = behavior
        self._indexes_monitor = indexes_monitor
        self._namespace_mode_resolver_blocking = namespace_mode_resolver_blocking
        # Mutable-list fields need per-instance copies (cannot live as
        # class defaults — first mutation would leak across instances).
        self._filter_records: List[_FilterRecord] = []
        self._operations: List[Any] = []
        self._specs: List[_OperationSpec] = []
        self._single_key: Optional[Key] = None
        self._keys: Optional[List[Key]] = None
        # All scalar / None / False fields use class-level defaults declared above.
        # Reuse session-cached policies when available; fall back to
        # computing them lazily from the behavior on first use.
        # MRT participation: when set, every policy produced by this
        # builder is stamped via _apply_txn. The cached policies can't be
        # reused under MRT because they were pre-computed without a txn, so
        # we null them out to force re-derivation from behavior.
        self._txn: Optional[Txn] = txn
        self._namespace_mode_resolver = namespace_mode_resolver
        self._namespace_mode: Optional[Mode] = None
        self._sdk_client = sdk_client
        if supports_server_compiled_ael is True:
            self._supports_server_compiled_ael = True
        elif (
            supports_server_compiled_ael is None
            and sdk_client is not None
            and getattr(sdk_client, "supports_server_compiled_ael", False)
        ):
            self._supports_server_compiled_ael = True
        if supports_query_selection is True:
            self._supports_query_selection = True
        elif (
            supports_query_selection is None
            and sdk_client is not None
            and getattr(sdk_client, "supports_query_selection", False)
        ):
            self._supports_query_selection = True
        if txn is None:
            self._base_read_policy: Optional[ReadPolicy] = cached_read_policy
            self._base_write_policy: Optional[WritePolicy] = cached_write_policy
            # Per-mode SC variants for bypass paths that resolve namespace
            # mode at use time and pick the right cached policy.
            self._base_read_policy_sc: Optional[ReadPolicy] = cached_read_policy_sc
            self._base_write_policy_sc: Optional[WritePolicy] = cached_write_policy_sc
        else:
            self._base_read_policy = None
            self._base_write_policy = None
            self._base_read_policy_sc = None
            self._base_write_policy_sc = None

    def _filter_expression_from_ael(self, ael: str) -> FilterExpression:
        return filter_expression_from_ael_string(
            ael,
            supports_server_compiled_ael=self._supports_server_compiled_ael,
        )

    def _resolve_where_filter_expression(self) -> None:
        """Materialize a pending string ``where()`` into ``_filter_expression``."""
        if self._where_ael is not None and self._filter_expression is None:
            self._filter_expression = self._filter_expression_from_ael(self._where_ael)

    def _resolve_default_filter_expression(self) -> None:
        """Materialize a pending string ``default_where()``."""
        if self._default_where_ael is not None and self._default_filter_expression is None:
            self._default_filter_expression = self._filter_expression_from_ael(
                self._default_where_ael,
            )

    def _effective_filter_expression(self) -> Optional[FilterExpression]:
        """Return the active filter, materializing pending AEL strings on demand."""
        # Hot path: spec finalization calls this once per segment. When no
        # string AEL is pending (the common case), answer with attribute
        # reads only — the resolve helpers would each re-check and return.
        if self._where_ael is None:
            if self._default_where_ael is None:
                return self._filter_expression or self._default_filter_expression
            self._resolve_default_filter_expression()
            return self._filter_expression or self._default_filter_expression
        self._resolve_where_filter_expression()
        self._resolve_default_filter_expression()
        return self._filter_expression or self._default_filter_expression

    def _apply_txn(self, policy: Any) -> Any:
        """Stamp this builder's captured txn on an outer policy in place.

        No-op when the builder was constructed outside a transactional
        session. Applied at every policy-construction site so the txn
        propagates uniformly without the caller touching the policy.

        Args:
            policy: A :class:`~aerospike_async.ReadPolicy`,
                :class:`~aerospike_async.WritePolicy`,
                :class:`~aerospike_async.QueryPolicy`, or
                :class:`~aerospike_async.BatchPolicy` (or ``None``).

        Returns:
            The same ``policy`` object, for fluent use.
        """
        if self._txn is not None and policy is not None:
            policy.txn = self._txn
        return policy

    def _implicit_txn_precheck(self, keys: Sequence[Key]) -> bool:
        """Cheap synchronous half of the implicit batch-write txn gate.

        The multi-key write dispatchers are inherently write-bearing, so
        the has-writes condition is implied; this checks SC namespace, no
        explicit txn (and no ``with_txn(None)`` opt-out), the setting, and
        that every key shares the builder's namespace. Callers confirm
        cluster MRT capability afterward — async paths via
        ``await sdk_client._supports_mrt()``, so the coroutine is only
        created once the cheap conditions pass.
        """
        if self._txn_opted_out or not implicit_txn_enabled(
            self._sdk_client, self._txn, self._namespace_mode
        ):
            return False
        # A transaction cannot span namespaces. Wrapping is SDK-initiated,
        # so decline it for a mixed-namespace batch instead of turning a
        # request the server would answer per key into a whole-batch
        # client-side rejection. Scanned last: the checks above reject the
        # overwhelmingly common AP case before we touch the keys.
        namespace = self._namespace
        return all(key.namespace == namespace for key in keys)

    def _implicit_txn_settings(self) -> Any:
        """Live transaction settings from the owning SDK client."""
        return self._sdk_client._sdk_settings.transactions

    def _resolved_namespace_mode(self) -> Mode:
        assert self._namespace_mode is not None
        return self._namespace_mode

    # -- Mixed-mode batch resolution ------------------------------------------
    # A batch may span namespaces whose consistency modes differ (AP vs SC).
    # Mode-scoped settings must resolve per key — durable-delete defaults on
    # SC are the sharp edge — while batch-level knobs resolve with SC
    # escalation: if any key is SC, the parent policy uses SC-scoped settings.

    def _collect_extra_batch_namespaces(self) -> Optional[set]:
        """Distinct key namespaces beyond the builder's own, or ``None``.

        ``None`` (the overwhelmingly common single-namespace case) means no
        further resolution work is needed.
        """
        extra: Optional[set] = None
        namespace = self._namespace
        for spec in self._specs:
            for key in spec.keys:
                ns = key.namespace
                if ns != namespace:
                    if extra is None:
                        extra = set()
                    extra.add(ns)
        return extra

    def _mode_for_namespace(self, namespace: str) -> Mode:
        """Resolved mode for one key's namespace (falls back to the builder's)."""
        modes = self._batch_namespace_modes
        if modes is not None and namespace != self._namespace:
            mode = modes.get(namespace)
            if mode is not None:
                return mode
        return self._resolved_namespace_mode()

    def _resolved_batch_mode(self) -> Mode:
        """Mode for batch-level (parent) policy resolution.

        SC-escalated: when any key in the batch lands in an SC namespace,
        the parent policy resolves with SC-scoped settings, mirroring the
        per-record/parent split the batch wire protocol itself has.
        """
        if self._batch_any_sc:
            return Mode.SC
        return self._resolved_namespace_mode()

    def _spec_modes_mixed(self, spec: "_OperationSpec") -> bool:
        """Whether one spec's keys span both consistency modes."""
        modes = self._batch_namespace_modes
        if modes is None:
            return False
        first: Optional[Mode] = None
        for key in spec.keys:
            mode = self._mode_for_namespace(key.namespace)
            if first is None:
                first = mode
            elif mode is not first:
                return True
        return False

    def _make_batch_policy(
        self, settings: Optional[Any],
    ) -> Optional[BatchPolicy]:
        """Build a BatchPolicy from settings and stamp the captured txn.

        Returns ``None`` when neither a settings bundle nor an active
        transaction is in play (the PAC tolerates a ``None`` batch policy
        in that case). Under MRT, always materializes a policy so the txn
        can ride along.

        Args:
            settings: Settings bundle from behavior (may be ``None``).

        Returns:
            A txn-stamped :class:`~aerospike_async.BatchPolicy`, or
            ``None`` when no policy is needed.
        """
        bp = to_batch_policy(settings) if settings is not None else None
        if self._txn is not None and bp is None:
            bp = BatchPolicy()
        return self._apply_txn(bp)

    def with_txn(self, txn: Optional[Txn]) -> Self:
        """Opt this builder into (or out of) a specific transaction.

        Overrides any transaction captured at construction. Pass ``None`` to
        opt out of an ambient transaction (useful inside a
        :class:`~aerospike_sdk.aio.transactional_session.TransactionalSession`
        when a single operation must run outside the MRT). ``None`` also
        opts a multi-key write out of implicit batch-write transactions
        (:attr:`~aerospike_sdk.policy.system_settings.TransactionSettings.implicit_batch_write_transactions`),
        guaranteeing the operation runs in no transaction at all.

        Args:
            txn: The :class:`~aerospike_async.Txn` to participate in, or
                ``None`` to run without a transaction.

        Returns:
            This builder for method chaining.

        Example::

            async with session.transaction() as tx:
                await tx.upsert(k1).bin("v").set_to(1).execute()
                # Run this one write outside the transaction:
                await tx.upsert(k2).with_txn(None).bin("v").set_to(2).execute()

        See Also:
            :meth:`aerospike_sdk.aio.session.Session.get_current_transaction`
        """
        self._txn = txn
        self._txn_opted_out = txn is None
        # Cached policies were built without this override — drop them so
        # subsequent policy lookups re-derive from behavior with the right
        # txn stamped on.
        self._base_read_policy = None
        self._base_write_policy = None
        return self

    def bins(self, bin_names: List[str]) -> Self:
        """Restrict the read to a non-empty set of bin names.

        Mutually exclusive with :meth:`with_no_bins`.

        Args:
            bin_names: Non-empty list of bin names to return.

        Returns:
            This builder for method chaining.

        Raises:
            ValueError: If ``bin_names`` is empty or :meth:`with_no_bins` was
                already called.

        Example::

            Restrict a query or key read to specific bins::

                stream = await session.query(users.id(1)).bins(["name", "email"]).execute()

        See Also:
            :meth:`with_no_bins`: Metadata-only reads without bin payloads.
            :meth:`bin`: Per-bin operations (CDT, expressions).
        """
        if self._with_no_bins:
            raise ValueError("Cannot specify both 'with_no_bins' and provide a list of bin names")
        if not bin_names:
            raise ValueError("bin_names must not be empty; use with_no_bins() for metadata-only reads")
        self._bins = bin_names
        self._with_no_bins = False
        return self

    def with_op_projection(self, *ops: Any) -> Self:
        """Project query results through one or more read operations.

        The server applies ``ops`` to every matching record and returns the
        operation results in place of the configured bin set. Mutually
        exclusive with :meth:`bins`: when a projection is set the server
        ignores the bin list. Subsequent calls replace the previous
        projection.

        Server compatibility:
            - Servers older than 8.1.2 accept only the basic ``get_bin`` /
              ``get_header`` ops.
            - Server 8.1.2+ also accepts CDT, expression, bit, and HLL
              reads — for example
              ``CdtOperation.select_values("bin", [...])``.

        Args:
            *ops: One or more native ``aerospike_async`` read operations.

        Returns:
            This builder for method chaining.

        Example::

            from aerospike_sdk import CTX, CdtOperation
            stream = await (
                session.query(users)
                    .with_op_projection(
                        CdtOperation.select_values(
                            "inventory", [CTX.map_key("books")]
                        ),
                    )
                    .execute()
            )
        """
        self._op_projection = list(ops) if ops else None
        return self

    def with_no_bins(self) -> Self:
        """
        Specify that no bins should be read (header-only query).
        
        This method is useful when you only need to check for record existence
        or get metadata like generation numbers, without reading the actual data.
        
        This method cannot be used together with bins().
        
        Returns:
            self for method chaining.
            
        Raises:
            ValueError: If used together with bins().
        """
        if self._bins is not None:
            raise ValueError("Cannot specify both 'with_no_bins' and provide a list of bin names")
        self._with_no_bins = True
        self._bins = []
        return self

    def filter(self, filter_obj: Filter) -> Self:
        """Add a secondary index filter to the query.

        Args:
            filter_obj: The filter to add.

        Returns:
            This builder for method chaining.
        """
        self._filter_records.append(_FilterRecord(filter=filter_obj))
        return self

    def filter_expression(self, expression: FilterExpression) -> Self:
        """
        Set a FilterExpression for server-side filtering.

        FilterExpression allows complex server-side filtering that doesn't
        require secondary indexes. This is more efficient than client-side
        filtering as it reduces network traffic and processing.

        Args:
            expression: The FilterExpression to apply.

        Returns:
            self for method chaining.

        Example::

            # Filter by multiple conditions server-side
            filter_exp = FilterExpression.and_([
                FilterExpression.eq(
                    FilterExpression.string_bin("category"),
                    FilterExpression.string_val("Shoes")
                ),
                FilterExpression.eq(
                    FilterExpression.string_bin("usage"),
                    FilterExpression.string_val("Sports")
                )
            ])
            recordset = await session.query("test", "products").filter_expression(filter_exp).execute()

        """
        self._filter_expression = expression
        return self

    @overload
    def where(self, expression: str) -> QueryBuilder: ...

    @overload
    def where(self, expression: FilterExpression) -> QueryBuilder: ...

    def where(
        self,
        expression: Union[str, FilterExpression],
    ) -> Self:
        """Apply a server-side filter for dataset queries or keyed reads that support it.

        String arguments are parsed with the AEL; prefer f-strings for
        dynamic literals. Pass a pre-built :class:`~aerospike_async.FilterExpression`
        when constructing filters programmatically.

        Args:
            expression: AEL string or ``FilterExpression``.

        Returns:
            This builder for chaining.

        Example::

            qb = session.query(ds).where("$.status == 'active'")
            qb = session.query(ds).where(f"$.score > {min_score}")

        See Also:
            :meth:`default_where`: Default filter for chained operations without their own.
            :meth:`filter_expression`: Attach an expression without AEL parsing.
        """
        if isinstance(expression, str):
            self._where_ael = expression
        else:
            self._where_ael = None
            self._filter_expression = expression
        return self

    def with_index_context(self, index_context: "IndexContext") -> Self:
        """Explicitly override the secondary index metadata used for filter generation.

        Most applications do **not** need this method. The client automatically
        discovers and caches secondary index metadata from the cluster in the
        background. Use this only when you need to force a specific index
        context that differs from the live cluster state.

        Args:
            index_context: Index metadata for the query's namespace.

        Returns:
            This builder for method chaining.

        See Also:
            :class:`~aerospike_sdk.ael.filter_gen.IndexContext`
        """
        self._index_context = index_context
        return self

    def with_policy(self, policy: QueryPolicy) -> Self:
        """
        Set the query policy.
        
        Args:
            policy: The query policy to use.
        
        Returns:
            self for method chaining.
        """
        self._policy = policy
        return self

    def with_read_policy(self, policy: ReadPolicy) -> Self:
        """
        Set the read policy (for single key or batch key queries).
        
        Args:
            policy: The read policy to use.
        
        Returns:
            self for method chaining.
        """
        self._read_policy = policy
        return self

    def partition(self, partition_filter: PartitionFilter) -> Self:
        """Restrict a dataset query using a PAC :class:`~aerospike_async.PartitionFilter`.

        Prefer :meth:`on_partition` or :meth:`on_partition_range` for common cases.

        Args:
            partition_filter: Built filter (all partitions, by id, by range, etc.).

        Returns:
            This builder for chaining.

        See Also:
            :meth:`on_partition_range`: Inclusive start, exclusive end partition ids.
        """
        self._partition_filter = partition_filter
        return self

    def on_partition(self, part_id: int) -> Self:
        """
        Target a specific partition for the query.

        This method restricts the query to a single partition. This can be useful
        for load balancing or when you know the data distribution across partitions.

        Args:
            part_id: The partition ID to target (0-4095)

        Returns:
            self for method chaining

        Raises:
            ValueError: If part_id is out of range

        Example::

                query = session.query(dataset).on_partition(5)

        See Also:
            :meth:`on_partition_range`: Target a contiguous span of partitions.
            :meth:`partition`: Apply a pre-built partition filter.
        """
        return self.on_partition_range(part_id, part_id + 1)

    def on_partition_range(self, start_incl: int, end_excl: int) -> Self:
        """
        Target a range of partitions for the query.

        This method restricts the query to a specific range of partitions. This
        can be useful for load balancing, parallel processing, or when you know
        the data distribution across partitions.

        The partition range can only be set once per query. Subsequent calls
        with different ranges will overwrite the previous range.

        Args:
            start_incl: Start partition (inclusive, 0-4095)
            end_excl: End partition (exclusive, 1-4096)

        Returns:
            self for method chaining

        Raises:
            ValueError: If partition range is invalid

        Example::

                # Query partitions 0-2047 (first half)
                query = session.query(dataset).on_partition_range(0, 2048)

                # Query partitions 100-199
                query = session.query(dataset).on_partition_range(100, 200)

        See Also:
            :meth:`on_partition`: Target a single partition.
            :meth:`partition`: Apply a pre-built partition filter.
        """
        # Partition range validation
        if start_incl < 0 or start_incl >= 4096:
            raise ValueError(f"Start partition must be in range 0-4095, not {start_incl}")
        if end_excl < 1 or end_excl > 4096:
            raise ValueError(f"End partition must be in range 1-4096, not {end_excl}")
        if start_incl >= end_excl:
            raise ValueError(
                f"Start partition ({start_incl}) must be < end partition ({end_excl})"
            )

        # PartitionFilter.by_range takes (begin, count), not (begin, end):
        # convert the exclusive end bound into a partition count.
        self._partition_filter = PartitionFilter.by_range(start_incl, end_excl - start_incl)
        return self

    def chunk_size(self, chunk_size: int) -> Self:
        """Tune server-side streaming chunk size (maps to query policy ``max_records`` chunking).

        This method controls how many records are fetched per chunk from the server
        when using server-side streaming. The chunk size affects memory usage and network
        round trips (Larger values reduce round trips; smaller values bound memory per fetch).
        This is distinct from client-side pagination.

        Args:
            chunk_size: Records per chunk; must be positive.

        Returns:
            This builder for chaining.

        Raises:
            ValueError: If ``chunk_size <= 0``.

        Example::

                query = session.query(dataset).chunk_size(100)

        See Also:
            :meth:`max_records`: Cap total records returned.
        """
        if chunk_size <= 0:
            raise ValueError(f"Chunk size must be > 0, not {chunk_size}")
        self._chunk_size = chunk_size
        return self

    def records_per_second(self, rps: int) -> Self:
        """
        Set the maximum records per second for the query.
        
        Args:
            rps: Maximum records per second to process.
        
        Returns:
            self for method chaining.
            
        Example::

                query = session.query(dataset).records_per_second(1000)
        """
        self._ensure_policy().records_per_second = rps
        return self

    def max_records(self, max_records: int) -> Self:
        """
        Set the maximum number of records to return.
        
        Args:
            max_records: Maximum number of records to return.
        
        Returns:
            self for method chaining.
            
        Example::

                query = session.query(dataset).max_records(10000)
        """
        self._ensure_policy().max_records = max_records
        return self

    def limit(self, limit: int) -> Self:
        """
        Set the maximum number of records to return (alias for max_records).
        
        This method is an alias for max_records().
        It limits the total number of records returned by the query.
        Once the limit is reached, the query will stop processing.
        
        Args:
            limit: Maximum number of records to return (must be > 0).
        
        Returns:
            self for method chaining.
            
        Raises:
            ValueError: If limit is <= 0.
            
        Example::

                query = session.query(dataset).limit(100)
        """
        if limit <= 0:
            raise ValueError(f"Limit must be > 0, not {limit}")
        return self.max_records(limit)

    def expected_duration(self, duration: "QueryDuration") -> Self:
        """
        Set the expected duration of the query.
        
        Args:
            duration: Expected duration (QueryDuration.LONG, QueryDuration.SHORT, or QueryDuration.LONG_RELAX_AP).
        
        Returns:
            self for method chaining.
            
        Example::

                from aerospike_async import QueryDuration
                query = session.query(dataset).expected_duration(QueryDuration.SHORT)
        """
        self._ensure_policy().expected_duration = duration
        return self

    def with_hint(self, hint: QueryHint) -> Self:
        """Attach a query hint for secondary index selection or scheduling.

        A hint can redirect which secondary index is used (``index_name``),
        remap the filter to a different bin (``bin_name``), or override the
        expected query duration (``query_duration``).  Only one call to
        ``with_hint`` is allowed per builder.

        Example::

            stream = await (
                session.query(dataset)
                    .filter(Filter.equal("age", 30))
                    .with_hint(QueryHint(index_name="age_idx"))
                    .execute()
            )

        Args:
            hint: A :class:`QueryHint` instance.

        Returns:
            This builder for method chaining.

        Raises:
            ValueError: If ``with_hint`` has already been called on this builder.

        See Also:
            :class:`QueryHint`
        """
        if self._query_hint is not None:
            raise ValueError("with_hint() can only be called once per query builder")
        self._query_hint = hint
        return self

    def replica(self, replica: "Replica") -> Self:
        """
        Set the replica preference for the query.
        
        Args:
            replica: Replica preference. One of ``Replica.MASTER``, ``Replica.MASTER_PROLES``,
                ``Replica.RANDOM``, ``Replica.SEQUENCE``, or ``Replica.PREFER_RACK``.
        
        Returns:
            self for method chaining.
        
        Example::

                from aerospike_async import Replica
                query = session.query(dataset).replica(Replica.SEQUENCE)
        """
        self._ensure_policy().replica = replica
        return self

    def base_policy(self, base_policy: "BasePolicy") -> Self:
        """
        Set the base policy for the query.
        
        Args:
            base_policy: The base policy to use.
        
        Returns:
            self for method chaining.
        
        Example::

                from aerospike_async import BasePolicy
                base = BasePolicy()
                query = session.query(dataset).base_policy(base)
        """
        self._ensure_policy().base_policy = base_policy
        return self

    def fail_on_filtered_out(self) -> Self:
        """Surface rows that fail a filter as ``FILTERED_OUT`` instead of omitting them.

        Applies to key-based reads where a filter excludes the record. Without this
        flag, filtered keys may be absent from the stream depending on policy.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`include_missing_keys`: Include missing-key rows in batch reads.
        """
        self._fail_on_filtered_out = True
        return self

    def include_missing_keys(self) -> Self:
        """Ensure batch/point reads emit one row per requested key, including not-found.

        Missing keys appear as non-OK :class:`~aerospike_sdk.record_result.RecordResult`
        entries (typically ``KEY_NOT_FOUND``) instead of being skipped.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`fail_on_filtered_out`: Filter mismatch vs missing key.
            :meth:`respond_all_keys`: Alias using the underlying client's name.
        """
        self._respond_all_keys = True
        return self

    def respond_all_keys(self) -> Self:
        """Alias for :meth:`include_missing_keys` (the underlying client's ``respondAllKeys`` name).

        Retained for callers familiar with the low-level client's policy name;
        :meth:`include_missing_keys` is the preferred name and identical in behavior.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`include_missing_keys`: Preferred name for this behavior.
        """
        return self.include_missing_keys()

    @overload
    def default_where(self, expression: str) -> QueryBuilder: ...

    @overload
    def default_where(self, expression: FilterExpression) -> QueryBuilder: ...

    def default_where(
        self,
        expression: Union[str, FilterExpression],
    ) -> Self:
        """Set a filter applied to any chained operation that does not call :meth:`where`.

        When a chain contains multiple operations (reads, writes, UDFs), each
        operation inherits this filter unless it supplies its own :meth:`where`.

        Example::

            stream = await (
                session.upsert(k1)
                    .bin("status").set_to("active")
                    .where(f"$.age >= {min_age}")
                .delete(k2, k3)
                .upsert(k4)
                    .bin("flag").set_to(True)
                .default_where("$.active == true")
                .execute()
            )
            # upsert(k1) keeps its own where(); the delete and
            # second upsert inherit default_where.

        Args:
            expression: AEL string or ``FilterExpression``.

        Returns:
            This builder for chaining.

        See Also:
            :meth:`where`: Per-operation filter on the current operation.
        """
        if isinstance(expression, str):
            self._default_where_ael = expression
            self._default_filter_expression = None
        else:
            self._default_where_ael = None
            self._default_filter_expression = expression
        return self

    def default_expire_record_after_seconds(self, seconds: int) -> Self:
        """Set a default TTL applied to chained operations that lack their own.

        Args:
            seconds: Time-to-live in seconds. A positive value sets an explicit
                TTL; the sentinels -1, -2, and 0 select never-expire, no-change,
                and namespace-default respectively. Not range-checked here — a
                value the client cannot represent is rejected when the write is
                built.

        Returns:
            self for method chaining.
        """
        self._default_ttl_seconds = seconds
        return self

    def default_expire_record_after(self, duration: timedelta) -> Self:
        """Set a default TTL using a :class:`datetime.timedelta`.

        Equivalent to :meth:`default_expire_record_after_seconds` with seconds
        derived from ``duration`` — applied to chained operations that lack
        their own TTL. A ``duration`` resolving to -1, -2, or 0 seconds selects
        the corresponding TTL sentinel.

        Args:
            duration: Time-to-live.

        Returns:
            self for method chaining.
        """
        self._default_ttl_seconds = _seconds_from_timedelta(duration)
        return self

    def default_expire_record_at(self, when: datetime) -> Self:
        """Set a default TTL so chained writes expire at an absolute point in time.

        A naive ``when`` is interpreted in local time; pass a timezone-aware
        ``datetime`` for explicit UTC or other zones.

        Args:
            when: Future point at which chained writes should expire.

        Returns:
            self for method chaining.

        Raises:
            ValueError: If ``when`` is not strictly in the future.
        """
        self._default_ttl_seconds = _seconds_until(when)
        return self

    def default_never_expire(self) -> Self:
        """Set the default TTL to never expire (TTL = -1)."""
        self._default_ttl_seconds = _TTL_NEVER_EXPIRE
        return self

    def default_with_no_change_in_expiration(self) -> Self:
        """Set the default to preserve each record's existing TTL (TTL = -2)."""
        self._default_ttl_seconds = _TTL_DONT_UPDATE
        return self

    def default_expiry_from_server_default(self) -> Self:
        """Set the default TTL to the namespace's server default (TTL = 0)."""
        self._default_ttl_seconds = _TTL_SERVER_DEFAULT
        return self

    def _ensure_policy(self) -> QueryPolicy:
        """Return the existing policy or create a default one."""
        if self._policy is None:
            self._policy = self._apply_txn(QueryPolicy())
        return self._policy

    def _set_current_keys(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> None:
        """Parse key argument(s) and set ``_single_key`` or ``_keys``."""
        if isinstance(arg1, list):
            if not arg1:
                raise ValueError("keys list cannot be empty")
            self._keys = list(arg1) + list(more_keys) if more_keys else arg1
        elif isinstance(arg1, Key):
            if more_keys:
                self._keys = [arg1, *more_keys]
            else:
                self._single_key = arg1
        else:
            raise TypeError(f"requires a Key or List[Key], got {type(arg1).__name__}")

    def _finalize_current_spec(self) -> None:
        """Package the current key/ops/bins/filter/op_type state into an _OperationSpec."""
        if self._single_key is not None:
            keys = [self._single_key]
        elif self._keys is not None:
            keys = self._keys
        else:
            return

        # Inline the no-AEL fast path: this runs once per segment, and the
        # resolver chain is only needed when a string ``where()`` is pending.
        filt = self._filter_expression
        if filt is None:
            if self._where_ael is None and self._default_where_ael is None:
                filt = self._default_filter_expression
            else:
                filt = self._effective_filter_expression()
        ttl = self._ttl_seconds if self._ttl_seconds is not None else self._default_ttl_seconds

        # Hand off the current operations list directly; allocate a fresh
        # one for the next spec instead of copying.
        self._specs.append(_OperationSpec(
            keys=keys,
            operations=self._operations,
            bins=self._bins,
            filter_expression=filt,
            op_type=self._op_type,
            generation=self._generation,
            ttl_seconds=ttl,
            durable_delete=self._durable_delete,
            durable_delete_command_default=self._durable_delete_command_default,
            contains_record_delete_op=self._record_delete_in_operations,
            udf_package=None,
            udf_function=None,
            udf_args=None,
        ))

        self._single_key = None
        self._keys = None
        self._operations = []
        self._bins = None
        self._with_no_bins = False
        self._filter_expression = None
        self._where_ael = None
        self._op_type = None
        self._generation = None
        self._ttl_seconds = None
        self._durable_delete = None
        self._durable_delete_command_default = None
        self._record_delete_in_operations = False

    def _set_current_keys_from_varargs(self, keys: tuple[Key, ...]) -> None:
        if len(keys) == 1:
            self._single_key = keys[0]
            self._keys = None
        else:
            self._keys = list(keys)
            self._single_key = None

    def _clear_pending_udf_state(self) -> None:
        self._udf_package = None
        self._udf_function = None
        self._udf_args = None

    def _finalize_udf_spec(self) -> None:
        if self._udf_function is None:
            return
        if self._udf_package is None:
            raise ValueError("UDF package name is required")
        if self._single_key is not None:
            keys: List[Key] = [self._single_key]
        elif self._keys is not None:
            keys = list(self._keys)
        else:
            return
        filt = self._effective_filter_expression()
        udf_args: Optional[List[Any]] = (
            list(self._udf_args) if self._udf_args is not None else None
        )
        self._specs.append(_OperationSpec(
            keys=keys,
            operations=[],
            bins=None,
            filter_expression=filt,
            op_type="udf",
            generation=None,
            ttl_seconds=None,
            durable_delete=self._durable_delete,
            durable_delete_command_default=self._durable_delete_command_default,
            contains_record_delete_op=False,
            udf_package=self._udf_package,
            udf_function=self._udf_function,
            udf_args=udf_args,
        ))
        self._single_key = None
        self._keys = None
        self._operations = []
        self._bins = None
        self._with_no_bins = False
        self._filter_expression = None
        self._op_type = None
        self._generation = None
        self._ttl_seconds = None
        self._durable_delete = None
        self._durable_delete_command_default = None
        self._record_delete_in_operations = False
        self._clear_pending_udf_state()

    def _specs_require_sequential_run(self) -> bool:
        return any(spec.op_type == "udf" for spec in self._specs)

    def _make_batch_udf_policy(
        self, spec: _OperationSpec, mode: Optional[Mode] = None,
    ) -> Optional[BatchUDFPolicy]:
        settings = (
            self._behavior.get_settings(
                OpKind.WRITE_NON_RETRYABLE,
                OpShape.BATCH,
                mode if mode is not None else self._resolved_namespace_mode(),
            )
            if self._behavior is not None else None
        )
        eff = resolve_durable_delete(
            settings.durable_delete if settings is not None else None,
            spec.durable_delete_command_default,
            spec.durable_delete,
        )
        has_settings = (
            spec.filter_expression is not None
            or spec.durable_delete is not None
            or spec.durable_delete_command_default is not None
            or eff
        )
        if not has_settings:
            return None
        up = BatchUDFPolicy()
        if spec.filter_expression is not None:
            up.filter_expression = spec.filter_expression
        up.durable_delete = eff
        return up

    @staticmethod
    def _should_include_result(
        result_code: ResultCode,
        respond_all_keys: bool,
        fail_on_filtered_out: bool,
    ) -> bool:
        """Decide whether to include a result in the stream.

        Decides whether to include a per-key result in the stream.
        """
        if result_code == ResultCode.OK:
            return True
        if result_code == ResultCode.KEY_NOT_FOUND_ERROR:
            return respond_all_keys
        if result_code == ResultCode.FILTERED_OUT:
            return fail_on_filtered_out or respond_all_keys
        return True

    def _filtered_batch_list(
        self,
        batch_records,
        disp: _ErrorDisposition = _ErrorDisposition.IN_STREAM,
        handler: ErrorHandler | None = None,
        op_type: Optional[str] = None,
    ) -> List[RecordResult]:
        """Filter batch records by disposition; return as a plain list.

        Same disposition semantics as :meth:`_filtered_batch_stream` (THROW
        raises on the first error, HANDLER dispatches to the callback and
        omits the entry, IN_STREAM includes errors). Returns a list so
        blocking-dispatch paths (sync collapse onto ``_blocking``) can
        skip the :class:`RecordStream` wrapping.
        """
        all_results = batch_records_to_results(list(batch_records))
        filtered: list[RecordResult] = []
        for r in all_results:
            if not r.is_ok and self._is_actionable(r.result_code, op_type):
                if disp is _ErrorDisposition.THROW:
                    raise _result_code_to_exception(r.result_code, str(r.result_code), r.in_doubt)
                if disp is _ErrorDisposition.HANDLER and handler is not None:
                    handler(r.key, r.index, _result_code_to_exception(
                        r.result_code, str(r.result_code), r.in_doubt))
                    continue

            if not self._should_include_result(
                r.result_code, self._respond_all_keys, self._fail_on_filtered_out
            ):
                continue

            filtered.append(r)
        return filtered

    def _filtered_batch_stream(
        self,
        batch_records,
        disp: _ErrorDisposition = _ErrorDisposition.IN_STREAM,
        handler: ErrorHandler | None = None,
        op_type: Optional[str] = None,
    ) -> RecordStream:
        """Convert batch records to a filtered RecordStream.

        Thin wrapper over :meth:`_filtered_batch_list` for async callers
        that hand the result back to streaming code.
        """
        return RecordStream.from_list(
            self._filtered_batch_list(batch_records, disp, handler, op_type),
        )

    def _is_actionable(self, rc: ResultCode, op_type: Optional[str]) -> bool:
        """Whether *rc* should be routed through disposition logic.

        ``KEY_NOT_FOUND_ERROR`` is only actionable when the operation
        explicitly requires an existing record (update, replace_if_exists).
        ``FILTERED_OUT`` is only actionable when ``fail_on_filtered_out``
        has been set.  All other non-OK codes are always actionable.
        """
        if rc == ResultCode.KEY_NOT_FOUND_ERROR:
            return op_type in self._WRITES_REQUIRING_EXISTING_KEY
        if rc == ResultCode.FILTERED_OUT:
            return self._fail_on_filtered_out
        return True

    def _handle_error(
        self,
        key: Key,
        exc: Exception,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
        index: int = 0,
        op_type: Optional[str] = None,
    ) -> RecordStream:
        """Route a per-key error according to the resolved disposition.

        The PAC raises ``ServerError`` for ``KEY_NOT_FOUND_ERROR`` and
        ``FILTERED_OUT`` rather than returning a sentinel. Whether these
        codes are routed through disposition depends on the operation
        context (see ``_is_actionable``).
        """
        pfc_exc = _convert_pac_exception(exc)
        rc = pfc_exc.result_code or ResultCode.OK
        in_doubt = pfc_exc.in_doubt
        _cmd_failed(op_type, rc, pfc_exc, self._client)

        if self._is_actionable(rc, op_type):
            if disp is _ErrorDisposition.THROW:
                raise pfc_exc from exc
            if disp is _ErrorDisposition.HANDLER and handler is not None:
                handler(key, index, pfc_exc)
                return RecordStream.from_list([])

        if not self._should_include_result(rc, self._respond_all_keys, self._fail_on_filtered_out):
            return RecordStream.from_list([])

        return RecordStream.from_error(key, rc, in_doubt, exception=pfc_exc)

    def _handle_batch_error_list(
        self,
        keys: List[Key],
        exc: Exception,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> List[RecordResult]:
        """List version of :meth:`_handle_batch_error` for blocking paths.

        Raises (THROW), dispatches and returns ``[]`` (HANDLER), or returns
        one error :class:`RecordResult` per key (IN_STREAM).
        """
        pfc_exc = _convert_pac_exception(exc)
        rc = pfc_exc.result_code or ResultCode.OK
        in_doubt = pfc_exc.in_doubt
        _cmd_failed("batch", rc, pfc_exc, self._client)

        if disp is _ErrorDisposition.THROW:
            raise pfc_exc from exc

        if disp is _ErrorDisposition.HANDLER and handler is not None:
            for i, key in enumerate(keys):
                handler(key, i, pfc_exc)
            return []

        return [
            RecordResult(
                key=key, record=None, result_code=rc,
                in_doubt=in_doubt, index=i, exception=pfc_exc,
            )
            for i, key in enumerate(keys)
        ]

    def _handle_batch_error(
        self,
        keys: List[Key],
        exc: Exception,
        disp: _ErrorDisposition,
        handler: ErrorHandler | None,
    ) -> RecordStream:
        """Route a batch-level error according to the resolved disposition.

        When the entire batch call fails (e.g. timeout, connection error),
        we create one error result per key.
        """
        return RecordStream.from_list(
            self._handle_batch_error_list(keys, exc, disp, handler),
        )

    def _make_read_policy(
        self, spec: _OperationSpec,
    ) -> ReadPolicy:
        """Build a ``ReadPolicy`` for single-key reads."""
        if self._read_policy is not None:
            rp = self._read_policy
        elif self._behavior is not None:
            if self._base_read_policy is None:
                self._base_read_policy = self._apply_txn(to_read_policy(
                    self._behavior.get_settings(
                        OpKind.READ, OpShape.POINT, self._resolved_namespace_mode())))
            if spec.filter_expression is None:
                return self._base_read_policy
            rp = self._apply_txn(to_read_policy(
                self._behavior.get_settings(
                    OpKind.READ, OpShape.POINT, self._resolved_namespace_mode())))
        else:
            rp = self._apply_txn(ReadPolicy())
        if spec.filter_expression is not None:
            rp.filter_expression = spec.filter_expression
        return rp

    def _make_write_policy(self, spec: _OperationSpec) -> WritePolicy:
        """Build a ``WritePolicy`` for single-key writes."""
        op_type = spec.op_type or "upsert"
        rea = _OP_TYPE_TO_REA.get(op_type)
        settings = (
            self._behavior.get_settings(
                OpKind.WRITE_NON_RETRYABLE, OpShape.POINT,
                self._resolved_namespace_mode(),
            )
            if self._behavior is not None else None
        )
        applies_dd = op_type == "delete" or spec.contains_record_delete_op
        effective_dd = self._effective_point_durable_delete(spec, settings)

        if self._behavior is not None:
            if self._base_write_policy is None:
                base_settings = settings
                if base_settings is not None and base_settings.durable_delete:
                    base_settings = Settings.merge(
                        base_settings, Settings(durable_delete=False))
                self._base_write_policy = self._apply_txn(to_write_policy(
                    base_settings) if base_settings is not None else WritePolicy())
            if (
                rea is None
                and spec.filter_expression is None
                and spec.generation is None
                and spec.ttl_seconds is None
                and spec.durable_delete is None
                and spec.durable_delete_command_default is None
                and not spec.contains_record_delete_op
                and not applies_dd
            ):
                return self._base_write_policy
            wp = self._apply_txn(to_write_policy(
                settings) if settings is not None else WritePolicy())
        else:
            wp = self._apply_txn(WritePolicy())
        if rea is not None:
            wp.record_exists_action = rea
        if spec.filter_expression is not None:
            wp.filter_expression = spec.filter_expression
        if spec.generation is not None:
            wp.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
            wp.generation = spec.generation
        if spec.ttl_seconds is not None:
            wp.expiration = _to_expiration(spec.ttl_seconds)
        if applies_dd:
            wp.durable_delete = effective_dd
        else:
            wp.durable_delete = False
        return wp

    def _make_batch_write_policy(
        self, spec: _OperationSpec, mode: Optional[Mode] = None,
    ) -> Optional[BatchWritePolicy]:
        """Build a ``BatchWritePolicy`` for multi-key batch writes.

        Carries ``record_exists_action`` so write verbs enforce record
        existence on the wire for the buffered single-spec path exactly as
        the mixed-spec and streaming paths do. *mode* scopes the
        durable-delete default to the row's namespace mode.
        """
        rea = _OP_TYPE_TO_REA.get(spec.op_type or "upsert")
        eff = (
            self._batch_write_effective_dd(spec, mode)
            if spec.contains_record_delete_op else False
        )
        has_settings = (
            rea is not None
            or spec.filter_expression is not None
            or spec.generation is not None
            or spec.ttl_seconds is not None
            or spec.durable_delete is not None
            or spec.durable_delete_command_default is not None
            or (spec.contains_record_delete_op and (
                eff
                or spec.durable_delete is not None
                or spec.durable_delete_command_default is not None
            ))
        )
        if not has_settings:
            return None
        bwp = BatchWritePolicy()
        if rea is not None:
            bwp.record_exists_action = rea
        if spec.filter_expression is not None:
            bwp.filter_expression = spec.filter_expression
        if spec.generation is not None:
            bwp.generation = spec.generation
        if spec.ttl_seconds is not None:
            bwp.expiration = _to_expiration(spec.ttl_seconds)
        if spec.contains_record_delete_op:
            bwp.durable_delete = eff
        return bwp

    def _resolve_index_context(self) -> None:
        """Auto-populate ``_index_context`` from the monitor when not set.

        The monitor's cached :class:`IndexContext` is at namespace granularity
        and has no ``query_set``. We derive a per-query copy with
        ``query_set=self._set_name`` so filter selection rejects indexes
        defined on a different set; cross-set indexes (those without a
        set name) remain eligible.
        """
        if self._index_context is not None:
            return
        if self._indexes_monitor is None:
            return
        ctx = self._indexes_monitor.get_index_context(self._namespace)
        if ctx is None:
            return
        if self._set_name and ctx.query_set != self._set_name:
            from aerospike_sdk.ael.filter_gen import IndexContext as _IndexContext
            ctx = _IndexContext.with_query_set(ctx.namespace, self._set_name, ctx.indexes)
        self._index_context = ctx

    def _dataset_set_name(self) -> Optional[str]:
        return self._set_name or None

    def _query_explain_index_hint(self, hint: Optional[QueryHint]) -> Optional[str]:
        if hint is None:
            return None
        return hint.index_name

    def _query_explain_where_flags(self, hint: Optional[QueryHint]) -> Optional[int]:
        if hint is None or QueryWhereFlags is None:
            return None
        flags = QueryWhereFlags.EXPLAIN
        if hint.require_index:
            flags |= QueryWhereFlags.REQUIRE_INDEX
        if hint.hard_hint:
            flags |= QueryWhereFlags.HARD_HINT
        if flags == QueryWhereFlags.EXPLAIN:
            return None
        return int(flags)

    def _raise_if_filtered_out_plan(self, plan: Any) -> None:
        """Phase-1 plan with no matching records; do not run execute."""
        if plan.is_filtered_out:
            raise _result_code_to_exception(
                ResultCode.FILTERED_OUT,
                "Query plan filtered out by server",
            )

    def _use_server_query_selection(self, hint: Optional[QueryHint]) -> bool:
        """Route string-AEL dataset queries through PAC explain→execute (field 44)."""
        if self._where_ael is None:
            return False
        if self._filter_records:
            return False
        if hint is not None and hint.bin_name is not None:
            return False
        return self._supports_query_selection

    def _apply_dataset_query_policy_filter(
        self,
        policy: QueryPolicy,
        *,
        use_server_query_selection: bool,
    ) -> None:
        if use_server_query_selection:
            return
        self._resolve_where_filter_expression()
        if self._filter_expression is not None:
            policy.filter_expression = self._filter_expression

    def _prepare_dataset_query_index_context(
        self,
        *,
        use_server_query_selection: bool,
    ) -> None:
        if self._where_ael is None or self._indexes_monitor is None:
            return
        if use_server_query_selection:
            return
        self._indexes_monitor.start(self._client)

    async def _wait_for_dataset_query_index_context(
        self,
        *,
        use_server_query_selection: bool,
    ) -> None:
        if self._where_ael is None or self._indexes_monitor is None:
            return
        if use_server_query_selection:
            return
        await asyncio.to_thread(self._indexes_monitor.wait_until_ready)

    def _wait_for_dataset_query_index_context_blocking(
        self,
        *,
        use_server_query_selection: bool,
    ) -> None:
        if self._where_ael is None or self._indexes_monitor is None:
            return
        if use_server_query_selection:
            return
        self._indexes_monitor.wait_until_ready()

    def _maybe_auto_generate_filters(
        self,
        hint: Optional[QueryHint],
        policy: QueryPolicy,
        *,
        use_server_query_selection: bool,
    ) -> None:
        if self._where_ael is None or self._index_context is None:
            return
        if use_server_query_selection:
            return
        self._auto_generate_filters(hint, policy)

    async def _run_dataset_query_async(
        self,
        policy: QueryPolicy,
        partition_filter: PartitionFilter,
        hint: Optional[QueryHint],
        statement: Statement,
        *,
        use_server_query_selection: bool,
    ) -> tuple[Any, Any | None]:
        """Run dataset query; returns (recordset, plan) when server selection was used."""
        if not use_server_query_selection:
            recordset = await self._client.query(
                statement, partition_filter, policy=policy,
            )
            return recordset, None

        assert self._where_ael is not None
        plan = await self._client.query_explain(
            self._namespace,
            self._where_ael,
            set_name=self._dataset_set_name(),
            index_name_hint=self._query_explain_index_hint(hint),
            explain_where_flags=self._query_explain_where_flags(hint),
            policy=policy,
        )
        log.debug(
            "Server query selection: explain→execute for %s.%s selection=%s index=%s",
            self._namespace,
            self._set_name,
            plan.selection,
            plan.index_name,
        )
        self._raise_if_filtered_out_plan(plan)
        recordset = await self._client.query_with_plan(
            statement, partition_filter, plan, policy=policy,
        )
        return recordset, plan

    def _run_dataset_query_blocking(
        self,
        policy: QueryPolicy,
        partition_filter: PartitionFilter,
        hint: Optional[QueryHint],
        statement: Statement,
        *,
        use_server_query_selection: bool,
    ) -> tuple[Any, Any | None]:
        if not use_server_query_selection:
            recordset = self._client.query_blocking(
                statement, partition_filter, policy=policy,
            )
            return recordset, None

        assert self._where_ael is not None
        plan = self._client.query_explain_blocking(
            self._namespace,
            self._where_ael,
            set_name=self._dataset_set_name(),
            index_name_hint=self._query_explain_index_hint(hint),
            explain_where_flags=self._query_explain_where_flags(hint),
            policy=policy,
        )
        log.debug(
            "Server query selection: explain→execute for %s.%s selection=%s index=%s",
            self._namespace,
            self._set_name,
            plan.selection,
            plan.index_name,
        )
        self._raise_if_filtered_out_plan(plan)
        recordset = self._client.query_with_plan_blocking(
            statement, partition_filter, plan, policy=policy,
        )
        return recordset, plan

    def _auto_generate_filters(
        self,
        hint: Optional[QueryHint],
        policy: QueryPolicy,
    ) -> None:
        """Parse AEL with index context to generate Filter + Exp.

        When a hint provides ``index_name`` or ``bin_name``, those overrides
        are forwarded to the filter generation pipeline.
        """
        if self._where_ael is None or self._index_context is None:
            return

        hint_index = hint.index_name if hint is not None else None
        hint_bin = hint.bin_name if hint is not None else None

        result = parse_ael_with_index(
            self._where_ael,
            self._index_context,
            hint_index_name=hint_index,
            hint_bin_name=hint_bin,
        )
        if result.filter is not None:
            self._filter_records.append(_FilterRecord(filter=result.filter))
            log.debug(
                "Auto-selected secondary index filter for query on %s.%s",
                self._namespace,
                self._set_name,
            )
        if result.exp is not None:
            policy.filter_expression = result.exp
    def _batch_policy_for(
        self, op_kind: "OpKind", op_shape: "OpShape",
    ) -> Optional[BatchPolicy]:
        """Shorthand: :meth:`_make_batch_policy` keyed off behavior settings.

        Resolves with :meth:`_resolved_batch_mode` — SC-escalated when the
        batch spans an SC namespace — since the parent policy applies to
        every key in the batch.
        """
        settings = (
            self._behavior.get_settings(
                op_kind, op_shape, self._resolved_batch_mode())
            if self._behavior is not None else None
        )
        return self._make_batch_policy(settings)

    def bin(self, bin_name: str) -> QueryBinBuilder[QueryBuilder]:
        """Start a bin-level read operation.

        Returns a :class:`QueryBinBuilder` for specifying how to read from
        the named bin (simple get, CDT navigation, or expression read).

        Args:
            bin_name: The bin to operate on.

        Returns:
            A QueryBinBuilder for method chaining.

        Example::

            rs = await (
                session.query(users.id(1))
                .bin("settings").on_map_key("theme").get_values()
                .bin("age").get()
                .execute()
            )
        """
        return QueryBinBuilder(cast("QueryBuilder", self), bin_name)

    def add_operation(self, op: Any) -> Self:
        """Append a read operation. Returns ``self`` so calls can chain."""
        self._operations.append(op)
        return self

    def with_write_operations(
        self, operations: Sequence[Any],
    ) -> Self:
        """Attach scalar write operations for a background dataset task.

        Prefer :meth:`aerospike_sdk.aio.session.Session.background_task` for
        chained bin writes. Use with :meth:`execute_background_task` on a dataset
        query (no keys).
        Only ``Operation`` and ``ExpOperation.write``-style writes are valid;
        list, map, bit, and HLL operations are rejected before calling the client.

        Args:
            operations: Sequence of write operations (e.g. ``Operation.put``,
                ``Operation.touch``).

        Returns:
            self for method chaining.
        """
        self._operations.extend(operations)
        return self

    def query(
        self,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> Self:
        """Chain another query with new key(s) for batch/point stacking.

        Finalizes the current query segment and begins a new one with
        the given key(s).  Each segment can have its own bins, operations,
        and filter expression.  Dataset (index) queries cannot be stacked.

        Args:
            arg1: A single :class:`Key` or a ``List[Key]``.
            *more_keys: Additional keys (varargs).

        Returns:
            ``self`` for method chaining.

        Raises:
            ValueError: If the current query is a dataset query (no keys).

        Example::

            rs = await (
                session.query(users.ids(1, 2, 3))
                .bin("map").get()
                .query(users.ids(4, 5, 6))
                .bin("name").get()
                .execute()
            )
        """
        if self._single_key is None and self._keys is None and not self._specs:
            raise ValueError(
                "Dataset (index) queries cannot be stacked. "
                "Query stacking is only supported for key-based queries."
            )

        self._finalize_current_spec()
        self._op_type = None
        self._set_current_keys(arg1, *more_keys)
        return self

    def execute_udf(self, *keys: Key) -> "UdfFunctionBuilder":
        """Finalize the current segment and chain a UDF execution on *keys*.

        The current read or write segment is packaged as-is; the returned
        builder targets the new key(s), and the whole chain still executes
        as one batch when the terminal ``execute()`` runs. Call
        ``function(package, name)`` next.

        Args:
            *keys: One or more :class:`~aerospike_async.Key` targets for the
                UDF segment.

        Returns:
            A ``UdfFunctionBuilder`` — call ``function`` next.

        Raises:
            ValueError: If no keys are provided, or if the current query is a
                dataset (index) query, which cannot be stacked.

        Example::

            rs = await (
                session.query(users.id(1))
                .bin("name").get()
                .execute_udf(users.id(2))
                .function("my_module", "my_fn")
                .execute()
            )

        See Also:
            :meth:`~aerospike_sdk.aio.session.Session.execute_udf`: Start a
                chain with a UDF instead.
        """
        if not keys:
            raise ValueError("At least one key is required")
        if self._single_key is None and self._keys is None and not self._specs:
            raise ValueError(
                "Dataset (index) queries cannot be stacked. "
                "Query stacking is only supported for key-based queries."
            )
        self._finalize_current_spec()
        self._set_current_keys_from_varargs(keys)
        return self._udf_function_builder_cls(self)

    # Bound by the async leaf module to its write-segment class (the sync
    # leaf overrides `_start_write_verb` outright, constructing its own
    # segment type). Same pattern as
    # ``_WriteSegmentBuilderBase._bin_builder_cls``.
    _write_segment_cls: type

    # Bound by each tier's udf module to its UdfFunctionBuilder class, so
    # the `execute_udf` chain transition stays runtime-agnostic here while
    # returning the right tier's builder. Same pattern as
    # ``_write_segment_cls``.
    _udf_function_builder_cls: type

    def _start_write_segment(
        self,
        op_type: str,
        arg1: Union[Key, List[Key]],
        *more_keys: Key,
    ) -> "WriteSegmentBuilder":
        """Finalize current spec, set up a write segment, return builder."""
        self._finalize_current_spec()
        self._op_type = op_type
        self._set_current_keys(arg1, *more_keys)
        return self._write_segment_cls(self)

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        return self._start_write_segment(op_type, arg1, *more_keys)

    def _build_statement(self) -> Statement:
        """Build a Statement object from the builder configuration."""
        bins = self._bins
        statement = Statement(self._namespace, self._set_name, bins)
        if self._filter_records:
            hint = self._query_hint
            needs_rebuild = hint is not None and (
                hint.index_name is not None or hint.bin_name is not None
            )
            filters = []
            for rec in self._filter_records:
                if needs_rebuild and hint is not None and rec.method is not None:
                    filters.append(rec.rebuild_for_hint(hint))
                else:
                    filters.append(rec.filter)
            statement.filters = filters
        if self._op_projection is not None:
            statement.set_operations(self._op_projection)
        return statement

    @staticmethod
    def _reject_unsupported_background_write_ops(
        operations: Sequence[Any],
    ) -> None:
        reject_unsupported_background_write_ops(operations)

    def _make_background_write_policy(self) -> WritePolicy:
        return make_background_write_policy(
            self._behavior,
            self._filter_expression,
            None,
            None,
            namespace_mode=self._resolved_namespace_mode(),
        )

    def _make_udf_write_policy(self, spec: _OperationSpec) -> WritePolicy:
        settings = None
        if self._behavior is not None:
            settings = self._behavior.get_settings(
                OpKind.WRITE_NON_RETRYABLE,
                OpShape.POINT,
                self._resolved_namespace_mode(),
            )
            wp = to_write_policy(settings)
        else:
            wp = WritePolicy()
        self._apply_txn(wp)
        wp.durable_delete = resolve_durable_delete(
            settings.durable_delete if settings is not None else None,
            spec.durable_delete_command_default,
            spec.durable_delete,
        )
        if spec.filter_expression is not None:
            wp.filter_expression = spec.filter_expression
        return wp

    def _effective_point_durable_delete(
        self, spec: _OperationSpec, settings: Optional[Settings],
    ) -> bool:
        if spec.op_type == "touch":
            return False
        setting_dd = settings.durable_delete if settings is not None else None
        return resolve_durable_delete(
            setting_dd,
            spec.durable_delete_command_default,
            spec.durable_delete,
        )

    def _batch_write_effective_dd(
        self, spec: _OperationSpec, mode: Optional[Mode] = None,
    ) -> bool:
        """Effective durable-delete for one batch row.

        *mode* is the row's namespace mode; ``None`` falls back to the
        builder's resolved mode (single-namespace batches).
        """
        if self._behavior is None:
            return resolve_durable_delete(
                None,
                spec.durable_delete_command_default,
                spec.durable_delete,
            )
        bset = self._behavior.get_settings(
            OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH,
            mode if mode is not None else self._resolved_namespace_mode(),
        )
        return resolve_durable_delete(
            bset.durable_delete,
            spec.durable_delete_command_default,
            spec.durable_delete,
        )

    def _make_batch_delete_policy(
        self, spec: _OperationSpec, mode: Optional[Mode] = None,
    ) -> Optional[BatchDeletePolicy]:
        """Build a ``BatchDeletePolicy`` for multi-key batch deletes.

        *mode* scopes the durable-delete default to the row's namespace mode.
        """
        eff = self._batch_write_effective_dd(spec, mode)
        has_settings = (
            spec.filter_expression is not None
            or spec.generation is not None
            or spec.durable_delete is not None
            or spec.durable_delete_command_default is not None
            or eff
        )
        if not has_settings:
            return None
        bdp = BatchDeletePolicy()
        if spec.filter_expression is not None:
            bdp.filter_expression = spec.filter_expression
        if spec.generation is not None:
            bdp.generation = spec.generation
        bdp.durable_delete = eff
        return bdp

    def _spec_to_batch_ops(
        self, spec: _OperationSpec,
    ) -> list:
        """Convert one spec into a list of ``BatchReadOp`` / ``BatchWriteOp``
        / ``BatchDeleteOp`` objects for the PAC mixed-batch API.

        Write-family row policies are mode-scoped (SC defaults durable
        delete), so when the batch spans namespaces the policy is resolved
        per row's mode — at most one policy object per mode per spec. The
        single-namespace case keeps the one-policy-shared-by-all-rows shape.
        """
        ops: list = []
        op_type = spec.op_type
        mixed = self._batch_namespace_modes is not None

        if op_type is None:
            brp = self._make_batch_read_policy(spec)
            for key in spec.keys:
                if spec.operations:
                    ops.append(BatchReadOp(key, operations=list(spec.operations), policy=brp))
                else:
                    ops.append(BatchReadOp(key, bins=spec.bins, policy=brp))
        elif op_type == "delete":
            if mixed:
                per_mode = {
                    mode: self._make_batch_delete_policy(spec, mode)
                    for mode in (Mode.AP, Mode.SC)
                }
                for key in spec.keys:
                    bdp = per_mode[self._mode_for_namespace(key.namespace)]
                    ops.append(BatchDeleteOp(key, policy=bdp))
            else:
                bdp = self._make_batch_delete_policy(spec)
                for key in spec.keys:
                    ops.append(BatchDeleteOp(key, policy=bdp))
        elif op_type == "touch":
            touch_ops = [Operation.touch()]
            if mixed:
                per_mode = {
                    mode: self._make_batch_write_policy_mixed(spec, mode)
                    for mode in (Mode.AP, Mode.SC)
                }
                for key in spec.keys:
                    bwp = per_mode[self._mode_for_namespace(key.namespace)]
                    ops.append(BatchWriteOp(key, touch_ops, policy=bwp))
            else:
                bwp = self._make_batch_write_policy_mixed(spec)
                for key in spec.keys:
                    ops.append(BatchWriteOp(key, touch_ops, policy=bwp))
        elif op_type == "exists":
            brp = self._make_batch_read_policy(spec)
            for key in spec.keys:
                ops.append(BatchReadOp(key, bins=[], policy=brp))
        else:
            write_ops = list(spec.operations)
            if mixed:
                per_mode = {
                    mode: self._make_batch_write_policy_mixed(spec, mode)
                    for mode in (Mode.AP, Mode.SC)
                }
                for key in spec.keys:
                    bwp = per_mode[self._mode_for_namespace(key.namespace)]
                    ops.append(BatchWriteOp(key, write_ops, policy=bwp))
            else:
                bwp = self._make_batch_write_policy_mixed(spec)
                for key in spec.keys:
                    ops.append(BatchWriteOp(key, write_ops, policy=bwp))
        return ops

    @staticmethod
    def _make_batch_read_policy(
        spec: _OperationSpec,
    ) -> Optional[BatchReadPolicy]:
        """Build a ``BatchReadPolicy`` from per-spec settings."""
        if spec.filter_expression is None:
            return None
        brp = BatchReadPolicy()
        brp.filter_expression = spec.filter_expression
        return brp

    def _make_batch_write_policy_mixed(
        self,
        spec: _OperationSpec,
        mode: Optional[Mode] = None,
    ) -> Optional[BatchWritePolicy]:
        """Build a ``BatchWritePolicy`` that includes ``record_exists_action``
        for use in mixed-batch calls.

        *mode* scopes the durable-delete default to the row's namespace mode.
        """
        op_type = spec.op_type or "upsert"
        rea = _OP_TYPE_TO_REA.get(op_type)
        eff = (
            self._batch_write_effective_dd(spec, mode)
            if spec.contains_record_delete_op else False
        )
        has_settings = (
            rea is not None
            or spec.filter_expression is not None
            or spec.generation is not None
            or spec.ttl_seconds is not None
            or spec.durable_delete is not None
            or spec.durable_delete_command_default is not None
            or (spec.contains_record_delete_op and (
                eff
                or spec.durable_delete is not None
                or spec.durable_delete_command_default is not None
            ))
        )
        if not has_settings:
            return None
        bwp = BatchWritePolicy()
        if rea is not None:
            bwp.record_exists_action = rea
        if spec.filter_expression is not None:
            bwp.filter_expression = spec.filter_expression
        if spec.generation is not None:
            bwp.generation_policy = GenerationPolicy.EXPECT_GEN_EQUAL
            bwp.generation = spec.generation
        if spec.ttl_seconds is not None:
            bwp.expiration = _to_expiration(spec.ttl_seconds)
        if spec.contains_record_delete_op:
            bwp.durable_delete = eff
        return bwp

class WriteBinBuilder(_WriteVerbs[_WriteSegmentBuilderBase]):
    """Per-bin write builder inside a :class:`WriteSegmentBuilder`.

    Start with :meth:`WriteSegmentBuilder.bin`. Scalar methods delegate to the
    segment; ``map_*`` and ``list_*`` append collection operations; ``hll_*``
    and ``bit_*`` append HyperLogLog and blob bit operations; nested CDT
    builders capture context for maps and lists. Write verbs on this class
    finalize the segment and start a new one on new keys.

    Example::

        Set a map key and append to a list within the same write::

            await (
                session.upsert(key)
                    .bin("config").on_map_key("level").set_to(5)
                    .bin("tags").list_append(value="new_tag")
                    .execute()
            )

    See Also:
        :class:`QueryBinBuilder`: Read-side analogue for queries.
    """

    __slots__ = ("_segment", "_bin")

    def __init__(self, segment: WriteSegmentBuilder, bin_name: str) -> None:
        self._segment = segment
        self._bin = bin_name

    # -- Scalar writes --------------------------------------------------------

    def set_to(self, value: Any) -> WriteSegmentBuilder:
        """Set the bin to *value* (``Operation.put``)."""
        return self._segment.set_to(self._bin, value)

    def set_to_geo_json(self, geo_json: str) -> WriteSegmentBuilder:
        """Set the bin to a GeoJSON value from its string form.

        The bin's server-side particle type is GEOJSON, not STRING. Equivalent
        to ``set_to(GeoJSON(geo_json))`` but reads naturally for spatial data.

        Args:
            geo_json: A GeoJSON string (e.g. a Point, Polygon, or AeroCircle).

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment.set_to(self._bin, GeoJSON(geo_json))

    def add(self, value: Any) -> WriteSegmentBuilder:
        """Add a numeric *value* to the bin (``Operation.add``)."""
        return self._segment.add(self._bin, value)

    def increment_by(self, value: Any) -> WriteSegmentBuilder:
        """Alias of :meth:`add`."""
        return self.add(value)

    def append(self, value: str) -> WriteSegmentBuilder:
        """String append (``Operation.append``)."""
        return self._segment.append(self._bin, value)

    def prepend(self, value: str) -> WriteSegmentBuilder:
        """String prepend (``Operation.prepend``)."""
        return self._segment.prepend(self._bin, value)

    def remove(self) -> WriteSegmentBuilder:
        """Drop the bin (write ``None``)."""
        return self._segment.remove_bin(self._bin)

    def get(self) -> WriteSegmentBuilder:
        """Return the bin value after writes complete (``Operation.get_bin``)."""
        return self._segment.get(self._bin)

    # -- CDT list structural operations ---------------------------------------

    def list_add(
        self, value: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
    ) -> WriteSegmentBuilder:
        """Add *value* to an ordered list (sorted insert).

        Args:
            value: Element to insert in sorted order.
            unique: Reject if the value already exists in the list.
            bounded: Reject if index is beyond the current list bounds.
            no_fail: Do not raise on write failures.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        policy = _resolve_list_policy(
            ListOrderType.ORDERED, unique=unique, bounded=bounded,
            no_fail=no_fail,
        )
        return self._segment._add_op(
            ListOperation.append(self._bin, value, policy),
        )

    def list_append(
        self, value: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
    ) -> WriteSegmentBuilder:
        """Append *value* to the end of an unordered list.

        Args:
            value: Value to append.
            unique: Reject if the value already exists in the list.
            bounded: Reject if index is beyond the current list bounds.
            no_fail: Do not raise on write failures.

        Example::
            .bin("tags").list_append(value="python")
        """
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded, no_fail=no_fail,
        )
        return self._segment._add_op(
            ListOperation.append(self._bin, value, policy),
        )

    # -- Collection-level map -------------------------------------------------

    def map_clear(self) -> WriteSegmentBuilder:
        """Remove all entries from the map bin.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(MapOperation.clear(self._bin))

    def map_size(self) -> WriteSegmentBuilder:
        """Return the map element count (read within operate).

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(MapOperation.size(self._bin))

    def map_upsert_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> WriteSegmentBuilder:
        """Put multiple map entries (create or update each key).

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Example::
            .bin("settings").map_upsert_items({"theme": "dark", "lang": "en"})
        """
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.DEFAULT,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        return self._segment._add_op(
            MapOperation.put_items(self._bin, pairs, policy),
        )

    def map_insert_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> WriteSegmentBuilder:
        """Put map entries only for keys that do not yet exist.

        Args:
            items: Mapping or sequence of ``(key, value)`` pairs.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.
        """
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.CREATE_ONLY,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        return self._segment._add_op(
            MapOperation.put_items(self._bin, pairs, policy),
        )

    def map_update_items(
        self, items: Any,
        *,
        order: MapOrder | None = None,
        persist_index: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> WriteSegmentBuilder:
        """Update existing map entries only (no new keys).

        Args:
            items: Key-value pairs to update for existing keys only.
            order: Map key order for the policy.
            persist_index: Maintain a persistent index on the map.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        pairs = _map_item_pairs(items)
        policy = _resolve_map_policy(
            MapWriteFlags.UPDATE_ONLY,
            order=order, persist_index=persist_index,
            no_fail=no_fail, partial=partial,
        )
        return self._segment._add_op(
            MapOperation.put_items(self._bin, pairs, policy),
        )

    def map_create(self, order: MapOrder) -> WriteSegmentBuilder:
        """Create an empty map with the given key order.

        Args:
            order: Map key sort order.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(MapOperation.create(self._bin, order))

    def map_set_policy(self, order: MapOrder) -> WriteSegmentBuilder:
        """Set map sort order policy without changing entries.

        Args:
            order: Map key sort order policy.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(
            MapOperation.set_map_policy(self._bin, MapPolicy(order, None)),
        )

    # -- Collection-level list ------------------------------------------------

    def list_clear(self) -> WriteSegmentBuilder:
        """Remove all elements from the list bin.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(ListOperation.clear(self._bin))

    def list_sort(
        self, flags: ListSortFlags = ListSortFlags.DEFAULT,
    ) -> WriteSegmentBuilder:
        """Sort the list bin.

        Args:
            flags: Sort behavior flags.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(ListOperation.sort(self._bin, flags))

    def list_size(self) -> WriteSegmentBuilder:
        """Return the list element count (read within operate).

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(ListOperation.size(self._bin))

    def list_append_items(
        self, items: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> WriteSegmentBuilder:
        """Append values to an unordered list.

        Args:
            items: Values to append.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.
        """
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        return self._segment._add_op(
            ListOperation.append_items(self._bin, items, policy),
        )

    def list_add_items(
        self, items: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> WriteSegmentBuilder:
        """Insert values into an ordered list (sorted positions).

        Args:
            items: Sequence of values to insert in sorted order.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        policy = _resolve_list_policy(
            ListOrderType.ORDERED, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        return self._segment._add_op(
            ListOperation.append_items(self._bin, items, policy),
        )

    def list_create(
        self, order: ListOrderType, *, pad: bool = False, persist_index: bool = False,
    ) -> WriteSegmentBuilder:
        """Create an empty list with the given order.

        Args:
            order: List element order.
            pad: Whether to pad with None entries.
            persist_index: Whether to persist element indices.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(
            ListOperation.create(self._bin, order, pad, persist_index),
        )

    def list_set_order(self, order: ListOrderType) -> WriteSegmentBuilder:
        """Set list sort order without changing elements.

        Args:
            order: List element order.

        Returns:
            The parent :class:`WriteSegmentBuilder`.
        """
        return self._segment._add_op(ListOperation.set_order(self._bin, order))

    # -- Index-based list (whole-bin) ----------------------------------------

    def list_insert(
        self, index: int, value: Any,
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
    ) -> WriteSegmentBuilder:
        """Insert *value* at *index* in an unordered list.

        Args:
            index: List index (0-based; negative counts from the end).
            value: Element to insert.
            unique: Reject if the value already exists in the list.
            bounded: Reject if index is beyond the current list bounds.
            no_fail: Do not raise on write failures.

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_append`, :meth:`QueryBinBuilder.list_get`
        """
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded, no_fail=no_fail,
        )
        return self._segment._add_op(
            ListOperation.insert(self._bin, index, value, policy),
        )

    def list_insert_items(
        self, index: int, items: Sequence[Any],
        *,
        unique: bool = False,
        bounded: bool = False,
        no_fail: bool = False,
        partial: bool = False,
    ) -> WriteSegmentBuilder:
        """Insert a sequence of values starting at *index*.

        Args:
            index: List index at which to insert the first element.
            items: Values to insert in order.
            unique: Reject items that already exist in the list.
            bounded: Reject inserts beyond the current list bounds.
            no_fail: Do not raise on write failures.
            partial: Allow partial success for bulk operations.

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_insert`, :meth:`list_append_items`
        """
        policy = _resolve_list_policy(
            None, unique=unique, bounded=bounded,
            no_fail=no_fail, partial=partial,
        )
        return self._segment._add_op(
            ListOperation.insert_items(self._bin, index, items, policy),
        )

    def list_set(self, index: int, value: Any) -> WriteSegmentBuilder:
        """Replace the element at *index* with *value*.

        Args:
            index: List index (0-based; negative counts from the end).
            value: New element value.

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_get`
        """
        return self._segment._add_op(ListOperation.set(self._bin, index, value))

    def list_increment(self, index: int, value: int = 1) -> WriteSegmentBuilder:
        """Add *value* to the numeric element at *index* (default increment is ``1``).

        Args:
            index: List index (0-based; negative counts from the end).
            value: Amount to add; ``1`` uses a dedicated server path.

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_set`
        """
        if value == 1:
            return self._segment._add_op(
                ListOperation.increment_by_one(self._bin, index),
            )
        return self._segment._add_op(
            ListOperation.increment(
                self._bin, index, value, _UNORDERED_LIST_POLICY,
            ),
        )

    def list_remove(self, index: int) -> WriteSegmentBuilder:
        """Remove the element at *index*.

        Args:
            index: List index (0-based; negative counts from the end).

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_remove_range`
        """
        return self._segment._add_op(ListOperation.remove(self._bin, index))

    def list_remove_range(
        self, index: int, count: Optional[int] = None,
    ) -> WriteSegmentBuilder:
        """Remove *count* elements starting at *index*, or all from *index* onward.

        Args:
            index: Starting list index.
            count: Number of elements to remove; ``None`` removes through the end.

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_remove`
        """
        if count is None:
            op = ListOperation.remove_range_from(self._bin, index)
        else:
            op = ListOperation.remove_range(self._bin, index, count)
        return self._segment._add_op(op)

    def list_pop(self, index: int) -> WriteSegmentBuilder:
        """Remove and return the element at *index* (read in the operate result).

        Args:
            index: List index (0-based; negative counts from the end).

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_pop_range`
        """
        return self._segment._add_op(ListOperation.pop(self._bin, index))

    def list_pop_range(
        self, index: int, count: Optional[int] = None,
    ) -> WriteSegmentBuilder:
        """Pop *count* elements from *index*, or from *index* through the end.

        Args:
            index: Starting list index.
            count: Number of elements; ``None`` pops through the end.

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_pop`
        """
        if count is None:
            op = ListOperation.pop_range_from(self._bin, index)
        else:
            op = ListOperation.pop_range(self._bin, index, count)
        return self._segment._add_op(op)

    def list_trim(self, index: int, count: int) -> WriteSegmentBuilder:
        """Keep only *count* elements starting at *index*; remove the rest.

        Args:
            index: Starting list index of the range to keep.
            count: Number of elements to keep.

        Returns:
            The parent :class:`WriteSegmentBuilder`.

        See Also:
            :meth:`list_remove_range`
        """
        return self._segment._add_op(
            ListOperation.trim(self._bin, index, count),
        )

    # -- HyperLogLog ----------------------------------------------------------

    def hll_init(
        self,
        config: HllConfig,
        *,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> WriteSegmentBuilder:
        """Initialize an empty HyperLogLog sketch in this bin.

        Use before :meth:`hll_add` on a new bin. ``create_only`` and
        ``update_only`` are mutually exclusive; passing both raises
        :class:`ValueError`.

        Example::

            await (
                session.upsert(key)
                .bin("visitors").hll_init(HllConfig.of(12))
                .execute()
            )

        Args:
            config: Index and minhash bit widths for the new sketch.
            create_only: Fail if the bin already exists.
            update_only: Fail if the bin does not already exist.
            no_fail: Skip the operation silently when a mode constraint blocks it.
            allow_fold: Allow folding so unions tolerate mismatched precisions.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        Raises:
            ValueError: If ``create_only`` and ``update_only`` are both true.

        See Also:
            :meth:`hll_add`: Add distinct values to the sketch.
            :meth:`QueryBinBuilder.hll_get_count`: Read cardinality in a query.
        """
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        return self._segment._add_op(
            HllOperation.init(
                self._bin,
                config.index_bit_count,
                config.min_hash_bit_count,
                flags,
            ),
        )

    def hll_add(
        self,
        values: Sequence[Any],
        *,
        config: Optional[HllConfig] = None,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> WriteSegmentBuilder:
        """Add distinct values to the HyperLogLog sketch in this bin.

        The server hashes each element into the sketch. Pass ``config=...`` to
        auto-create the sketch on the first call with that precision; omit it
        to inherit defaults from an existing sketch.

        Example::

            await (
                session.upsert(key)
                .bin("visitors").hll_add(["user-1", "user-2"])
                .execute()
            )

        Args:
            values: Sequence of values (e.g. strings or blobs) to add.
            config: Optional HLL config used to auto-create the bin on first
                use. When ``None``, inherits the existing sketch's bit widths.
            create_only: Fail if the bin already exists.
            update_only: Fail if the bin does not already exist.
            no_fail: Skip the operation silently when a mode constraint blocks it.
            allow_fold: Allow folding so unions tolerate mismatched precisions.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        Raises:
            ValueError: If ``create_only`` and ``update_only`` are both true.

        See Also:
            :meth:`hll_init`: Create an empty sketch explicitly.
            :meth:`hll_get_count`: Read cardinality in the same operate batch.
        """
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        index_bit_count = config.index_bit_count if config is not None else -1
        min_hash_bit_count = config.min_hash_bit_count if config is not None else -1
        return self._segment._add_op(
            HllOperation.add(
                self._bin,
                list(values),
                index_bit_count,
                min_hash_bit_count,
                flags,
            ),
        )

    def hll_set_union(
        self,
        hll_list: Sequence[Any],
        *,
        create_only: bool = False,
        update_only: bool = False,
        no_fail: bool = False,
        allow_fold: bool = False,
    ) -> WriteSegmentBuilder:
        """Merge other HyperLogLog sketches into this bin (destructive union).

        Each entry in ``hll_list`` is typically another HLL blob (``bytes``)
        returned from a prior read.

        Example::

            await (
                session.upsert(key)
                .bin("merged").hll_set_union([other_hll_blob])
                .execute()
            )

        Args:
            hll_list: Sketches to union into the target bin.
            create_only: Fail if the bin already exists.
            update_only: Fail if the bin does not already exist.
            no_fail: Skip the operation silently when a mode constraint blocks it.
            allow_fold: Allow folding so unions tolerate mismatched precisions.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        Raises:
            ValueError: If ``create_only`` and ``update_only`` are both true.

        See Also:
            :meth:`hll_get_union`: Non-destructive union read.
            :meth:`hll_add`: Add raw values instead of whole sketches.
        """
        flags = _resolve_hll_flags(
            create_only=create_only, update_only=update_only,
            no_fail=no_fail, allow_fold=allow_fold,
        )
        return self._segment._add_op(
            HllOperation.set_union(self._bin, list(hll_list), flags),
        )

    def hll_fold(self, index_bit_count: int) -> WriteSegmentBuilder:
        """Reduce sketch precision to a lower ``index_bit_count`` (merge registers).

        Example::
            await session.update(key).bin("hll").hll_fold(10).execute()

        Args:
            index_bit_count: New (smaller) index bit width after folding.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`hll_init`: Initial precision when creating a sketch.
        """
        return self._segment._add_op(HllOperation.fold(self._bin, index_bit_count))

    def hll_refresh_count(self) -> WriteSegmentBuilder:
        """Refresh the cached cardinality estimate stored with the sketch.

        Example::
            await session.update(key).bin("hll").hll_refresh_count().execute()

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`hll_get_count`: Read the estimate in the same batch.
        """
        return self._segment._add_op(HllOperation.refresh_count(self._bin))

    def hll_get_count(self) -> WriteSegmentBuilder:
        """Read the estimated cardinality in a multi-operation write (``operate``).

        The result is returned for this bin when the write completes. For a
        read-only path, use :meth:`QueryBinBuilder.hll_get_count`.

        Example::
            stream = await ( session.update(key) .bin("hll") .hll_get_count() .execute() )

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.hll_get_count`: Same read on a query builder.
            :meth:`hll_add`: Populate the sketch before counting.
        """
        return self._segment._add_op(HllOperation.get_count(self._bin))

    def hll_describe(self) -> WriteSegmentBuilder:
        """Read index and min-hash bit counts describing the stored sketch.

        Example::
            await session.update(key).bin("hll").hll_describe().execute()

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.hll_describe`: Same read on a query builder.
        """
        return self._segment._add_op(HllOperation.describe(self._bin))

    def hll_get_union(self, hll_list: Sequence[Any]) -> WriteSegmentBuilder:
        """Read the union sketch without modifying the stored bin.

        Example::
            await ( session.update(key) .bin("hll") .hll_get_union([peer_blob]) .execute() )

        Args:
            hll_list: Other sketches (blobs) to include in the union result.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.hll_get_union`: Same read on a query builder.
            :meth:`hll_set_union`: Persist a union into the bin.
        """
        return self._segment._add_op(
            HllOperation.get_union(self._bin, list(hll_list)),
        )

    def hll_get_union_count(self, hll_list: Sequence[Any]) -> WriteSegmentBuilder:
        """Read the estimated cardinality of the union with other sketches.

        Example::
            await ( session.update(key) .bin("hll") .hll_get_union_count([peer_blob]) .execute() )

        Args:
            hll_list: Other sketches to union for the estimate.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.hll_get_union_count`: Same read on a query.
        """
        return self._segment._add_op(
            HllOperation.get_union_count(self._bin, list(hll_list)),
        )

    def hll_get_intersect_count(self, hll_list: Sequence[Any]) -> WriteSegmentBuilder:
        """Read the estimated intersection cardinality with other sketches.

        Example::
            await ( session.update(key) .bin("hll") .hll_get_intersect_count([peer_blob]) .execute() )

        Args:
            hll_list: Other sketches included in the intersection estimate.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.hll_get_intersect_count`: Same read on a query.
        """
        return self._segment._add_op(
            HllOperation.get_intersect_count(self._bin, list(hll_list)),
        )

    def hll_get_similarity(self, hll_list: Sequence[Any]) -> WriteSegmentBuilder:
        """Read Jaccard similarity between this sketch and other sketches.

        Example::
            await ( session.update(key) .bin("hll") .hll_get_similarity([peer_blob]) .execute() )

        Args:
            hll_list: Other sketches to compare.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.hll_get_similarity`: Same read on a query.
        """
        return self._segment._add_op(
            HllOperation.get_similarity(self._bin, list(hll_list)),
        )

    # -- Bit (blob) -----------------------------------------------------------

    def bit_resize(
        self,
        byte_size: int,
        resize_flags: Optional[Any] = None,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Grow or shrink the raw bytes backing this bin.

        When ``resize_flags`` is ``None``, :attr:`~aerospike_sdk.BitwiseResizeFlags.DEFAULT`
        is used. When ``policy`` is ``None``, a default :class:`~aerospike_sdk.BitPolicy`
        is built from :attr:`~aerospike_sdk.BitWriteFlags.DEFAULT`.

        Example::
            await session.upsert(key).bin("flags").bit_resize(4).execute()

        Args:
            byte_size: Target size of the blob in bytes.
            resize_flags: Optional :class:`~aerospike_sdk.BitwiseResizeFlags` value; ``None`` selects ``DEFAULT``.
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_insert`: Insert raw bytes at an offset.
            :meth:`QueryBinBuilder.bit_get`: Read bits in a query.
        """
        return self._segment._add_op(
            BitOperation.resize(
                self._bin,
                byte_size,
                _resize_flags_or_default(resize_flags),
                _bit_policy_or_default(policy),
            ),
        )

    def bit_insert(
        self,
        byte_offset: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Insert ``value`` (bytes) at a byte offset in the blob bin.

        Example::
            await session.update(key).bin("blob").bit_insert(0, b"\\x01\\x02").execute()

        Args:
            byte_offset: Byte position at which to insert.
            value: Bytes to insert.
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_remove`: Remove a byte range.
        """
        return self._segment._add_op(
            BitOperation.insert(
                self._bin,
                byte_offset,
                value,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_remove(
        self,
        byte_offset: int,
        byte_size: int,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Remove ``byte_size`` bytes starting at ``byte_offset``.

        Example::
            await session.update(key).bin("blob").bit_remove(0, 2).execute()

        Args:
            byte_offset: Start of the range to remove.
            byte_size: Number of bytes to remove.
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_insert`: Insert bytes at an offset.
        """
        return self._segment._add_op(
            BitOperation.remove(
                self._bin,
                byte_offset,
                byte_size,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_set(
        self,
        bit_offset: int,
        bit_size: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Overwrite ``bit_size`` bits at ``bit_offset`` with ``value``.

        ``value`` is typically a small ``bytes`` object whose bits replace the
        range (see server documentation for encoding).

        Example::
            await session.update(key).bin("blob").bit_set(0, 8, b"\\xff").execute()

        Args:
            bit_offset: Starting bit index within the blob.
            bit_size: Width of the field in bits.
            value: Bits to write (commonly ``bytes``).
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_get`: Read the same range in an operate or query.
            :meth:`bit_or`, :meth:`bit_xor`, :meth:`bit_and`, :meth:`bit_not`
        """
        return self._segment._add_op(
            BitOperation.set(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_or(
        self,
        bit_offset: int,
        bit_size: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Bitwise OR ``value`` into the ``bit_size`` bits at ``bit_offset``.

        Example::
            await session.update(key).bin("blob").bit_or(0, 8, b"\\x0f").execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Field width in bits.
            value: Right-hand side of the OR (typically ``bytes``).
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_and`
            :meth:`bit_xor`
            :meth:`bit_not`
        """
        return self._segment._add_op(
            _bitwise_or(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_xor(
        self,
        bit_offset: int,
        bit_size: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Bitwise XOR ``value`` into the ``bit_size`` bits at ``bit_offset``.

        Example::
            await session.update(key).bin("blob").bit_xor(0, 8, b"\\xff").execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Field width in bits.
            value: Right-hand side of the XOR (typically ``bytes``).
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_or`
            :meth:`bit_and`
            :meth:`bit_not`
        """
        return self._segment._add_op(
            BitOperation.xor(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_and(
        self,
        bit_offset: int,
        bit_size: int,
        value: Any,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Bitwise AND ``value`` into the ``bit_size`` bits at ``bit_offset``.

        Example::
            await session.update(key).bin("blob").bit_and(0, 8, b"\\xf0").execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Field width in bits.
            value: Right-hand side of the AND (typically ``bytes``).
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_or`
            :meth:`bit_xor`
            :meth:`bit_not`
        """
        return self._segment._add_op(
            _bitwise_and(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_not(
        self,
        bit_offset: int,
        bit_size: int,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Invert every bit in the range ``[bit_offset, bit_offset + bit_size)``.

        Example::
            await session.update(key).bin("blob").bit_not(0, 8).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Field width in bits.
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_or`
            :meth:`bit_xor`
            :meth:`bit_and`
        """
        return self._segment._add_op(
            _bitwise_not(
                self._bin,
                bit_offset,
                bit_size,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_lshift(
        self,
        bit_offset: int,
        bit_size: int,
        shift: int,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Left-shift the ``bit_size`` bits at ``bit_offset`` by ``shift`` bits.

        Example::
            await session.update(key).bin("blob").bit_lshift(0, 16, 2).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Field width in bits.
            shift: Number of bits to shift left.
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_rshift`
        """
        return self._segment._add_op(
            BitOperation.lshift(
                self._bin,
                bit_offset,
                bit_size,
                shift,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_rshift(
        self,
        bit_offset: int,
        bit_size: int,
        shift: int,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Right-shift the ``bit_size`` bits at ``bit_offset`` by ``shift`` bits.

        Example::
            await session.update(key).bin("blob").bit_rshift(0, 16, 2).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Field width in bits.
            shift: Number of bits to shift right.
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_lshift`
        """
        return self._segment._add_op(
            BitOperation.rshift(
                self._bin,
                bit_offset,
                bit_size,
                shift,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_add(
        self,
        bit_offset: int,
        bit_size: int,
        value: int,
        signed: bool,
        action: Any,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Add ``value`` to the integer encoded in ``bit_size`` bits at ``bit_offset``.

        ``action`` selects overflow behavior (for example :attr:`~aerospike_sdk.BitwiseOverflowActions.WRAP`).

        Example::
            from aerospike_sdk import BitwiseOverflowActions
            await ( session.update(key) .bin("blob") .bit_add(0, 16, 1, False, BitwiseOverflowActions.WRAP) .execute() )

        Args:
            bit_offset: Starting bit index of the integer field.
            bit_size: Width of the integer in bits.
            value: Amount to add.
            signed: ``True`` if the stored integer is signed.
            action: Overflow policy (:class:`~aerospike_sdk.BitwiseOverflowActions`).
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_subtract`
            :meth:`bit_set_int`
            :meth:`bit_get_int`
        """
        return self._segment._add_op(
            BitOperation.add(
                self._bin,
                bit_offset,
                bit_size,
                value,
                signed,
                action,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_subtract(
        self,
        bit_offset: int,
        bit_size: int,
        value: int,
        signed: bool,
        action: Any,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Subtract ``value`` from the integer in ``bit_size`` bits at ``bit_offset``.

        Example::
            from aerospike_sdk import BitwiseOverflowActions
            await ( session.update(key) .bin("blob") .bit_subtract(0, 16, 1, False, BitwiseOverflowActions.SATURATE) .execute() )

        Args:
            bit_offset: Starting bit index of the integer field.
            bit_size: Width of the integer in bits.
            value: Amount to subtract.
            signed: ``True`` if the stored integer is signed.
            action: Overflow policy (:class:`~aerospike_sdk.BitwiseOverflowActions`).
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_add`
            :meth:`bit_set_int`
        """
        return self._segment._add_op(
            BitOperation.subtract(
                self._bin,
                bit_offset,
                bit_size,
                value,
                signed,
                action,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_set_int(
        self,
        bit_offset: int,
        bit_size: int,
        value: int,
        policy: Optional[Any] = None,
    ) -> WriteSegmentBuilder:
        """Write integer ``value`` into ``bit_size`` bits at ``bit_offset``.

        Example::
            await session.update(key).bin("blob").bit_set_int(0, 16, 42).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Width of the integer in bits.
            value: Integer to store.
            policy: Optional :class:`~aerospike_sdk.BitPolicy`; ``None`` selects a default policy.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_get_int`
            :meth:`bit_add`
        """
        return self._segment._add_op(
            BitOperation.set_int(
                self._bin,
                bit_offset,
                bit_size,
                value,
                _bit_policy_or_default(policy),
            ),
        )

    def bit_get(self, bit_offset: int, bit_size: int) -> WriteSegmentBuilder:
        """Read ``bit_size`` bits at ``bit_offset`` as raw bytes in a write operate.

        For read-only access, use :meth:`QueryBinBuilder.bit_get`.

        Example::
            await session.update(key).bin("blob").bit_get(0, 8).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Number of bits to read.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.bit_get`
            :meth:`bit_set`
        """
        return self._segment._add_op(BitOperation.get(self._bin, bit_offset, bit_size))

    def bit_count(self, bit_offset: int, bit_size: int) -> WriteSegmentBuilder:
        """Count bits set to ``1`` in ``bit_size`` bits starting at ``bit_offset``.

        Example::
            await session.update(key).bin("blob").bit_count(0, 8).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Number of bits to scan.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.bit_count`
        """
        return self._segment._add_op(BitOperation.count(self._bin, bit_offset, bit_size))

    def bit_lscan(self, bit_offset: int, bit_size: int, value: bool) -> WriteSegmentBuilder:
        """Return the leftmost bit index in the range matching ``value``.

        ``value`` is ``True`` to search for a set bit (``1``) or ``False`` for
        an unset bit (``0``).

        Example::
            await session.update(key).bin("blob").bit_lscan(0, 8, True).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Number of bits to scan.
            value: ``True`` for set bits, ``False`` for unset bits.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_rscan`
            :meth:`QueryBinBuilder.bit_lscan`
        """
        return self._segment._add_op(
            BitOperation.lscan(self._bin, bit_offset, bit_size, value),
        )

    def bit_rscan(self, bit_offset: int, bit_size: int, value: bool) -> WriteSegmentBuilder:
        """Return the rightmost bit index in the range matching ``value``.

        Example::
            await session.update(key).bin("blob").bit_rscan(0, 8, False).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Number of bits to scan.
            value: ``True`` for set bits, ``False`` for unset bits.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`bit_lscan`
            :meth:`QueryBinBuilder.bit_rscan`
        """
        return self._segment._add_op(
            BitOperation.rscan(self._bin, bit_offset, bit_size, value),
        )

    def bit_get_int(
        self, bit_offset: int, bit_size: int, signed: bool,
    ) -> WriteSegmentBuilder:
        """Decode an integer from ``bit_size`` bits at ``bit_offset``.

        Example::
            await session.update(key).bin("blob").bit_get_int(0, 16, False).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Width of the integer in bits.
            signed: ``True`` to interpret as two's-complement signed.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.bit_get_int`
            :meth:`bit_set_int`
        """
        return self._segment._add_op(
            BitOperation.get_int(self._bin, bit_offset, bit_size, signed),
        )

    # -- Server-side string operations (server 8.1.3+) ------------------------
    #
    # The ``str_*`` family wraps server-side string read/modify ops. Each
    # method registers a single op on the surrounding write segment and
    # returns the parent for chaining. Reads return their result under this
    # bin's name in the record when the operate completes. Modify ops mutate
    # the bin in place. Use :class:`StringWriteFlags`, :class:`StringRegexFlags`,
    # and :class:`StringNumericType` for the bitmask / enum arguments.
    #
    # CTX navigation on string ops (operating on a string nested inside a
    # list or map bin) is not exposed on this chainable surface — drop to the
    # lower-level ``StringOperation.<name>(bin, ctx=[...])`` factory plus
    # ``append_operations(op)`` for that case.

    # ---- String reads -------------------------------------------------------

    def str_strlen(self) -> WriteSegmentBuilder:
        """Register a strlen read: Unicode codepoint count of this string bin.

        Returns the codepoint count under this bin's name (NOT the UTF-8
        byte count — use :meth:`str_byte_length` for bytes).

        Example::

            stream = await (
                session.upsert(key)
                    .bin("greeting").set_to("héllo")
                    .bin("greeting").str_strlen()
                    .execute()
            )

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.

        See Also:
            :meth:`QueryBinBuilder.str_strlen`: Same read on a query builder.
            :meth:`str_byte_length`: UTF-8 byte length instead of codepoint count.
        """
        return self._segment._add_op(StringOperation.strlen(self._bin))

    def str_substr(self, start: int, end: Optional[int] = None) -> WriteSegmentBuilder:
        """Register a substr read.

        With ``end`` omitted, returns codepoints from ``start`` to the end of
        the string. With ``end`` set, returns the half-open codepoint range
        ``[start, end)``. Negative ``start`` counts from the end. Out-of-bounds
        indexes are clamped — no error is raised.

        Note:
            ``end`` is end-exclusive (NOT a length). To extract three
            codepoints starting at index 2, pass ``start=2, end=5``.

        Example::

            stream = await (
                session.upsert(key)
                    .bin("greeting").set_to("hello world")
                    .bin("greeting").str_substr(0, 5)  # → "hello"
                    .execute()
            )

        Args:
            start: Codepoint index to start at. Negative counts from end.
            end: End-exclusive codepoint index. ``None`` means run to end.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(StringOperation.substr(self._bin, start, end))

    def str_char_at(self, index: int) -> WriteSegmentBuilder:
        """Register a char-at read: returns the codepoint at ``index`` as a
        one-codepoint string. Negative ``index`` counts from the end.

        Example::
            stream = await session.upsert(key).bin("s").str_char_at(1).execute()

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(StringOperation.char_at(self._bin, index))

    def str_find(self, needle: str, occurrence: Optional[int] = None) -> WriteSegmentBuilder:
        """Register a find read: codepoint index of the first occurrence of
        ``needle``, or the N-th occurrence when ``occurrence`` is given.

        Returns -1 if the needle is absent. Occurrence numbering is 1-based
        (``1`` = first match); ``-1`` selects the last match.

        Example::
            stream = await session.upsert(key).bin("s").str_find("world").execute()

        Args:
            needle: Substring to locate.
            occurrence: 1-based match index. ``None`` means first occurrence.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.find(self._bin, needle, occurrence),
        )

    def str_contains(self, needle: str) -> WriteSegmentBuilder:
        """Register a contains read: ``True`` iff this bin contains ``needle``.

        Result is a Python ``bool`` (the server returns a native msgpack
        boolean for the seven boolean-shaped read ops).

        Example::
            stream = await session.upsert(key).bin("s").str_contains("hello").execute()

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(StringOperation.contains(self._bin, needle))

    def str_starts_with(self, prefix: str) -> WriteSegmentBuilder:
        """Register a starts-with read: ``True`` iff this bin starts with ``prefix``."""
        return self._segment._add_op(StringOperation.starts_with(self._bin, prefix))

    def str_ends_with(self, suffix: str) -> WriteSegmentBuilder:
        """Register an ends-with read: ``True`` iff this bin ends with ``suffix``."""
        return self._segment._add_op(StringOperation.ends_with(self._bin, suffix))

    def str_to_integer(self) -> WriteSegmentBuilder:
        """Register a to-integer read: parses this bin as ``int64``.

        Server returns ``PARAMETER_ERROR`` if the bin doesn't parse as an
        integer.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(StringOperation.to_integer(self._bin))

    def str_to_double(self) -> WriteSegmentBuilder:
        """Register a to-double read: parses this bin as ``float64``.

        Server returns ``PARAMETER_ERROR`` if the bin doesn't parse as a
        double.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(StringOperation.to_double(self._bin))

    def str_byte_length(self) -> WriteSegmentBuilder:
        """Register a byte-length read: UTF-8 byte count of this string bin.

        Differs from :meth:`str_strlen` for non-ASCII content (where one
        codepoint can encode to multiple bytes).
        """
        return self._segment._add_op(StringOperation.byte_length(self._bin))

    def str_is_numeric(self, numeric_type: Optional[StringNumericType] = None) -> WriteSegmentBuilder:
        """Register an is-numeric read: ``True`` iff this bin parses as a number.

        Pass ``numeric_type`` to restrict to ``StringNumericType.INT`` or
        ``StringNumericType.FLOAT``; default ``ANY`` accepts either.

        Example::

            stream = await (
                session.upsert(key).bin("count")
                    .str_is_numeric(StringNumericType.INT)
                    .execute()
            )

        Args:
            numeric_type: Restrict to one numeric class. ``None`` = either.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.is_numeric(self._bin, numeric_type),
        )

    def str_is_upper(self) -> WriteSegmentBuilder:
        """Register an is-upper read: ``True`` iff every cased codepoint is uppercase."""
        return self._segment._add_op(StringOperation.is_upper(self._bin))

    def str_is_lower(self) -> WriteSegmentBuilder:
        """Register an is-lower read: ``True`` iff every cased codepoint is lowercase."""
        return self._segment._add_op(StringOperation.is_lower(self._bin))

    def str_to_blob(self) -> WriteSegmentBuilder:
        """Register a to-blob read: UTF-8 bytes of this string bin as a blob."""
        return self._segment._add_op(StringOperation.to_blob(self._bin))

    def str_split(self, separator: Optional[str] = None) -> WriteSegmentBuilder:
        """Register a split read.

        With ``separator`` omitted, returns one element per Unicode codepoint
        (per-codepoint split). With ``separator`` set, splits on the
        substring; absent separator → singleton list with the whole string.

        Args:
            separator: Substring to split on. ``None`` = codepoint-wise.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(StringOperation.split(self._bin, separator))

    def str_b64_decode(self) -> WriteSegmentBuilder:
        """Register a base64-decode read: treats the bin as base64 text, returns bytes."""
        return self._segment._add_op(StringOperation.b64_decode(self._bin))

    def str_regex_compare(self, pattern: str, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a regex-compare read: ``True`` iff ``pattern`` matches this bin.

        Uses ICU regex syntax. Combine ``StringRegexFlags`` constants with
        bitwise OR for the ``flags`` argument.

        Example::

            stream = await (
                session.upsert(key).bin("s")
                    .str_regex_compare("^hello.*", StringRegexFlags.CASE_INSENSITIVE)
                    .execute()
            )

        Args:
            pattern: ICU regex pattern.
            flags: OR-combined :class:`StringRegexFlags` bitmask. Defaults to 0.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.regex_compare(self._bin, pattern, int(flags)),
        )

    # ---- String modifies ----------------------------------------------------

    def str_insert(self, index: int, value: str, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register an insert modify: splice ``value`` into this bin at codepoint ``index``.

        Negative ``index`` counts from the end of the string. Out-of-bounds
        indexes clamp.

        Args:
            index: Codepoint index to splice at.
            value: String to insert.
            flags: OR-combined :class:`StringWriteFlags` bitmask
                (``NO_FAIL`` suppresses the op on missing-bin error).

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.insert(self._bin, index, value, flags=int(flags)),
        )

    def str_overwrite(self, index: int, value: str, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register an overwrite modify: overwrite codepoints starting at ``index`` with ``value``.

        May extend the bin's length when ``value`` runs past the end.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.overwrite(self._bin, index, value, flags=int(flags)),
        )

    def str_concat(self, value: Union[str, List[str]], *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a concat modify: append ``value`` (single string or list of strings).

        The wire format is always list-of-strings — passing a single
        ``str`` is wrapped in a 1-element list internally.

        Args:
            value: String to append, or list of strings to append in order.
            flags: OR-combined :class:`StringWriteFlags` bitmask.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.concat(self._bin, value, flags=int(flags)),
        )

    def str_append(self, value: str, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register an append modify: add ``value`` to the end of the bin.

        The single-value form (server sub-op 67). Use :meth:`str_concat` for
        the list form.

        Args:
            value: String to append to the end of the bin.
            flags: OR-combined :class:`StringWriteFlags` bitmask.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.append(self._bin, value, flags=int(flags)),
        )

    def str_prepend(self, value: str, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a prepend modify: add ``value`` to the start of the bin.

        Distinct from :meth:`str_insert` at index 0 — this is the server's
        dedicated prepend sub-op (68).

        Args:
            value: String to prepend to the start of the bin.
            flags: OR-combined :class:`StringWriteFlags` bitmask.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.prepend(self._bin, value, flags=int(flags)),
        )

    def str_snip(self, start: int, end: int, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a snip modify: remove the half-open codepoint range ``[start, end)``.

        Note:
            ``end`` is REQUIRED. The server-side snip op cannot dispatch a
            1-arg form. To remove the suffix from ``start`` to the bin's
            end, pass the codepoint length explicitly (e.g. from a paired
            :meth:`str_strlen` read).

        Args:
            start: Codepoint index where the removed range starts.
            end: Exclusive end of the removed range.
            flags: OR-combined :class:`StringWriteFlags` bitmask.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.snip(self._bin, start, end, flags=int(flags)),
        )

    def str_replace(
        self, needle: str, replacement: str, *, flags: int | StringWriteFlags | StringRegexFlags = 0,
    ) -> WriteSegmentBuilder:
        """Register a replace modify: replace the first occurrence of ``needle``.

        Use :meth:`str_replace_all` to replace every occurrence.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.replace(self._bin, needle, replacement, flags=int(flags)),
        )

    def str_replace_all(
        self, needle: str, replacement: str, *, flags: int | StringWriteFlags | StringRegexFlags = 0,
    ) -> WriteSegmentBuilder:
        """Register a replace-all modify: replace every occurrence of ``needle``."""
        return self._segment._add_op(
            StringOperation.replace_all(self._bin, needle, replacement, flags=int(flags)),
        )

    def str_regex_replace(
        self, pattern: str, replacement: str, flags: int | StringWriteFlags | StringRegexFlags = 0,
    ) -> WriteSegmentBuilder:
        """Register a regex-replace modify.

        Replaces the first match of ``pattern`` with ``replacement``. Set
        the ``GLOBAL`` bit in ``flags`` (``StringRegexFlags.GLOBAL``) to
        replace every match.

        Note:
            ``flags`` here is :class:`StringRegexFlags` (regex behavior),
            NOT :class:`StringWriteFlags`. The server's regex-replace op has
            no slot for write flags on the wire; ``str_regex_replace`` does
            not accept a ``write_flags=`` kwarg for that reason.

        Args:
            pattern: ICU regex pattern.
            replacement: Replacement text.
            flags: OR-combined :class:`StringRegexFlags` bitmask.

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(
            StringOperation.regex_replace(self._bin, pattern, replacement, int(flags)),
        )

    def str_upper(self, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register an upper modify: uppercase the bin in place."""
        return self._segment._add_op(StringOperation.upper(self._bin, flags=int(flags)))

    def str_lower(self, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a lower modify: lowercase the bin in place."""
        return self._segment._add_op(StringOperation.lower(self._bin, flags=int(flags)))

    def str_case_fold(self, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a case-fold modify: locale-independent lowercase, useful for comparison keys."""
        return self._segment._add_op(StringOperation.case_fold(self._bin, flags=int(flags)))

    def str_normalize_nfc(self, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a normalize-NFC modify: Unicode NFC normalization in place.

        Already-normalized strings are unchanged.
        """
        return self._segment._add_op(StringOperation.normalize_nfc(self._bin, flags=int(flags)))

    def str_trim_start(self, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a trim-start modify: strip whitespace from the start of the bin."""
        return self._segment._add_op(StringOperation.trim_start(self._bin, flags=int(flags)))

    def str_trim_end(self, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a trim-end modify: strip whitespace from the end of the bin."""
        return self._segment._add_op(StringOperation.trim_end(self._bin, flags=int(flags)))

    def str_trim(self, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a trim modify: strip whitespace from both ends of the bin."""
        return self._segment._add_op(StringOperation.trim(self._bin, flags=int(flags)))

    def str_pad_start(
        self, target_length: int, pad_string: str, *, flags: int | StringWriteFlags | StringRegexFlags = 0,
    ) -> WriteSegmentBuilder:
        """Register a pad-start modify: left-pad with ``pad_string`` to ``target_length`` codepoints.

        No-op when the bin is already at or above ``target_length``.
        """
        return self._segment._add_op(
            StringOperation.pad_start(self._bin, target_length, pad_string, flags=int(flags)),
        )

    def str_pad_end(
        self, target_length: int, pad_string: str, *, flags: int | StringWriteFlags | StringRegexFlags = 0,
    ) -> WriteSegmentBuilder:
        """Register a pad-end modify: right-pad with ``pad_string`` to ``target_length`` codepoints.

        No-op when the bin is already at or above ``target_length``.
        """
        return self._segment._add_op(
            StringOperation.pad_end(self._bin, target_length, pad_string, flags=int(flags)),
        )

    def str_repeat(self, count: int, *, flags: int | StringWriteFlags | StringRegexFlags = 0) -> WriteSegmentBuilder:
        """Register a repeat modify: repeat the bin contents ``count`` times.

        ``count`` must be non-negative.
        """
        return self._segment._add_op(
            StringOperation.repeat(self._bin, count, flags=int(flags)),
        )

    def str_to_string(self) -> WriteSegmentBuilder:
        """Register a to-string read: convert a non-string bin to its string representation.

        Accepts ``int``, ``float``, ``string``, or ``blob`` source types;
        any other type returns ``BIN_TYPE_ERROR`` from the server. Has no
        ``flags`` argument and no CDT-context support (the wire op has no
        payload to carry a CTX wrapper).

        Returns:
            The parent :class:`WriteSegmentBuilder` for chaining.
        """
        return self._segment._add_op(StringOperation.to_string(self._bin))

    # -- Expression operations ------------------------------------------------

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> WriteSegmentBuilder:
        """Read a computed value into this bin using an AEL expression."""
        return self._segment.select_from(
            self._bin, expression, ignore_eval_failure=ignore_eval_failure,
        )

    def insert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> WriteSegmentBuilder:
        """Write expression result only if bin does not already exist."""
        return self._segment.insert_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    def update_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> WriteSegmentBuilder:
        """Write expression result only if bin already exists."""
        return self._segment.update_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    def upsert_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_op_failure: bool = False,
        ignore_eval_failure: bool = False,
        delete_if_null: bool = False,
    ) -> WriteSegmentBuilder:
        """Write expression result, creating or overwriting the bin."""
        return self._segment.upsert_from(
            self._bin, expression,
            ignore_op_failure=ignore_op_failure,
            ignore_eval_failure=ignore_eval_failure,
            delete_if_null=delete_if_null,
        )

    # -- Map navigation (singular -> CdtWriteBuilder) --------------------------

    def on_map_index(self, index: int) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a map element by index.

        Args:
            index: List index (0-based, negative counts from end).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_index(b, index, rt),
            lambda rt: MapOperation.remove_by_index(b, index, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_index(index),
        )

    def on_map_key(
        self, key: Any, *, create_type: Optional[MapOrder] = None,
    ) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a map element by key.

        Args:
            key: Map key to target.
            create_type: If set, use a create-on-missing context for this key
                with the given map key order.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element(s).
        """
        b = self._bin
        _mp = MapPolicy(None, None)
        if create_type is not None:
            to_ctx = lambda: CTX.map_key_create(key, create_type)
        else:
            to_ctx = lambda: CTX.map_key(key)
        return CdtWriteBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_key(b, key, rt),
            lambda rt: MapOperation.remove_by_key(b, key, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=to_ctx,
            set_to_factory=lambda v: MapOperation.put(b, key, v, _mp),
            add_factory=lambda v: MapOperation.increment_value(b, key, v, _mp),
        )

    def on_map_rank(self, rank: int) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a map element by rank (0 = lowest value)."""
        b = self._bin
        return CdtWriteBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_rank(b, rank, rt),
            lambda rt: MapOperation.remove_by_rank(b, rank, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_rank(rank),
        )

    # -- Map navigation (invertable -> CdtWriteInvertableBuilder) -------------

    def on_map_value(self, value: Any) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements matching a value.

        Args:
            value: Value to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_value(b, value, rt),
            lambda rt: MapOperation.remove_by_value(b, value, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_value(value),
        )

    def on_map_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements by index range.

        Args:
            index: List index (0-based, negative counts from end).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        if count is None:
            get_f = lambda rt: MapOperation.get_by_index_range_from(b, index, rt)
            rm_f = lambda rt: MapOperation.remove_by_index_range_from(b, index, rt)
        else:
            get_f = lambda rt: MapOperation.get_by_index_range(b, index, count, rt)
            rm_f = lambda rt: MapOperation.remove_by_index_range(b, index, count, rt)
        return CdtWriteInvertableBuilder(
            self._segment, get_f, rm_f, MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_key_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements by key range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_key_range(b, start, end, rt),
            lambda rt: MapOperation.remove_by_key_range(b, start, end, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements by rank range.

        Args:
            rank: Rank position (0 = lowest value).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        if count is None:
            get_f = lambda rt: MapOperation.get_by_rank_range_from(b, rank, rt)
            rm_f = lambda rt: MapOperation.remove_by_rank_range_from(b, rank, rt)
        else:
            get_f = lambda rt: MapOperation.get_by_rank_range(b, rank, count, rt)
            rm_f = lambda rt: MapOperation.remove_by_rank_range(b, rank, count, rt)
        return CdtWriteInvertableBuilder(
            self._segment, get_f, rm_f, MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements by value range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_value_range(b, start, end, rt),
            lambda rt: MapOperation.remove_by_value_range(b, start, end, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_key_relative_index_range(
        self, key: Any, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map entries by index range relative to an anchor key.

        Args:
            key: Map key to target.
            index: Index offset from the anchor key.
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_key_relative_index_range(
                b, key, index, count, rt,
            ),
            lambda rt: MapOperation.remove_by_key_relative_index_range(
                b, key, index, count, rt,
            ),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map entries by value rank range relative to an anchor value.

        Args:
            value: Value to match.
            rank: Rank offset from the anchor value.
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            lambda rt: MapOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_key_list(self, keys: List[Any]) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements matching a list of keys.

        Args:
            keys: Map keys to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_key_list(b, keys, rt),
            lambda rt: MapOperation.remove_by_key_list(b, keys, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_list(self, values: List[Any]) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to map elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: MapOperation.get_by_value_list(b, values, rt),
            lambda rt: MapOperation.remove_by_value_list(b, values, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    # -- List navigation (singular -> CdtWriteBuilder) ------------------------

    def on_list_index(
        self, index: int,
        *,
        order: Optional[ListOrderType] = None,
        pad: bool = False,
    ) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a list element by index.

        Args:
            index: List index (0-based, negative counts from end).
            order: If set (or if *pad* is ``True``), use create-on-missing
                list context with this order; when only *pad* is ``True``,
                defaults to :data:`~aerospike_async.ListOrderType.UNORDERED`.
            pad: When using create-on-missing context, allow sparse indexes.

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element(s).

        Example::
            .bin("items").on_list_index(0).set_to("first")
        """
        b = self._bin
        use_create = order is not None or pad
        if use_create:
            eff_order = order if order is not None else ListOrderType.UNORDERED
            to_ctx = lambda: CTX.list_index_create(index, eff_order, pad)
        else:
            to_ctx = lambda: CTX.list_index(index)
        return CdtWriteBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_index(b, index, rt),
            lambda rt: ListOperation.remove_by_index(b, index, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=to_ctx,
        )

    def on_list_rank(self, rank: int) -> CdtWriteBuilder[WriteSegmentBuilder]:
        """Navigate to a list element by rank (0 = lowest value).

        Args:
            rank: Rank position (0 = lowest value).

        Returns:
            :class:`CdtWriteBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_rank(b, rank, rt),
            lambda rt: ListOperation.remove_by_rank(b, rank, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_rank(rank),
        )

    # -- List navigation (invertable -> CdtWriteInvertableBuilder) ------------

    def on_list_value(self, value: Any) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements matching a value.

        Args:
            value: Value to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_value(b, value, rt),
            lambda rt: ListOperation.remove_by_value(b, value, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_value(value),
        )

    def on_list_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements by index range.

        Args:
            index: List index (0-based, negative counts from end).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_index_range(b, index, count, rt),
            lambda rt: ListOperation.remove_by_index_range(b, index, count, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements by rank range.

        Args:
            rank: Rank position (0 = lowest value).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_rank_range(b, rank, count, rt),
            lambda rt: ListOperation.remove_by_rank_range(b, rank, count, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_value_range(
        self, start: Any, end: Any,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements by value range [start, end).

        Args:
            start: Inclusive range start.
            end: Exclusive range end.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_value_range(b, start, end, rt),
            lambda rt: ListOperation.remove_by_value_range(b, start, end, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements by value rank range relative to an anchor value.

        Args:
            value: Value to match.
            rank: Rank offset from the anchor value.
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            lambda rt: ListOperation.remove_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_value_list(self, values: List[Any]) -> CdtWriteInvertableBuilder[WriteSegmentBuilder]:
        """Navigate to list elements matching a list of values.

        Args:
            values: Values to match.

        Returns:
            :class:`CdtWriteInvertableBuilder` for writing the targeted element(s).
        """
        b = self._bin
        return CdtWriteInvertableBuilder(
            self._segment,
            lambda rt: ListOperation.get_by_value_list(b, values, rt),
            lambda rt: ListOperation.remove_by_value_list(b, values, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=None,
        )

    # -- Convenience transitions (delegate to segment) ------------------------

    def bin(self, bin_name: str) -> WriteBinBuilder:
        """Start the next bin operation without leaving the write segment."""
        return WriteBinBuilder(self._segment, bin_name)

    def query(
        self, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> QueryBuilder:
        """Shortcut: finalize write segment and start a read segment."""
        return self._segment.query(arg1, *more_keys)

    def execute_udf(self, *keys: Key) -> "UdfFunctionBuilder":
        """Shortcut: finalize the write segment and chain a UDF on *keys*.

        See :meth:`QueryBuilder.execute_udf` for semantics; the whole
        chain still executes as one batch.
        """
        return self._segment.execute_udf(*keys)

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        return self._segment._start_write_verb(op_type, arg1, *more_keys)

    async def execute(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Shortcut: execute all accumulated specs."""
        return await self._segment.execute(on_error)

    async def stream(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Lazy streaming variant — see :meth:`QueryBuilder.stream`."""
        return await self._segment.stream(on_error)

    @deprecated("Renamed to stream(); execute_stream() will be removed at GA.")
    async def execute_stream(
        self, on_error: OnError | None = None,
    ) -> RecordStream:
        """Deprecated alias for :meth:`stream`.

        :meta private:
        """
        return await self.stream(on_error)


# Bind the bin-builder factory hook now that WriteBinBuilder is defined.
# The base class lives in aerospike_sdk.operations_shared and uses this
# class attribute to instantiate the tier-appropriate (here, tier-neutral)
# WriteBinBuilder without taking a hard reference to a tier module. Both
# async and sync write-segment subclasses inherit this binding.
_WriteSegmentBuilderBase._bin_builder_cls = WriteBinBuilder


class QueryBinBuilder(_WriteVerbs[_WriteSegmentBuilderBase], Generic[_T]):
    """Per-bin reads and CDT navigation for :class:`QueryBuilder` (and sync twin).

    The type parameter is the parent builder type; the parent must implement
    ``add_operation``. Use :meth:`get` for whole-bin reads, :meth:`select_from`
    for expression reads, ``on_map_*`` / ``on_list_*`` for paths, ``hll_*`` /
    ``bit_*`` for HyperLogLog and blob bit reads, then
    :meth:`QueryBuilder.execute`. Write verbs delegate to the parent to chain
    writes after reads.

    Example::

        Read map keys and list size in a single query::

            stream = await (
                session.query(key)
                    .bin("settings").on_map_key("theme").get_values()
                    .bin("tags").list_size()
                    .execute()
            )

    See Also:
        :class:`WriteBinBuilder`: Per-bin write builder.
        :class:`~aerospike_sdk.aio.operations.cdt_read.CdtReadBuilder`: Nested reads.
    """

    __slots__ = ("_parent", "_bin")

    def __init__(self, parent: _T, bin_name: str) -> None:
        self._parent = parent
        self._bin = bin_name

    # -- Simple read ----------------------------------------------------------

    def get(self) -> _T:
        """Include the bin value in the read result.

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`select_from`: Virtual bin from an expression.
        """
        self._parent.add_operation(Operation.get_bin(self._bin))  # type: ignore[union-attr]
        return self._parent

    def map_size(self) -> _T:
        """Return the number of entries in the map."""
        self._parent.add_operation(MapOperation.size(self._bin))  # type: ignore[union-attr]
        return self._parent

    def list_size(self) -> _T:
        """Read list length into the operate/read result."""
        self._parent.add_operation(ListOperation.size(self._bin))  # type: ignore[union-attr]
        return self._parent

    def list_get(self, index: int) -> _T:
        """Read the list element at *index* into the query result.

        Args:
            index: List index (0-based; negative counts from the end).

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`list_get_range`, :meth:`WriteBinBuilder.list_set`
        """
        self._parent.add_operation(ListOperation.get(self._bin, index))  # type: ignore[union-attr]
        return self._parent

    def list_get_range(self, index: int, count: Optional[int] = None) -> _T:
        """Read a contiguous slice of the list starting at *index*.

        Args:
            index: Starting list index.
            count: Number of elements; ``None`` reads through the end.

        Returns:
            The parent builder for chaining.

        See Also:
            :meth:`list_get`
        """
        if count is None:
            op = ListOperation.get_range_from(self._bin, index)
        else:
            op = ListOperation.get_range(self._bin, index, count)
        self._parent.add_operation(op)  # type: ignore[union-attr]
        return self._parent

    # -- HyperLogLog reads ----------------------------------------------------

    def hll_get_count(self) -> _T:
        """Read the estimated HyperLogLog cardinality for this bin.

        The estimate appears under this bin's name in the record returned from
        :meth:`QueryBuilder.execute`. To read during a multi-op write, use
        :meth:`WriteBinBuilder.hll_get_count`.

        Example::
            stream = await session.query(key).bin("visitors").hll_get_count().execute()

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.hll_get_count`
            :meth:`WriteBinBuilder.hll_add`
        """
        self._parent.add_operation(HllOperation.get_count(self._bin))  # type: ignore[union-attr]
        return self._parent

    def hll_describe(self) -> _T:
        """Read index and min-hash bit parameters describing the stored sketch.

        Example::
            stream = await session.query(key).bin("visitors").hll_describe().execute()

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.hll_describe`
        """
        self._parent.add_operation(HllOperation.describe(self._bin))  # type: ignore[union-attr]
        return self._parent

    def hll_get_union(self, hll_list: Sequence[Any]) -> _T:
        """Read the union sketch of this bin and ``hll_list`` without updating storage.

        Example::
            stream = await ( session.query(key).bin("hll").hll_get_union([peer_blob]).execute() )

        Args:
            hll_list: Other HLL blobs to include in the union result.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.hll_get_union`
            :meth:`hll_get_union_count`
        """
        self._parent.add_operation(
            HllOperation.get_union(self._bin, list(hll_list)),
        )  # type: ignore[union-attr]
        return self._parent

    def hll_get_union_count(self, hll_list: Sequence[Any]) -> _T:
        """Read the estimated cardinality of the union with other sketches.

        Example::
            stream = await ( session.query(key).bin("hll").hll_get_union_count([peer_blob]).execute() )

        Args:
            hll_list: Other sketches included in the union estimate.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.hll_get_union_count`
            :meth:`hll_get_intersect_count`
        """
        self._parent.add_operation(
            HllOperation.get_union_count(self._bin, list(hll_list)),
        )  # type: ignore[union-attr]
        return self._parent

    def hll_get_intersect_count(self, hll_list: Sequence[Any]) -> _T:
        """Read the estimated intersection cardinality with other sketches.

        Example::
            stream = await ( session.query(key) .bin("hll") .hll_get_intersect_count([peer_blob]) .execute() )

        Args:
            hll_list: Other sketches included in the intersection estimate.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.hll_get_intersect_count`
            :meth:`hll_get_union_count`
        """
        self._parent.add_operation(
            HllOperation.get_intersect_count(self._bin, list(hll_list)),
        )  # type: ignore[union-attr]
        return self._parent

    def hll_get_similarity(self, hll_list: Sequence[Any]) -> _T:
        """Read Jaccard similarity between this sketch and other sketches.

        Example::
            stream = await ( session.query(key).bin("hll").hll_get_similarity([peer_blob]).execute() )

        Args:
            hll_list: Other sketches to compare.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.hll_get_similarity`
        """
        self._parent.add_operation(
            HllOperation.get_similarity(self._bin, list(hll_list)),
        )  # type: ignore[union-attr]
        return self._parent

    # -- Bit (blob) reads -----------------------------------------------------

    def bit_get(self, bit_offset: int, bit_size: int) -> _T:
        """Read ``bit_size`` bits at ``bit_offset`` as raw bytes.

        Example::
            stream = await session.query(key).bin("blob").bit_get(0, 8).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Number of bits to read.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.bit_get`
            :meth:`bit_get_int`
        """
        self._parent.add_operation(
            BitOperation.get(self._bin, bit_offset, bit_size),
        )  # type: ignore[union-attr]
        return self._parent

    def bit_count(self, bit_offset: int, bit_size: int) -> _T:
        """Count bits set to ``1`` in the given range.

        Example::
            stream = await session.query(key).bin("blob").bit_count(0, 8).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Number of bits to scan.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.bit_count`
        """
        self._parent.add_operation(
            BitOperation.count(self._bin, bit_offset, bit_size),
        )  # type: ignore[union-attr]
        return self._parent

    def bit_lscan(self, bit_offset: int, bit_size: int, value: bool) -> _T:
        """Scan from the left for the first set (``True``) or unset (``False``) bit.

        Example::
            stream = await session.query(key).bin("blob").bit_lscan(0, 8, True).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Number of bits to scan.
            value: ``True`` to find a ``1`` bit, ``False`` to find a ``0`` bit.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`bit_rscan`
            :meth:`WriteBinBuilder.bit_lscan`
        """
        self._parent.add_operation(
            BitOperation.lscan(self._bin, bit_offset, bit_size, value),
        )  # type: ignore[union-attr]
        return self._parent

    def bit_rscan(self, bit_offset: int, bit_size: int, value: bool) -> _T:
        """Scan from the right for the first set (``True``) or unset (``False``) bit.

        Example::
            stream = await session.query(key).bin("blob").bit_rscan(0, 8, False).execute()

        Args:
            bit_offset: Starting bit index.
            bit_size: Number of bits to scan.
            value: ``True`` to find a ``1`` bit, ``False`` to find a ``0`` bit.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`bit_lscan`
            :meth:`WriteBinBuilder.bit_rscan`
        """
        self._parent.add_operation(
            BitOperation.rscan(self._bin, bit_offset, bit_size, value),
        )  # type: ignore[union-attr]
        return self._parent

    def bit_get_int(self, bit_offset: int, bit_size: int, signed: bool) -> _T:
        """Decode an integer from ``bit_size`` bits at ``bit_offset``.

        Example::
            stream = await ( session.query(key).bin("blob").bit_get_int(0, 16, False).execute() )

        Args:
            bit_offset: Starting bit index.
            bit_size: Width of the integer in bits.
            signed: ``True`` for two's-complement signed decoding.

        Returns:
            The parent query builder for chaining.

        See Also:
            :meth:`WriteBinBuilder.bit_get_int`
            :meth:`WriteBinBuilder.bit_set_int`
        """
        self._parent.add_operation(
            BitOperation.get_int(self._bin, bit_offset, bit_size, signed),
        )  # type: ignore[union-attr]
        return self._parent

    # -- Server-side string read operations (server 8.1.3+) -------------------
    #
    # See :class:`WriteBinBuilder` for the parallel write-side surface +
    # modify-op family. Only reads make sense on a query builder; users
    # writing AND reading in the same operate should use ``WriteBinBuilder``
    # via ``session.upsert(key).bin(...).str_*()``.

    def str_strlen(self) -> _T:
        """Read the Unicode codepoint count of this string bin (NOT byte count)."""
        self._parent.add_operation(StringOperation.strlen(self._bin))  # type: ignore[union-attr]
        return self._parent

    def str_substr(self, start: int, end: Optional[int] = None) -> _T:
        """Read codepoints from ``start`` to ``end`` (exclusive); or to bin end when ``end=None``."""
        self._parent.add_operation(  # type: ignore[union-attr]
            StringOperation.substr(self._bin, start, end),
        )
        return self._parent

    def str_char_at(self, index: int) -> _T:
        """Read the codepoint at ``index`` as a one-codepoint string."""
        self._parent.add_operation(StringOperation.char_at(self._bin, index))  # type: ignore[union-attr]
        return self._parent

    def str_find(self, needle: str, occurrence: Optional[int] = None) -> _T:
        """Read the codepoint index of the first (or N-th) occurrence of ``needle``; -1 if absent."""
        self._parent.add_operation(  # type: ignore[union-attr]
            StringOperation.find(self._bin, needle, occurrence),
        )
        return self._parent

    def str_contains(self, needle: str) -> _T:
        """Read a bool: ``True`` iff this bin contains ``needle`` as a substring."""
        self._parent.add_operation(StringOperation.contains(self._bin, needle))  # type: ignore[union-attr]
        return self._parent

    def str_starts_with(self, prefix: str) -> _T:
        """Read a bool: ``True`` iff this bin starts with ``prefix``."""
        self._parent.add_operation(StringOperation.starts_with(self._bin, prefix))  # type: ignore[union-attr]
        return self._parent

    def str_ends_with(self, suffix: str) -> _T:
        """Read a bool: ``True`` iff this bin ends with ``suffix``."""
        self._parent.add_operation(StringOperation.ends_with(self._bin, suffix))  # type: ignore[union-attr]
        return self._parent

    def str_to_integer(self) -> _T:
        """Parse this bin as an ``int64``. Server returns PARAMETER_ERROR on bad input."""
        self._parent.add_operation(StringOperation.to_integer(self._bin))  # type: ignore[union-attr]
        return self._parent

    def str_to_double(self) -> _T:
        """Parse this bin as a ``float64``. Server returns PARAMETER_ERROR on bad input."""
        self._parent.add_operation(StringOperation.to_double(self._bin))  # type: ignore[union-attr]
        return self._parent

    def str_byte_length(self) -> _T:
        """Read the UTF-8 byte count of this string bin."""
        self._parent.add_operation(StringOperation.byte_length(self._bin))  # type: ignore[union-attr]
        return self._parent

    def str_is_numeric(self, numeric_type: Optional[StringNumericType] = None) -> _T:
        """Read a bool: ``True`` iff this bin parses as a number.

        Pass ``numeric_type`` to restrict to ``StringNumericType.INT`` /
        ``StringNumericType.FLOAT``; default ``ANY`` accepts either.
        """
        self._parent.add_operation(  # type: ignore[union-attr]
            StringOperation.is_numeric(self._bin, numeric_type),
        )
        return self._parent

    def str_is_upper(self) -> _T:
        """Read a bool: ``True`` iff every cased codepoint is uppercase."""
        self._parent.add_operation(StringOperation.is_upper(self._bin))  # type: ignore[union-attr]
        return self._parent

    def str_is_lower(self) -> _T:
        """Read a bool: ``True`` iff every cased codepoint is lowercase."""
        self._parent.add_operation(StringOperation.is_lower(self._bin))  # type: ignore[union-attr]
        return self._parent

    def str_to_blob(self) -> _T:
        """Read the UTF-8 bytes of this string bin as a blob."""
        self._parent.add_operation(StringOperation.to_blob(self._bin))  # type: ignore[union-attr]
        return self._parent

    def str_split(self, separator: Optional[str] = None) -> _T:
        """Read a list of strings.

        With ``separator=None`` returns one element per codepoint; with a
        separator returns the substrings between separator occurrences
        (singleton list when separator is absent).
        """
        self._parent.add_operation(StringOperation.split(self._bin, separator))  # type: ignore[union-attr]
        return self._parent

    def str_b64_decode(self) -> _T:
        """Treat this bin as base64-encoded text; return the decoded bytes as a blob."""
        self._parent.add_operation(StringOperation.b64_decode(self._bin))  # type: ignore[union-attr]
        return self._parent

    def str_regex_compare(self, pattern: str, flags: int | StringWriteFlags | StringRegexFlags = 0) -> _T:
        """Read a bool: ``True`` iff the ICU regex ``pattern`` matches this bin.

        ``flags`` is an OR-combined :class:`StringRegexFlags` bitmask.
        """
        self._parent.add_operation(  # type: ignore[union-attr]
            StringOperation.regex_compare(self._bin, pattern, int(flags)),
        )
        return self._parent

    def str_to_string(self) -> _T:
        """Convert a non-string bin to its string representation.

        Accepts ``int`` / ``float`` / ``string`` / ``blob`` source types;
        any other type returns ``BIN_TYPE_ERROR`` from the server.
        """
        self._parent.add_operation(StringOperation.to_string(self._bin))  # type: ignore[union-attr]
        return self._parent

    # -- Expression read ------------------------------------------------------

    def select_from(
        self,
        expression: Union[str, FilterExpression],
        *,
        ignore_eval_failure: bool = False,
    ) -> _T:
        """Read a computed value into this bin using an AEL expression.

        The result appears as a virtual bin in the returned record.

        Args:
            expression: AEL string or pre-built FilterExpression.
            ignore_eval_failure: If True, silently return None when the
                expression cannot be evaluated (e.g. missing bin).

        Returns:
            The parent builder for method chaining.
        """
        flags = ExpReadFlags.EVAL_NO_FAIL if ignore_eval_failure else ExpReadFlags.DEFAULT
        if isinstance(expression, str):
            materialize = getattr(self._parent, "_filter_expression_from_ael", None)
            if materialize is not None:
                expr = materialize(expression)
            else:
                expr = filter_expression_from_ael_string(
                    expression,
                    supports_server_compiled_ael=getattr(
                        self._parent, "_supports_server_compiled_ael", False,
                    ),
                )
        else:
            expr = expression
        self._parent.add_operation(ExpOperation.read(self._bin, expr, flags))  # type: ignore[union-attr]
        return self._parent

    # -- Write transitions (delegate to parent) -------------------------------

    def _start_write_verb(
        self, op_type: str, arg1: Union[Key, List[Key]], *more_keys: Key,
    ) -> WriteSegmentBuilder:
        return self._parent._start_write_verb(op_type, arg1, *more_keys)  # type: ignore[union-attr]

    # -- Map navigation (singular -> CdtReadBuilder) --------------------------

    def on_map_index(self, index: int) -> CdtReadBuilder[_T]:
        """Navigate to a map element by index.

        Args:
            index: List index (0-based, negative counts from end).

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element(s).
        """
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_index(b, index, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_index(index),
        )

    def on_map_key(
        self, key: Any, *, create_type: Optional[MapOrder] = None,
    ) -> CdtReadBuilder[_T]:
        """Navigate to a map element by key.

        Args:
            key: Map key to target.
            create_type: If set, use a create-on-missing context for this key
                with the given map key order.

        Returns:
            :class:`CdtReadBuilder` for reading the targeted element(s).
        """
        b = self._bin
        if create_type is not None:
            to_ctx = lambda: CTX.map_key_create(key, create_type)
        else:
            to_ctx = lambda: CTX.map_key(key)
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key(b, key, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=to_ctx,
        )

    def on_map_rank(self, rank: int) -> CdtReadBuilder[_T]:
        """Navigate to a map element by rank (0 = lowest value)."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_rank(b, rank, rt),
            MapReturnType, is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_rank(rank),
        )

    # -- Map navigation (singular invertable -> CdtReadInvertableBuilder) -----

    def on_map_value(self, value: Any) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements matching a value."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_value(b, value, rt),
            MapReturnType,
            is_map=True,
            bin_name=b, to_ctx=lambda: CTX.map_value(value),
        )

    # -- Map navigation (range -> CdtReadInvertableBuilder) -------------------

    def on_map_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements by index range."""
        b = self._bin
        if count is None:
            factory = lambda rt: MapOperation.get_by_index_range_from(b, index, rt)
        else:
            factory = lambda rt: MapOperation.get_by_index_range(b, index, count, rt)
        return CdtReadInvertableBuilder(
            self._parent, factory, MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_key_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements by key range [start, end)."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key_range(b, start, end, rt),
            MapReturnType,
            is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements by rank range."""
        b = self._bin
        if count is None:
            factory = lambda rt: MapOperation.get_by_rank_range_from(b, rank, rt)
        else:
            factory = lambda rt: MapOperation.get_by_rank_range(b, rank, count, rt)
        return CdtReadInvertableBuilder(
            self._parent, factory, MapReturnType, is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements by value range [start, end)."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_value_range(b, start, end, rt),
            MapReturnType,
            is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_key_relative_index_range(
        self, key: Any, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map entries by index range relative to an anchor key.

        Args:
            key: Map key to target.
            index: List index (0-based, negative counts from end).
            count: Maximum entries to select; ``None`` for all remaining.

        Returns:
            :class:`CdtReadInvertableBuilder` for reading the targeted element(s).
        """
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key_relative_index_range(
                b, key, index, count, rt,
            ),
            MapReturnType,
            is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map entries by value rank range relative to an anchor value."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            MapReturnType,
            is_map=True,
            bin_name=b, to_ctx=None,
        )

    # -- Map navigation (list selectors -> CdtReadInvertableBuilder) ----------

    def on_map_key_list(self, keys: List[Any]) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements matching a list of keys."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_key_list(b, keys, rt),
            MapReturnType,
            is_map=True,
            bin_name=b, to_ctx=None,
        )

    def on_map_value_list(self, values: List[Any]) -> CdtReadInvertableBuilder[_T]:
        """Navigate to map elements matching a list of values."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: MapOperation.get_by_value_list(b, values, rt),
            MapReturnType,
            is_map=True,
            bin_name=b, to_ctx=None,
        )

    # -- List navigation (singular -> CdtReadBuilder) -------------------------

    def on_list_index(
        self, index: int,
        *,
        order: Optional[ListOrderType] = None,
        pad: bool = False,
    ) -> CdtReadBuilder[_T]:
        """Navigate to a list element by index.

        Args:
            order: If set (or if *pad* is ``True``), use create-on-missing
                list context with this order; when only *pad* is ``True``,
                defaults to :data:`~aerospike_async.ListOrderType.UNORDERED`.
            pad: When using create-on-missing context, allow sparse indexes.

        Example::
            .bin("items").on_list_index(-1).get_values()
        """
        b = self._bin
        use_create = order is not None or pad
        if use_create:
            eff_order = order if order is not None else ListOrderType.UNORDERED
            to_ctx = lambda: CTX.list_index_create(index, eff_order, pad)
        else:
            to_ctx = lambda: CTX.list_index(index)
        return CdtReadBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_index(b, index, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=to_ctx,
        )

    def on_list_rank(self, rank: int) -> CdtReadBuilder[_T]:
        """Navigate to a list element by rank (0 = lowest value)."""
        b = self._bin
        return CdtReadBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_rank(b, rank, rt),
            ListReturnType, is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_rank(rank),
        )

    # -- List navigation (singular invertable -> CdtReadInvertableBuilder) ----

    def on_list_value(self, value: Any) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements matching a value."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_value(b, value, rt),
            ListReturnType,
            is_map=False,
            bin_name=b, to_ctx=lambda: CTX.list_value(value),
        )

    # -- List navigation (range -> CdtReadInvertableBuilder) ------------------

    def on_list_index_range(
        self, index: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements by index range."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_index_range(b, index, count, rt),
            ListReturnType,
            is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_rank_range(
        self, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements by rank range."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_rank_range(b, rank, count, rt),
            ListReturnType,
            is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_value_range(
        self, start: Any, end: Any,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements by value range [start, end)."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_value_range(b, start, end, rt),
            ListReturnType,
            is_map=False,
            bin_name=b, to_ctx=None,
        )

    def on_list_value_relative_rank_range(
        self, value: Any, rank: int, count: Optional[int] = None,
    ) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements by value rank range relative to an anchor value."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_value_relative_rank_range(
                b, value, rank, count, rt,
            ),
            ListReturnType,
            is_map=False,
            bin_name=b, to_ctx=None,
        )

    # -- List navigation (list selector -> CdtReadInvertableBuilder) ----------

    def on_list_value_list(self, values: List[Any]) -> CdtReadInvertableBuilder[_T]:
        """Navigate to list elements matching a list of values."""
        b = self._bin
        return CdtReadInvertableBuilder(
            self._parent,
            lambda rt: ListOperation.get_by_value_list(b, values, rt),
            ListReturnType,
            is_map=False,
            bin_name=b, to_ctx=None,
        )
