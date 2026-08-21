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

"""Typed exceptions for the SDK client.

Subclasses of :class:`AerospikeError` mirror common server and client outcomes so
callers can handle failures selectively (for example ``except GenerationError``)
instead of comparing result codes everywhere.

At public boundaries, errors from the underlying async client are normalized with
:func:`_convert_pac_exception`. Callers should chain causes explicitly:
``raise _convert_pac_exception(exc) from exc``.
"""

from __future__ import annotations

from aerospike_async.exceptions import (
    AerospikeError as PacAerospikeError,
    ConnectionError as PacConnectionError,
    InvalidNodeError as PacInvalidNodeError,
    MaxErrorRate as PacMaxErrorRate,
    ServerError as PacServerError,
    TimeoutError as PacTimeoutError,
    UDFBadResponse as PacUDFBadResponse,
)
# Re-exported for callers that need the raw server result code or the
# PAC-level error types without importing aerospike_async directly.
from aerospike_async import SubCode
from aerospike_async.exceptions import ResultCode
from aerospike_async.exceptions import SecurityNotEnabled as SecurityNotEnabled
from aerospike_async.exceptions import ServerError as ServerError
from aerospike_async.exceptions import ValueError as PacValueError  # noqa: F401


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class AerospikeError(Exception):
    """Base class for SDK failures.

    Raised directly when no more specific subclass applies, including
    unmapped server result codes (see :func:`_result_code_to_exception`).
    Prefer catching concrete subclasses when you need targeted handling, and
    fall back to this type for all other Aerospike-related errors.

    Attributes:
        result_code: Server :class:`~aerospike_async.exceptions.ResultCode` when
            the failure came from a result code; ``None`` for purely client-side
            issues (for example connection setup).
        in_doubt: ``True`` when a write may have completed on the server despite
            the error; safe retry usually requires a read-verify strategy.
        sub_code: Server-supplied numeric subcode refining ``result_code`` when
            extended error detail was requested (see ``error_detail_verbosity``).
            ``None`` means no detail was returned (verbosity off, or a
            client-side failure); ``0`` (``SubCode.NONE``) means detail was
            returned but this failure has no finer subcode than its result code.
            Only meaningful together with ``result_code`` — subcode values are
            scoped to their parent code, not globally unique.
        server_message: Human-readable message from the server when detail was
            requested at message-level verbosity or higher; ``None`` when no
            detail was returned. May be present even when ``sub_code`` is ``0``.
        hint: Client-side guidance for result codes whose cause is a common,
            recognizable misconfiguration -- appended to the message and kept
            here separately so callers can log or suppress it independently.
            ``None`` when the SDK has nothing to add beyond what the server
            reported. Unlike ``server_message`` this is generated locally and
            never travels the wire.
        exp_trace: Structured expression trace the server attaches to
            expression-build failures at trace-level verbosity; ``None``
            otherwise. Surfaced as an opaque passthrough of the underlying
            client value.
        node: Cluster node the failing attempt targeted, when the retry loop
            recorded one; ``None`` for failures that never reached node
            selection (for example, argument validation).
        iteration: Number of attempts made before the command failed, when
            recorded; ``None`` when the failure precedes the retry loop.
        sub_exceptions: Exceptions from prior retry attempts of the same
            command, oldest first, each converted to this hierarchy. Empty
            when the command was not retried.
        base_message: The failure message without the retry-context decoration
            the full message carries; ``None`` when no undecorated form was
            recorded.

    Example::
        try:
            stream = await session.query(key).bins(["x"]).execute()
            await stream.first_or_raise()
        except AerospikeError as err:
            code = err.result_code
            ...

    See Also:
        :func:`_result_code_to_exception`: Maps result codes to this type or a
            subclass.
    """

    def __init__(
        self,
        message: str = "",
        *,
        result_code: ResultCode | None = None,
        in_doubt: bool = False,
        sub_code: int | None = None,
        server_message: str | None = None,
        exp_trace: object | None = None,
        node: str | None = None,
        iteration: int | None = None,
        sub_exceptions: tuple[AerospikeError, ...] = (),
        base_message: str | None = None,
        hint: str | None = None,
    ) -> None:
        super().__init__(message)
        self.result_code = result_code
        self.in_doubt = in_doubt
        self.sub_code = sub_code
        self.server_message = server_message
        self.exp_trace = exp_trace
        self.node = node
        self.iteration = iteration
        self.sub_exceptions = sub_exceptions
        self.base_message = base_message
        self.hint = hint


# ---------------------------------------------------------------------------
# Timeout / connectivity
# ---------------------------------------------------------------------------

class TimeoutError(AerospikeError):
    """Raised when an operation exceeds a client or server timeout.

    Covers socket-level timeouts and server-reported timeout result codes.
    This type shares a name with Python's built-in :exc:`TimeoutError`; always
    import it from :mod:`aerospike_sdk` or this module when handling SDK
    client timeouts.

    Attributes:
        client: ``True`` when the client's own deadline fired (socket or
            total timeout expired locally); ``False`` when the server
            reported the timeout result code. A client timeout says nothing
            about server-side progress — pair it with ``in_doubt`` when
            deciding whether a write needs read-verification.
        result_code: Set when the server returned a timeout-related code;
            otherwise often ``None`` for client-side timeouts.

    See Also:
        :class:`ConnectionError`: Cluster reachability rather than deadline
            exceeded.

    Example::
        try:
            await stream.first_or_raise()
        except TimeoutError as err:
            if err.client:
                ...  # local deadline; the server may still be working
            else:
                ...  # the server itself timed the operation out
    """

    def __init__(self, message: str = "", *, client: bool = False, **kwargs: object) -> None:
        super().__init__(message, **kwargs)  # type: ignore[arg-type]
        self.client = client


class ConnectionError(AerospikeError):
    """Raised when the client cannot establish or keep a cluster connection.

    Typical causes include refused sockets, TLS handshake failure, or loss of
    connectivity mid-flight. Distinct from :class:`TimeoutError`, which signals
    a deadline rather than an immediate transport failure.

    Attributes:
        result_code: Usually ``None`` because the failure occurs before a server
            result code is available.

    Example::
        try:
            async with Client(...) as client:
                ...
        except ConnectionError:
            ...  # cluster unreachable
    """


class InvalidNodeError(AerospikeError):
    """Raised when the chosen node is unknown, wrong role, or not usable.

    Use for diagnosing cluster topology or client routing issues rather than
    application-level data errors.

    Attributes:
        result_code: Usually ``None``.
    """


class InvalidNamespaceError(AerospikeError):
    """Raised when the namespace is missing or not defined on the cluster.

    Often indicates a configuration mismatch between application and cluster.

    Attributes:
        result_code: Typically ``ResultCode.INVALID_NAMESPACE`` when mapped from
            a server response.

    Example::
        try:
            await session.query(bad_ds).execute()
        except InvalidNamespaceError:
            ...  # namespace not configured on cluster
    """


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------

class SecurityError(AerospikeError):
    """Base class for authentication, authorization, and security policy errors.

    Several distinct server result codes collapse to this type when they do not
    warrant a dedicated subclass. Catch :class:`AuthenticationError` or
    :class:`AuthorizationError` first if you need finer granularity.

    Attributes:
        result_code: The security-related code returned by the server, when
            applicable.
    """


class AuthenticationError(SecurityError):
    """Raised when credentials are rejected or the session is not authenticated.

    Examples include invalid user, expired password, or not authenticated
    responses from the server.

    See Also:
        :class:`AuthorizationError`: Valid identity but disallowed operation.
    """


class AuthorizationError(SecurityError):
    """Raised when the authenticated principal may not perform the operation.

    Distinct from :class:`AuthenticationError`, which indicates identity or
    credential problems rather than policy denial.
    """


# ---------------------------------------------------------------------------
# Data integrity
# ---------------------------------------------------------------------------

class GenerationError(AerospikeError):
    """Raised when a write fails due to a record generation mismatch.

    The record was modified since it was read, or the expected generation did
    not match. Retrying blindly usually requires re-reading the record and
    reapplying the logical update.

    Attributes:
        result_code: Typically ``ResultCode.GENERATION_ERROR``.

    Example::

        try:
            await (
                session.upsert(key)
                    .put({"x": 1})
                    .ensure_generation_is(3)
                    .execute()
            )
        except GenerationError:
            ...  # record was modified by another writer

    See Also:
        :meth:`~aerospike_sdk.aio.session.Session.upsert`: Common write path
            that can enforce generations on builders.

    """


class QuotaError(AerospikeError):
    """Raised when a server-side quota or limit is exceeded.

    Handling is usually operational (throttle, increase limits, or partition
    workload) rather than a single-record retry.
    """


class SerializationError(AerospikeError):
    """Raised when a value cannot be encoded for the wire or decoded from it.

    Check bin types and application serializers when this appears on puts or
    reads.
    """


# ---------------------------------------------------------------------------
# Record and key
# ---------------------------------------------------------------------------

class RecordNotFoundError(AerospikeError):
    """Raised when no record exists for the requested key.

    Surfaces on reads and on writes that require an existing record (update,
    replace, touch), and on batch/point reads when
    :meth:`~aerospike_sdk.aio.operations.query.QueryBuilder.include_missing_keys`
    promotes a not-found key to an error result.

    Attributes:
        result_code: ``ResultCode.KEY_NOT_FOUND_ERROR``.

    Example::

        try:
            await session.update(key).put({"count": 1}).execute_or_raise()
        except RecordNotFoundError:
            ...  # nothing to update
    """


class RecordExistsError(AerospikeError):
    """Raised when a create-only write finds the key already present.

    Produced by insert-style (create-only) writes when a record already exists
    for the key; the idiomatic way to detect a duplicate on insert.

    Attributes:
        result_code: ``ResultCode.KEY_EXISTS_ERROR``.

    Example::

        try:
            await session.insert(key).put({"count": 1}).execute_or_raise()
        except RecordExistsError:
            ...  # key already taken
    """


class RecordTooBigError(AerospikeError):
    """Raised when a record exceeds the server's configured size limit.

    Reduce the bin payload or raise the namespace's size ceiling; retrying the
    same record unchanged will fail again.

    Attributes:
        result_code: ``ResultCode.RECORD_TOO_BIG``.
    """


class FilteredOutError(AerospikeError):
    """Raised when an operation is skipped because its filter expression was false.

    Only surfaces as an error when
    :meth:`~aerospike_sdk.aio.operations.query.QueryBuilder.fail_on_filtered_out`
    is set; otherwise a filtered record is silently omitted from results.

    Attributes:
        result_code: ``ResultCode.FILTERED_OUT``.

    See Also:
        :class:`RecordNotFoundError`: A missing record rather than a filter miss.
    """


# ---------------------------------------------------------------------------
# Bin
# ---------------------------------------------------------------------------

class BinError(AerospikeError):
    """Base class for bin-level failures.

    Covers an over-long bin name and serves as the parent for the more specific
    bin errors below; catch this to handle any bin-scoped failure at once.

    Attributes:
        result_code: A bin-related code such as ``ResultCode.BIN_NAME_TOO_LONG``.
    """


class BinExistsError(BinError):
    """Raised when a create-only bin operation finds the bin already present.

    Attributes:
        result_code: ``ResultCode.BIN_EXISTS_ERROR``.
    """


class BinNotFoundError(BinError):
    """Raised when an update-only bin operation targets a missing bin.

    Attributes:
        result_code: ``ResultCode.BIN_NOT_FOUND``.
    """


class BinTypeError(BinError):
    """Raised when an operation is incompatible with the bin's stored type.

    For example, arithmetic on a bin holding a string. Distinct from
    :class:`BinOpInvalidError`, which concerns the bin's current *value*.

    Attributes:
        result_code: ``ResultCode.BIN_TYPE_ERROR``.
    """


class BinOpInvalidError(BinError):
    """Raised when an operation cannot be applied to the bin's current value.

    For example, a list operation against a bin that does not hold a list.

    Attributes:
        result_code: ``ResultCode.OP_NOT_APPLICABLE``.
    """


# ---------------------------------------------------------------------------
# Collection (CDT) element
# ---------------------------------------------------------------------------

class ElementError(AerospikeError):
    """Base class for list/map element-level failures inside CDT operations."""


class ElementNotFoundError(ElementError):
    """Raised when a list index or map key is absent under an update-only mode.

    Attributes:
        result_code: ``ResultCode.ELEMENT_NOT_FOUND``.
    """


class ElementExistsError(ElementError):
    """Raised when a list index or map key is present under a create-only mode.

    Attributes:
        result_code: ``ResultCode.ELEMENT_EXISTS``.
    """


# ---------------------------------------------------------------------------
# Capacity / resource exhaustion
# ---------------------------------------------------------------------------

class CapacityError(AerospikeError):
    """Raised on server or client resource exhaustion.

    Covers server memory pressure, device I/O overload, and full server-side
    queues. Handling is operational (throttle, retry with backoff, or add
    capacity) rather than a single-record retry.

    Attributes:
        result_code: A capacity-related code such as
            ``ResultCode.SERVER_MEM_ERROR`` or ``ResultCode.DEVICE_OVERLOAD``.
    """


class KeyBusyError(CapacityError):
    """Raised when too many concurrent operations target one record (hot key).

    A transient contention signal; a brief backoff before retry usually clears
    it. Catch :class:`CapacityError` to handle it alongside other exhaustion.

    Attributes:
        result_code: ``ResultCode.KEY_BUSY``.
    """


# ---------------------------------------------------------------------------
# Secondary index
# ---------------------------------------------------------------------------

class SecondaryIndexError(AerospikeError):
    """Base class for secondary-index management failures.

    Named ``SecondaryIndexError`` rather than ``IndexError`` so it does not
    shadow the built-in :exc:`IndexError`. Covers index creation limits,
    out-of-memory, and not-readable states; catch this for any index failure.

    Attributes:
        result_code: An index-related code such as ``ResultCode.INDEX_OOM``.
    """


class IndexNotFoundError(SecondaryIndexError):
    """Raised when a referenced secondary index does not exist.

    Attributes:
        result_code: ``ResultCode.INDEX_NOT_FOUND``.
    """


class IndexAlreadyExistsError(SecondaryIndexError):
    """Raised when creating a secondary index that already exists.

    Attributes:
        result_code: ``ResultCode.INDEX_FOUND``.
    """


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------

class QueryError(AerospikeError):
    """Base class for query and scan execution failures.

    Covers generic query errors, aborted scans, and query network failures.
    Timeout-flavored query codes raise :class:`TimeoutError` instead, and a
    full query queue raises :class:`CapacityError` — catch this class for
    failures of the query itself.

    Attributes:
        result_code: A query-related code such as ``ResultCode.QUERY_GENERIC``
            or ``ResultCode.SCAN_ABORT``.
    """


class QueryTerminatedError(QueryError):
    """Raised when a query stops early (aborted, canceled, or server-terminated).

    Partial rows may already have been delivered on streaming paths; this error
    represents the overall query outcome, not a single-key failure inside a
    batch. Catch :class:`QueryError` to handle it alongside other query
    failures.

    Attributes:
        result_code: May include ``ResultCode.QUERY_ABORTED`` or related codes.
    """


class UdfError(AerospikeError):
    """Raised when a server-side UDF execution reports a failure.

    The server ran the registered function and it returned an error (or the
    response was not a valid UDF result). The failure detail is in the
    message; check the UDF's own error string when debugging Lua code.

    Attributes:
        result_code: ``ResultCode.UDF_BAD_RESPONSE``.

    Example::

        try:
            await session.execute_udf(key).function("mod", "fn").execute()
        except UdfError:
            ...  # the function itself failed on the server
    """


class BatchError(AerospikeError):
    """Raised when the server's batch subsystem rejects a batch request.

    Signals a batch-level condition (for example, batch disabled on the
    server) rather than a per-key failure — per-key outcomes surface as
    :class:`~aerospike_sdk.record_result.RecordResult` errors in the stream.
    Full batch queues raise :class:`CapacityError` instead.

    Attributes:
        result_code: A batch-related code such as ``ResultCode.BATCH_DISABLED``.
    """


class BackoffError(AerospikeError):
    """Raised when the server signals rate limiting or requires backoff.

    Callers may retry after a delay or reduce request pressure.
    """


class MaxErrorRate(BackoffError):
    """Raised when the client's per-node circuit breaker trips.

    The breaker is governed by ``Client(...)``'s ``max_error_rate`` and
    ``error_rate_window`` keywords (or :class:`~aerospike_async.ClientPolicy`
    fields of the same name). Once a node's error count crosses
    ``max_error_rate`` within the current window, subsequent commands routed
    to that node fail fast with this exception until the next window resets.
    Catch :class:`BackoffError` to handle this together with other server-side
    backoff signals.

    Example::

        try:
            await session.read(key).execute()
        except MaxErrorRate:
            ...  # node is in cooldown; route around it or wait
    """


class TransactionError(AerospikeError):
    """Base class for multi-record transaction (MRT) failures.

    Covers a transaction that is blocked, expired, aborted, already committed,
    version-mismatched, or has exceeded its write limit. :class:`CommitError`
    is the commit-phase specialization; catch this base to handle any MRT
    failure at once.

    Attributes:
        result_code: An MRT-related code such as ``ResultCode.MRT_EXPIRED``.
    """


class CommitError(TransactionError):
    """Raised when a multi-record transaction commit does not complete successfully.

    Additional fields expose verify or roll-forward details when the underlying
    client provides them.

    Attributes:
        commit_error_type: Implementation-defined label for the failure phase,
            if available.
        verify_records: Verify-phase records or summaries, if available.
        roll_records: Roll-forward or rollback-phase records, if available.
        result_code: Server or client result associated with the commit, when set.
        in_doubt: Inherited; ``True`` when commit outcome may be ambiguous on
            the server.
    """

    def __init__(
        self,
        message: str = "",
        *,
        commit_error_type: object | None = None,
        verify_records: list | None = None,
        roll_records: list | None = None,
        result_code: ResultCode | None = None,
        in_doubt: bool = False,
        **kwargs: object,
    ) -> None:
        # Detail and retry-context fields (sub_code, server_message,
        # exp_trace, node, iteration, ...) pass through to the base so a
        # commit failure is not the one place they get dropped.
        super().__init__(
            message, result_code=result_code, in_doubt=in_doubt,
            **kwargs,  # type: ignore[arg-type]
        )
        self.commit_error_type = commit_error_type
        self.verify_records = verify_records
        self.roll_records = roll_records


# ---------------------------------------------------------------------------
# Factory: ResultCode -> typed exception
# ---------------------------------------------------------------------------

# Server codes without a meaningfully distinct handling story (for example
# INVALID_GEOJSON, PARAMETER_ERROR) fall through to AerospikeError; client-side
# failures never reach this map (they convert by PAC exception type instead).

_RC_TO_TYPE: dict[ResultCode, type[AerospikeError]] = {
    ResultCode.GENERATION_ERROR: GenerationError,
    # Authentication (identity / credential problems)
    ResultCode.NOT_AUTHENTICATED: AuthenticationError,
    ResultCode.INVALID_USER: AuthenticationError,
    ResultCode.INVALID_PASSWORD: AuthenticationError,
    ResultCode.INVALID_CREDENTIAL: AuthenticationError,
    ResultCode.EXPIRED_PASSWORD: AuthenticationError,
    # Authorization (valid identity, disallowed operation)
    ResultCode.ROLE_VIOLATION: AuthorizationError,
    ResultCode.NOT_ALLOWLISTED: AuthorizationError,
    # Security (catch-all for remaining security codes)
    ResultCode.ILLEGAL_STATE: SecurityError,
    ResultCode.USER_ALREADY_EXISTS: SecurityError,
    ResultCode.FORBIDDEN_PASSWORD: SecurityError,
    ResultCode.SECURITY_NOT_SUPPORTED: SecurityError,
    ResultCode.SECURITY_NOT_ENABLED: SecurityError,
    ResultCode.SECURITY_SCHEME_NOT_SUPPORTED: SecurityError,
    ResultCode.EXPIRED_SESSION: SecurityError,
    ResultCode.INVALID_ROLE: SecurityError,
    ResultCode.ROLE_ALREADY_EXISTS: SecurityError,
    ResultCode.INVALID_PRIVILEGE: SecurityError,
    ResultCode.INVALID_ALLOWLIST: SecurityError,
    # Quota
    ResultCode.QUOTA_EXCEEDED: QuotaError,
    ResultCode.QUOTAS_NOT_ENABLED: QuotaError,
    ResultCode.INVALID_QUOTA: QuotaError,
    # Timeout
    ResultCode.TIMEOUT: TimeoutError,
    ResultCode.QUERY_TIMEOUT: TimeoutError,
    # Namespace
    ResultCode.INVALID_NAMESPACE: InvalidNamespaceError,
    # Query / scan
    ResultCode.QUERY_ABORTED: QueryTerminatedError,
    ResultCode.QUERY_GENERIC: QueryError,
    ResultCode.SCAN_ABORT: QueryError,
    ResultCode.QUERY_NETIO_ERR: QueryError,
    ResultCode.QUERY_DUPLICATE: QueryError,
    # UDF
    ResultCode.UDF_BAD_RESPONSE: UdfError,
    # Batch subsystem (full queues are capacity, below)
    ResultCode.BATCH_DISABLED: BatchError,
    # Record / key
    ResultCode.KEY_NOT_FOUND_ERROR: RecordNotFoundError,
    ResultCode.KEY_EXISTS_ERROR: RecordExistsError,
    ResultCode.RECORD_TOO_BIG: RecordTooBigError,
    ResultCode.FILTERED_OUT: FilteredOutError,
    # Bin
    ResultCode.BIN_NAME_TOO_LONG: BinError,
    ResultCode.BIN_EXISTS_ERROR: BinExistsError,
    ResultCode.BIN_NOT_FOUND: BinNotFoundError,
    ResultCode.BIN_TYPE_ERROR: BinTypeError,
    ResultCode.OP_NOT_APPLICABLE: BinOpInvalidError,
    # Collection (CDT) element
    ResultCode.ELEMENT_NOT_FOUND: ElementNotFoundError,
    ResultCode.ELEMENT_EXISTS: ElementExistsError,
    # Capacity / resource exhaustion
    ResultCode.SERVER_MEM_ERROR: CapacityError,
    ResultCode.DEVICE_OVERLOAD: CapacityError,
    ResultCode.QUERY_QUEUE_FULL: CapacityError,
    ResultCode.BATCH_QUEUES_FULL: CapacityError,
    ResultCode.BATCH_MAX_REQUESTS_EXCEEDED: CapacityError,
    ResultCode.KEY_BUSY: KeyBusyError,
    ResultCode.XDR_KEY_BUSY: KeyBusyError,
    # Secondary index
    ResultCode.INDEX_NOT_FOUND: IndexNotFoundError,
    ResultCode.INDEX_FOUND: IndexAlreadyExistsError,
    ResultCode.INDEX_OOM: SecondaryIndexError,
    ResultCode.INDEX_MAX_COUNT: SecondaryIndexError,
    ResultCode.INDEX_NAME_MAX_LEN: SecondaryIndexError,
    ResultCode.INDEX_NOT_READABLE: SecondaryIndexError,
    ResultCode.INDEX_GENERIC: SecondaryIndexError,
    # Transactions (multi-record)
    ResultCode.MRT_BLOCKED: TransactionError,
    ResultCode.MRT_EXPIRED: TransactionError,
    ResultCode.MRT_VERSION_MISMATCH: TransactionError,
    ResultCode.MRT_TOO_MANY_WRITES: TransactionError,
    ResultCode.MRT_ABORTED: TransactionError,
    ResultCode.MRT_COMMITTED: TransactionError,
    ResultCode.MRT_ALREADY_LOCKED: TransactionError,
    ResultCode.MRT_MONITOR_EXISTS: TransactionError,
}


# Guidance for failures whose cause is a recognizable condition the result code
# alone does not name. Deliberately small: text that restates the code name is
# noise, and this is appended to a user-facing message.
#
# Precedence is narrowest-first -- an explicit ``hint=`` from a caller holding
# the operation, then the (code, subcode) pair, then the code alone.

# Keyed on the *pair*: subcode integers are scoped to their parent code, not
# globally unique (BIN_NAME_COUNT_TOO_LARGE and FORBID_XDR_FILTER_BLOCKED are
# both 1). Populated only when the server sends detail, which requires
# ``error_detail_verbosity`` and a server that supplies it.
_SUBCODE_GUIDANCE: dict[tuple[ResultCode, int], str] = {
    (ResultCode.BIN_NAME_TOO_LONG, SubCode.BIN_NAME_COUNT_TOO_LARGE): (
        "The record would exceed the server's per-record bin-count limit. The "
        "bin names themselves are fine -- the record has too many bins."
    ),
    (ResultCode.FAIL_FORBIDDEN, SubCode.FORBID_DURABILITY_VIOLATION): (
        "A non-durable delete was refused because it would violate durability. "
        "Strong-consistency namespaces require durable deletes: enable durable "
        "delete on the operation, or on the Behavior the session carries."
    ),
    (ResultCode.FAIL_FORBIDDEN, SubCode.FORBID_SET_COUNT_STOP_WRITES): (
        "The set reached its record-count stop-writes limit. Writes stay "
        "refused until the count falls or the limit is raised."
    ),
    (ResultCode.FAIL_FORBIDDEN, SubCode.FORBID_SET_SIZE_STOP_WRITES): (
        "The set reached its size stop-writes limit. Writes stay refused until "
        "the set shrinks or the limit is raised."
    ),
    (ResultCode.FAIL_FORBIDDEN, SubCode.FORBID_CLOCK_SKEW_STOP_WRITES): (
        "Writes are stopped because clocks across the cluster have drifted too "
        "far apart. This is a cluster health problem, not a client one -- check "
        "time synchronization on the server nodes."
    ),
    (ResultCode.FAIL_FORBIDDEN, SubCode.FORBID_TRUNCATED): (
        "The set or namespace is mid-truncate, so writes are refused until the "
        "truncate completes. Retrying after it finishes should succeed."
    ),
    (ResultCode.FAIL_FORBIDDEN, SubCode.FORBID_XDR_FILTER_BLOCKED): (
        "An XDR ship filter at the destination rejected this write. The filter "
        "is configured on the destination cluster, not in the client."
    ),
    (ResultCode.FAIL_FORBIDDEN, SubCode.FORBID_REPLACE_CONFLICT_RESOLVING): (
        "A replace was refused while the record was being conflict-resolved. "
        "Retry, or use an update rather than a replace."
    ),
}

# Fallback when no subcode is available. These must not rank causes: the code
# covers several, and naming one would misdirect whenever it is not that one.
_RC_GUIDANCE: dict[ResultCode, str] = {
    ResultCode.BIN_NAME_TOO_LONG: (
        "The server rejected the operation over bin naming. This code covers "
        "two causes: a bin name longer than 15 characters, or a record with "
        "too many bins. Enable extended error detail to get the subcode that "
        "tells them apart."
    ),
    ResultCode.FAIL_FORBIDDEN: (
        "The server forbade this operation given its current state. Causes "
        "include a durability violation, set-level stop-writes limits, cluster "
        "clock skew, an in-progress truncate, XDR filtering, and conflict "
        "resolution. Enable extended error detail to get the subcode that "
        "identifies which; namespace configuration is visible through "
        "session.info().namespace_details(<namespace>)."
    ),
    ResultCode.UNSUPPORTED_FEATURE: (
        "The cluster or namespace does not support something this operation "
        "requires. Multi-record transactions, for example, need a "
        "strong-consistency namespace on a server build that supports them; "
        "check the namespace mode with session.namespace_sc_status(<namespace>)."
    ),
}


def _result_code_to_exception(
    result_code: ResultCode,
    message: str = "",
    in_doubt: bool = False,
    *,
    sub_code: int | None = None,
    server_message: str | None = None,
    exp_trace: object | None = None,
    node: str | None = None,
    iteration: int | None = None,
    sub_exceptions: tuple[AerospikeError, ...] = (),
    base_message: str | None = None,
    hint: str | None = None,
) -> AerospikeError:
    """Map a server result code to the appropriate typed exception.

    When the code is one whose cause is a recognizable misconfiguration, local
    guidance is appended to *message* and kept on the exception as ``hint``. Pass
    ``hint`` explicitly to override the generic text with something narrower --
    a caller holding the operation knows more than the code alone conveys.
    """
    cls = _RC_TO_TYPE.get(result_code, AerospikeError)
    if hint is not None:
        guidance: str | None = hint
    elif sub_code:
        # A subcode names the exact condition, so it beats the per-code text.
        # Falsy covers both "absent" and SubCode.NONE.
        guidance = (
            _SUBCODE_GUIDANCE.get((result_code, sub_code))
            or _RC_GUIDANCE.get(result_code)
        )
    else:
        guidance = _RC_GUIDANCE.get(result_code)
    if guidance:
        message = f"{message}\n{guidance}" if message else guidance
    return cls(
        message,
        result_code=result_code,
        in_doubt=in_doubt,
        sub_code=sub_code,
        server_message=server_message,
        exp_trace=exp_trace,
        node=node,
        iteration=iteration,
        sub_exceptions=sub_exceptions,
        base_message=base_message,
        hint=guidance,
    )


# ---------------------------------------------------------------------------
# Boundary converter: PAC exception -> PSDK exception
# ---------------------------------------------------------------------------

def _retry_context_kwargs(exc: Exception) -> dict:
    """Extract the retry/diagnostic context a PAC exception carries.

    ``getattr`` defaults keep this tolerant of PAC versions predating a
    field. Prior-attempt exceptions are themselves converted, so
    ``sub_exceptions`` is homogeneous in this hierarchy (PAC sub-errors
    never nest further, so the recursion is single-level).
    """
    subs = getattr(exc, "sub_exceptions", None)
    return {
        "node": getattr(exc, "node", None),
        "iteration": getattr(exc, "iteration", None),
        "base_message": getattr(exc, "base_message", None),
        "sub_exceptions": (
            tuple(_convert_pac_exception(s) for s in subs) if subs else ()
        ),
    }


def _convert_pac_exception(exc: Exception, *, hint: str | None = None) -> AerospikeError:
    """Convert a PAC exception to the appropriate PSDK typed exception.

    The original exception is **not** set as ``__cause__`` here; callers
    should use ``raise _convert_pac_exception(e) from e``.

    Args:
        exc: The PAC exception to convert.
        hint: Guidance narrower than the result code alone supports, from a
            caller that still has the operation in scope. Replaces the generic
            text for that code. Ignored for failures that carry no result code.
    """
    if isinstance(exc, AerospikeError):
        return exc

    if isinstance(exc, PacServerError):
        return _result_code_to_exception(
            exc.result_code,
            str(exc),
            exc.in_doubt,
            sub_code=getattr(exc, "sub_code", None),
            server_message=getattr(exc, "server_message", None),
            exp_trace=getattr(exc, "exp_trace", None),
            hint=hint,
            **_retry_context_kwargs(exc),
        )

    if isinstance(exc, PacMaxErrorRate):
        return MaxErrorRate(
            str(exc), in_doubt=getattr(exc, "in_doubt", False),
            **_retry_context_kwargs(exc),
        )

    if isinstance(exc, PacTimeoutError):
        # PAC's TimeoutError is core's client-side deadline; a server-reported
        # timeout arrives as a ServerError with the TIMEOUT result code and
        # keeps the default client=False.
        return TimeoutError(
            str(exc), client=True, in_doubt=getattr(exc, "in_doubt", False),
            **_retry_context_kwargs(exc),
        )

    if isinstance(exc, PacConnectionError):
        return ConnectionError(
            str(exc), in_doubt=getattr(exc, "in_doubt", False),
            **_retry_context_kwargs(exc),
        )

    if isinstance(exc, PacInvalidNodeError):
        return InvalidNodeError(
            str(exc), in_doubt=getattr(exc, "in_doubt", False),
            **_retry_context_kwargs(exc),
        )

    if isinstance(exc, PacUDFBadResponse):
        return _result_code_to_exception(
            ResultCode.UDF_BAD_RESPONSE,
            str(exc),
            getattr(exc, "in_doubt", False),
            **_retry_context_kwargs(exc),
        )

    if isinstance(exc, PacAerospikeError):
        return AerospikeError(
            str(exc), in_doubt=getattr(exc, "in_doubt", False),
            **_retry_context_kwargs(exc),
        )

    return AerospikeError(str(exc))
