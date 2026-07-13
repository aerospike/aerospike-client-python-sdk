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
from aerospike_async.exceptions import ResultCode


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
    ) -> None:
        super().__init__(message)
        self.result_code = result_code
        self.in_doubt = in_doubt


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
        result_code: Set when the server returned a timeout-related code;
            otherwise often ``None`` for client-side timeouts.

    See Also:
        :class:`ConnectionError`: Cluster reachability rather than deadline
            exceeded.

    Example::
        try:
            await stream.first_or_raise()
        except TimeoutError:
            ...  # retry or fall back
    """


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

class QueryTerminatedError(AerospikeError):
    """Raised when a query stops early (aborted, canceled, or server-terminated).

    Partial rows may already have been delivered on streaming paths; this error
    represents the overall query outcome, not a single-key failure inside a
    batch.

    Attributes:
        result_code: May include ``ResultCode.QUERY_ABORTED`` or related codes.
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
    ) -> None:
        super().__init__(message, result_code=result_code, in_doubt=in_doubt)
        self.commit_error_type = commit_error_type
        self.verify_records = verify_records
        self.roll_records = roll_records


# ---------------------------------------------------------------------------
# Factory: ResultCode -> typed exception
# ---------------------------------------------------------------------------

# Codes not yet exposed by the PAC are omitted; they will fall through to
# AerospikeError until the PAC adds them.

_RC_TO_TYPE: dict[ResultCode, type[AerospikeError]] = {
    ResultCode.GENERATION_ERROR: GenerationError,
    # Authentication
    ResultCode.NOT_AUTHENTICATED: AuthenticationError,
    ResultCode.INVALID_USER: AuthenticationError,
    # Security (catch-all for remaining security codes)
    ResultCode.ILLEGAL_STATE: SecurityError,
    ResultCode.USER_ALREADY_EXISTS: SecurityError,
    ResultCode.FORBIDDEN_PASSWORD: SecurityError,
    ResultCode.SECURITY_NOT_SUPPORTED: SecurityError,
    ResultCode.SECURITY_NOT_ENABLED: SecurityError,
    ResultCode.SECURITY_SCHEME_NOT_SUPPORTED: SecurityError,
    # Timeout
    ResultCode.TIMEOUT: TimeoutError,
    ResultCode.QUERY_TIMEOUT: TimeoutError,
    # Namespace
    ResultCode.INVALID_NAMESPACE: InvalidNamespaceError,
    # Query terminated
    ResultCode.QUERY_ABORTED: QueryTerminatedError,
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


def _result_code_to_exception(
    result_code: ResultCode,
    message: str = "",
    in_doubt: bool = False,
) -> AerospikeError:
    """Map a ``ResultCode`` to the appropriate typed exception.

    Map a server result code to the appropriate typed exception.
    """
    cls = _RC_TO_TYPE.get(result_code, AerospikeError)
    return cls(message, result_code=result_code, in_doubt=in_doubt)


# ---------------------------------------------------------------------------
# Boundary converter: PAC exception -> PSDK exception
# ---------------------------------------------------------------------------

def _convert_pac_exception(exc: Exception) -> AerospikeError:
    """Convert a PAC exception to the appropriate PSDK typed exception.

    The original exception is **not** set as ``__cause__`` here; callers
    should use ``raise convert_pac_exception(e) from e``.
        :func:`_result_code_to_exception`
    """
    if isinstance(exc, AerospikeError):
        return exc

    if isinstance(exc, PacServerError):
        return _result_code_to_exception(exc.result_code, str(exc), exc.in_doubt)

    if isinstance(exc, PacMaxErrorRate):
        return MaxErrorRate(str(exc))

    if isinstance(exc, PacTimeoutError):
        return TimeoutError(str(exc))

    if isinstance(exc, PacConnectionError):
        return ConnectionError(str(exc))

    if isinstance(exc, PacInvalidNodeError):
        return InvalidNodeError(str(exc))

    if isinstance(exc, PacUDFBadResponse):
        return _result_code_to_exception(
            ResultCode.UDF_BAD_RESPONSE,
            str(exc),
            getattr(exc, "in_doubt", False),
        )

    if isinstance(exc, PacAerospikeError):
        return AerospikeError(str(exc))

    return AerospikeError(str(exc))
