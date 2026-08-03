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

"""Tests for the SDK exception hierarchy, factory, and dependency converter."""

import pytest
from aerospike_sdk.exceptions import (
    AerospikeError,
    AuthenticationError,
    AuthorizationError,
    BackoffError,
    BinError,
    BinExistsError,
    BinNotFoundError,
    BinOpInvalidError,
    BinTypeError,
    CapacityError,
    CommitError,
    ConnectionError,
    _convert_pac_exception,
    ElementError,
    ElementExistsError,
    ElementNotFoundError,
    FilteredOutError,
    GenerationError,
    IndexAlreadyExistsError,
    IndexNotFoundError,
    InvalidNamespaceError,
    InvalidNodeError,
    BatchError,
    KeyBusyError,
    QueryError,
    QueryTerminatedError,
    QuotaError,
    RecordExistsError,
    RecordNotFoundError,
    RecordTooBigError,
    _result_code_to_exception,
    ResultCode,
    SecondaryIndexError,
    SecurityError,
    SerializationError,
    TimeoutError,
    TransactionError,
    UdfError,
)
# The dependency-converter tests construct real PAC exceptions. PSDK's own
# AerospikeError/ConnectionError/TimeoutError shadow the PAC names, so pull
# the PAC types via the Pac* aliases the exceptions module already binds.
from aerospike_sdk.exceptions import (
    PacAerospikeError,
    PacConnectionError,
    PacInvalidNodeError,
    PacServerError,
    PacTimeoutError,
    PacUDFBadResponse,
)

from aerospike_sdk.ael.exceptions import NoApplicableFilterError


class TestExceptionHierarchy:
    """Verify the inheritance tree matches the design."""

    def test_base_is_exception(self):
        assert issubclass(AerospikeError, Exception)

    def test_direct_subclasses(self):
        direct = [
            TimeoutError,
            ConnectionError,
            InvalidNodeError,
            InvalidNamespaceError,
            SecurityError,
            GenerationError,
            QuotaError,
            SerializationError,
            QueryTerminatedError,
            BackoffError,
            # Record-level errors are flat (not grouped under a record base).
            RecordNotFoundError,
            RecordExistsError,
            RecordTooBigError,
            FilteredOutError,
            # Family base classes.
            BinError,
            ElementError,
            CapacityError,
            SecondaryIndexError,
            TransactionError,
        ]
        for cls in direct:
            assert issubclass(cls, AerospikeError), f"{cls.__name__} should be a subclass of AerospikeError"

    def test_security_subtree(self):
        assert issubclass(AuthenticationError, SecurityError)
        assert issubclass(AuthorizationError, SecurityError)
        assert issubclass(AuthenticationError, AerospikeError)
        assert issubclass(AuthorizationError, AerospikeError)

    def test_bin_subtree(self):
        for cls in (BinExistsError, BinNotFoundError, BinTypeError, BinOpInvalidError):
            assert issubclass(cls, BinError)
            assert issubclass(cls, AerospikeError)

    def test_element_subtree(self):
        for cls in (ElementNotFoundError, ElementExistsError):
            assert issubclass(cls, ElementError)
            assert issubclass(cls, AerospikeError)

    def test_capacity_subtree(self):
        assert issubclass(KeyBusyError, CapacityError)
        assert issubclass(KeyBusyError, AerospikeError)

    def test_index_subtree(self):
        for cls in (IndexNotFoundError, IndexAlreadyExistsError):
            assert issubclass(cls, SecondaryIndexError)
            assert issubclass(cls, AerospikeError)

    def test_commit_is_transaction(self):
        """CommitError specializes TransactionError so both catch it."""
        assert issubclass(CommitError, TransactionError)
        assert issubclass(CommitError, AerospikeError)

    def test_secondary_index_does_not_shadow_builtin(self):
        """The index error is deliberately not named IndexError (a builtin)."""
        assert SecondaryIndexError is not IndexError
        assert not issubclass(SecondaryIndexError, IndexError)

    def test_not_cross_linked(self):
        """Typed siblings should not be subclasses of each other."""
        assert not issubclass(GenerationError, SecurityError)
        assert not issubclass(TimeoutError, ConnectionError)
        assert not issubclass(QuotaError, SecurityError)
        assert not issubclass(RecordExistsError, RecordNotFoundError)
        assert not issubclass(BinExistsError, BinNotFoundError)
        assert not issubclass(ElementExistsError, ElementNotFoundError)


class TestAerospikeErrorFields:
    """Verify base exception carries the expected attributes."""

    def test_defaults(self):
        err = AerospikeError("boom")
        assert str(err) == "boom"
        assert err.result_code is None
        assert err.in_doubt is False

    def test_result_code(self):
        err = AerospikeError("fail", result_code=ResultCode.GENERATION_ERROR)
        assert err.result_code == ResultCode.GENERATION_ERROR

    def test_in_doubt(self):
        err = AerospikeError("maybe", in_doubt=True)
        assert err.in_doubt is True

    def test_fields_inherited(self):
        err = GenerationError("gen", result_code=ResultCode.GENERATION_ERROR, in_doubt=True)
        assert err.result_code == ResultCode.GENERATION_ERROR
        assert err.in_doubt is True


class TestCommitErrorFields:
    """CommitError carries extra MRT-related attributes."""

    def test_defaults(self):
        err = CommitError("txn failed")
        assert err.commit_error_type is None
        assert err.verify_records is None
        assert err.roll_records is None

    def test_extra_fields(self):
        err = CommitError(
            "txn failed",
            commit_error_type="VERIFY_FAIL",
            verify_records=["r1"],
            roll_records=["r2"],
            in_doubt=True,
        )
        assert err.commit_error_type == "VERIFY_FAIL"
        assert err.verify_records == ["r1"]
        assert err.roll_records == ["r2"]
        assert err.in_doubt is True


class TestResultCodeToException:
    """Verify the factory maps result codes to the correct exception type."""

    def test_generation_error(self):
        exc = _result_code_to_exception(ResultCode.GENERATION_ERROR, "gen mismatch")
        assert type(exc) is GenerationError
        assert exc.result_code == ResultCode.GENERATION_ERROR
        assert str(exc) == "gen mismatch"

    @pytest.mark.parametrize("code", [ResultCode.NOT_AUTHENTICATED, ResultCode.INVALID_USER])
    def test_authentication_error(self, code):
        exc = _result_code_to_exception(code, "auth fail")
        assert type(exc) is AuthenticationError

    @pytest.mark.parametrize("code", [
        ResultCode.ILLEGAL_STATE,
        ResultCode.USER_ALREADY_EXISTS,
        ResultCode.FORBIDDEN_PASSWORD,
        ResultCode.SECURITY_NOT_SUPPORTED,
        ResultCode.SECURITY_NOT_ENABLED,
        ResultCode.SECURITY_SCHEME_NOT_SUPPORTED,
    ])
    def test_security_error(self, code):
        exc = _result_code_to_exception(code, "sec fail")
        assert type(exc) is SecurityError

    @pytest.mark.parametrize("code", [ResultCode.TIMEOUT, ResultCode.QUERY_TIMEOUT])
    def test_timeout_error(self, code):
        exc = _result_code_to_exception(code, "timed out")
        assert type(exc) is TimeoutError

    def test_invalid_namespace_error(self):
        exc = _result_code_to_exception(ResultCode.INVALID_NAMESPACE, "bad ns")
        assert type(exc) is InvalidNamespaceError

    def test_query_terminated_error(self):
        exc = _result_code_to_exception(ResultCode.QUERY_ABORTED, "aborted")
        assert type(exc) is QueryTerminatedError

    @pytest.mark.parametrize("code,expected", [
        (ResultCode.KEY_NOT_FOUND_ERROR, RecordNotFoundError),
        (ResultCode.KEY_EXISTS_ERROR, RecordExistsError),
        (ResultCode.RECORD_TOO_BIG, RecordTooBigError),
        (ResultCode.FILTERED_OUT, FilteredOutError),
        (ResultCode.BIN_NAME_TOO_LONG, BinError),
        (ResultCode.BIN_EXISTS_ERROR, BinExistsError),
        (ResultCode.BIN_NOT_FOUND, BinNotFoundError),
        (ResultCode.BIN_TYPE_ERROR, BinTypeError),
        (ResultCode.OP_NOT_APPLICABLE, BinOpInvalidError),
        (ResultCode.ELEMENT_NOT_FOUND, ElementNotFoundError),
        (ResultCode.ELEMENT_EXISTS, ElementExistsError),
        (ResultCode.SERVER_MEM_ERROR, CapacityError),
        (ResultCode.DEVICE_OVERLOAD, CapacityError),
        (ResultCode.QUERY_QUEUE_FULL, CapacityError),
        (ResultCode.KEY_BUSY, KeyBusyError),
        (ResultCode.XDR_KEY_BUSY, KeyBusyError),
        (ResultCode.INDEX_NOT_FOUND, IndexNotFoundError),
        (ResultCode.INDEX_FOUND, IndexAlreadyExistsError),
        (ResultCode.INDEX_OOM, SecondaryIndexError),
        (ResultCode.MRT_EXPIRED, TransactionError),
        (ResultCode.MRT_BLOCKED, TransactionError),
    ])
    def test_targeted_subclass_mapping(self, code, expected):
        exc = _result_code_to_exception(code, "boom")
        assert type(exc) is expected
        assert exc.result_code == code

    def test_unmapped_code_falls_through(self):
        exc = _result_code_to_exception(ResultCode.PARAMETER_ERROR, "bad param")
        assert type(exc) is AerospikeError
        assert exc.result_code == ResultCode.PARAMETER_ERROR

    def test_in_doubt_propagated(self):
        exc = _result_code_to_exception(ResultCode.GENERATION_ERROR, "gen", in_doubt=True)
        assert exc.in_doubt is True


class TestConvertPacException:
    """Verify PAC-to-PSDK exception conversion."""

    def test_server_error_mapped(self):
        pac = PacServerError("gen mismatch", ResultCode.GENERATION_ERROR)
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is GenerationError
        assert pfc.result_code == ResultCode.GENERATION_ERROR
        assert pfc.in_doubt is False

    def test_server_error_in_doubt_propagated(self):
        pac = PacServerError("write failed", ResultCode.SERVER_ERROR, True)
        pfc = _convert_pac_exception(pac)
        assert pfc.in_doubt is True

    def test_pac_timeout(self):
        pac = PacTimeoutError("timed out")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is TimeoutError
        assert pfc.in_doubt is False

    def test_pac_timeout_in_doubt_propagated(self):
        pac = PacTimeoutError("timed out")
        pac.in_doubt = True
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is TimeoutError
        assert pfc.in_doubt is True

    def test_pac_connection(self):
        pac = PacConnectionError("conn refused")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is ConnectionError
        assert pfc.in_doubt is False

    def test_pac_connection_in_doubt_propagated(self):
        pac = PacConnectionError("conn reset mid-write")
        pac.in_doubt = True
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is ConnectionError
        assert pfc.in_doubt is True

    def test_pac_invalid_node(self):
        pac = PacInvalidNodeError("node gone")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is InvalidNodeError

    def test_pac_udf_bad_response(self):
        pac = PacUDFBadResponse("1000:Invalid value")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is UdfError
        assert pfc.result_code == ResultCode.UDF_BAD_RESPONSE

    def test_pac_generic_aerospike_error(self):
        pac = PacAerospikeError("something broke")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is AerospikeError
        assert pfc.in_doubt is False

    def test_pac_generic_in_doubt_propagated(self):
        pac = PacAerospikeError("batch failed over an in-doubt write")
        pac.in_doubt = True
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is AerospikeError
        assert pfc.in_doubt is True

    def test_unknown_exception_wrapped(self):
        pfc = _convert_pac_exception(RuntimeError("wat"))
        assert type(pfc) is AerospikeError
        assert "wat" in str(pfc)

    def test_cause_chaining(self):
        """Callers should use ``raise ... from`` for proper chaining."""
        pac = PacServerError("gen fail", ResultCode.GENERATION_ERROR)
        pfc = _convert_pac_exception(pac)
        try:
            raise pfc from pac
        except GenerationError as caught:
            assert caught.__cause__ is pac


class TestNoApplicableFilterError:
    """Verify NoApplicableFilterError is independent of AerospikeError."""

    def test_is_exception(self):
        assert issubclass(NoApplicableFilterError, Exception)
        assert not issubclass(NoApplicableFilterError, AerospikeError)

    def test_raise_and_catch(self):
        with pytest.raises(NoApplicableFilterError):
            raise NoApplicableFilterError("no filter for this expression")


class TestRetryContextPropagation:
    """Retry/diagnostic context flows from PAC exceptions onto the base."""

    def test_defaults_when_pac_lacks_fields(self):
        pfc = _convert_pac_exception(PacTimeoutError("timed out"))
        assert pfc.node is None
        assert pfc.iteration is None
        assert pfc.base_message is None
        assert pfc.sub_exceptions == ()

    def test_client_side_fields_propagated(self):
        pac = PacTimeoutError("Error 9: retried out")
        pac.node = "BB9020011AC4202"
        pac.iteration = 3
        pac.base_message = "Client Timeout: Timeout after 3 tries"
        pac.sub_exceptions = [PacTimeoutError("attempt 1"), PacTimeoutError("attempt 2")]

        pfc = _convert_pac_exception(pac)
        assert type(pfc) is TimeoutError
        assert pfc.node == "BB9020011AC4202"
        assert pfc.iteration == 3
        assert pfc.base_message == "Client Timeout: Timeout after 3 tries"
        assert len(pfc.sub_exceptions) == 2
        # Prior attempts are converted into this hierarchy too.
        assert all(isinstance(s, TimeoutError) for s in pfc.sub_exceptions)
        assert all(isinstance(s, AerospikeError) for s in pfc.sub_exceptions)

    def test_server_error_fields_propagated(self):
        pac = PacServerError(
            "fail", ResultCode.GENERATION_ERROR, True, 2, "conflict", None,
            "BB9020011AC4202", 4, "Server error: GenerationError", None,
        )
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is GenerationError
        assert pfc.node == "BB9020011AC4202"
        assert pfc.iteration == 4
        assert pfc.base_message == "Server error: GenerationError"
        assert pfc.sub_exceptions == ()
        assert pfc.sub_code == 2
        assert pfc.server_message == "conflict"

    def test_server_error_sub_exceptions_converted(self):
        pac = PacServerError(
            "fail", ResultCode.TIMEOUT, False, None, None, None,
            None, 2, None, [PacTimeoutError("attempt 1")],
        )
        pfc = _convert_pac_exception(pac)
        assert len(pfc.sub_exceptions) == 1
        assert isinstance(pfc.sub_exceptions[0], TimeoutError)


class TestTimeoutProvenance:
    """The client/server discriminator on TimeoutError."""

    def test_default_is_server(self):
        assert TimeoutError("t").client is False

    def test_pac_client_timeout_sets_client_true(self):
        pfc = _convert_pac_exception(PacTimeoutError("deadline"))
        assert type(pfc) is TimeoutError
        assert pfc.client is True

    def test_server_timeout_code_keeps_client_false(self):
        pac = PacServerError("timeout", ResultCode.TIMEOUT, True)
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is TimeoutError
        assert pfc.client is False

    def test_query_timeout_is_server_side(self):
        pfc = _result_code_to_exception(ResultCode.QUERY_TIMEOUT, "qt")
        assert type(pfc) is TimeoutError
        assert pfc.client is False


class TestSubsystemTypedMappings:
    """Result-code coverage added with the subsystem classes."""

    def test_udf_query_batch_quota(self):
        expected = {
            ResultCode.UDF_BAD_RESPONSE: UdfError,
            ResultCode.QUERY_GENERIC: QueryError,
            ResultCode.SCAN_ABORT: QueryError,
            ResultCode.QUERY_NETIO_ERR: QueryError,
            ResultCode.QUERY_DUPLICATE: QueryError,
            ResultCode.QUERY_ABORTED: QueryTerminatedError,
            ResultCode.BATCH_DISABLED: BatchError,
            ResultCode.BATCH_QUEUES_FULL: CapacityError,
            ResultCode.BATCH_MAX_REQUESTS_EXCEEDED: CapacityError,
            ResultCode.QUOTA_EXCEEDED: QuotaError,
            ResultCode.QUOTAS_NOT_ENABLED: QuotaError,
            ResultCode.INVALID_QUOTA: QuotaError,
        }
        for code, cls in expected.items():
            assert type(_result_code_to_exception(code)) is cls, code

    def test_security_family_split(self):
        authn = [
            ResultCode.INVALID_PASSWORD, ResultCode.INVALID_CREDENTIAL,
            ResultCode.EXPIRED_PASSWORD, ResultCode.NOT_AUTHENTICATED,
            ResultCode.INVALID_USER,
        ]
        authz = [ResultCode.ROLE_VIOLATION, ResultCode.NOT_ALLOWLISTED]
        flat = [
            ResultCode.EXPIRED_SESSION, ResultCode.INVALID_ROLE,
            ResultCode.ROLE_ALREADY_EXISTS, ResultCode.INVALID_PRIVILEGE,
            ResultCode.INVALID_ALLOWLIST,
        ]
        for code in authn:
            assert type(_result_code_to_exception(code)) is AuthenticationError, code
        for code in authz:
            assert type(_result_code_to_exception(code)) is AuthorizationError, code
        for code in flat:
            assert type(_result_code_to_exception(code)) is SecurityError, code
        # All of them are catchable as SecurityError.
        for code in authn + authz + flat:
            assert isinstance(_result_code_to_exception(code), SecurityError)

    def test_query_terminated_is_a_query_error(self):
        assert issubclass(QueryTerminatedError, QueryError)


class TestTypedCoverageMatchesDependency:
    """Every result code the dependency types must be typed here too.

    Guards the two maps against drifting: a code PAC gives a dedicated
    subclass should never fall through to the bare base in this SDK.
    """

    def test_pac_typed_codes_are_typed_here(self):
        from aerospike_async.exceptions import _RC_TO_CLS
        from aerospike_sdk.exceptions import _RC_TO_TYPE

        untyped = [code for code in _RC_TO_CLS if code not in _RC_TO_TYPE]
        # PARAMETER_ERROR is deliberate: PAC types it (InvalidRequest) while
        # this SDK keeps it on the base pending a dedicated class decision.
        allowed = {ResultCode.PARAMETER_ERROR}
        assert set(untyped) <= allowed, f"codes typed by PAC but not here: {untyped}"


class TestSubCodeCatalogReExport:
    """The SubCode catalog is a re-export, never a hand-kept copy."""

    def test_identity(self):
        import aerospike_async
        import aerospike_sdk

        assert aerospike_sdk.SubCode is aerospike_async.SubCode

    def test_spec_named_families_present(self):
        from aerospike_sdk import SubCode

        for name in (
            "NONE", "OPNOT_CDT_INDEX_OUT_OF_BOUNDS", "PARAM_TTL_INVALID",
            "FORBID_TRUNCATED", "UNSUPP_FEAT_GENERIC",
        ):
            assert hasattr(SubCode, name), name
