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
from aerospike_async import Operation, SubCode
from aerospike_sdk.operations_shared import _SingleKeyWriteSegmentBase
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

class TestResultCodeGuidance:
    """Local guidance appended to codes whose cause is a known misconfiguration."""

    def test_bin_name_too_long_explains_the_limit(self):
        # The server names no bin, so the hint has to say what to look for.
        exc = _result_code_to_exception(ResultCode.BIN_NAME_TOO_LONG, "Code: BinNameTooLong")
        assert "15 characters" in exc.hint
        assert "Code: BinNameTooLong" in str(exc)
        assert "15 characters" in str(exc)

    def test_fail_forbidden_does_not_rank_causes(self):
        # The code covers several conditions; naming one would misdirect
        # whenever it is not that one.
        exc = _result_code_to_exception(ResultCode.FAIL_FORBIDDEN, "boom")
        assert "subcode" in exc.hint
        assert "clock skew" in exc.hint and "stop-writes" in exc.hint

    def test_bin_name_too_long_names_both_meanings(self):
        # Code 21 is also returned for too many bins, not just a long name.
        exc = _result_code_to_exception(ResultCode.BIN_NAME_TOO_LONG, "boom")
        assert "too many bins" in exc.hint

    def test_unsupported_feature_points_at_namespace_mode(self):
        exc = _result_code_to_exception(ResultCode.UNSUPPORTED_FEATURE, "boom")
        assert "strong-consistency" in exc.hint

    def test_codes_without_guidance_carry_no_hint(self):
        # Guidance that restates the code name is noise; most codes get none.
        exc = _result_code_to_exception(ResultCode.KEY_NOT_FOUND_ERROR, "missing")
        assert exc.hint is None
        assert str(exc) == "missing"

    def test_explicit_hint_overrides_the_generic_text(self):
        exc = _result_code_to_exception(
            ResultCode.FAIL_FORBIDDEN, "boom", hint="bin 'x' is the problem"
        )
        assert exc.hint == "bin 'x' is the problem"
        assert "nsup-period" not in str(exc)

    def test_hint_stands_alone_when_there_is_no_message(self):
        exc = _result_code_to_exception(ResultCode.BIN_NAME_TOO_LONG, "")
        assert str(exc) == exc.hint

    def test_guidance_survives_the_pac_boundary(self):
        # Real errors arrive as PAC exceptions, so the converter must carry it.
        pac = PacServerError("Code: BinNameTooLong", ResultCode.BIN_NAME_TOO_LONG)
        converted = _convert_pac_exception(pac)
        assert converted.hint is not None
        assert "15 characters" in converted.hint
        assert "15 characters" in str(converted)


class TestBinNameHint:
    """The over-long-bin-name hint built from an operation list."""

    @staticmethod
    def _hint(*bin_names):
        # Exercise the shared helper against real operations without standing
        # up a segment: it reads only ``_ops``.
        class _Segment:
            _ops = [Operation.put(name, 1) for name in bin_names]
            _bin_name_hint = _SingleKeyWriteSegmentBase._bin_name_hint

        return _Segment()._bin_name_hint()

    def test_names_the_single_offender(self):
        hint = self._hint("ok", "a" * 16, "fine")
        assert repr("a" * 16) in hint
        assert "'ok'" not in hint and "'fine'" not in hint
        assert "15-character" in hint

    def test_lists_offenders_sorted_and_pluralized(self):
        hint = self._hint("z" * 16, "y" * 18)
        assert hint.startswith("Bin names ")
        # Sorted, so the text is stable regardless of operation order.
        assert hint.index(repr("y" * 18)) < hint.index(repr("z" * 16))
        assert "exceed the" in hint

    def test_singular_wording_for_one_offender(self):
        hint = self._hint("a" * 16)
        assert hint.startswith("Bin name ")
        assert "exceeds the" in hint

    def test_boundary_length_is_not_an_offender(self):
        # 15 is legal; 16 is not.
        assert self._hint("a" * 15) is None
        assert self._hint("a" * 16) is not None

    def test_no_offenders_yields_no_hint(self):
        # Falls back to the generic guidance for the code.
        assert self._hint("ok", "fine") is None

    def test_duplicate_offenders_are_listed_once(self):
        hint = self._hint("a" * 16, "a" * 16)
        assert hint.count(repr("a" * 16)) == 1
        assert hint.startswith("Bin name ")

    def test_operations_without_a_bin_name_are_ignored(self):
        class _Segment:
            _ops = [Operation.touch(), Operation.get(), Operation.put("a" * 16, 1)]
            _bin_name_hint = _SingleKeyWriteSegmentBase._bin_name_hint

        hint = _Segment()._bin_name_hint()
        assert repr("a" * 16) in hint


class TestSubCodeGuidance:
    """A subcode names the exact condition, so it outranks the per-code text."""

    def test_durability_violation_is_named_exactly(self):
        # The ticket's non-durable-delete-on-SC case, from the server's answer
        # rather than a client-side guess.
        exc = _result_code_to_exception(
            ResultCode.FAIL_FORBIDDEN, "boom",
            sub_code=SubCode.FORBID_DURABILITY_VIOLATION,
        )
        assert "durable delete" in exc.hint
        assert "clock skew" not in exc.hint

    def test_clock_skew_is_reported_as_a_cluster_problem(self):
        exc = _result_code_to_exception(
            ResultCode.FAIL_FORBIDDEN, "boom",
            sub_code=SubCode.FORBID_CLOCK_SKEW_STOP_WRITES,
        )
        assert "time synchronization" in exc.hint
        assert "durable delete" not in exc.hint

    def test_bin_count_subcode_corrects_the_generic_reading(self):
        # Same parent code as an over-long name, but a different cause.
        exc = _result_code_to_exception(
            ResultCode.BIN_NAME_TOO_LONG, "boom",
            sub_code=SubCode.BIN_NAME_COUNT_TOO_LARGE,
        )
        assert "too many bins" in exc.hint
        assert "15 characters" not in exc.hint

    def test_same_subcode_integer_under_different_parents(self):
        # BIN_NAME_COUNT_TOO_LARGE and FORBID_XDR_FILTER_BLOCKED are both 1,
        # so the lookup must key on the pair, never the subcode alone.
        assert SubCode.BIN_NAME_COUNT_TOO_LARGE == SubCode.FORBID_XDR_FILTER_BLOCKED
        bins = _result_code_to_exception(
            ResultCode.BIN_NAME_TOO_LONG, "", sub_code=SubCode.BIN_NAME_COUNT_TOO_LARGE)
        xdr = _result_code_to_exception(
            ResultCode.FAIL_FORBIDDEN, "", sub_code=SubCode.FORBID_XDR_FILTER_BLOCKED)
        assert "too many bins" in bins.hint
        assert "XDR ship filter" in xdr.hint

    def test_unknown_subcode_falls_back_to_the_code_text(self):
        # New server subcodes must degrade, not blank out the guidance.
        exc = _result_code_to_exception(ResultCode.FAIL_FORBIDDEN, "boom", sub_code=9999)
        assert "subcode" in exc.hint

    def test_subcode_none_uses_the_code_text(self):
        exc = _result_code_to_exception(
            ResultCode.FAIL_FORBIDDEN, "boom", sub_code=SubCode.NONE)
        assert "subcode" in exc.hint

    def test_explicit_hint_still_wins_over_a_subcode(self):
        exc = _result_code_to_exception(
            ResultCode.BIN_NAME_TOO_LONG, "boom",
            sub_code=SubCode.BIN_NAME_COUNT_TOO_LARGE,
            hint="bin 'x' is too long",
        )
        assert exc.hint == "bin 'x' is too long"
