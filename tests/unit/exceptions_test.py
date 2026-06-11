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
from aerospike_async.exceptions import (
    AerospikeError as PacAerospikeError,
    ConnectionError as PacConnectionError,
    InvalidNodeError as PacInvalidNodeError,
    ServerError as PacServerError,
    TimeoutError as PacTimeoutError,
    UDFBadResponse as PacUDFBadResponse,
)
from aerospike_async.exceptions import ResultCode

from aerospike_sdk.ael.exceptions import NoApplicableFilterError
from aerospike_sdk.exceptions import (
    AerospikeError,
    AuthenticationError,
    AuthorizationError,
    BackoffError,
    CommitError,
    ConnectionError,
    GenerationError,
    InvalidNamespaceError,
    InvalidNodeError,
    QueryTerminatedError,
    QuotaError,
    SecurityError,
    SerializationError,
    TimeoutError,
    _convert_pac_exception,
    _result_code_to_exception,
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
            CommitError,
        ]
        for cls in direct:
            assert issubclass(cls, AerospikeError), f"{cls.__name__} should be a subclass of AerospikeError"

    def test_security_subtree(self):
        assert issubclass(AuthenticationError, SecurityError)
        assert issubclass(AuthorizationError, SecurityError)
        assert issubclass(AuthenticationError, AerospikeError)
        assert issubclass(AuthorizationError, AerospikeError)

    def test_not_cross_linked(self):
        """Typed siblings should not be subclasses of each other."""
        assert not issubclass(GenerationError, SecurityError)
        assert not issubclass(TimeoutError, ConnectionError)
        assert not issubclass(QuotaError, SecurityError)


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

    def test_unmapped_code_falls_through(self):
        exc = _result_code_to_exception(ResultCode.KEY_NOT_FOUND_ERROR, "not found")
        assert type(exc) is AerospikeError
        assert exc.result_code == ResultCode.KEY_NOT_FOUND_ERROR

    def test_in_doubt_propagated(self):
        exc = _result_code_to_exception(ResultCode.GENERATION_ERROR, "gen", in_doubt=True)
        assert exc.in_doubt is True


class TestConvertPacException:
    """Verify PAC-to-PFC exception conversion."""

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

    def test_pac_connection(self):
        pac = PacConnectionError("conn refused")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is ConnectionError

    def test_pac_invalid_node(self):
        pac = PacInvalidNodeError("node gone")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is InvalidNodeError

    def test_pac_udf_bad_response(self):
        pac = PacUDFBadResponse("1000:Invalid value")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is AerospikeError
        assert pfc.result_code == ResultCode.UDF_BAD_RESPONSE

    def test_pac_generic_aerospike_error(self):
        pac = PacAerospikeError("something broke")
        pfc = _convert_pac_exception(pac)
        assert type(pfc) is AerospikeError

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
