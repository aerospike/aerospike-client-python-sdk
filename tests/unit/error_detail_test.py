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

"""Unit tests for the extended-error-detail surface: the ``ErrorDetailVerbosity``
/ ``SubCode`` re-exports, ``error_detail_verbosity`` settings resolution and
policy mapping, the ``errorDetailVerbosity`` config key, and the exception
passthrough. Behavior against a live cluster is covered in
``tests/integration/async/error_detail_test.py``.
"""

from aerospike_async import ReadPolicy, WritePolicy
from aerospike_async.exceptions import ResultCode
from aerospike_async.exceptions import ServerError as PacServerError

import aerospike_sdk
from aerospike_sdk import ErrorDetailVerbosity, ExpressionTrace, SubCode
from aerospike_sdk.exceptions import AerospikeError, _convert_pac_exception
from aerospike_sdk.policy import sdk_config_loader as loader
from aerospike_sdk.policy.behavior_settings import Scope, Settings
from aerospike_sdk.policy.policy_mapper import (
    apply_to_read_policy,
    apply_to_write_policy,
    to_batch_policy,
    to_query_policy,
    to_read_policy,
    to_write_policy,
)


class TestReExports:
    """The verbosity enum and subcode catalog are exposed at the top level."""

    def test_verbosity_levels(self):
        assert ErrorDetailVerbosity.NONE == 0
        assert ErrorDetailVerbosity.SUBCODE == 1
        assert ErrorDetailVerbosity.MESSAGE == 2
        assert ErrorDetailVerbosity.EXPRESSION_TRACE == 3

    def test_subcode_catalog_present(self):
        assert SubCode.NONE == 0
        assert SubCode.PARAM_TTL_INVALID == 1

    def test_expression_trace_present(self):
        assert ExpressionTrace.PHASE_BUILD == 1
        assert ExpressionTrace.PHASE_EVAL == 2
        assert ExpressionTrace.LANG_MSGPACK == 1
        assert ExpressionTrace.LANG_AEL == 2

    def test_in_public_all(self):
        assert "ErrorDetailVerbosity" in aerospike_sdk.__all__
        assert "SubCode" in aerospike_sdk.__all__
        assert "ExpressionTrace" in aerospike_sdk.__all__


class TestSettingsResolution:
    """``error_detail_verbosity`` resolves like any other Settings field."""

    def test_merge_override_wins(self):
        base = Settings(error_detail_verbosity=1)
        override = Settings(error_detail_verbosity=2)
        assert Settings.merge(base, override).error_detail_verbosity == 2

    def test_merge_none_inherits_base(self):
        base = Settings(error_detail_verbosity=2)
        override = Settings(error_detail_verbosity=None)
        assert Settings.merge(base, override).error_detail_verbosity == 2

    def test_default_is_none(self):
        assert Settings().error_detail_verbosity is None


class TestPolicyMapping:
    """The verbosity crosses into every PAC policy the mapper builds."""

    def test_to_read_policy(self):
        p = to_read_policy(Settings(error_detail_verbosity=2))
        assert p.error_detail_verbosity == 2

    def test_to_write_policy(self):
        p = to_write_policy(Settings(error_detail_verbosity=2))
        assert p.error_detail_verbosity == 2

    def test_to_query_policy(self):
        p = to_query_policy(Settings(error_detail_verbosity=1))
        assert p.error_detail_verbosity == 1

    def test_to_batch_policy(self):
        p = to_batch_policy(Settings(error_detail_verbosity=3))
        assert p.error_detail_verbosity == 3

    def test_apply_fills_unset_policy(self):
        # A user-supplied policy at the default (0) is filled by behavior.
        policy = ReadPolicy()
        apply_to_read_policy(Settings(error_detail_verbosity=2), policy)
        assert policy.error_detail_verbosity == 2

    def test_apply_does_not_clobber_explicit(self):
        # A user who set verbosity explicitly wins over behavior defaults.
        policy = WritePolicy()
        policy.error_detail_verbosity = 1
        apply_to_write_policy(Settings(error_detail_verbosity=2), policy)
        assert policy.error_detail_verbosity == 1


class TestConfigKey:
    """The ``errorDetailVerbosity`` behaviors key maps to the Settings field."""

    def test_parsed_into_settings(self):
        specs = loader.parse_behaviors(
            "behaviors:\n  b:\n    allOperations:\n"
            "      errorDetailVerbosity: 2\n"
        )
        assert specs["b"].patches[Scope.ALL].error_detail_verbosity == 2


class TestExceptionPassthrough:
    """A PAC ServerError's detail surfaces on the converted PSDK exception."""

    def test_detail_passes_through(self):
        pac = PacServerError(
            "list index 99 out of bounds",
            ResultCode.OP_NOT_APPLICABLE,
            False,
            1,
            "index 99 out of bounds (subcode=1)",
            None,
        )
        psdk = _convert_pac_exception(pac)
        assert isinstance(psdk, AerospikeError)
        assert psdk.result_code == ResultCode.OP_NOT_APPLICABLE
        assert psdk.sub_code == 1
        assert psdk.server_message == "index 99 out of bounds (subcode=1)"

    def test_absent_detail_is_none(self):
        pac = PacServerError(
            "key not found", ResultCode.KEY_NOT_FOUND_ERROR, False, None, None, None
        )
        psdk = _convert_pac_exception(pac)
        assert psdk.sub_code is None
        assert psdk.server_message is None
        assert psdk.exp_trace is None

    def test_base_exception_defaults_none(self):
        err = AerospikeError("x")
        assert err.sub_code is None
        assert err.server_message is None
        assert err.exp_trace is None
