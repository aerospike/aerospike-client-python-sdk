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

"""Tests for client-side circuit-breaker plumbing.

These tests verify the wiring between PSDK ``Client(...)`` constructor
keywords, the underlying :class:`~aerospike_async.ClientPolicy`, and the
``MaxErrorRate`` exception. They do not contact a server.
"""

from aerospike_async import ClientPolicy
from aerospike_async.exceptions import MaxErrorRate as PacMaxErrorRate

from aerospike_sdk import BackoffError, Client, MaxErrorRate, SyncClient
from aerospike_sdk.exceptions import _convert_pac_exception


class TestExceptionHierarchy:
    """``MaxErrorRate`` slots into ``BackoffError`` so existing rate-limit handlers catch it."""

    def test_max_error_rate_is_backoff_error(self):
        assert issubclass(MaxErrorRate, BackoffError)

    def test_max_error_rate_distinct_from_backoff(self):
        # But it is its own type so users can target it specifically.
        assert MaxErrorRate is not BackoffError


class TestClientPlumbing:
    """``Client(...)`` keyword arguments mutate the resolved ``ClientPolicy``.

    These tests construct clients without connecting, so they don't depend
    on a live cluster. The ``aerospike_host`` fixture (defined in the
    repo-root ``conftest.py``) is still used as the seeds string for
    consistency with the rest of the test suite — a connect attempt would
    use ``AEROSPIKE_HOST`` if set.
    """

    def test_aio_client_sets_max_error_rate(self, aerospike_host):
        c = Client(aerospike_host, max_error_rate=42)
        assert c._policy.max_error_rate == 42

    def test_aio_client_sets_error_rate_window(self, aerospike_host):
        c = Client(aerospike_host, error_rate_window=4)
        assert c._policy.error_rate_window == 4

    def test_aio_client_overrides_user_policy(self, aerospike_host):
        # When both an explicit policy and a kwarg are supplied, the kwarg
        # wins (the kwarg is the more recent caller intent).
        cp = ClientPolicy()
        cp.max_error_rate = 5
        c = Client(aerospike_host, policy=cp, max_error_rate=99)
        assert c._policy is cp
        assert c._policy.max_error_rate == 99

    def test_aio_client_default_is_pac_default(self, aerospike_host):
        c = Client(aerospike_host)
        # No override means the PAC defaults flow through unchanged.
        assert c._policy.max_error_rate == 100
        assert c._policy.error_rate_window == 1

    def test_sync_client_sets_max_error_rate(self, aerospike_host):
        sc = SyncClient(aerospike_host, max_error_rate=11)
        try:
            assert sc._policy is not None
            assert sc._policy.max_error_rate == 11
        finally:
            sc.close()

    def test_sync_client_no_kwargs_uses_default_policy(self, aerospike_host):
        # Without any kwargs and no explicit policy, the inherited Client
        # constructor materializes a default ClientPolicy. The user-visible
        # invariant (PAC sees a default policy at connect time) is unchanged.
        sc = SyncClient(aerospike_host)
        try:
            assert sc._policy is not None
            assert isinstance(sc._policy, ClientPolicy)
        finally:
            sc.close()


class TestExceptionMapping:
    """``_convert_pac_exception`` translates PAC's ``MaxErrorRate`` to PSDK's ``MaxErrorRate``."""

    def test_pac_max_error_rate_maps(self):
        pac_exc = PacMaxErrorRate("node 10.0.0.1:3000 backing off")
        sdk_exc = _convert_pac_exception(pac_exc)
        assert isinstance(sdk_exc, MaxErrorRate)
        assert "10.0.0.1:3000" in str(sdk_exc)
