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

"""What ``TlsBuilder`` actually does with the options it accepts.

These assert on the built config rather than on a connection: the failure
being guarded against is silent, so a test that only checks "TLS connects"
cannot see it. A builder that accepts a protocol restriction and discards it
still connects perfectly well -- against a protocol the caller excluded.
"""

import pytest

from aerospike_sdk import ClusterDefinition
from aerospike_sdk.sync import ClusterDefinition as SyncClusterDefinition


def _builders():
    """The async and sync builders, which are parallel implementations."""
    return [
        ClusterDefinition("localhost", 4333).with_tls_config_of(),
        SyncClusterDefinition("localhost", 4333).with_tls_config_of(),
    ]


class TestTlsNameOnly:

    @pytest.mark.parametrize("builder", _builders())
    def test_tls_name_only_still_builds_a_config(self, builder):
        """A ``tls_name`` with no CA file must verify against the system trust
        store, not silently produce a plaintext connection.

        Returning ``None`` here is the worst failure in this file: the seed
        string still gets tls-names stamped, so the caller believes TLS is on.
        """
        config = builder.tls_name("cluster.example.com").build_tls_config()
        assert config is not None


class TestProtocolsReachTheConfig:
    """The rejection test is the one that proves pass-through.

    A built config is opaque from Python, so "it built" cannot distinguish a
    protocol that reached rustls from one that was dropped. An *invalid* name
    can: it only raises if the value got far enough to be validated. Verified
    by re-introducing the drop -- the acceptance test still passed, the
    rejection test failed.
    """

    @pytest.mark.parametrize("builder", _builders())
    def test_valid_protocol_is_accepted(self, builder):
        config = builder.tls_name("x").protocols("TLSv1.3").build_tls_config()
        assert config is not None

    @pytest.mark.parametrize("builder", _builders())
    def test_unknown_protocol_is_rejected(self, builder):
        """Rejected, not ignored. Dropping a restriction is how a caller ends
        up negotiating exactly what they excluded -- and this is also what
        proves the name reached rustls at all."""
        with pytest.raises(ValueError, match="protocol"):
            builder.tls_name("x").protocols("SSLv3").build_tls_config()


class TestCiphersReachTheConfig:
    """As above: the rejection test carries the proof."""

    @pytest.mark.parametrize("builder", _builders())
    def test_valid_cipher_is_accepted(self, builder):
        config = (
            builder.tls_name("x")
            .ciphers("TLS13_AES_256_GCM_SHA384")
            .build_tls_config()
        )
        assert config is not None

    @pytest.mark.parametrize("builder", _builders())
    def test_unknown_cipher_is_rejected(self, builder):
        with pytest.raises(ValueError, match="cipher"):
            builder.tls_name("x").ciphers("NOT_A_SUITE").build_tls_config()


class TestForLoginOnlyIsNotSilentlyDropped:

    @pytest.mark.parametrize("builder", _builders())
    def test_for_login_only_raises_rather_than_lying(self, builder):
        """Login-only TLS cannot be honored: the underlying connection decides
        TLS once at connect and has no way to drop to cleartext afterwards.
        Accepting the flag and ignoring it would misrepresent the transport to
        the caller, so raise instead.

        Deliberately a plain assertion and not an xfail. An xfail would
        claim this is expected to start working, and that is undecided:
        whether login-only TLS should be built at all is an open question,
        since it trades data-plane encryption for throughput. If the answer is
        no, raising is the permanent behavior and this test is correct as
        written.
        """
        with pytest.raises(NotImplementedError, match="login"):
            builder.tls_name("x").for_login_only(True).build_tls_config()


class TestUnsatisfiableRestrictionIsRefused:

    @pytest.mark.parametrize("builder", _builders())
    def test_version_and_suite_that_cannot_agree(self, builder):
        """A restriction no handshake could satisfy must fail loudly.

        Pairing TLS 1.2 with a suite that exists only in TLS 1.3 leaves no
        usable suite. Silently ignoring one of the two would connect anyway,
        on terms the caller did not ask for -- the failure mode a security
        knob can least afford. Rejecting at construction also means the
        caller learns before a connection is attempted.
        """
        configured = (
            builder.tls_name("x")
            .protocols("TLSv1.2")
            .ciphers("TLS13_AES_256_GCM_SHA384")
        )
        with pytest.raises(Exception, match="no usable cipher suites"):
            configured.build_tls_config()
