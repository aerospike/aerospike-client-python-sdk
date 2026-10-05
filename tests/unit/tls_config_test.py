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

"""What ``with_tls_config`` actually does with the options it accepts.

These assert on the built config rather than on a connection: the failure
being guarded against is silent, so a test that only checks "TLS connects"
cannot see it. A definition that accepts a protocol restriction and discards it
still connects perfectly well -- against a protocol the caller excluded.
"""

import pytest

from aerospike_sdk import ClusterDefinition
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.sync import ClusterDefinition as SyncClusterDefinition


def _config(**tls):
    return ClusterDefinition("localhost", 4333).with_tls_config(**tls)._tls.build_tls_config()


class TestTlsNameOnly:

    def test_tls_name_only_still_builds_a_config(self):
        """A ``tls_name`` with no CA file must verify against the system trust
        store, not silently produce a plaintext connection.

        Returning ``None`` here is the worst failure in this file: the seed
        string still gets tls-names stamped, so the caller believes TLS is on.
        """
        assert _config(tls_name="cluster.example.com") is not None


class TestProtocolsReachTheConfig:
    """The rejection test is the one that proves pass-through.

    A built config is opaque from Python, so "it built" cannot distinguish a
    protocol that reached rustls from one that was dropped. An *invalid* name
    can: it only raises if the value got far enough to be validated.
    """

    def test_valid_protocol_is_accepted(self):
        assert _config(tls_name="x", protocols=["TLSv1.3"]) is not None

    def test_unknown_protocol_is_rejected(self):
        with pytest.raises(ValueError, match="protocol"):
            _config(tls_name="x", protocols=["SSLv3"])

    def test_bare_string_is_refused(self):
        """A lone string would otherwise be split into one "protocol" per character."""
        with pytest.raises(TypeError, match="protocols"):
            ClusterDefinition("localhost", 4333).with_tls_config(protocols="TLSv1.3")


class TestCiphersReachTheConfig:
    """As above: the rejection test carries the proof."""

    def test_valid_cipher_is_accepted(self):
        assert _config(tls_name="x", ciphers=["TLS13_AES_256_GCM_SHA384"]) is not None

    def test_unknown_cipher_is_rejected(self):
        with pytest.raises(ValueError, match="cipher"):
            _config(tls_name="x", ciphers=["NOT_A_SUITE"])

    def test_bare_string_is_refused(self):
        with pytest.raises(TypeError, match="ciphers"):
            ClusterDefinition("localhost", 4333).with_tls_config(ciphers="NOT_A_LIST")


class TestForLoginOnlyIsNotSilentlyDropped:

    def test_login_only_reaches_the_config(self):
        """The flag rides on the built config; the client core does the rest."""
        assert _config(tls_name="x", for_login_only=True).for_login_only is True
        assert _config(tls_name="x").for_login_only is False


class TestClientCertificatePair:

    @pytest.mark.parametrize("half", [
        {"client_cert_file": "/certs/client.pem"},
        {"client_key_file": "/certs/client.key"},
    ])
    def test_one_half_of_the_pair_is_refused(self, half):
        """Either file alone would quietly fall back to server-only TLS."""
        with pytest.raises(ValueError, match="client_cert_file and client_key_file"):
            ClusterDefinition("localhost", 4333).with_tls_config(**half)


class TestEmptyRestrictionIsRefused:

    @pytest.mark.parametrize("field", ["protocols", "ciphers"])
    def test_empty_list_is_refused(self, field):
        """An empty list would otherwise mean "no restriction", allowing every
        default -- the opposite of what an explicit empty restriction asks."""
        with pytest.raises(ValueError, match=field):
            ClusterDefinition("localhost", 4333).with_tls_config(**{field: []})


class TestUnsatisfiableRestrictionIsRefused:

    def test_version_and_suite_that_cannot_agree(self):
        """A restriction no handshake could satisfy must fail loudly at connect.

        Pairing TLS 1.2 with a suite that exists only in TLS 1.3 leaves no
        usable suite. Silently ignoring one of the two would connect anyway,
        on terms the caller did not ask for -- the failure mode a security
        knob can least afford.
        """
        cd = SyncClusterDefinition("localhost", 4333).with_tls_config(
            tls_name="x",
            protocols=["TLSv1.2"],
            ciphers=["TLS13_AES_256_GCM_SHA384"],
        )
        with pytest.raises(AerospikeError, match="no usable cipher suites"):
            cd.connect()


class TestRepeatedCalls:

    def test_a_second_call_replaces_the_first(self):
        cd = (
            ClusterDefinition("localhost", 4333)
            .with_tls_config(tls_name="old", for_login_only=True)
            .with_tls_config(tls_name="new")
        )
        assert cd._tls.tls_name == "new"
        assert cd._tls.for_login_only is False


class TestUnreadableFileAtConnect:

    def test_missing_ca_file_raises_an_sdk_error(self, tmp_path):
        """The CA file is opened at connect, so its failure must arrive as an
        SDK exception like every other connect failure."""
        missing = tmp_path / "ca.pem"
        cd = SyncClusterDefinition("localhost", 4333).with_tls_config(ca_file=str(missing))
        with pytest.raises(AerospikeError, match="ca.pem"):
            cd.connect()


class TestSyncDefinition:

    def test_sync_definition_takes_the_same_settings(self):
        cd = SyncClusterDefinition("localhost", 4333).with_tls_config(tls_name="myTls")
        assert cd._tls.tls_name == "myTls"
        assert cd._tls.build_tls_config() is not None
