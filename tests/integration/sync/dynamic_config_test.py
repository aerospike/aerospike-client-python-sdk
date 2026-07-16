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

"""Dynamic-configuration integration coverage.

Mirrors the dynamic-config coverage of the reference clients: load a good YAML,
tolerate a bad field, fall back on a missing file, and (where observable)
prove an override reaches an operation and survives a reload.

The dynamic-config subsystem lives in the Rust core and is reached via the
``AEROSPIKE_CLIENT_CONFIG_URL`` environment variable, so the underlying async
client must be built with the dynamic-config feature enabled.
"""

import contextlib
import os

import pytest

from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.sync import ClusterDefinition

_GOOD = 'version: "1.0.0"\ndynamic:\n  read:\n    max_retries: 7\n'
_BAD_VALUE = 'version: "1.0.0"\ndynamic:\n  read:\n    max_retries: not_a_number\n'
_MALFORMED = 'version: "1.0.0"\ndynamic: [unbalanced : bracket\n'
_NO_VERSION = 'dynamic:\n  read:\n    max_retries: 7\n'
_BAD_VERSION_VALUE = 'version: "0.0.9"\ndynamic:\n  read:\n    max_retries: 7\n'


def _host_port() -> tuple[str, int]:
    hostport = os.environ.get("AEROSPIKE_HOST", "127.0.0.1:3100")
    host, port = hostport.split(":", 1)
    return host, int(port)


def _write(tmp_path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text)
    return "file://" + str(path)


@contextlib.contextmanager
def _client(config_url: str | None):
    """Connect a sync client with (or without) a dynamic-config URL in the env.

    The env var is read by the core at connect time, so it must be set before
    ``connect()`` and restored afterward.
    """
    prev = os.environ.get("AEROSPIKE_CLIENT_CONFIG_URL")
    if config_url is None:
        os.environ.pop("AEROSPIKE_CLIENT_CONFIG_URL", None)
    else:
        os.environ["AEROSPIKE_CLIENT_CONFIG_URL"] = config_url
    host, port = _host_port()
    cluster = ClusterDefinition(host, port).connect()
    try:
        yield cluster
    finally:
        cluster.close()
        if prev is None:
            os.environ.pop("AEROSPIKE_CLIENT_CONFIG_URL", None)
        else:
            os.environ["AEROSPIKE_CLIENT_CONFIG_URL"] = prev


def _round_trip(cluster) -> dict:
    session = cluster.create_session(Behavior.DEFAULT)
    key = DataSet.of("test", "dynconf_it").id("k1")
    session.upsert(key).put({"n": 1}).execute()
    return session.query(key).execute().first_or_raise().record.bins


# --- load / fallback / tolerance (client stays usable) ---------------------

def test_valid_config_loads_and_client_operates(tmp_path):
    """A well-formed dynamic config loads and the client reads/writes normally."""
    with _client(_write(tmp_path, "good.yaml", _GOOD)) as cluster:
        assert _round_trip(cluster) == {"n": 1}


def test_missing_file_falls_back_to_defaults(tmp_path):
    """A missing config file must not break the client (PRD fallback)."""
    with _client("file://" + str(tmp_path / "nope.yaml")) as cluster:
        assert _round_trip(cluster) == {"n": 1}


def test_bad_field_value_is_skipped_client_operates(tmp_path):
    """An unparseable field value is skipped; the client still operates.

    Matches the reference clients' invalid-property behavior (skip + keep
    default, no exception).
    """
    with _client(_write(tmp_path, "badval.yaml", _BAD_VALUE)) as cluster:
        assert _round_trip(cluster) == {"n": 1}


def test_unrecognized_version_value_is_tolerated(tmp_path):
    """A present-but-unrecognized ``version`` is tolerated; config still loads.

    Matches the classic client's invalid-version behavior — the core only
    requires the ``version`` key to be present, not to hold a specific value.
    """
    with _client(_write(tmp_path, "badver.yaml", _BAD_VERSION_VALUE)) as cluster:
        assert _round_trip(cluster) == {"n": 1}


# --- structural errors: core fail-softs (logs + ignores), client stays usable ---
# PSDK follows the core's fail-soft policy — inputs are validated at the core,
# not re-validated in the client — so a structurally broken dynamic config is
# logged and ignored rather than raised, and the client always connects with
# usable defaults. This is a deliberate divergence from the classic client,
# which raised on these.

def test_malformed_yaml_is_tolerated_client_operates(tmp_path):
    """Malformed YAML is logged and ignored; the client still operates."""
    with _client(_write(tmp_path, "malformed.yaml", _MALFORMED)) as cluster:
        assert _round_trip(cluster) == {"n": 1}


def test_missing_version_is_tolerated_client_operates(tmp_path):
    """A config missing the top-level `version` is ignored; the client still operates."""
    with _client(_write(tmp_path, "nover.yaml", _NO_VERSION)) as cluster:
        assert _round_trip(cluster) == {"n": 1}


# --- override / reload effect (blocked on a resolved-policy getter) ---------

@pytest.mark.skip(
    reason="Not testable yet — blocked on rust-core exposing the resolved read policy. "
    "core's resolve_read() is pub(crate) and nothing surfaces the effective policy through "
    "PAC, so an override's effect can't be observed from the client. Override was verified "
    "manually with a temporary core resolve_read debug log (max_retries 2 -> 7). Un-skip when "
    "core exposes a resolved-settings getter.",
)
def test_override_changes_resolved_read_policy(tmp_path):
    raise AssertionError("resolved read policy is not observable through the PSDK surface")


@pytest.mark.skip(
    reason="Not testable yet — same missing resolved-policy getter as the override test. "
    "resolve_read() is pub(crate), so a reloaded value can't be read back through PAC. "
    "Un-skip when core exposes a resolved-settings getter.",
)
def test_reload_reflects_updated_resolved_policy(tmp_path):
    raise AssertionError("resolved read policy is not observable through the PSDK surface")
