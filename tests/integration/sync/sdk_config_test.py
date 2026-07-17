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

"""SDK config file integration coverage (sync).

Connects with ``AEROSPIKE_SDK_CONFIG_URL`` pointing at generated YAML and
asserts the resolved settings reach the client, precedence holds, fail-soft
never blocks a connect, and hot-reload swaps settings on a live client.
"""

import contextlib
import os
import time

from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.policy import SystemSettings, TransactionSettings
from aerospike_sdk.sync import ClusterDefinition

_IMPLICIT_FALSE_MAXCONNS = """
system:
  DEFAULT:
    connections:
      maximumConnectionsPerNode: 77
    transactions:
      implicitBatchWriteTransactions: false
"""
_IMPLICIT_TRUE = """
system:
  DEFAULT:
    transactions:
      implicitBatchWriteTransactions: true
"""
_IMPLICIT_FALSE = """
system:
  DEFAULT:
    transactions:
      implicitBatchWriteTransactions: false
"""
_MALFORMED = "system: [unbalanced : bracket\n"


def _host_port() -> tuple[str, int]:
    hostport = os.environ.get("AEROSPIKE_HOST", "127.0.0.1:3100")
    host, port = hostport.split(":", 1)
    return host, int(port)


def _write(tmp_path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text)
    return str(path)


@contextlib.contextmanager
def _sdk_config_env(path: str | None):
    """Set (or clear) AEROSPIKE_SDK_CONFIG_URL for the duration of a connect."""
    prev = os.environ.get("AEROSPIKE_SDK_CONFIG_URL")
    if path is None:
        os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)
    else:
        os.environ["AEROSPIKE_SDK_CONFIG_URL"] = path
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)
        else:
            os.environ["AEROSPIKE_SDK_CONFIG_URL"] = prev


def _round_trip(cluster) -> dict:
    session = cluster.create_session(Behavior.DEFAULT)
    key = DataSet.of("test", "sdkconf_it").id("k1")
    session.upsert(key).put({"n": 1}).execute()
    return session.query(key).execute().first_or_raise().record.bins


def _bump_mtime(path: str) -> None:
    stat = os.stat(path)
    os.utime(path, (stat.st_atime, stat.st_mtime + 2))


def test_config_reaches_client_and_policy(tmp_path):
    """File settings land on the client holder and the built ClientPolicy."""
    host, port = _host_port()
    with _sdk_config_env(_write(tmp_path, "sdk.yaml", _IMPLICIT_FALSE_MAXCONNS)):
        with ClusterDefinition(host, port).connect() as cluster:
            client = cluster._sdk_client
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is False
            assert client._policy.max_conns_per_node == 77
            assert _round_trip(cluster) == {"n": 1}


def test_file_wins_over_programmatic_per_field(tmp_path):
    """The file wins only for fields it provides; others keep programmatic values."""
    host, port = _host_port()
    programmatic = SystemSettings(
        max_connections_per_node=50,
        conn_pools_per_node=2,
        transactions=TransactionSettings(number_of_attempts=9),
    )
    with _sdk_config_env(_write(tmp_path, "sdk.yaml", _IMPLICIT_FALSE_MAXCONNS)):
        with (
            ClusterDefinition(host, port)
            .with_system_settings(programmatic)
            .connect()
        ) as cluster:
            client = cluster._sdk_client
            assert client._policy.max_conns_per_node == 77
            assert client._policy.conn_pools_per_node == 2
            settings = client._sdk_settings
            assert settings.transactions.implicit_batch_write_transactions is False
            assert settings.transactions.number_of_attempts == 9


def test_missing_file_fail_soft_defaults_apply(tmp_path):
    """A missing config file never blocks a connect; hard defaults apply."""
    host, port = _host_port()
    with _sdk_config_env(str(tmp_path / "nope.yaml")):
        with ClusterDefinition(host, port).connect() as cluster:
            client = cluster._sdk_client
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is True
            assert _round_trip(cluster) == {"n": 1}


def test_malformed_file_fail_soft_programmatic_wins(tmp_path):
    """A malformed file is ignored; programmatic settings still apply."""
    host, port = _host_port()
    programmatic = SystemSettings(
        transactions=TransactionSettings(implicit_batch_write_transactions=False),
    )
    with _sdk_config_env(_write(tmp_path, "bad.yaml", _MALFORMED)):
        with (
            ClusterDefinition(host, port)
            .with_system_settings(programmatic)
            .connect()
        ) as cluster:
            client = cluster._sdk_client
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is False
            assert _round_trip(cluster) == {"n": 1}


def test_hot_reload_swaps_transactions(tmp_path):
    """Rewriting the file swaps the live settings holder within the poll cadence."""
    host, port = _host_port()
    path = _write(tmp_path, "sdk.yaml", _IMPLICIT_TRUE)
    with _sdk_config_env(path):
        with ClusterDefinition(host, port).connect() as cluster:
            client = cluster._sdk_client
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is True

            with open(path, "w") as fh:
                fh.write(_IMPLICIT_FALSE)
            _bump_mtime(path)

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if not client._sdk_settings.transactions.implicit_batch_write_transactions:
                    break
                time.sleep(0.2)
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is False


def test_hot_reload_broken_file_keeps_last_good(tmp_path):
    """A file that stops parsing mid-run keeps the last-good settings."""
    host, port = _host_port()
    path = _write(tmp_path, "sdk.yaml", _IMPLICIT_FALSE)
    with _sdk_config_env(path):
        with ClusterDefinition(host, port).connect() as cluster:
            client = cluster._sdk_client
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is False

            with open(path, "w") as fh:
                fh.write(_MALFORMED)
            _bump_mtime(path)

            # Give the poller time to see the broken file; settings must hold.
            time.sleep(2.5)
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is False
