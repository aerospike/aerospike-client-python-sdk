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

"""SDK config file integration coverage (async).

The async side owns a distinct hot-reload implementation (an ``asyncio.Task``
poller rather than the sync daemon thread) and its own ClusterDefinition
wiring, so both are exercised here; parse/precedence behavior is shared
loader code covered by the unit and sync suites.
"""

import asyncio
import contextlib
import os

import pytest

from aerospike_sdk import Behavior, ClusterDefinition, DataSet
from aerospike_sdk.policy import get_behavior

_IMPLICIT_TRUE = """
system:
  DEFAULT:
    transactions:
      implicitBatchWriteTransactions: true
"""
_IMPLICIT_FALSE = """
system:
  DEFAULT:
    connections:
      maximumConnectionsPerNode: 88
    transactions:
      implicitBatchWriteTransactions: false
"""


def _host_port(aerospike_host: str) -> tuple[str, int]:
    if ":" in aerospike_host:
        host, port_str = aerospike_host.split(":", 1)
        return host, int(port_str)
    return aerospike_host, 3000


def _write(tmp_path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text)
    return str(path)


@contextlib.contextmanager
def _sdk_config_env(path: str):
    prev = os.environ.get("AEROSPIKE_SDK_CONFIG_URL")
    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = path
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)
        else:
            os.environ["AEROSPIKE_SDK_CONFIG_URL"] = prev


def _bump_mtime(path: str) -> None:
    stat = os.stat(path)
    os.utime(path, (stat.st_atime, stat.st_mtime + 2))


async def test_config_reaches_client_and_operates(aerospike_host, tmp_path):
    """File settings land on the async client and the client operates."""
    host, port = _host_port(aerospike_host)
    with _sdk_config_env(_write(tmp_path, "sdk.yaml", _IMPLICIT_FALSE)):
        async with await ClusterDefinition(host, port).connect() as cluster:
            client = cluster._sdk_client
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is False
            assert client._policy.max_conns_per_node == 88

            session = cluster.create_session(Behavior.DEFAULT)
            key = DataSet.of("test", "sdkconf_it_async").id("k1")
            await session.upsert(key).put({"n": 1}).execute()
            stream = await session.query(key).execute()
            result = await stream.first_or_raise()
            assert result.record.bins == {"n": 1}


async def test_hot_reload_swaps_on_async_client(aerospike_host, tmp_path):
    """The asyncio poller swaps the settings holder when the file changes."""
    host, port = _host_port(aerospike_host)
    path = _write(tmp_path, "sdk.yaml", _IMPLICIT_TRUE)
    with _sdk_config_env(path):
        async with await ClusterDefinition(host, port).connect() as cluster:
            client = cluster._sdk_client
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is True

            with open(path, "w") as fh:
                fh.write(_IMPLICIT_FALSE)
            _bump_mtime(path)

            loop = asyncio.get_running_loop()
            deadline = loop.time() + 5.0
            while loop.time() < deadline:
                if not client._sdk_settings.transactions.implicit_batch_write_transactions:
                    break
                await asyncio.sleep(0.2)
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is False


async def test_behaviors_section_defines_usable_behavior(aerospike_host, tmp_path):
    """A file-defined behavior is registered at connect and drives real ops."""
    host, port = _host_port(aerospike_host)
    yaml_text = (
        "behaviors:\n"
        "  cfg-reads:\n"
        "    allOperations:\n"
        "      abandonCallAfter: 5s\n"
        "      maximumNumberOfCallAttempts: 2\n"
    )
    with _sdk_config_env(_write(tmp_path, "sdk.yaml", yaml_text)):
        async with await ClusterDefinition(host, port).connect() as cluster:
            behavior = get_behavior("cfg-reads")
            assert behavior is not None

            session = cluster.create_session(behavior)
            assert session._cached_read_policy.total_timeout == 5_000
            assert session._cached_read_policy.max_retries == 1

            key = DataSet.of("test", "sdkconf_bhv_async").id("k1")
            await session.upsert(key).put({"n": 1}).execute()
            stream = await session.query(key).execute()
            result = await stream.first_or_raise()
            assert result.record.bins == {"n": 1}


async def test_behaviors_hot_reload_updates_live_session(aerospike_host, tmp_path):
    """Editing a behavior in the file updates an already-created session."""
    host, port = _host_port(aerospike_host)
    yaml_text = (
        "behaviors:\n"
        "  cfg-hot:\n"
        "    allOperations:\n"
        "      abandonCallAfter: 5s\n"
    )
    path = _write(tmp_path, "sdk.yaml", yaml_text)
    with _sdk_config_env(path):
        async with await ClusterDefinition(host, port).connect() as cluster:
            session = cluster.create_session(get_behavior("cfg-hot"))
            assert session._cached_read_policy.total_timeout == 5_000

            with open(path, "w") as fh:
                fh.write(yaml_text.replace("5s", "12s"))
            _bump_mtime(path)

            loop = asyncio.get_running_loop()
            deadline = loop.time() + 5.0
            while loop.time() < deadline:
                if session._cached_read_policy.total_timeout == 12_000:
                    break
                await asyncio.sleep(0.2)
            assert session._cached_read_policy.total_timeout == 12_000


async def test_named_profile_selected_by_cluster_name(aerospike_host, tmp_path):
    """A ``system.<cluster-name>`` profile layers over ``DEFAULT`` at connect.

    Requires the server to have a configured cluster name (validated on the
    wire by ``validate_cluster_name_is``); skips when the server has none.
    """
    host, port = _host_port(aerospike_host)
    async with await ClusterDefinition(host, port).connect() as probe:
        by_node = await probe._sdk_client.underlying_client.info("cluster-name")
    names = {v for v in by_node.values() if v and v != "null"}
    if not names:
        pytest.skip("server has no cluster-name configured")
    cluster_name = names.pop()

    yaml_text = (
        "system:\n"
        "  DEFAULT:\n"
        "    transactions:\n"
        "      implicitBatchWriteTransactions: true\n"
        f"  {cluster_name}:\n"
        "    transactions:\n"
        "      implicitBatchWriteTransactions: false\n"
    )
    with _sdk_config_env(_write(tmp_path, "sdk.yaml", yaml_text)):
        async with await (
            ClusterDefinition(host, port)
            .validate_cluster_name_is(cluster_name)
            .connect()
        ) as cluster:
            client = cluster._sdk_client
            assert client._sdk_settings.transactions.implicit_batch_write_transactions is False
