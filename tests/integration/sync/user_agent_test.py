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

"""Server-side verification that PSDK stamps its own client identifier.

PSDK overrides the underlying async client's user-agent id with its own
``python-sdk-<version>``. This connects, reads back the identifiers the server
has recorded, and confirms PSDK's is present — proving the id reaches the
server, not merely that it is set on the policy (the client sends it
fire-and-forget, so a broken send is invisible to a plain round-trip).

Requires a server that supports the ``user-agents`` info command (>= 8.1);
skips cleanly on older servers.
"""

import base64
import os
import socket
import time

import pytest

from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.sync import ClusterDefinition


def _host_port() -> tuple[str, int]:
    host, port = os.environ.get("AEROSPIKE_HOST", "127.0.0.1:3100").split(":", 1)
    return host, int(port)


def _server_user_agents(host: str, port: int) -> list[str]:
    """Return the client identifiers the server currently tracks (decoded).

    Sends the raw ``user-agents`` info command; skips if the server does not
    support it. Each returned entry is the decoded ``version,client_id,app_id``.
    """
    conn = socket.create_connection((host, port), 3)
    try:
        body = b"user-agents\n"
        header = (2 << 56) | (1 << 48) | len(body)  # proto version 2, type 1 (info)
        conn.sendall(header.to_bytes(8, "big") + body)
        size = int.from_bytes(conn.recv(8), "big") & 0xFFFFFFFFFFFF
        data = b""
        while len(data) < size:
            data += conn.recv(size - len(data))
    finally:
        conn.close()

    payload = data.decode(errors="replace").split("\t", 1)[-1].strip()
    if not payload or "ERROR" in payload:
        pytest.skip("server does not support the `user-agents` info command (needs >= 8.1)")
    return [
        base64.b64decode(entry.split("user-agent=")[1].split(":")[0]).decode(errors="replace")
        for entry in payload.split(";")
        if "user-agent=" in entry
    ]


def _poll_user_agents(host, port, predicate, tries=10, delay=0.5):
    """Poll the server's tracked user-agents until ``predicate`` matches one.

    The user-agent is sent during node validation and aggregated server-side,
    so a single read can race the registration.
    """
    agents: list[str] = []
    for _ in range(tries):
        agents = _server_user_agents(host, port)
        if any(predicate(ua) for ua in agents):
            break
        time.sleep(delay)
    return agents


def test_psdk_user_agent_reaches_server():
    """A PSDK connection registers its full user-agent on the server.

    Verifies both fields in one connection: the ``python-sdk-*`` client id the
    SDK stamps automatically, and the application id set via
    :meth:`ClusterDefinition.app_id`.
    """
    host, port = _host_port()
    app_id = "psdk-itest-app"
    cluster = ClusterDefinition(host, port).app_id(app_id).connect()
    try:
        # Force a real node connection so the user-agent is sent and registered.
        session = cluster.create_session(Behavior.DEFAULT)
        session.upsert(DataSet.of("test", "user_agent").id("k1")).put({"n": 1}).execute()

        def _match(ua: str) -> bool:
            fields = ua.split(",")
            return len(fields) >= 3 and fields[1].startswith("python-sdk-") and fields[2] == app_id

        agents = _poll_user_agents(host, port, _match)
        assert any(_match(ua) for ua in agents), (
            f"PSDK user-agent (python-sdk-* client id + app id {app_id!r}) not found: {agents}"
        )
    finally:
        cluster.close()
