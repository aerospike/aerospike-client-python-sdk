# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A TCP proxy that severs the connection after a set number of client commands.

Some client behavior is only reachable when a specific command in a multi-step
exchange fails. A transaction commit is three steps -- mark-roll-forward, the
roll-forward batch, then closing the transaction monitor -- and an abandoned
roll-forward means the second one did not land. Nothing the server offers makes
that happen on demand, so the connection is cut instead.

The gate sits between client and server, forwards a chosen number of *data*
messages, then stops forwarding and closes. Info traffic is not counted:
cluster tending runs on its own cadence, and counting it would make the
allowance depend on timing rather than on the commands the test issues.

Usage::

    async with TcpGate.open("127.0.0.1", 3130) as gate:
        cluster_def = ClusterDefinition("127.0.0.1", gate.port).force_single_node()
        ...
        gate.refuse_after_client_messages(1)   # the roll-forward fails

Every partition must be owned by the node behind the gate, since the gate is
the client's only route. ``force_single_node`` restricts the node set but does
not rewrite the partition map, so a multi-node cluster leaves most partitions
unroutable -- point this at a single-node cluster.

Provenance (per repo rules):
    reference: client/src/test/java/com/aerospike/client/sdk/TcpGate.java
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Optional

# Every proto message starts with a version byte, a type byte, then a 48-bit
# body length.
_PROTO_HEADER_SIZE = 8
_AS_MSG_TYPE = 3
_MSG_TYPE_COMPRESSED = 4

# -1 means "forward everything"; the gate only trips once an allowance is set.
_UNLIMITED = -1


class TcpGate:
    """Forwards client traffic to a server until its allowance runs out."""

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._allowance = _UNLIMITED
        self._server: Optional[asyncio.AbstractServer] = None
        self._tasks: set[asyncio.Task] = set()

    @classmethod
    async def open(cls, host: str, port: int) -> TcpGate:
        """Start a gate in front of ``host:port`` on an ephemeral local port."""
        gate = cls(host, port)
        gate._server = await asyncio.start_server(gate._handle, "127.0.0.1", 0)
        return gate

    @property
    def port(self) -> int:
        """The local port clients should connect to."""
        assert self._server is not None, "gate is not open"
        return self._server.sockets[0].getsockname()[1]

    def refuse_after_client_messages(self, count: int) -> None:
        """Forward ``count`` more data messages, then stop forwarding.

        Set this inside the transaction, immediately before the commit, so the
        allowance covers only the commit's own commands.
        """
        self._allowance = count

    async def _handle(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        try:
            server_reader, server_writer = await asyncio.open_connection(
                self._host, self._port
            )
        except OSError:
            client_writer.close()
            return

        # Client -> server is the direction that counts and trips; the return
        # direction is a plain relay that ends when its peer goes away.
        up = asyncio.create_task(self._pump_up(client_reader, server_writer))
        down = asyncio.create_task(self._relay(server_reader, client_writer))
        for task in (up, down):
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        await asyncio.wait({up, down}, return_when=asyncio.FIRST_COMPLETED)
        for writer in (server_writer, client_writer):
            writer.close()
        for task in (up, down):
            task.cancel()

    async def _pump_up(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Forward client bytes, counting data messages, until the gate trips."""
        header = bytearray()
        body_remaining = 0

        while True:
            try:
                chunk = await reader.read(65536)
            except (ConnectionError, asyncio.CancelledError):
                return
            if not chunk:
                return

            offset = 0
            while offset < len(chunk):
                if body_remaining == 0:
                    take = min(_PROTO_HEADER_SIZE - len(header), len(chunk) - offset)
                    header += chunk[offset : offset + take]
                    offset += take
                    if len(header) < _PROTO_HEADER_SIZE:
                        break

                    is_data = header[1] in (_AS_MSG_TYPE, _MSG_TYPE_COMPRESSED)
                    body_remaining = int.from_bytes(header[2:], "big")
                    header.clear()

                    if is_data and not self._spend():
                        # This message is past the allowance: stop forwarding
                        # and drop the connection mid-command.
                        return
                else:
                    skipped = min(body_remaining, len(chunk) - offset)
                    body_remaining -= skipped
                    offset += skipped

            try:
                writer.write(chunk)
                await writer.drain()
            except (ConnectionError, asyncio.CancelledError):
                return

    async def _relay(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while True:
            try:
                chunk = await reader.read(65536)
                if not chunk:
                    return
                writer.write(chunk)
                await writer.drain()
            except (ConnectionError, asyncio.CancelledError):
                return

    def _spend(self) -> bool:
        """Return ``False`` once the allowance is used up, tripping the gate."""
        if self._allowance == _UNLIMITED:
            return True
        if self._allowance <= 0:
            return False
        self._allowance -= 1
        return True

    async def close(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def __aenter__(self) -> TcpGate:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()
