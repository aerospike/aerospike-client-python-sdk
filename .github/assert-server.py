#!/usr/bin/env python3
"""Fail the job unless the server under test is reachable and is the one we asked for.

The integration suite *skips* when it cannot reach a cluster rather than failing.
That is the right behavior locally -- not everyone has every cluster shape -- but
in CI it means a broken image pull, a mistyped tag, or a server that never
finished starting would produce a green run that tested nothing. This asserts the
three things a green run has to mean, before any test runs.

Speaks the info protocol directly so it needs no client library and no server
tooling: the enterprise images ship no asinfo, and the wheel under test is not
necessarily installed at this point in the job.

Env:
    SERVER_HOST     default 127.0.0.1
    SERVER_PORT     default 3000
    EXPECTED_BUILD  e.g. "8.1.3.0"; the tag's version prefix must match the
                    server's reported build. Empty disables the check.
"""

from __future__ import annotations

import os
import socket
import struct
import sys


def info(host: str, port: int, command: str, timeout: float = 5.0) -> str:
    """Send one info command and return its value."""
    payload = (command + "\n").encode()
    # 1 byte version, 1 byte type (info), 6-byte big-endian payload length.
    header = struct.pack("!BB", 2, 1) + struct.pack("!Q", len(payload))[2:]
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall(header + payload)
        head = b""
        while len(head) < 8:
            chunk = sock.recv(8 - len(head))
            if not chunk:
                raise OSError("connection closed while reading the info header")
            head += chunk
        size = struct.unpack("!Q", b"\x00\x00" + head[2:8])[0]
        body = b""
        while len(body) < size:
            chunk = sock.recv(size - len(body))
            if not chunk:
                raise OSError("connection closed while reading the info body")
            body += chunk
    # Responses come back as "<command>\t<value>\n".
    return body.decode().split("\t", 1)[1].strip()


def main() -> None:
    host = os.environ.get("SERVER_HOST", "127.0.0.1")
    port = int(os.environ.get("SERVER_PORT", "3000"))
    expected = os.environ.get("EXPECTED_BUILD", "").split("-")[0].strip()

    try:
        build = info(host, port, "build")
        edition = info(host, port, "edition")
    except OSError as exc:
        sys.exit(
            f"::error::No Aerospike server answering at {host}:{port} ({exc}). "
            "The suite would have skipped its way to a green run."
        )

    print(f"server at {host}:{port} -> build {build}, {edition}")

    if expected and not build.startswith(expected):
        sys.exit(f"::error::Expected server build {expected}, got {build}")

    if "Enterprise" not in edition:
        sys.exit(
            f"::error::Expected an enterprise server, got {edition!r}. "
            "Enterprise-gated suites would skip."
        )


if __name__ == "__main__":
    main()
