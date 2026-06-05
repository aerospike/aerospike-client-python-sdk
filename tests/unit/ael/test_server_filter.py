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

from __future__ import annotations

import os

import pytest

from aerospike_sdk import parse_ael
from aerospike_sdk.ael.server_filter import filter_expression_from_ael_string
from tests.pac_compat import skip_if_pac_lacks_from_server_compiled_ael


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def test_server_filter_uses_parse_when_not_supported() -> None:
    print(
        "\n[server_filter] UNIT (no TCP to Aerospike): supports_server_compiled_ael=False "
        "→ expect client parse_ael path",
        flush=True,
    )
    fe = filter_expression_from_ael_string(
        "$.x > 1",
        supports_server_compiled_ael=False,
    )
    assert fe == parse_ael("$.x > 1")
    print("  → branch: client parse (same as parse_ael)", flush=True)


def test_server_filter_uses_server_compiled_when_supported() -> None:
    print(
        "\n[server_filter] UNIT (no TCP to Aerospike): supports_server_compiled_ael=True "
        "→ expect FilterExpression.from_server_compiled_ael (PAC wire only)",
        flush=True,
    )
    skip_if_pac_lacks_from_server_compiled_ael()
    fe = filter_expression_from_ael_string(
        "$.x > 1",
        supports_server_compiled_ael=True,
    )
    assert fe != parse_ael("$.x > 1")
    print(
        "  → branch: server-compiled *wire* via PAC (does NOT prove a server applied it)",
        flush=True,
    )


def test_server_filter_falls_back_when_pac_lacks_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    print(
        "\n[server_filter] UNIT (no TCP): monkeypatch removes FilterExpression → expect parse_ael",
        flush=True,
    )
    class _NoFactory:
        pass

    import aerospike_sdk.ael.server_filter as sf

    monkeypatch.setattr(sf, "FilterExpression", _NoFactory)
    fe = sf.filter_expression_from_ael_string(
        "$.x > 1",
        supports_server_compiled_ael=True,
    )
    assert fe == parse_ael("$.x > 1")
    print("  → branch: client parse (fallback)", flush=True)


def test_server_filter_respects_force_env(monkeypatch: pytest.MonkeyPatch) -> None:
    print(
        "\n[server_filter] UNIT (no TCP): AEROSPIKE_SDK_FORCE_CLIENT_AEL_PARSE=1 "
        "→ expect parse_ael even if server flag would be true",
        flush=True,
    )
    monkeypatch.setenv("AEROSPIKE_SDK_FORCE_CLIENT_AEL_PARSE", "1")
    try:
        fe = filter_expression_from_ael_string(
            "$.x > 1",
            supports_server_compiled_ael=True,
        )
        assert fe == parse_ael("$.x > 1")
        print("  → branch: client parse (env override)", flush=True)
    finally:
        monkeypatch.delenv("AEROSPIKE_SDK_FORCE_CLIENT_AEL_PARSE", raising=False)


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _truthy_env("AEROSPIKE_LIVE_PROBE_SERVER_COMPILED_AEL"),
    reason=(
        "Offline by default. Live cluster banner: "
        "AEROSPIKE_LIVE_PROBE_SERVER_COMPILED_AEL=1 AEROSPIKE_HOST=127.0.0.1:3000 "
        "pytest tests/unit/ael/test_server_filter.py::test_live_cluster_prints_server_compiled_gate -s"
    ),
)
async def test_live_cluster_prints_server_compiled_gate() -> None:
    """Connect to seeds and print whether nodes/SDK gate server-compiled AEL (≥ 8.1.3)."""
    skip_if_pac_lacks_from_server_compiled_ael()
    host = (os.environ.get("AEROSPIKE_HOST") or "").strip()
    if not host:
        pytest.skip("Set AEROSPIKE_HOST for live probe (e.g. 127.0.0.1:3000)")

    from aerospike_async import ClientPolicy, new_client

    from aerospike_sdk import Client

    print(
        f"\n[server_filter] LIVE CLUSTER PROBE seeds={host!r} "
        "(Version.supports_server_compiled_ael → ≥ 8.1.3.0)",
        flush=True,
    )

    pac = await new_client(ClientPolicy(), host)
    try:
        nodes = await pac.nodes()
        active = [n for n in nodes if n.is_active]
        if not active:
            print("  → no active nodes (unexpected); cannot evaluate gate", flush=True)
        for n in active:
            v = n.version
            ok = v.supports_server_compiled_ael()
            print(
                f"  → active node version={v} "
                f"supports_server_compiled_ael={ok}",
                flush=True,
            )
    finally:
        await pac.close()

    async with Client(host) as client:
        gate = client.supports_server_compiled_ael
        print(
            f"  → SDK Client.supports_server_compiled_ael={gate} "
            "(True only if every active node reports support + PAC API + no force-env)",
            flush=True,
        )
