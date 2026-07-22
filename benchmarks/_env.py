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

"""Shared environment helpers for benchmark scripts.

Loads aerospike.env (or aerospike.env.example) from the repo root so that
benchmarks pick up the same connection settings as pytest and examples without
requiring the user to manually export variables.
"""

from __future__ import annotations

import os
from pathlib import Path

from aerospike_async import AuthMode, ClientPolicy, TlsConfig


def _load_env_file(path: Path, *, override: bool = False) -> None:
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"'")
                if override or key not in os.environ:
                    os.environ[key] = value


def ensure_env() -> None:
    """Load aerospike.env (or .example) into ``os.environ``."""
    root = Path(__file__).resolve().parent.parent
    env_local = root / "aerospike.env"
    env_example = root / "aerospike.env.example"
    if env_local.exists():
        _load_env_file(env_local, override=False)
    elif env_example.exists():
        _load_env_file(env_example, override=False)


def default_host() -> str:
    """Return the seed host string from the environment, e.g. ``127.0.0.1:3100``."""
    return os.environ.get("AEROSPIKE_HOST", "127.0.0.1:3000")


def default_client_policy() -> ClientPolicy:
    """Return a :class:`ClientPolicy` honouring ``AEROSPIKE_USE_SERVICES_ALTERNATE``."""
    policy = ClientPolicy()
    v = os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "false").strip().lower()
    policy.use_services_alternate = v in ("true", "1", "yes")
    return policy


_AUTH_MODES = {
    "INTERNAL": AuthMode.INTERNAL,
    "EXTERNAL": AuthMode.EXTERNAL,
    "PKI": AuthMode.PKI,
}


def client_policy_from_config(cfg: object) -> ClientPolicy:
    """Build a :class:`ClientPolicy` from a :class:`WorkloadConfig`.

    ``use_services_alternate`` falls back to ``AEROSPIKE_USE_SERVICES_ALTERNATE``
    when the CLI flag is unset, so running from macOS against a container that
    publishes ``alternate-access-address`` just works via ``aerospike.env``.
    """
    policy = ClientPolicy()
    env_alt = os.environ.get(
        "AEROSPIKE_USE_SERVICES_ALTERNATE", "").strip().lower() in ("true", "1", "yes")
    cli_alt = getattr(cfg, "services_alternate", None)
    policy.use_services_alternate = cli_alt if cli_alt is not None else env_alt

    if getattr(cfg, "seed_only_cluster", False):
        policy.seed_only_cluster = True

    ca = getattr(cfg, "tls_ca_file", None)
    cert = getattr(cfg, "tls_cert_file", None)
    key = getattr(cfg, "tls_key_file", None)
    if ca:
        if cert and key:
            policy.tls_config = TlsConfig.with_client_auth(ca, cert, key)
        else:
            policy.tls_config = TlsConfig(ca)

    mode_str = getattr(cfg, "auth_mode", None)
    user = getattr(cfg, "auth_user", None)
    password = getattr(cfg, "auth_password", None)
    if mode_str:
        mode = _AUTH_MODES[mode_str.upper()]
        if mode == AuthMode.PKI:
            policy.set_auth_mode(mode)
        else:
            policy.set_auth_mode(mode, user=user or "", password=password or "")

    return policy


# Load on import so that callers get env vars populated immediately.
ensure_env()


def definition_from_config(cfg: object):
    """Build an async :class:`ClusterDefinition` mirroring :func:`client_policy_from_config`.

    Used by the AsyncPool bench mode, whose contract is definition-based.
    Returns ``None`` when the config needs a knob the definition path does
    not expose (``seed_only_cluster``) — the caller falls back to the
    deprecated ``client_factory`` shape in that case.
    """
    from aerospike_sdk import ClusterDefinition, Host

    if getattr(cfg, "seed_only_cluster", False):
        return None

    definition = ClusterDefinition(hosts=Host.parse_hosts(cfg.seeds, 3000))

    env_alt = os.environ.get(
        "AEROSPIKE_USE_SERVICES_ALTERNATE", "").strip().lower() in ("true", "1", "yes")
    cli_alt = getattr(cfg, "services_alternate", None)
    if (cli_alt if cli_alt is not None else env_alt):
        definition.using_services_alternate()

    ca = getattr(cfg, "tls_ca_file", None)
    cert = getattr(cfg, "tls_cert_file", None)
    key = getattr(cfg, "tls_key_file", None)
    if ca:
        tls = definition.with_tls_config_of().ca_file(ca)
        if cert and key:
            tls.client_cert_file(cert).client_key_file(key)
        tls.done()

    mode_str = getattr(cfg, "auth_mode", None)
    user = getattr(cfg, "auth_user", None)
    password = getattr(cfg, "auth_password", None)
    if mode_str:
        mode = _AUTH_MODES[mode_str.upper()]
        if mode == AuthMode.PKI:
            definition.with_certificate_credentials()
        elif mode == AuthMode.EXTERNAL:
            definition.with_external_credentials(user or "", password or "")
        else:
            definition.with_native_credentials(user or "", password or "")

    return definition
