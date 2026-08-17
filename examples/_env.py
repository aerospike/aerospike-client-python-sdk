"""Shared environment helpers for example scripts.

Loads aerospike.env (or aerospike.env.example) from the repo root so that
examples pick up the same connection settings as pytest without requiring
the user to manually export variables.
"""

import logging
import os
from pathlib import Path
from typing import Optional

def _load_env_file(path: Path, *, override: bool = True) -> None:
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


def _ensure_env() -> None:
    root = Path(__file__).resolve().parent.parent
    env_local = root / "aerospike.env"
    env_example = root / "aerospike.env.example"
    if env_local.exists():
        _load_env_file(env_local, override=False)
    elif env_example.exists():
        _load_env_file(env_example, override=False)


_ensure_env()


def _configure_logging() -> None:
    log_level = os.environ.get("AEROSPIKE_LOG_LEVEL", "").upper()
    if not log_level:
        return
    numeric = getattr(logging, log_level, None)
    if numeric is None:
        return
    log_file = os.environ.get("AEROSPIKE_LOG_FILE")
    handler: logging.Handler
    if log_file:
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    ))
    for prefix in ("aerospike_core", "aerospike_async", "aerospike_sdk"):
        logger = logging.getLogger(prefix)
        logger.setLevel(numeric)
        logger.addHandler(handler)


_configure_logging()


def _host_and_port() -> tuple[str, int]:
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    if ":" in host:
        hostname, port_str = host.split(":", 1)
        return hostname, int(port_str)
    return host, 3000


def _services_alternate() -> bool:
    return os.environ.get(
        "AEROSPIKE_USE_SERVICES_ALTERNATE", "",
    ).lower() in ("true", "1", "yes")


def connect():
    """Build an async ClusterDefinition from environment variables."""
    from aerospike_sdk import ClusterDefinition

    hostname, port = _host_and_port()
    cluster_def = ClusterDefinition(hostname, port)
    if _services_alternate():
        cluster_def = cluster_def.using_services_alternate()
    return cluster_def


def sync_connect():
    """Build a sync ClusterDefinition from environment variables."""
    from aerospike_sdk.sync import ClusterDefinition

    hostname, port = _host_and_port()
    cluster_def = ClusterDefinition(hostname, port)
    if _services_alternate():
        cluster_def = cluster_def.using_services_alternate()
    return cluster_def


def _apply_auth(cluster_def):
    """Apply ``AEROSPIKE_AUTH_*`` credentials to a ClusterDefinition, if set."""
    mode = os.environ.get("AEROSPIKE_AUTH_MODE", "").strip().upper()
    if not mode:
        return cluster_def
    user = os.environ.get("AEROSPIKE_AUTH_USER", "")
    password = os.environ.get("AEROSPIKE_AUTH_PASSWORD", "")
    if mode == "EXTERNAL":
        cluster_def.with_external_credentials(user, password)
    elif mode == "PKI":
        cluster_def.with_certificate_credentials()
    else:  # INTERNAL
        cluster_def.with_native_credentials(user, password)
    return cluster_def


def sc_host() -> Optional[str]:
    """The strong-consistency seed, if configured (``AEROSPIKE_HOST_SC``)."""
    return os.environ.get("AEROSPIKE_HOST_SC")


def sc_namespace() -> str:
    """The strong-consistency namespace to use for MRT/roster examples."""
    return os.environ.get("AEROSPIKE_SC_NAMESPACE", "test_sc")


def connect_sc():
    """Async ClusterDefinition for the strong-consistency seed + auth.

    Falls back to the default seed when ``AEROSPIKE_HOST_SC`` is unset, so
    SC-requiring examples degrade to a clean capability skip rather than a
    connection error.
    """
    from aerospike_sdk import ClusterDefinition

    host = sc_host() or os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    hostname, port_str = host.split(":", 1) if ":" in host else (host, "3000")
    cluster_def = ClusterDefinition(hostname, int(port_str))
    if _services_alternate():
        cluster_def = cluster_def.using_services_alternate()
    return _apply_auth(cluster_def)


async def server_at_least(session, version: tuple[int, ...]) -> bool:
    """True if every node's build is >= ``version`` (e.g. ``(8, 1, 3)``)."""
    from aerospike_sdk.aio.info import InfoCommands

    builds = await InfoCommands(session).build()

    def parse(b: str) -> tuple[int, ...]:
        parts = []
        for piece in b.split("."):
            parts.append(int("".join(c for c in piece if c.isdigit()) or 0))
        return tuple(parts)

    return all(parse(b) >= version for b in builds)
