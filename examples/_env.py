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


def _parse_host(host: str) -> tuple[str, int]:
    if ":" in host:
        hostname, port_str = host.split(":", 1)
        return hostname, int(port_str)
    return host, 3000


def _host_and_port() -> tuple[str, int]:
    return _parse_host(os.environ.get("AEROSPIKE_HOST", "localhost:3000"))


def _services_alternate() -> bool:
    return os.environ.get(
        "AEROSPIKE_USE_SERVICES_ALTERNATE", "",
    ).lower() in ("true", "1", "yes")


def connect(*, sc: bool = False):
    """Build an async ClusterDefinition from environment variables.

    When ``sc`` is True, uses ``AEROSPIKE_HOST_SC`` (falling back to
    ``AEROSPIKE_HOST``) and applies ``AEROSPIKE_AUTH_*`` credentials if set.
    """
    from aerospike_sdk import ClusterDefinition

    if sc:
        host = sc_host() or os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    else:
        host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")

    hostname, port = _parse_host(host)
    cluster_def = ClusterDefinition(hostname, port)
    if _services_alternate():
        cluster_def = cluster_def.using_services_alternate()
    if sc:
        cluster_def = _apply_auth(cluster_def)
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

from aerospike_sdk import Behavior, DataSet


class ExampleMeta(type):
    """Make ``await Example(...)`` run the async ``__init__``."""

    def __call__(cls, *args, **kwargs):
        async def _construct():
            self = cls.__new__(cls)
            await self.__init__(*args, **kwargs)
            return self

        return _construct()


class Example(metaclass=ExampleMeta):
    _skipped = False

    async def __init__(self, behavior: Behavior = Behavior.DEFAULT, *, sc: bool = False):
        self._behavior = behavior
        self._sc = sc
        self.cluster = await connect(sc=sc).connect()
        self.session = self.cluster.create_session(behavior)
        self.users = DataSet.of("test", "users")
        self.key = self.users.id("user123")

    async def cleanup(self) -> None:
        if self.cluster is not None:
            await self.cluster.close()
            self.cluster = None
            self.session = None

    async def run(self) -> None:
        raise NotImplementedError


class SyncExample:
    _skipped = False

    def __init__(self, behavior: Behavior = Behavior.DEFAULT):
        self._behavior = behavior
        self.cluster = sync_connect().connect()
        self.session = self.cluster.create_session(behavior)
        self.users = DataSet.of("test", "users")
        self.key = self.users.id("user123")

    def cleanup(self) -> None:
        if self.cluster is not None:
            self.cluster.close()
            self.cluster = None
            self.session = None

    def run(self) -> None:
        raise NotImplementedError


class SdkConfigFileExample(Example):
    _CONFIG = Path(__file__).resolve().parent / "sdk-config-example.yaml"
    config_path: Path = _CONFIG

    async def __init__(self, host: "SdkConfigFileExample | None" = None):
        if host is None:
            os.environ["AEROSPIKE_SDK_CONFIG_URL"] = str(self.config_path)
            await super().__init__()
        else:
            self._behavior = host._behavior
            self._sc = host._sc
            self.cluster = host.cluster
            self.session = host.session
            self.users = host.users
            self.key = host.key

    async def cleanup(self) -> None:
        os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)
        await super().cleanup()
