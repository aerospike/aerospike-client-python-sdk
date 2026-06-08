"""
Pytest configuration to load environment variables from aerospike.env.

If aerospike.env exists, only that file is read (override=True); aerospike.env.example
is not merged. If aerospike.env is missing, aerospike.env.example supplies defaults
for variables not already in os.environ (override=False).
"""
import asyncio
import logging
import os
import time

import pytest
import pytest_asyncio
from pathlib import Path

from aerospike_async import AuthMode, ClientPolicy, new_client


def load_env_file(env_file_path, *, override: bool = True) -> None:
    """Load KEY=value / export KEY=value lines from a file into os.environ."""
    if not os.path.exists(env_file_path):
        return

    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue

            # Parse export VAR=value format
            if line.startswith('export '):
                line = line[7:]  # Remove 'export ' prefix

            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                if override or key not in os.environ:
                    os.environ[key] = value


def pytest_configure(config):
    """Called after command line options have been parsed and all plugins and initial conftest files been loaded."""
    root = Path(__file__).parent
    env_local = root / "aerospike.env"
    env_example = root / "aerospike.env.example"
    if env_local.exists():
        load_env_file(env_local, override=True)
        print(f"Loaded environment variables from {env_local}\n")
    else:
        # Defaults only for unset keys so CI and explicit exports keep precedence.
        load_env_file(env_example, override=False)
        print(f"Loaded default environment variables from {env_example} (no {env_local.name})\n")
    
    # Configure logging from AEROSPIKE_LOG_LEVEL / AEROSPIKE_LOG_FILE
    log_level = os.environ.get("AEROSPIKE_LOG_LEVEL", "").upper()
    if log_level:
        numeric = getattr(logging, log_level, None)
        if numeric is None:
            print(f"Warning: invalid AEROSPIKE_LOG_LEVEL={log_level!r}, ignoring\n")
        else:
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

    # Ensure python path includes the tests directory for imports
    import sys
    tests_dir = Path(__file__).parent / "tests"
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))


_AUTH_MODES = {
    "INTERNAL": AuthMode.INTERNAL,
    "EXTERNAL": AuthMode.EXTERNAL,
    "PKI": AuthMode.PKI,
}


def _use_services_alternate_from_env() -> bool:
    v = os.environ.get('AEROSPIKE_USE_SERVICES_ALTERNATE', 'true').strip().lower()
    return v in ('true', '1', 'yes')


@pytest.fixture(scope="session")
def client_policy():
    """Fixture providing ClientPolicy from AEROSPIKE_* env vars.

    Reads AEROSPIKE_USE_SERVICES_ALTERNATE, AEROSPIKE_AUTH_MODE,
    AEROSPIKE_AUTH_USER, and AEROSPIKE_AUTH_PASSWORD.
    """
    policy = ClientPolicy()
    policy.use_services_alternate = _use_services_alternate_from_env()

    mode_str = os.environ.get('AEROSPIKE_AUTH_MODE', '').strip().upper()
    if mode_str and mode_str in _AUTH_MODES:
        mode = _AUTH_MODES[mode_str]
        user = os.environ.get('AEROSPIKE_AUTH_USER', '')
        password = os.environ.get('AEROSPIKE_AUTH_PASSWORD', '')
        if mode == AuthMode.PKI:
            policy.set_auth_mode(mode)
        else:
            policy.set_auth_mode(mode, user=user, password=password)

    return policy


@pytest.fixture(scope="session")
def aerospike_host():
    """Fixture providing the Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST', 'localhost:3000')


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def enterprise(aerospike_host, client_policy):
    """True when the test cluster is Enterprise Edition (queried via info)."""
    client = await new_client(client_policy, aerospike_host)
    try:
        result = await client.info("edition")
        return any("Enterprise" in v for v in result.values())
    finally:
        await client.close()


@pytest.fixture
def wait_for_index():
    """Fixture returning an async helper that retries until a secondary index is queryable.

    Usage::

        await wait_for_index(client, "test", "my_set", Filter.range("age", 0, 100))
    """
    async def _wait(client, ns, set_name, sindex_filter, *, timeout=5.0, interval=0.25):
        deadline = time.monotonic() + timeout
        last_err = None
        while time.monotonic() < deadline:
            try:
                stream = await client.query(ns, set_name).filter(sindex_filter).execute()
                async for _ in stream:
                    break
                stream.close()
                return
            except Exception as exc:
                if "IndexNotReadable" not in str(exc):
                    raise
                last_err = exc
                await asyncio.sleep(interval)
        raise last_err  # type: ignore[misc]

    return _wait


@pytest.fixture
def sync_wait_for_index():
    """Fixture returning a sync helper that retries until a secondary index is queryable.

    Usage::

        sync_wait_for_index(client, "test", "my_set", Filter.range("age", 0, 100))
    """
    def _wait(client, ns, set_name, sindex_filter, *, timeout=5.0, interval=0.25):
        deadline = time.monotonic() + timeout
        last_err = None
        while time.monotonic() < deadline:
            try:
                stream = client.query(ns, set_name).filter(sindex_filter).execute()
                for _ in stream:
                    break
                stream.close()
                return
            except Exception as exc:
                if "IndexNotReadable" not in str(exc):
                    raise
                last_err = exc
                time.sleep(interval)
        raise last_err  # type: ignore[misc]

    return _wait


@pytest.fixture(scope="session")
def aerospike_host_tls():
    """Fixture providing the TLS-enabled Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST_TLS', 'localhost:3107')


@pytest.fixture(scope="session")
def aerospike_host_sec():
    """Fixture providing the security-enabled Aerospike host for tests"""
    return os.environ.get('AEROSPIKE_HOST_SEC', 'localhost:3109')

