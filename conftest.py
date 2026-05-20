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

from aerospike_async import AuthMode, ClientPolicy, new_client, new_client_blocking
from aerospike_async.exceptions import ConnectionError as PacConnectionError


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
    # aerospike.env uses override=True so local hosts/auth win; preserve an
    # explicit invoking-shell AEROSPIKE_LOG_LEVEL (e.g. DEBUG for connect traces).
    log_level_before_env_file = os.environ.get("AEROSPIKE_LOG_LEVEL")
    if env_local.exists():
        load_env_file(env_local, override=True)
        if log_level_before_env_file:
            os.environ["AEROSPIKE_LOG_LEVEL"] = log_level_before_env_file
        print(f"Loaded environment variables from {env_local}\n")
    else:
        # Defaults only for unset keys so CI and explicit exports keep precedence.
        load_env_file(env_example, override=False)
        print(f"Loaded default environment variables from {env_example} (no {env_local.name})\n")
    
    # Configure logging from AEROSPIKE_LOG_LEVEL / AEROSPIKE_LOG_FILE
    log_level = os.environ.get("AEROSPIKE_LOG_LEVEL", "").upper()
    # pyproject defaults log_cli_level to WARNING; allow SDK DEBUG lines through.
    if log_level == "DEBUG":
        setattr(config.option, "log_cli_level", "DEBUG")
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

    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000").strip()
    sc = os.environ.get("AEROSPIKE_HOST_SC", "").strip()
    if sc and host == sc:
        print(
            f"\nIntegration routing: AEROSPIKE_HOST and AEROSPIKE_HOST_SC both resolve to "
            f"{host!r}, so general tests hit the same seed as SC suites. Point "
            "AEROSPIKE_HOST at your AP/default cluster and AEROSPIKE_HOST_SC at SC only.\n",
        )


_AUTH_MODES = {
    "INTERNAL": AuthMode.INTERNAL,
    "EXTERNAL": AuthMode.EXTERNAL,
    "PKI": AuthMode.PKI,
}


def _use_services_alternate_from_env() -> bool:
    v = os.environ.get('AEROSPIKE_USE_SERVICES_ALTERNATE', 'true').strip().lower()
    return v in ('true', '1', 'yes')


def _apply_auth_from_env(policy: ClientPolicy) -> None:
    """Apply ``AEROSPIKE_AUTH_*`` env vars to *policy*, if any are set.

    Used by seed-specific policy fixtures whose target cluster requires
    authentication (SC, SEC). The default :func:`client_policy` does not
    call this — sending credentials to a cluster that does not require
    them can cost ~1s per ``new_client`` due to the auth handshake on
    some configurations.
    """
    mode_str = os.environ.get('AEROSPIKE_AUTH_MODE', '').strip().upper()
    if mode_str and mode_str in _AUTH_MODES:
        mode = _AUTH_MODES[mode_str]
        user = os.environ.get('AEROSPIKE_AUTH_USER', '')
        password = os.environ.get('AEROSPIKE_AUTH_PASSWORD', '')
        if mode == AuthMode.PKI:
            policy.set_auth_mode(mode)
        else:
            policy.set_auth_mode(mode, user=user, password=password)


@pytest.fixture(scope="session")
def client_policy():
    """Default ClientPolicy for the AP test seed (``AEROSPIKE_HOST``).

    Reads only ``AEROSPIKE_USE_SERVICES_ALTERNATE``. Does **not** apply
    ``AEROSPIKE_AUTH_*`` env vars; the AP/default cluster is expected
    to allow unauthenticated access. SC / SEC fixtures use their own
    auth-aware policies instead.
    """
    policy = ClientPolicy()
    policy.use_services_alternate = _use_services_alternate_from_env()
    return policy


@pytest.fixture(scope="session")
def client_policy_sc():
    """ClientPolicy for the SC test seed (``AEROSPIKE_HOST_SC``).

    Reads ``AEROSPIKE_USE_SERVICES_ALTERNATE`` and applies
    ``AEROSPIKE_AUTH_*`` env vars when set, since SC clusters in the
    standard local test rig run with security enabled.
    """
    policy = ClientPolicy()
    policy.use_services_alternate = _use_services_alternate_from_env()
    _apply_auth_from_env(policy)
    return policy


@pytest.fixture(scope="session")
def client_policy_sec():
    """ClientPolicy for the security-enabled seed (``AEROSPIKE_HOST_SEC``).

    Reads ``AEROSPIKE_USE_SERVICES_ALTERNATE`` and applies
    ``AEROSPIKE_AUTH_*`` env vars when set.
    """
    policy = ClientPolicy()
    policy.use_services_alternate = _use_services_alternate_from_env()
    _apply_auth_from_env(policy)
    return policy


@pytest.fixture(scope="session")
def aerospike_host():
    """Fixture providing the Aerospike seed for general integration tests.

    Reads ``AEROSPIKE_HOST`` (default ``localhost:3000``). SC-only suites use
    :func:`aerospike_host_sc` instead.
    """
    return os.environ.get("AEROSPIKE_HOST", "localhost:3000")


@pytest.fixture(scope="session")
def aerospike_host_sc():
    """Seed for SC / MRT / durable-delete integration tests.

    Uses ``AEROSPIKE_HOST_SC`` when set; otherwise the same seed as
    :func:`aerospike_host` (CI and single-cluster setups).

    Probes the seed once at session scope and ``pytest.skip``s every
    dependent test when the SC cluster is unreachable, rather than
    surfacing a connect error per test. Uses :func:`new_client_blocking`
    so we don't need an asyncio loop just to probe.
    """
    sc = os.environ.get("AEROSPIKE_HOST_SC", "").strip()
    seed = sc if sc else os.environ.get("AEROSPIKE_HOST", "localhost:3000")

    # Build a probe-only ClientPolicy: short timeout, same auth/services-alt
    # config as the real client_policy_sc. We don't reuse the fixture's
    # policy here because (a) it would create a fixture cycle and (b) we
    # want a tight timeout for the probe specifically.
    probe_policy = ClientPolicy()
    probe_policy.use_services_alternate = _use_services_alternate_from_env()
    _apply_auth_from_env(probe_policy)
    probe_policy.timeout = 2000  # 2s — enough for a healthy cluster, fast skip otherwise

    try:
        client = new_client_blocking(probe_policy, seed)
    except PacConnectionError as exc:
        pytest.skip(
            f"SC cluster at {seed!r} is unreachable "
            f"(AEROSPIKE_HOST_SC={os.environ.get('AEROSPIKE_HOST_SC', '')!r}). "
            f"Start the SC cluster or unset AEROSPIKE_HOST_SC to fall back to "
            f"AEROSPIKE_HOST. Underlying error: {exc}"
        )
    else:
        client.close_blocking()

    return seed


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def enterprise(aerospike_host, client_policy):
    """True when the test cluster is Enterprise Edition (queried via info)."""
    client = await new_client(client_policy, aerospike_host)
    try:
        result = await client.info("edition")
        return any("Enterprise" in v for v in result.values())
    finally:
        await client.close()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def enterprise_sc(aerospike_host_sc, client_policy_sc):
    """True when the SC test seed (``AEROSPIKE_HOST_SC`` or ``AEROSPIKE_HOST``) is Enterprise."""
    client = await new_client(client_policy_sc, aerospike_host_sc)
    try:
        result = await client.info("edition")
        return any("Enterprise" in v for v in result.values())
    finally:
        await client.close()


@pytest.fixture(scope="session")
def wait_for_index():
    """Return an async helper that retries until a secondary index is queryable.

    Session-scoped so module-scoped integration clients may depend on it without
    a pytest scope mismatch.

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


@pytest.fixture(scope="session")
def wait_for_set_visible():
    """Return an async helper that polls a set scan until ``expected`` records are visible.

    Point writes ack as soon as they are committed, but set scans / SI queries
    can lag a few milliseconds behind the ack as the partition map and any
    secondary-index entries catch up. Fixtures that insert N records and then
    expect a scan to see them should call this before yielding to tests so the
    suite is robust to CI runner load. Replaces fixed ``asyncio.sleep`` waits
    that previously guessed a wall-clock value.

    Usage::

        await wait_for_set_visible(session, "test", "my_set", 4)
    """
    async def _wait(session, ns, set_name, expected, *, timeout=5.0, interval=0.05):
        deadline = time.monotonic() + timeout
        last_seen = -1
        while time.monotonic() < deadline:
            stream = await session.query(ns, set_name).execute()
            seen = 0
            async for _ in stream:
                seen += 1
            stream.close()
            if seen >= expected:
                return
            last_seen = seen
            await asyncio.sleep(interval)
        raise TimeoutError(
            f"{ns}.{set_name}: only {last_seen}/{expected} records visible "
            f"to set scan within {timeout}s"
        )

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


@pytest.fixture(scope="session")
def aerospike_host_8_1_2():
    """Seed for an 8.1.2+ Aerospike cluster, when one is available locally.

    Returns ``None`` when ``AEROSPIKE_HOST_8_1_2`` is unset; tests that need
    8.1.2+ behavior should accept this fixture and ``pytest.skip`` when it is
    ``None`` rather than failing.
    """
    return os.environ.get('AEROSPIKE_HOST_8_1_2')


@pytest.fixture
def aerospike_host_812_required(aerospike_host_8_1_2):
    """Returns the 8.1.2+ host or skips the dependent test cleanly.

    Tests that exercise server-8.1.2-only features opt in by depending on
    this fixture (typically via a ``_812``-suffixed client fixture). When
    ``AEROSPIKE_HOST_8_1_2`` is unset the dependent test is skipped with a
    clear message rather than running against the wrong cluster. When set,
    the test is auto-routed to the 8.1.2+ seed, so a single ``pytest`` run
    can exercise the broad surface against ``AEROSPIKE_HOST`` and the
    8.1.2-only subset against ``AEROSPIKE_HOST_8_1_2``.
    """
    if not aerospike_host_8_1_2:
        pytest.skip(
            "AEROSPIKE_HOST_8_1_2 is unset; this test requires an 8.1.2+ "
            "cluster. Set AEROSPIKE_HOST_8_1_2 in aerospike.env to enable."
        )
    return aerospike_host_8_1_2


def _parse_build_string(build: str):
    """Parse a server build string (e.g. ``8.1.2.1``) into ``(M, m, p, b)``.

    Tolerates trailing suffixes on the build component to match the core's
    regex-based parser. Returns ``None`` if the string does not start with
    four dot-separated integers.
    """
    parts = build.split('.')
    if len(parts) < 4:
        return None
    try:
        return tuple(int(p) for p in parts[:4])
    except ValueError:
        try:
            fourth = parts[3]
            cut = 0
            while cut < len(fourth) and fourth[cut].isdigit():
                cut += 1
            return (int(parts[0]), int(parts[1]), int(parts[2]), int(fourth[:cut]))
        except Exception:
            return None


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def server_version(aerospike_host, client_policy):
    """Probe the seed for ``build`` info and return ``(M, m, p, b)``.

    Returns ``None`` if the probe fails. Tests that need a version
    comparison should short-circuit on ``None`` (skip or fall through to
    server-side enforcement).
    """
    if not aerospike_host:
        return None
    try:
        client = await new_client(client_policy, aerospike_host)
    except Exception:
        return None
    try:
        info = await client.info("build")
    finally:
        await client.close()
    for raw in info.values():
        if not raw:
            continue
        if "=" in raw:
            _, _, value = raw.partition("=")
            parsed = _parse_build_string(value.strip())
        else:
            parsed = _parse_build_string(raw.strip())
        if parsed is not None:
            return parsed
    return None


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_query_ops_projection_ext(server_version):
    """``True`` when the seed cluster accepts non-basic-read ops in queries.

    Mirrors the per-node feature in the Rust core (server >= 8.1.2). Tests
    that need extended reads in ``Statement.set_operations`` (or its PSDK
    facade ``QueryBuilder.with_op_projection``) should ``pytest.skip``
    when this is ``False``.
    """
    return server_version is not None and server_version >= (8, 1, 2, 0)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_enhanced_expression_api(server_version):
    """``True`` when the cluster supports the 8.1.2 enhanced expression API.

    Covers native ``in_list`` / ``map_keys`` / ``map_values`` ExpOps,
    ``CTX.map_keys_in`` / ``and_filter`` helpers, and the path-form
    expression operators (``exp_select_*`` / ``exp_modify_*`` /
    ``exp_remove``). Server >= 8.1.2.
    """
    return server_version is not None and server_version >= (8, 1, 2, 0)

