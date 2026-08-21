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
from aerospike_sdk.aio.session import _parse_namespace_info_body
from aerospike_async.exceptions import ConnectionError as PacConnectionError
from aerospike_sdk.sync.info import InfoCommands as SyncInfoCommands


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

    config.addinivalue_line(
        "markers",
        "requires_mode(mode): run only when the general namespace's server-derived mode "
        "('ap' or 'sc') matches; enforced by the _enforce_requires_mode fixture.",
    )

    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000").strip()
    sc = os.environ.get("AEROSPIKE_HOST_SC", "").strip()
    pinned = os.environ.get("AEROSPIKE_SC_NAMESPACE", "").strip()
    # State the routing unconditionally. The SC suites silently fall back to
    # AEROSPIKE_HOST when AEROSPIKE_HOST_SC is unset, which otherwise makes a
    # green run indistinguishable from one that never reached an SC namespace.
    # The namespace half is confirmed later, on a live connection, by
    # :func:`_report_sc_routing`.
    # Mode-axis SC leg (AEROSPIKE_GENERAL_AUTH): the *general* suites are auth-aware and
    # routed to the SC seed + AEROSPIKE_NAMESPACE, so report that, not the AP seed —
    # otherwise a green SC-leg run looks like it ran against the AP default.
    sc_leg = _general_auth_enabled()
    general_seed = (sc or host) if sc_leg else host
    general_ns = os.environ.get("AEROSPIKE_NAMESPACE", "").strip() or "test"
    general_note = (
        f" [SC leg: auth on, namespace {general_ns!r}]" if sc_leg
        else f" [namespace {general_ns!r}]"
    )
    sc_seed_note = (
        "(AEROSPIKE_HOST_SC)" if sc
        else "(AEROSPIKE_HOST_SC unset - fell back to AEROSPIKE_HOST)"
    )
    ns_note = (
        f"{pinned!r} (pinned via AEROSPIKE_SC_NAMESPACE)" if pinned
        else "auto-select (AEROSPIKE_SC_NAMESPACE unset)"
    )
    print(
        "\nIntegration routing:\n"
        f"  general suites -> {general_seed!r}{general_note}\n"
        f"  SC suites      -> {(sc or host)!r} {sc_seed_note}\n"
        f"  SC namespace   -> {ns_note}\n",
    )
    if sc and host == sc and not sc_leg:
        print(
            f"AEROSPIKE_HOST and AEROSPIKE_HOST_SC both resolve to "
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


def _apply_auth_to_definition(cluster_def) -> None:
    """Apply ``AEROSPIKE_AUTH_*`` env vars to a ClusterDefinition, if set.

    Mirror of :func:`_apply_auth_from_env` for the ``ClusterDefinition``
    entry path (async or sync — both expose the same credential methods).
    Delegates to :mod:`tests.integration.general_auth`, the single source
    for the definition-path auth contract (raw-definition test sites import
    it directly).
    """
    from tests.integration.general_auth import apply_auth_to_definition
    apply_auth_to_definition(cluster_def)


def _general_auth_enabled() -> bool:
    """Whether the *general* (default) suites should authenticate — opt-in only.

    Controlled by ``AEROSPIKE_GENERAL_AUTH`` (the ``make test-sc`` leg sets it). The
    default AP fast path stays **no-auth** (unset): sending credentials to a cluster
    that does not require them costs ~1s per ``new_client`` on some configs, which is
    exactly what :func:`client_policy`'s no-auth contract protects. When set, the
    default :func:`client_policy` / :func:`make_cluster_definition` apply
    ``AEROSPIKE_AUTH_*`` just like the SC/SEC fixtures, so the general suites can reach
    an auth-required SC seed for the Mode axis. New env var, so it
    is not clobbered by ``aerospike.env`` (which loads ``override=True``).
    """
    from tests.integration.general_auth import general_auth_enabled
    return general_auth_enabled()


@pytest.fixture(scope="session")
def make_cluster_definition():
    """Factory for ClusterDefinitions mirroring the ClientPolicy fixtures.

    ``make_cluster_definition(seed)`` builds a ClusterDefinition for the AP seed
    (services-alternate from env, no auth — same contract as
    :func:`client_policy`); ``auth=True`` applies ``AEROSPIKE_AUTH_*`` (the
    :func:`client_policy_sc` / :func:`client_policy_sec` contract);
    ``sync=True`` returns the ``aerospike_sdk.sync`` cluster_def.

    The default (``auth=False``) path *also* applies ``AEROSPIKE_AUTH_*`` when the
    :func:`_general_auth_enabled` opt-in (``AEROSPIKE_GENERAL_AUTH``) is set, so the
    general suites can reach an auth-required SC seed on the Mode-axis ``make test-sc``
    leg without disturbing the no-auth AP fast path.
    """
    from aerospike_sdk import ClusterDefinition, Host
    from aerospike_sdk.sync import ClusterDefinition as SyncClusterDefinition
    from aerospike_sdk.sync import Host as SyncHost

    def _make(seed: str, *, auth: bool = False, sync: bool = False):
        if sync:
            cluster_def = SyncClusterDefinition(hosts=SyncHost.parse_hosts(seed, 3000))
        else:
            cluster_def = ClusterDefinition(hosts=Host.parse_hosts(seed, 3000))
        if _use_services_alternate_from_env():
            cluster_def.using_services_alternate()
        if auth or _general_auth_enabled():
            _apply_auth_to_definition(cluster_def)
        return cluster_def

    return _make


@pytest.fixture(scope="session")
def client_policy():
    """Default ClientPolicy for the AP test seed (``AEROSPIKE_HOST``).

    Reads only ``AEROSPIKE_USE_SERVICES_ALTERNATE``. Does **not** apply
    ``AEROSPIKE_AUTH_*`` env vars by default; the AP/default cluster is expected
    to allow unauthenticated access. SC / SEC fixtures use their own
    auth-aware policies instead.

    Exception: the :func:`_general_auth_enabled` opt-in (``AEROSPIKE_GENERAL_AUTH``,
    set by the Mode-axis ``make test-sc`` leg) makes this default policy apply
    ``AEROSPIKE_AUTH_*`` too, so the general suites can reach an auth-required SC seed.
    The no-auth AP fast path is unchanged whenever the opt-in is unset.
    """
    policy = ClientPolicy()
    policy.use_services_alternate = _use_services_alternate_from_env()
    if _general_auth_enabled():
        _apply_auth_from_env(policy)
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

    On the Mode-axis SC leg (:func:`_general_auth_enabled` / ``AEROSPIKE_GENERAL_AUTH``
    set, via ``make test-sc``) the general suites target ``AEROSPIKE_HOST_SC`` when it
    is set, so they reach the SC seed. This reuses the established SC-seed var and
    sidesteps ``aerospike.env``'s ``override=True`` (which would ignore an inline
    ``AEROSPIKE_HOST``); it falls back to ``AEROSPIKE_HOST`` on single-cluster setups
    where ``AEROSPIKE_HOST_SC`` is unset.
    """
    from tests.integration.general_auth import general_seed
    return general_seed()


@pytest.fixture(scope="session")
def general_namespace_is_sc(aerospike_host, pytestconfig):
    """Server-derived: is the general suites' target namespace strong-consistency?

    Probes the general seed once and runs the same ``namespace/<ns>`` info scan as
    :func:`_report_sc_routing` on ``general_namespace()``. Drives ``@requires_mode`` and
    :func:`sc_aware_delete` off the *server's* verdict rather than the
    ``AEROSPIKE_NAMESPACE`` string (which would misclassify a differently-named SC
    namespace). Returns ``False`` (treat as AP) on any probe failure.
    """
    from tests.integration.namespace import general_namespace

    ns = general_namespace()
    probe = ClientPolicy()
    probe.use_services_alternate = _use_services_alternate_from_env()
    if _general_auth_enabled():
        _apply_auth_from_env(probe)
    probe.timeout = 2000

    def _degraded(reason: str) -> bool:
        # Announce, never infer. Silently failing open to AP would turn the SC leg into an
        # AP-shaped run — sc_aware_delete stops issuing durable deletes (FailForbidden,
        # swallowed → cleanup stops), @requires_mode('sc') skips and @requires_mode('ap')
        # runs on SC — the exact invisible coverage loss this axis exists to prevent.
        emit = _terminal_emit(pytestconfig)
        emit("")
        if _general_auth_enabled():
            emit(
                f"Mode axis: SC-mode probe FAILED for {ns!r} on {aerospike_host!r} ({reason}). "
                f"The SC leg is DEGRADED to AP-shaped behavior — treat this run's SC results as "
                f"INVALID until the probe succeeds.",
            )
        else:
            emit(f"Mode axis: {ns!r} treated as AP (SC probe unavailable: {reason}).")
        return False

    try:
        client = new_client_blocking(probe, aerospike_host)
    except Exception as exc:
        return _degraded(f"connect error: {exc}")
    try:
        missing = False
        sc_val = None
        for body in client.info_blocking(f"namespace/{ns}").values():
            if not body:
                continue
            exists, sc_opt = _parse_namespace_info_body(body)
            if not exists:
                missing = True
                break
            if sc_opt is not None:
                sc_val = sc_opt
        if missing:
            return _degraded("namespace not present on the seed")
        return bool(sc_val)
    except Exception as exc:
        return _degraded(f"info scan error: {exc}")
    finally:
        client.close_blocking()


@pytest.fixture(scope="session")
def sc_aware_delete(general_namespace_is_sc):
    """Best-effort cleanup delete that is durable when the target namespace is SC.

    Non-durable delete is ``FailForbidden`` on strong-consistency namespaces; issuing a
    durable delete there keeps setup/teardown working in both modes without marking the
    test AP-only. Errors are swallowed — this is teardown, not an assertion. Reserve
    ``@requires_mode`` for tests whose *assertion* is mode-specific.
    """
    async def _del(session, *keys):
        for k in keys:
            builder = session.delete(k)
            if general_namespace_is_sc:
                builder = builder.with_durable_delete()
            try:
                await builder.execute()
            except Exception:
                pass

    return _del


@pytest.fixture(autouse=True)
def _enforce_requires_mode(request):
    """Skip a ``@requires_mode(...)`` test when the general namespace's mode doesn't match.

    Resolves the server-derived mode lazily (only for marked tests), so unmarked tests
    never pay the probe.
    """
    marker = request.node.get_closest_marker("requires_mode")
    if marker is None:
        return
    from tests.integration.namespace import requires_mode_skip_reason

    reason = requires_mode_skip_reason(
        marker.args[0], request.getfixturevalue("general_namespace_is_sc"),
    )
    if reason:
        pytest.skip(reason)


def _terminal_emit(config):
    """Return a writer that bypasses pytest's per-test output capture.

    SC routing lines are emitted from session-scoped fixtures, where a bare
    ``print`` is captured and only surfaces under ``-s`` or on failure. The
    terminal reporter writes straight to the console; fall back to ``print``
    when it is absent (``-p no:terminal``).
    """
    reporter = config.pluginmanager.get_plugin("terminalreporter")
    return reporter.write_line if reporter is not None else print


def _report_sc_routing(client, config) -> None:
    """Report which namespace the SC suites will use, and whether it is really SC.

    Positive confirmation, so a green SC run cannot be mistaken for one that
    quietly skipped or landed on an AP namespace. Mirrors
    ``resolve_sc_namespace``'s choice (a pinned ``AEROSPIKE_SC_NAMESPACE`` wins,
    otherwise the lone SC namespace) and reports the same ``namespace/<name>``
    verdict the suites gate on, rather than a second opinion. Diagnostic only —
    it never fails a run.
    """
    emit = _terminal_emit(config)

    pinned = os.environ.get("AEROSPIKE_SC_NAMESPACE", "").strip()
    try:
        verdicts = {}
        for ns in sorted(SyncInfoCommands(client).namespaces()):
            # Same multi-node scan as ``Session.namespace_sc_status``: a node
            # reporting the namespace as unknown wins, otherwise the last node
            # to report ``strong-consistency`` decides.
            missing = False
            sc_val = None
            for body in client.info_blocking(f"namespace/{ns}").values():
                if not body:
                    continue
                exists, sc_opt = _parse_namespace_info_body(body)
                if not exists:
                    missing = True
                    break
                if sc_opt is not None:
                    sc_val = sc_opt
            verdicts[ns] = not missing and bool(sc_val)
    except Exception as exc:
        emit("")
        emit(f"SC routing: namespace check unavailable ({exc})")
        return

    table = ", ".join(f"{ns}(is_sc={v})" for ns, v in verdicts.items()) or "(none reported)"
    emit("")
    emit(f"SC routing: namespaces on SC seed: {table}")

    sc_names = [ns for ns, is_sc in verdicts.items() if is_sc]
    if pinned:
        chosen, why = pinned, "pinned"
    elif len(sc_names) == 1:
        chosen, why = sc_names[0], "auto-selected"
    else:
        reason = (
            "several SC namespaces - set AEROSPIKE_SC_NAMESPACE"
            if sc_names else
            "no namespace has strong-consistency"
        )
        emit(f"SC routing: unresolved ({reason}) -> SC suites will SKIP")
        return

    is_sc = verdicts.get(chosen)
    if is_sc:
        emit(f"SC routing: SC suites will use namespace {chosen!r} ({why}) -> is_sc=True")
    elif is_sc is False:
        emit(
            f"SC routing: namespace {chosen!r} ({why}) is AP mode (is_sc=False) "
            "-> SC suites will SKIP",
        )
    else:
        emit(
            f"SC routing: namespace {chosen!r} ({why}) is not present on this seed "
            "-> SC suites will SKIP",
        )


@pytest.fixture(scope="session")
def aerospike_host_sc(pytestconfig):
    """Seed for SC / MRT / durable-delete integration tests.

    Uses ``AEROSPIKE_HOST_SC`` when set; otherwise the same seed as
    :func:`aerospike_host` (CI and single-cluster setups).

    Probes the seed once at session scope and ``pytest.skip``s every
    dependent test when the SC cluster is unreachable, rather than
    surfacing a connect error per test. Uses :func:`new_client_blocking`
    so we don't need an asyncio loop just to probe — and therefore catches
    PAC's ``ConnectionError``, not the SDK-level one it converts to.
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
        # Announce it as well as skipping: a bare skip is a single 's' unless the
        # run asked for -rs, which makes "no SC cluster" look like a green SC run.
        emit = _terminal_emit(pytestconfig)
        emit("")
        emit(
            f"SC routing: SC seed {seed!r} is UNREACHABLE "
            "-> every SC suite will SKIP",
        )
        pytest.skip(
            f"SC cluster at {seed!r} is unreachable "
            f"(AEROSPIKE_HOST_SC={os.environ.get('AEROSPIKE_HOST_SC', '')!r}). "
            f"Start the SC cluster or unset AEROSPIKE_HOST_SC to fall back to "
            f"AEROSPIKE_HOST. Underlying error: {exc}"
        )
    else:
        _report_sc_routing(client, pytestconfig)
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
    async def _wait(
        client, ns, set_name, sindex_filter, *, timeout=10.0, interval=0.25, stable=2,
    ):
        # Server-side SI readiness is not monotonic right after create/drop —
        # a single successful probe can be followed by a brief IndexNotReadable
        # window. Require `stable` consecutive readable probes so the very next
        # query in the test does not race that flicker.
        deadline = time.monotonic() + timeout
        last_err = None
        hits = 0
        session = client.create_session()
        while time.monotonic() < deadline:
            try:
                stream = await session.query(ns, set_name).filter(sindex_filter).execute()
                async for _ in stream:
                    break
                stream.close()
                hits += 1
                if hits >= stable:
                    return
                await asyncio.sleep(interval)
            except Exception as exc:
                if "IndexNotReadable" not in str(exc):
                    raise
                hits = 0  # a flicker resets the streak
                last_err = exc
                await asyncio.sleep(interval)
        raise last_err  # type: ignore[misc]

    return _wait


@pytest.fixture(scope="session")
def wait_for_set_visible():
    """Return an async helper that polls a set scan until exactly ``expected`` records are visible.

    Point writes ack as soon as they are committed, but set scans / SI queries
    can lag a few milliseconds behind the ack as the partition map and any
    secondary-index entries catch up. Fixtures that insert N records and then
    expect a scan to see them should call this before yielding to tests so the
    suite is robust to CI runner load. Uses ``seen == expected`` (not ``>=``) so
    truncate lag or leftover rows from a prior run cannot satisfy the check early.

    Usage::

        await wait_for_set_visible(session, "test", "my_set", 4)
    """
    async def _wait(
        session, ns, set_name, expected,
        *, timeout=5.0, interval=0.05, settle=0.1,
    ):
        deadline = time.monotonic() + timeout
        last_seen = -1
        while time.monotonic() < deadline:
            stream = await session.query(ns, set_name).execute()
            seen = 0
            async for _ in stream:
                seen += 1
            stream.close()
            if seen == expected:
                # Brief settle pause — scan-count visibility precedes CDT-bin
                # storage / filter-expression readiness by a few tens of ms
                # on busier CI runners. Without this, AEL CDT-path filters
                # (e.g. ``$.numbers.[0] > 50``, ``$.values.[1:3].count() == 2``)
                # observed against just-seeded records can return 0 matches
                # even though the scan sees the records. Observed on 3.12
                # and 3.13t CI runs 2026-06-05.
                if settle > 0:
                    await asyncio.sleep(settle)
                return
            last_seen = seen
            await asyncio.sleep(interval)
        raise TimeoutError(
            f"{ns}.{set_name}: expected exactly {expected} records visible to set scan, "
            f"last saw {last_seen} within {timeout}s"
        )

    return _wait


@pytest.fixture(scope="session")
def sync_wait_for_set_visible():
    """Return a sync helper that polls a set scan until exactly ``expected`` records are visible.

    Sync counterpart of :func:`wait_for_set_visible`. Uses ``seen == expected`` so
    truncate lag or leftover rows cannot satisfy the check early.
    """
    def _wait(
        session, ns, set_name, expected,
        *, timeout=5.0, interval=0.05, settle=0.1,
    ):
        deadline = time.monotonic() + timeout
        last_seen = -1
        while time.monotonic() < deadline:
            stream = session.query(ns, set_name).execute()
            seen = 0
            for _ in stream:
                seen += 1
            stream.close()
            if seen == expected:
                if settle > 0:
                    time.sleep(settle)
                return
            last_seen = seen
            time.sleep(interval)
        raise TimeoutError(
            f"{ns}.{set_name}: expected exactly {expected} records visible to set scan, "
            f"last saw {last_seen} within {timeout}s"
        )

    return _wait


@pytest.fixture(scope="session")
def sync_wait_for_index():
    """Fixture returning a sync helper that retries until a secondary index is queryable.

    Session-scoped so module- or session-scoped integration clients may depend on
    it without a pytest scope mismatch.

    Usage::

        sync_wait_for_index(client, "test", "my_set", Filter.range("age", 0, 100))
    """
    def _wait(
        client, ns, set_name, sindex_filter, *, timeout=10.0, interval=0.25, stable=2,
    ):
        # Server-side SI readiness is not monotonic right after create/drop —
        # a single successful probe can be followed by a brief IndexNotReadable
        # window. Require `stable` consecutive readable probes so the very next
        # query in the test does not race that flicker.
        deadline = time.monotonic() + timeout
        last_err = None
        hits = 0
        session = client.create_session()
        while time.monotonic() < deadline:
            try:
                stream = session.query(ns, set_name).filter(sindex_filter).execute()
                for _ in stream:
                    break
                stream.close()
                hits += 1
                if hits >= stable:
                    return
                time.sleep(interval)
            except Exception as exc:
                if "IndexNotReadable" not in str(exc):
                    raise
                hits = 0  # a flicker resets the streak
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


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def aerospike_host_812_required(aerospike_host, server_version):
    """The default seed, required to be server >= 8.1.2, else skip.

    Single-host model: version-gated tests connect to the default
    ``AEROSPIKE_HOST`` and skip unless it is 8.1.2+. Point ``AEROSPIKE_HOST``
    at an 8.1.2+ build to run them; CI covers the version spread via a server
    matrix rather than a dedicated host var. Skips when the seed is < 8.1.2 or
    unreachable (``server_version`` probes to ``None``).
    """
    if server_version is None or server_version < SERVER_8_1_2:
        pytest.skip(
            "default cluster is not 8.1.2+ (or unreachable); point "
            "AEROSPIKE_HOST at an 8.1.2+ build to run these tests"
        )
    return aerospike_host


# Named server-version floors for capability gates. Compare against the
# ``(M, m, p, b)`` tuple from :func:`server_version`. Centralized so feature
# checks reference an intent-named constant instead of an inline magic tuple
# (mirrors the Java clients' ``SERVER_VERSION_*`` constants). Add a new floor
# here rather than inlining a tuple in a new ``supports_*`` gate.
SERVER_8_1_1 = (8, 1, 1, 0)
SERVER_8_1_2 = (8, 1, 2, 0)
SERVER_8_1_3 = (8, 1, 3, 0)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_string_operations(server_version):
    """``True`` when the (default-host) cluster supports server-side string ops.

    Covers the ``str_*`` builder / ``StringOperation`` surface and string
    filter expressions (server >= 8.1.3, gated server-side via the core's
    ``Version::supports_string_operations``). Single-host model: point
    ``AEROSPIKE_HOST`` at an 8.1.3+ build to exercise these; CI covers the
    version spread via a server matrix rather than a dedicated host var.
    Tests should ``pytest.skip`` when this is ``False``.
    """
    return server_version is not None and server_version >= SERVER_8_1_3


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
    return server_version is not None and server_version >= SERVER_8_1_2


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_enhanced_expression_api(server_version):
    """``True`` when the cluster supports the 8.1.2 enhanced expression API.

    Covers native ``in_list`` / ``map_keys`` / ``map_values`` ExpOps,
    ``CTX.map_keys_in`` / ``and_filter`` helpers, and the path-form
    expression operators (``exp_select_*`` / ``exp_modify_*`` /
    ``exp_remove``). Server >= 8.1.2.
    """
    return server_version is not None and server_version >= SERVER_8_1_2


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_error_detail(server_version):
    """``True`` when the cluster supplies extended server error detail.

    Covers ``error_detail_verbosity`` and the resulting ``AerospikeError``
    ``sub_code`` / ``server_message`` / ``exp_trace``. Server >= 8.1.3;
    older servers ignore the request flags. Tests that assert on error
    detail should ``pytest.skip`` when this is ``False``.
    """
    return server_version is not None and server_version >= SERVER_8_1_3


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_blob_index(server_version):
    """``True`` when the cluster supports blob secondary indexes.

    Covers ``IndexBuilder.blob()`` and blob equality filters served by a
    secondary index. Server >= 7.0. Tests that create blob indexes should
    ``pytest.skip`` when this is ``False``.
    """
    return server_version is not None and server_version >= (7, 0, 0, 0)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def supports_vector_bins(server_version):
    """``True`` when the (default-host) cluster likely supports ``VECTOR`` bins.

    Covers storing/retrieving :class:`~aerospike_sdk.Vector` bin values
    (put/get, ``set_to_vector``, and vectors nested in CDT list/map bins).

    TODO(vector-capability-gate): interim/temporary. Unlike the other
    ``supports_*`` gates, neither the Rust core nor PAC assigns a
    ``supports_vector_bins()`` version yet -- ``VECTOR`` particle support is
    still an unreleased, dev-server-only feature with no assigned version
    floor. This reuses the 8.1.3 floor only because current dev builds happen
    to report that version (``git describe``-style, e.g.
    ``8.1.3.0-76-g<hash>``); it will false-positive on a genuine (non-dev)
    8.1.3+ release that lacks ``VECTOR`` support. Mirrors the same interim
    fixture in the ``aerospike-async`` test suite. Replace with a real
    capability check once the core assigns one, and drop this TODO.

    Vector *search* (distance expressions + Top-K ``ORDER BY <bin> LIMIT k``)
    is a separate, not-yet-implemented feature server-side and is intentionally
    NOT covered by this gate -- see ``tests/integration/async/vector_test.py``.
    """
    return server_version is not None and server_version >= SERVER_8_1_3

