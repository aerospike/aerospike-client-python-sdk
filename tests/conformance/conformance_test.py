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

"""The shared discovery conformance fixtures, run through the SDK.

Each row of the ``aerospike-client-conformance`` fixture set is configured
the way an application configures the SDK: its ``discovery`` and
``translation`` blocks as the SDK config file, its seeds and ``ip_map`` on
the :class:`ClusterDefinition`. The policy that connect would build from them
then drives PNC's conformance hooks, which run the client's own address
translation, seed refresh and merge, and certificate name matching without a
cluster.

Every row is compared with the result ``python-declared.yaml`` declares for
it; a row with no declaration is declared ``pass`` (``blocked`` when the
fixture blocks it). The run writes ``conformance-report.json`` in the shape
of the fixture set's report schema.

``CONFORMANCE_DIR`` points at a checkout of the fixture set at the version
``FIXTURE_SET_VERSION`` names; without it every row is skipped.
``CONFORMANCE_REPORT`` overrides the report path,
``build/conformance/conformance-report.json`` by default::

    CONFORMANCE_DIR=../aerospike-client-conformance python -m pytest tests/conformance
"""

import ipaddress
import json
import logging
import os
import subprocess
import sys
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import yaml
from aerospike_native import (
    Host as PncHost,
    _conformance_tls_name_matches,
    _conformance_translate,
    _ConformanceRefresher,
)
from aerospike_native.exceptions import AerospikeError as PncAerospikeError

import aerospike_sdk
from aerospike_sdk import AddressTranslationError, ClusterDefinition, Host
from aerospike_sdk.exceptions import _convert_pnc_exception
from aerospike_sdk.loggers import SdkLoggers
from aerospike_sdk.policy.sdk_config_loader import ENV_VAR, load_at_connect

RUNTIME = "python"
FIXTURE_SET_VERSION = "1.1.0"
FIXTURE_ROOTS = ("discovery", "tls")
FIXTURE_KINDS = ("translate", "collision", "merge", "dns", "aws-tags", "tls")
DECLARED = Path(__file__).with_name("python-declared.yaml")
REPORT = Path(__file__).parents[2] / "build" / "conformance" / "conformance-report.json"

PASS, FAIL, UNSUPPORTED, BLOCKED = "pass", "fail", "unsupported", "blocked"
RESULTS = (PASS, FAIL, UNSUPPORTED, BLOCKED)

# Timeout given to a provider whose row sets none: generous for a loopback
# server.
LOOPBACK_TIMEOUT_MS = 2000

# The fixture server checks the signature's shape, not its value, so any
# uppercase alphanumeric key id works.
FIXTURE_ACCESS_KEY_ID = "AKIAFIXTURE0000000000"
FIXTURE_SECRET = "fixture-secret-access-key"


@dataclass
class Row:
    file: str
    kind: str
    case: str
    data: Dict[str, Any]
    path: Path
    blocked_by: Optional[str] = None
    declared: str = PASS
    declared_reason: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.file}:{self.case}"


@dataclass
class Outcome:
    result: str
    reason: Optional[str] = None

    @classmethod
    def fail(cls, reason: str) -> "Outcome":
        return cls(FAIL, reason)


@dataclass
class Report:
    rows: List[Dict[str, str]] = field(default_factory=list)

    def add(self, row: Row, actual: Outcome) -> None:
        reason = (
            row.declared_reason
            if row.declared_reason and row.declared == actual.result
            else actual.reason
        )
        entry = {"file": row.file, "case": row.case, "result": actual.result}
        if reason:
            entry["reason"] = reason
        if actual.reason and actual.reason != reason:
            entry["detail"] = actual.reason
        self.rows.append(entry)

    def write(self, fixture_set_version: str) -> Path:
        rows = sorted(self.rows, key=lambda r: f"{r['file']}:{r['case']}")
        counts = {result: sum(r["result"] == result for r in rows) for result in RESULTS}
        report = {
            "runtime": RUNTIME,
            "clientVersion": aerospike_sdk.__version__,
            "fixtureSetVersion": fixture_set_version,
            "counts": counts,
            "rows": rows,
        }
        path = Path(os.environ.get("CONFORMANCE_REPORT", REPORT))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2) + "\n")
        return path


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def fixture_dir() -> Optional[Path]:
    """The fixture set checkout, checked against the pinned version."""
    raw = os.environ.get("CONFORMANCE_DIR")
    if not raw:
        return None
    root = Path(raw).resolve()
    version_file = root / "VERSION"
    if not version_file.is_file():
        raise RuntimeError(f"Not a conformance fixture set, VERSION not found: {root}")
    version = version_file.read_text().strip()
    if version != FIXTURE_SET_VERSION:
        raise RuntimeError(
            f"Conformance fixture set {version} found, {FIXTURE_SET_VERSION} expected: "
            "check out that release or update the declarations"
        )
    return root


def load_rows(root: Path) -> List[Row]:
    """Every fixture row under the fixture roots, as the report collector reads them."""
    rows = []
    for path in sorted(p for base in FIXTURE_ROOTS for p in (root / base).rglob("*.yaml")):
        doc = yaml.safe_load(path.read_text())
        kind = doc.get("kind") if isinstance(doc, dict) else None
        if kind not in FIXTURE_KINDS:
            continue
        file = path.relative_to(root).as_posix()
        for item in doc["rows"]:
            blocked_by = item.get("blockedBy") if item.get("state") == BLOCKED else None
            rows.append(Row(
                file=file, kind=kind, case=item["case"], data=item, path=path,
                blocked_by=blocked_by, declared=BLOCKED if blocked_by else PASS,
            ))
    if not rows:
        raise RuntimeError(f"no fixture cases under {root}")
    apply_declarations(rows)
    return rows


def apply_declarations(rows: List[Row]) -> None:
    """``python-declared.yaml``: fixture file to case to ``{result, reason}``.

    A stale declaration fails the run instead of hiding a row.
    """
    doc = yaml.safe_load(DECLARED.read_text())
    if doc is None:
        return
    by_key = {(row.file, row.case): row for row in rows}
    files = {row.file for row in rows}
    for file, cases in doc.items():
        if file not in files:
            raise RuntimeError(f"{DECLARED.name}: unknown fixture file {file}")
        for case, declaration in cases.items():
            where = f"{DECLARED.name}: {file}:{case}"
            row = by_key.get((file, case))
            if row is None:
                raise RuntimeError(f"{where}: unknown case")
            unknown = set(declaration) - {"result", "reason"}
            if unknown:
                raise RuntimeError(f"{where}: unknown keys {sorted(unknown)}")
            result, reason = declaration.get("result"), declaration.get("reason")
            if result not in RESULTS:
                raise RuntimeError(f"{where}: invalid result {result!r}")
            if result != PASS and not reason:
                raise RuntimeError(f"{where}: a {result} declaration needs a reason")
            row.declared, row.declared_reason = result, reason


FIXTURES = fixture_dir()
ROWS = load_rows(FIXTURES) if FIXTURES else []

pytestmark = pytest.mark.skipif(
    FIXTURES is None,
    reason=f"CONFORMANCE_DIR is not set; check out aerospike-client-conformance "
    f"v{FIXTURE_SET_VERSION} and point CONFORMANCE_DIR at it",
)


@pytest.fixture(scope="module")
def report():
    collected = Report()
    yield collected
    if collected.rows:
        path = collected.write(FIXTURE_SET_VERSION)
        print(f"\nCONFORMANCE report {path}")


@pytest.mark.parametrize("row", ROWS, ids=str)
def test_row(row, report, request, tmp_path, monkeypatch, caplog):
    if row.blocked_by:
        actual = Outcome(BLOCKED, row.blocked_by)
    elif row.kind == "translate":
        actual = translate_row(row, SdkConfig(tmp_path, monkeypatch, caplog))
    elif row.kind == "collision":
        # The collision checks run inside the tend, on peers that connected;
        # PNC has no hook that applies them to a peers reply without a cluster.
        actual = Outcome(UNSUPPORTED, "no hook runs the collision checks without a cluster")
    elif row.kind == "merge":
        actual = merge_row(row, SdkConfig(tmp_path, monkeypatch, caplog))
    elif row.kind == "tls":
        actual = tls_row(row)
    else:
        server = request.getfixturevalue("fixture_server")
        actual = provider_row(row, server, SdkConfig(tmp_path, monkeypatch, caplog), monkeypatch)
    report.add(row, actual)
    detail = f": {actual.reason}" if actual.reason else ""
    assert actual.result == row.declared, f"declared {row.declared}, actual {actual.result}{detail}"


# ---------------------------------------------------------------------------
# The SDK configuration surface
# ---------------------------------------------------------------------------


class ConfigNotApplied(Exception):
    """The row's configuration did not apply whole."""


@dataclass
class SdkConfig:
    """Builds the policy connect would build from a row's ``config``."""

    tmp_path: Path
    monkeypatch: pytest.MonkeyPatch
    caplog: pytest.LogCaptureFixture

    def policy(self, config: Dict[str, Any]):
        """The policy and static seeds for *config*.

        Raises:
            ConfigNotApplied: If the SDK rejected or skipped any of it.
        """
        profile = {key: config[key] for key in ("discovery", "translation") if key in config}
        path = self.tmp_path / "aerospike-sdk.yaml"
        path.write_text(yaml.safe_dump({"system": {"DEFAULT": profile}}))
        self.monkeypatch.setenv(ENV_VAR, str(path))

        cd = ClusterDefinition(
            hosts=[Host(seed["host"], seed["port"]) for seed in config.get("seeds") or []],
        )
        if "ip_map" in config:
            cd.ip_map(string_map(config["ip_map"]))

        self.caplog.clear()
        with self.caplog.at_level(logging.WARNING, logger=SdkLoggers.BEHAVIOR):
            try:
                settings, _, _ = load_at_connect(None, cd._programmatic_settings(), strict=True)
            except ValueError as exc:
                raise ConfigNotApplied(str(exc)) from exc
        skipped = [r.getMessage() for r in self.caplog.records if r.name == SdkLoggers.BEHAVIOR]
        if skipped:
            raise ConfigNotApplied("; ".join(skipped))
        return cd._get_policy(settings), cd._build_seeds()


# ---------------------------------------------------------------------------
# translate
# ---------------------------------------------------------------------------


def translate_row(row: Row, sdk: SdkConfig) -> Outcome:
    expect, advertised = row.data["expect"], row.data["input"]
    try:
        policy, _ = sdk.policy(row.data.get("config") or {})
    except ConfigNotApplied as exc:
        return Outcome.fail(f"config: {exc}")

    error = expect.get("error")
    try:
        actual = _conformance_translate(
            policy,
            advertised["nodeName"],
            advertised["host"],
            advertised["port"],
            tls_name=advertised.get("tlsName"),
            alternate=advertised["sourceList"] == "alternate",
        )
    except PncAerospikeError as pnc_error:
        converted = _convert_pnc_exception(pnc_error)
        if error is None or error["category"] != "translation-miss":
            return Outcome.fail(f"threw {converted!r}")
        if not isinstance(converted, AddressTranslationError):
            return Outcome.fail(f"translation miss raised {type(converted).__name__}: {converted}")
        message = str(converted)
        for fact in error.get("messageContains", {}).values():
            if str(fact) not in message:
                return Outcome.fail(f"message does not contain {fact}: {message}")
        return Outcome(PASS)

    got = endpoint_string(actual.name, actual.port, actual.tls_name)
    if error is not None:
        return Outcome.fail(f"expected {error['category']} error, got {got}")
    expected = endpoint_string(expect["host"], expect["port"], expect.get("tlsName"))
    if expected != got:
        return Outcome.fail(f"expected {expected}, got {got}")
    return Outcome(PASS)


def endpoint_string(host: str, port: int, tls_name: Optional[str]) -> str:
    return f"host={host} port={port} tlsName={tls_name if tls_name is not None else 'null'}"


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------


def merge_row(row: Row, sdk: SdkConfig) -> Outcome:
    try:
        policy, static_seeds = sdk.policy(row.data["config"])
    except ConfigNotApplied as exc:
        return Outcome.fail(f"config: {exc}")
    results = [pnc_hosts(result) for result in row.data["input"].get("discoveryResults") or []]
    refresher = _ConformanceRefresher.scripted(policy, results, static_seeds)

    snapshots = [refresher.initial()[2]]
    snapshots += [refresher.refresh()[2] for _ in results[1:]]
    for i, expected in enumerate(row.data["expect"]["snapshots"]):
        exp = seeds_string(pnc_hosts(expected))
        got = seeds_string(snapshots[i]) if i < len(snapshots) else ""
        if exp != got:
            return Outcome.fail(f"snapshot {i}: expected {exp}, got {got}")
    return Outcome(PASS)


def seeds_string(hosts: List[PncHost]) -> str:
    return "[" + ", ".join(f"{h.name} {h.port}" for h in hosts) + "]"


# ---------------------------------------------------------------------------
# tls
# ---------------------------------------------------------------------------


def tls_row(row: Row) -> Outcome:
    """Matches the reference identifier against the subject alternative
    names of the row's certificate, as the TLS handshake does."""
    tls_name, shape = row.data["input"]["tlsName"], row.data["input"]["shape"]
    certificate = (row.path.parent / "certs" / f"{shape}.pem").read_bytes()
    got = "accept" if _conformance_tls_name_matches(certificate, tls_name) else "reject"
    expected = row.data["expect"]["outcome"]
    if expected != got:
        return Outcome.fail(f"expected {expected}, got {got}")
    return Outcome(PASS)


# ---------------------------------------------------------------------------
# dns and aws-tags
# ---------------------------------------------------------------------------


class FixtureServer:
    """``discovery/providers/fixture-server.py``: a DNS server and an EC2
    endpoint answering what the selected refresh says."""

    def __init__(self, root: Path):
        script = root / "discovery" / "providers" / "fixture-server.py"
        self.process = subprocess.Popen(
            [sys.executable, "-I", str(script)], stdout=subprocess.PIPE, text=True,
        )
        line = self.process.stdout.readline()
        try:
            ports = json.loads(line)
            self.http, self.dns = int(ports["http"]), int(ports["dns"])
        except (ValueError, KeyError, TypeError) as exc:
            self.close()
            raise RuntimeError(f"fixture server printed no ports: {line!r}") from exc

    def select(self, refresh: Any, region: Optional[str]) -> None:
        """Selects the refresh the server answers next."""
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.http}/fixture/select",
            data=json.dumps({"refresh": refresh, "region": region}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status != 200:
                raise RuntimeError(f"fixture select failed: {response.read()!r}")

    def close(self) -> None:
        self.process.kill()
        self.process.wait()


@pytest.fixture(scope="module")
def fixture_server():
    server = FixtureServer(FIXTURES)
    yield server
    server.close()


def provider_row(
    row: Row, server: FixtureServer, sdk: SdkConfig, monkeypatch: pytest.MonkeyPatch,
) -> Outcome:
    discovery = dict(row.data["config"]["discovery"])
    provider = discovery["provider"]
    if (row.kind, provider) not in (("dns", "dns"), ("aws-tags", "aws-tags")):
        return Outcome(UNSUPPORTED, f"provider {provider} is not implemented for kind {row.kind}")

    # Point the provider at the fixture server, through the same file keys an
    # application would use for a resolver or an EC2 endpoint.
    block_name = "dns" if provider == "dns" else "aws"
    block = dict(discovery[block_name])
    block.setdefault("timeout_ms", LOOPBACK_TIMEOUT_MS)
    if provider == "dns":
        block["servers"] = [f"127.0.0.1:{server.dns}"]
    else:
        block["endpoint"] = f"http://127.0.0.1:{server.http}"
        block.setdefault("init_timeout_ms", block["timeout_ms"])
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", FIXTURE_ACCESS_KEY_ID)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", FIXTURE_SECRET)
        monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    discovery[block_name] = block
    discovery["merge_policy"] = "discovery_only"
    region = block.get("region")

    try:
        policy, static_seeds = sdk.policy({"discovery": discovery})
    except ConfigNotApplied as exc:
        return Outcome.fail(f"config: {exc}")
    refresher = _ConformanceRefresher(policy, static_seeds)

    refreshes = zip(row.data["input"]["refreshes"], row.data["expect"]["refreshes"])
    for i, (refresh, expect) in enumerate(refreshes):
        server.select(refresh, region)
        hosts, error, seeds = refresher.initial() if i == 0 else refresher.refresh()
        if "hosts" in expect:
            if error is not None:
                return Outcome.fail(f"refresh {i}: expected hosts, got error {error}")
            exp, got = host_set(pnc_hosts(expect["hosts"])), host_set(hosts)
            if exp != got:
                return Outcome.fail(f"refresh {i}: expected hosts {exp}, got {got}")
        else:
            category = expect["error"]["category"]
            if error is None:
                return Outcome.fail(f"refresh {i}: expected {category} error, got hosts {host_set(hosts)}")
            message = str(error)
            if FIXTURE_SECRET in message:
                return Outcome.fail(f"refresh {i}: the error message carries the secret key")
            for fact in expect["error"].get("messageContains") or []:
                if str(fact) not in message:
                    return Outcome.fail(f"refresh {i}: message does not contain {fact}: {message}")
        exp, got = host_set(pnc_hosts(expect["seeds"])), host_set(seeds)
        if exp != got:
            return Outcome.fail(f"refresh {i}: expected seeds {exp}, got {got}")
    return Outcome(PASS)


def host_set(hosts: List[PncHost]) -> List[tuple]:
    """Hosts as the provider fixtures compare them: by host, port and tlsName
    in any order, addresses compared as addresses."""
    return sorted((canonical_host(h.name), h.port, h.tls_name or "") for h in hosts)


def canonical_host(name: str) -> str:
    try:
        return str(ipaddress.ip_address(name))
    except ValueError:
        return name.lower()


# ---------------------------------------------------------------------------
# Fixture values
# ---------------------------------------------------------------------------


def pnc_hosts(seeds: Optional[List[Dict[str, Any]]]) -> List[PncHost]:
    return [PncHost(s["host"], s["port"], s.get("tlsName")) for s in seeds or []]


def string_map(value: Dict[Any, Any]) -> Dict[str, str]:
    return {str(k): str(v) for k, v in value.items()}
