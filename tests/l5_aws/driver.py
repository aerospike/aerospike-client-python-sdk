#!/usr/bin/env python3
"""The SDK's L5 driver for the ``l5-aws`` harness in aerospike-client-conformance.

Implements ``l5-aws/DRIVER.md`` contract version 3: builds one async client
for the shape the harness asks for, runs a fixed read/write load, polls the
client's routing, and reports what the client sees as JSON lines on stdout.
The driver measures and reports; the harness makes every pass/fail decision.

Commands arrive one per line on stdin (``serve STEP``, ``watch HOST:PORT
NODE``, ``reset``, ``quit``); end of input is ``quit``. Everything that is
not a JSON object goes to stderr.

Check it locally against one server, with its node name from
``asinfo -v node``::

    printf 'serve smoke\\nwatch 127.0.0.1:3000 <node>\\nquit\\n' | (sleep 5; cat) | \\
      python tests/l5_aws/driver.py --seed 127.0.0.1:3000 --shape port-map \\
      --port-map <node>=127.0.0.1:3000 --user admin --tend-interval 250 \\
      --timeout 1000 --namespace test --set smoke
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import socket
import sys
import threading
import time
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aerospike_sdk
from aerospike_sdk import (
    Behavior,
    ClusterDefinition,
    DataSet,
    PassThroughAddressTranslator,
    PortMapAddressTranslator,
    RecordNotFoundError,
    TranslationMissMode,
)
from aerospike_sdk.policy import Settings, SystemSettings

CONTRACT = 3
CLIENT_NAME = "python-sdk"
PARTITIONS = 4096

ROUTING_POLL_MS = 10
HEARTBEAT_MS = 100
REACHABILITY_EVERY_MS = 500
REACHABILITY_TIMEOUT_MS = 1000
WATCH_PROBE_MS = 10
WATCH_CONNECT_TIMEOUT_S = 0.05
WATCH_SERVING_ROUNDS = 3
WATCH_GIVE_UP_MS = 60_000
QUIT_GRACE_S = 30
SERVE_CONCURRENCY = 16

_START = time.monotonic()
_OUT_LOCK = threading.Lock()


def now_ms() -> int:
    """Milliseconds on the driver's monotonic clock since it started."""
    return int((time.monotonic() - _START) * 1000)


def emit(obj: Dict[str, Any]) -> None:
    """Write one JSON object to stdout and flush it.

    The warning handler calls this from the client's logging thread, so the
    write is serialized with the asyncio side.
    """
    line = json.dumps(obj, separators=(",", ":"))
    with _OUT_LOCK:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def parse_port_map(text: str) -> Dict[str, str]:
    """``NODE=HOST:PORT,...`` to a node-name to ``host:port`` mapping."""
    mapping: Dict[str, str] = {}
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        node, sep, target = item.partition("=")
        if not sep or not node or not target:
            raise ValueError(f"bad --port-map entry: {item!r}")
        mapping[node] = target
    return mapping


def split_host_port(text: str) -> Tuple[str, int]:
    """``host:port``, with an IPv6 host in brackets, to ``(host, port)``."""
    host, sep, port = text.rpartition(":")
    if not sep:
        raise ValueError(f"expected host:port, got {text!r}")
    return host.strip("[]"), int(port)


KEY_HASH_BATCH = 512


def _fold_keys(keys: List[Any], dataset: DataSet, step: str, start: int, count: int) -> int:
    """Hash ``STEP-<start>`` .. ``STEP-<start+count-1>`` into *keys*, keeping the
    first key that lands in each partition; return how many are still missing."""
    for n in range(start, start + count):
        key = dataset.id(f"{step}-{n}")
        pid = key.partition_id
        if keys[pid] is None:
            keys[pid] = key
    return keys.count(None)


def pick_partition_keys(dataset: DataSet, step: str) -> List[Any]:
    """One key per partition: ``STEP-0``, ``STEP-1``, ... keeping the first that
    lands in each partition, until every partition has one."""
    keys: List[Any] = [None] * PARTITIONS
    start = 0
    while _fold_keys(keys, dataset, step, start, KEY_HASH_BATCH):
        start += KEY_HASH_BATCH
    return keys


async def pick_partition_keys_yielding(dataset: DataSet, step: str) -> List[Any]:
    """:func:`pick_partition_keys`, yielding to the event loop between hash
    batches so the load, the routing poll and the heartbeats keep their timing."""
    keys: List[Any] = [None] * PARTITIONS
    start = 0
    while _fold_keys(keys, dataset, step, start, KEY_HASH_BATCH):
        start += KEY_HASH_BATCH
        await asyncio.sleep(0)
    return keys


# -- Routing view -------------------------------------------------------------

class NodeView:
    """What the driver reports about one node the client holds."""

    __slots__ = ("name", "host", "port", "address", "active")

    def __init__(self, name: str, host: str, port: int, address: str, active: bool) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.address = address
        self.active = active

    def key(self) -> Tuple[str, str, int, bool]:
        return (self.name, self.host, self.port, self.active)


def unowned_partitions(
    masters: Optional[Sequence[Any]], held: Dict[str, NodeView],
) -> Tuple[Optional[int], Optional[Dict[str, int]]]:
    """Count the partitions whose master is missing, inactive, or not a node the
    client holds under that name, and the partitions each node masters.

    ``masters`` is the client's map (one entry per partition, ``None`` where
    no node claims it); ``None`` when the client does not expose it.
    """
    if masters is None:
        return None, None
    unowned = 0
    owners: Dict[str, int] = {}
    for master in masters:
        if master is None:
            unowned += 1
            continue
        name = master.name
        view = held.get(name)
        if view is None or not view.active or not master.is_active:
            unowned += 1
            continue
        owners[name] = owners.get(name, 0) + 1
    return unowned, owners


class Routing:
    """The client's routing as the driver last read it."""

    def __init__(self) -> None:
        self.nodes: Dict[str, NodeView] = {}
        self.unowned: Optional[int] = None
        self.owners: Optional[Dict[str, int]] = None
        self.reachable: Dict[str, Optional[bool]] = {}
        self.unowned_since: Optional[int] = None
        self.longest_unowned_ms = 0

    def signature(self) -> Tuple[Any, ...]:
        return (tuple(sorted(v.key() for v in self.nodes.values())), self.unowned)

    def read(self, cluster: Any, pnc_client: Any, namespace: str) -> bool:
        """Re-read the routing; return whether it changed."""
        before = self.signature()
        nodes: Dict[str, NodeView] = {}
        for node in cluster.nodes():
            host, port = node.host
            address, _, _ = node.address.rpartition(":")
            nodes[node.name] = NodeView(node.name, host, port, address.strip("[]"), node.is_active)
        self.nodes = nodes
        self.reachable = {name: self.reachable.get(name) for name in nodes}
        masters = pnc_client._partition_masters(namespace)
        self.unowned, self.owners = unowned_partitions(masters, nodes)
        self._track_unowned()
        return self.signature() != before

    def _track_unowned(self) -> None:
        t = now_ms()
        if self.unowned:
            if self.unowned_since is None:
                self.unowned_since = t
            self.longest_unowned_ms = max(self.longest_unowned_ms, t - self.unowned_since)
        else:
            self.unowned_since = None

    def reset(self) -> None:
        self.longest_unowned_ms = 0
        self.unowned_since = now_ms() if self.unowned else None

    def usable(self, name: str) -> bool:
        view = self.nodes.get(name)
        if view is None or not view.active:
            return False
        return self.unowned is None or self.unowned == 0

    def snapshot(self, reason: str, load: "Load") -> Dict[str, Any]:
        self._track_unowned()
        obj: Dict[str, Any] = {
            "type": "snapshot",
            "t": now_ms(),
            "reason": reason,
            "nodes": [
                {
                    "name": v.name,
                    "host": v.host,
                    "port": v.port,
                    "address": v.address,
                    "active": v.active,
                    "reachable": self.reachable.get(v.name),
                }
                for v in sorted(self.nodes.values(), key=lambda v: v.name)
            ],
        }
        if self.unowned is not None:
            obj["unowned"] = self.unowned
            obj["owners"] = self.owners
            obj["longestUnownedMs"] = self.longest_unowned_ms
        obj.update(load.counters())
        return obj


# -- Load ---------------------------------------------------------------------

class Load:
    """The fixed load: each worker runs one command at a time until stopped."""

    def __init__(self, session: Any, dataset: DataSet, workers: int, keys: int,
                 tend_interval_ms: int) -> None:
        self.session = session
        self.dataset = dataset
        self.workers = workers
        self.keys = keys
        self.retry_sleep_s = tend_interval_ms / 1000
        self.succeeded = 0
        self.failed = 0
        self.last_error: Optional[str] = None
        self.max_latency_ms = 0
        self.reset_at = 0
        self.first_success = asyncio.Event()
        # Set once ``ready`` is written: a failure before it has no line to follow.
        self.report_failures = False
        self.stopping = False
        self.tasks: List[asyncio.Task] = []

    def counters(self) -> Dict[str, Any]:
        return {
            "succeeded": self.succeeded,
            "failed": self.failed,
            "maxLatencyMs": self.max_latency_ms,
            "lastError": self.last_error,
        }

    def reset(self) -> None:
        self.max_latency_ms = 0
        self.reset_at = now_ms()

    def start(self) -> None:
        self.tasks = [asyncio.create_task(self._worker(w)) for w in range(self.workers)]

    async def stop(self) -> None:
        self.stopping = True
        if self.tasks:
            done, pending = await asyncio.wait(self.tasks, timeout=QUIT_GRACE_S)
            for task in pending:
                task.cancel()

    async def _worker(self, w: int) -> None:
        i = 0
        while not self.stopping:
            k = (w + i * self.workers) % self.keys
            key = self.dataset.id(f"load-{k}")
            started = now_ms()
            while True:
                attempt_started = now_ms()
                try:
                    if i % 2 == 0:
                        await self.session.put(key, {"v": i})
                    else:
                        await self.session.get(key)
                    break
                except RecordNotFoundError:
                    # A read of a key no worker has written yet: the record's
                    # absence is the answer, not a failed attempt.
                    break
                except Exception as e:  # any error the client raises is a failed attempt
                    self.failed += 1
                    self.last_error = f"{type(e).__name__}: {e}"
                    if self.report_failures:
                        emit({"type": "failure", "t": now_ms(), "startedAt": attempt_started,
                              "message": self.last_error})
                    if self.stopping:
                        return
                    await asyncio.sleep(self.retry_sleep_s)
            self.succeeded += 1
            if started >= self.reset_at:
                self.max_latency_ms = max(self.max_latency_ms, now_ms() - started)
            self.first_success.set()
            i += 1


# -- Watch probe --------------------------------------------------------------

class ListenerProbe:
    """Rounds of connects to every address a listener's name resolves to.

    The listener serves from the first of ``WATCH_SERVING_ROUNDS`` consecutive
    rounds in which every address connected: the client may connect to any of
    them, so the measurement starts only when none refuses.
    """

    def __init__(self) -> None:
        self.serving_at: Optional[int] = None
        self.last_closed_at: Optional[int] = None
        self.first_connected_at: Dict[str, Optional[int]] = {}
        self._streak_started: Optional[int] = None
        self._streak = 0

    def round(self, t: int, results: Sequence[Tuple[str, bool]]) -> None:
        """Record one round that started at *t*: ``(address, connected)`` per address."""
        for address, connected in results:
            first = self.first_connected_at.setdefault(address, None)
            if connected and first is None:
                self.first_connected_at[address] = t
        if results and all(connected for _, connected in results):
            if self._streak == 0:
                self._streak_started = t
            self._streak += 1
            if self._streak >= WATCH_SERVING_ROUNDS and self.serving_at is None:
                self.serving_at = self._streak_started
        else:
            self._streak = 0
            self.last_closed_at = t

    def addresses(self) -> List[Dict[str, Any]]:
        return [{"address": address, "firstConnectedAt": t}
                for address, t in self.first_connected_at.items()]


# -- Driver -------------------------------------------------------------------

class WarningHandler(logging.Handler):
    """Each client warning or error becomes a ``log`` line."""

    def emit(self, record: logging.LogRecord) -> None:
        emit({"type": "log", "t": now_ms(), "level": record.levelname, "message": self.format(record)})


class ValidatorTraceHandler(logging.Handler):
    """The client's per-address validation outcomes, on stderr with the
    driver's clock, so a row's log shows which address answered when. The
    harness keeps stderr beside the row's checks; stdout stays the contract."""

    def emit(self, record: logging.LogRecord) -> None:
        with _OUT_LOCK:
            sys.stderr.write(f"t={now_ms()} {self.format(record)}\n")
            sys.stderr.flush()


class Driver:
    def __init__(self, options: argparse.Namespace) -> None:
        self.options = options
        self.dataset = DataSet.of(options.namespace, options.set)
        self.routing = Routing()
        self.cluster: Any = None
        self.pnc_client: Any = None
        self.load: Optional[Load] = None
        self.watch_task: Optional[asyncio.Task] = None
        self.tasks: List[asyncio.Task] = []

    def cluster_definition(self) -> ClusterDefinition:
        o = self.options
        host, port = split_host_port(o.seed)
        cd = ClusterDefinition(host, port)
        if o.user:
            cd = cd.with_native_credentials(o.user, os.environ.get("AEROSPIKE_ADMIN_PASSWORD", ""))
        on_miss = TranslationMissMode.STRICT if o.on_miss == "strict" else TranslationMissMode.PASS_THROUGH
        if o.shape == "pass-through":
            cd = cd.using_services_alternate().with_address_translation(
                PassThroughAddressTranslator(), on_miss=on_miss,
            )
        else:
            if not o.port_map:
                raise SystemExit("--shape port-map needs --port-map")
            cd = cd.using_services_alternate(False).with_address_translation(
                PortMapAddressTranslator(parse_port_map(o.port_map)), on_miss=on_miss,
            )
        timeout = timedelta(milliseconds=o.timeout)
        return cd.with_system_settings(SystemSettings(
            tend_interval=timedelta(milliseconds=o.tend_interval),
            tend_timeout=timeout,
            wait_for_connection_to_complete=timeout,
        ))

    def behavior(self) -> Behavior:
        timeout = timedelta(milliseconds=self.options.timeout)
        return Behavior.DEFAULT.derive_with_changes(
            "l5-driver",
            all=Settings(max_retries=0, socket_timeout=timeout, total_timeout=timeout),
        )

    async def run(self) -> int:
        handler = WarningHandler(level=logging.WARNING)
        handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.WARNING)
        trace = ValidatorTraceHandler(level=logging.DEBUG)
        trace.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        validator = logging.getLogger("aerospike_core.cluster.node_validator")
        validator.addHandler(trace)
        validator.setLevel(logging.DEBUG)
        validator.propagate = False
        try:
            self.cluster = await self.cluster_definition().connect()
        except Exception as e:
            emit({"type": "error", "t": now_ms(), "message": f"client not created: {e}"})
            print(f"client not created: {e}", file=sys.stderr)
            return 3
        self.pnc_client = self.cluster._sdk_client.underlying_client
        session = self.cluster.create_session(self.behavior())
        self.load = Load(session, self.dataset, self.options.workers, self.options.keys,
                         self.options.tend_interval)
        self.routing.read(self.cluster, self.pnc_client, self.options.namespace)
        self.load.start()
        self.tasks = [
            asyncio.create_task(self._routing_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._reachability_loop()),
        ]
        await self.load.first_success.wait()
        emit({
            "type": "ready", "t": now_ms(), "contract": CONTRACT,
            "epochMs": int(time.time() * 1000), "client": CLIENT_NAME,
            "clientVersion": aerospike_sdk.__version__, "shape": self.options.shape,
        })
        self.load.report_failures = True
        await self._commands()
        if self.watch_task is not None and not self.watch_task.done():
            # A watch that quit interrupts still owes its reply; it gives up
            # on its own 60 s after the listener served.
            await self.watch_task
        for task in self.tasks:
            task.cancel()
        await self.load.stop()
        await self.cluster.close()
        return 0

    # -- background loops --

    async def _routing_loop(self) -> None:
        while True:
            if self.routing.read(self.cluster, self.pnc_client, self.options.namespace):
                emit(self.routing.snapshot("change", self.load))
            await asyncio.sleep(ROUTING_POLL_MS / 1000)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_MS / 1000)
            emit(self.routing.snapshot("heartbeat", self.load))

    async def _reachability_loop(self) -> None:
        while True:
            names = list(self.routing.nodes)
            await asyncio.gather(*(self._reach(name) for name in names))
            await asyncio.sleep(REACHABILITY_EVERY_MS / 1000)

    async def _reach(self, name: str) -> None:
        try:
            node = self.cluster.get_node(name)
            answer = await asyncio.wait_for(node.info("node"), REACHABILITY_TIMEOUT_MS / 1000)
            ok = answer.get("node") == name
        except Exception:
            ok = False
        if name in self.routing.reachable:
            self.routing.reachable[name] = ok

    # -- commands --

    async def _commands(self) -> None:
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        while True:
            line = await reader.readline()
            if not line:
                return
            words = line.decode("utf-8", "replace").split()
            if not words:
                continue
            command, args = words[0], words[1:]
            if command == "quit":
                return
            if command == "serve" and len(args) == 1:
                await self._serve(args[0])
            elif command == "reset" and not args:
                self.routing.reset()
                self.load.reset()
                emit({"type": "reset", "t": now_ms()})
            elif command == "watch" and len(args) == 2:
                if self.watch_task is not None and not self.watch_task.done():
                    self._error("a watch is already running")
                else:
                    self.watch_task = asyncio.create_task(self._watch(args[0], args[1]))
            else:
                self._error(f"unknown command: {line.decode('utf-8', 'replace').strip()}")

    def _error(self, message: str) -> None:
        emit({"type": "error", "t": now_ms(), "message": message})

    async def _serve(self, step: str) -> None:
        started = now_ms()
        session = self.cluster.create_session(self.behavior())
        keys = await pick_partition_keys_yielding(self.dataset, step)
        written = read = mismatched = failures = 0
        first_failure: Optional[str] = None
        gate = asyncio.Semaphore(SERVE_CONCURRENCY)

        async def one(pid: int) -> Tuple[int, int, int, int, Optional[str]]:
            value = f"{step}-{pid}"
            async with gate:
                try:
                    await session.put(keys[pid], {"v": value})
                except Exception as e:
                    return 0, 0, 0, 1, f"partition {pid} put: {type(e).__name__}: {e}"
                try:
                    record = await session.get(keys[pid])
                except RecordNotFoundError:
                    return 1, 0, 1, 0, None
                except Exception as e:
                    return 1, 0, 0, 1, f"partition {pid} get: {type(e).__name__}: {e}"
            return (1, 1, 0, 0, None) if record.bins.get("v") == value else (1, 0, 1, 0, None)

        for w, r, m, f, failure in await asyncio.gather(*(one(pid) for pid in range(PARTITIONS))):
            written += w
            read += r
            mismatched += m
            failures += f
            if failure and first_failure is None:
                first_failure = failure
        emit({
            "type": "served", "t": now_ms(), "step": step, "partitions": PARTITIONS,
            "written": written, "read": read, "mismatched": mismatched, "failures": failures,
            "firstFailure": first_failure, "elapsedMs": now_ms() - started,
        })

    async def _watch(self, address: str, node: str) -> None:
        try:
            host, port = split_host_port(address)
            probe = ListenerProbe()
            while probe.serving_at is None:
                round_started = time.monotonic()
                t = now_ms()
                addresses = await self._resolve(host, port)
                connected = await asyncio.gather(*(self._tcp_open(a, port) for a in addresses))
                probe.round(t, list(zip(addresses, connected)))
                # Rounds never overlap: a slow round delays the next tick.
                await asyncio.sleep(
                    max(0.0, WATCH_PROBE_MS / 1000 - (time.monotonic() - round_started)))
            serving_at = probe.serving_at
            usable_at: Optional[int] = None
            while usable_at is None and now_ms() - serving_at < WATCH_GIVE_UP_MS:
                if self.routing.usable(node):
                    usable_at = now_ms()
                    break
                await asyncio.sleep(WATCH_PROBE_MS / 1000)
            emit({
                "type": "watch", "t": now_ms(), "address": address, "node": node,
                "servingAt": serving_at, "lastClosedAt": probe.last_closed_at,
                "addresses": probe.addresses(), "usableAt": usable_at,
                "usableMs": None if usable_at is None else usable_at - serving_at,
                "ownershipChecked": self.routing.unowned is not None,
            })
        except ValueError as e:
            self._error(f"watch: {e}")

    @staticmethod
    async def _resolve(host: str, port: int) -> List[str]:
        """Every address *host* resolves to, in resolution order; none on a failed lookup."""
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(
                host, port, type=socket.SOCK_STREAM)
        except OSError:
            return []
        return list(dict.fromkeys(info[4][0] for info in infos))

    @staticmethod
    async def _tcp_open(host: str, port: int) -> bool:
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), WATCH_CONNECT_TIMEOUT_S)
        except (OSError, asyncio.TimeoutError):
            return False
        writer.close()
        return True


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--seed", required=True)
    parser.add_argument("--shape", required=True, choices=("pass-through", "port-map"))
    parser.add_argument("--port-map")
    parser.add_argument("--on-miss", default="strict", choices=("strict", "pass-through"))
    parser.add_argument("--user")
    parser.add_argument("--tend-interval", type=int, required=True)
    parser.add_argument("--timeout", type=int, required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--set", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--keys", type=int, default=1000)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    options = parse_args(argv)
    if options.port_map and options.shape != "port-map":
        print("--port-map only applies to --shape port-map", file=sys.stderr)
        return 2
    return asyncio.run(Driver(options).run())


if __name__ == "__main__":
    sys.exit(main())
