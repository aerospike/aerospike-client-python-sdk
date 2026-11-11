"""The L5 driver's pure parts: flag parsing, key selection and ownership math."""

import importlib.util
import sys
from pathlib import Path

import pytest

from aerospike_sdk import DataSet

_DRIVER = Path(__file__).resolve().parents[1] / "l5_aws" / "driver.py"
_spec = importlib.util.spec_from_file_location("l5_driver", _DRIVER)
driver = importlib.util.module_from_spec(_spec)
sys.modules["l5_driver"] = driver
_spec.loader.exec_module(driver)


class _Node:
    def __init__(self, name: str, active: bool = True) -> None:
        self.name = name
        self.is_active = active


def _held(*names: str, inactive: str = "") -> dict:
    return {
        name: driver.NodeView(name, "db.example.internal", 30000 + i, "10.0.0.1", name != inactive)
        for i, name in enumerate(names)
    }


class TestFlags:
    def test_port_map_parses_node_to_host_port(self):
        assert driver.parse_port_map("1A0=db:30001, 2A0=db:30002") == {
            "1A0": "db:30001", "2A0": "db:30002",
        }

    def test_bad_port_map_entry_is_rejected(self):
        with pytest.raises(ValueError):
            driver.parse_port_map("1A0")

    def test_seed_host_port_handles_ipv6_brackets(self):
        assert driver.split_host_port("[2001:db8::1]:3000") == ("2001:db8::1", 3000)
        assert driver.split_host_port("db.example.internal:30287") == ("db.example.internal", 30287)

    def test_defaults_match_the_contract(self):
        options = driver.parse_args([
            "--seed", "db:3000", "--shape", "pass-through", "--tend-interval", "250",
            "--timeout", "1000", "--namespace", "test", "--set", "l5",
        ])
        assert (options.on_miss, options.workers, options.keys) == ("strict", 4, 1000)


class TestServeKeys:
    def test_one_key_per_partition_keeping_the_first(self):
        keys = driver.pick_partition_keys(DataSet.of("test", "l5"), "after")
        assert len(keys) == driver.PARTITIONS
        assert [key.partition_id for key in keys] == list(range(driver.PARTITIONS))
        ordinals = [int(str(key.value).split("-")[1]) for key in keys]
        assert len(set(ordinals)) == driver.PARTITIONS
        assert min(ordinals) == 0


class TestOwnership:
    def test_no_map_means_no_counts(self):
        assert driver.unowned_partitions(None, _held("1A0")) == (None, None)

    def test_counts_masters_and_the_unowned(self):
        held = _held("1A0", "2A0", "3A0", inactive="3A0")
        masters = [_Node("1A0")] * 2 + [_Node("2A0")] + [None] + [_Node("3A0")] + [
            _Node("4A0"),  # a master the client does not hold
            _Node("1A0", active=False),  # held, but the map's node object is inactive
        ]
        unowned, owners = driver.unowned_partitions(masters, held)
        assert unowned == 4
        assert owners == {"1A0": 2, "2A0": 1}


class _PncNode:
    def __init__(self, name: str, port: int, active: bool = True) -> None:
        self.name = name
        self.host = ("db.example.internal", port)
        self.address = f"10.0.0.{port - 30000}:{port}"
        self.is_active = active


class _Cluster:
    def __init__(self, *nodes: _PncNode) -> None:
        self._nodes = list(nodes)

    def nodes(self):
        return list(self._nodes)


class _PncClient:
    def __init__(self, masters) -> None:
        self.masters = masters

    def _partition_masters(self, namespace: str):
        return self.masters


class TestRouting:
    def test_read_reports_a_change_only_when_the_routing_differs(self):
        routing = driver.Routing()
        a, b = _PncNode("1A0", 30001), _PncNode("2A0", 30002)
        cluster = _Cluster(a, b)
        masters = [a] * 2048 + [b] * 2048
        assert routing.read(cluster, _PncClient(masters), "test")
        assert not routing.read(cluster, _PncClient(masters), "test")
        assert routing.unowned == 0 and routing.owners == {"1A0": 2048, "2A0": 2048}
        assert routing.nodes["2A0"].address == "10.0.0.2"

        b.is_active = False
        assert routing.read(cluster, _PncClient(masters), "test")
        assert routing.unowned == 2048
        assert routing.read(_Cluster(a), _PncClient([a] * 4096), "test")
        assert routing.unowned == 0 and set(routing.reachable) == {"1A0"}

    def test_usable_needs_an_active_node_and_full_ownership(self):
        routing = driver.Routing()
        routing.nodes = _held("1A0", "2A0", inactive="2A0")
        routing.unowned = 0
        assert routing.usable("1A0")
        assert not routing.usable("2A0")
        assert not routing.usable("9Z9")
        routing.unowned = 1
        assert not routing.usable("1A0")
        routing.unowned = None
        assert routing.usable("1A0")


class TestListenerProbe:
    def test_serving_starts_at_the_first_of_three_rounds_with_every_address(self):
        probe = driver.ListenerProbe()
        probe.round(10, [("10.0.0.1", True), ("10.0.0.2", False)])
        probe.round(20, [("10.0.0.1", True), ("10.0.0.2", True)])
        probe.round(30, [("10.0.0.1", True), ("10.0.0.2", True)])
        assert probe.serving_at is None
        probe.round(40, [("10.0.0.1", True), ("10.0.0.2", True)])
        assert probe.serving_at == 20 and probe.last_closed_at == 10
        assert probe.addresses() == [
            {"address": "10.0.0.1", "firstConnectedAt": 10},
            {"address": "10.0.0.2", "firstConnectedAt": 20},
        ]

    def test_a_refused_round_restarts_the_count(self):
        probe = driver.ListenerProbe()
        probe.round(10, [("10.0.0.1", True)])
        probe.round(20, [("10.0.0.1", True)])
        probe.round(30, [("10.0.0.1", False)])
        probe.round(40, [("10.0.0.1", True)])
        probe.round(50, [("10.0.0.1", True)])
        assert probe.serving_at is None and probe.last_closed_at == 30
        probe.round(60, [("10.0.0.1", True)])
        assert probe.serving_at == 40

    def test_a_round_that_resolved_nothing_does_not_serve(self):
        probe = driver.ListenerProbe()
        for t in (10, 20, 30):
            probe.round(t, [])
        assert probe.serving_at is None and probe.last_closed_at == 30
        probe.round(40, [("10.0.0.1", False)])
        assert probe.addresses() == [{"address": "10.0.0.1", "firstConnectedAt": None}]
