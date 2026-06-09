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

import argparse

import pytest

from benchmarks.config import (
    WorkloadKind,
    build_arg_parser,
    config_from_args,
    parse_latency_arg,
    parse_workload_arg,
)
from benchmarks.record_spec import first_integer_bin, parse_bin_spec
from benchmarks.stats import latency_column_labels, latency_threshold_ms


def test_parse_bin_spec() -> None:
    fields = parse_bin_spec("I1,S4,B8")
    assert [f.name for f in fields] == ["b0", "b1", "b2"]
    assert fields[0].kind == "int"
    assert fields[1].kind == "str" and fields[1].size == 4
    assert fields[2].kind == "bytes" and fields[2].size == 8


def test_parse_bin_spec_invalid() -> None:
    with pytest.raises(ValueError):
        parse_bin_spec("")


def test_first_integer_bin() -> None:
    assert first_integer_bin(parse_bin_spec("I1")) == "b0"
    with pytest.raises(ValueError):
        first_integer_bin(parse_bin_spec("S8"))


def test_latency_thresholds_shift_1() -> None:
    assert latency_threshold_ms(1, 1) == 1.0
    assert latency_threshold_ms(2, 1) == 2.0
    assert latency_threshold_ms(3, 1) == 4.0
    labels = latency_column_labels(7, 1)
    assert labels[0] == "<=1ms"
    assert labels[1] == ">1ms"
    assert labels[2] == ">2ms"


def test_latency_labels_shift_3() -> None:
    labels = latency_column_labels(4, 3)
    assert len(labels) == 4
    assert labels[2] == ">8ms"
    assert labels[3] == ">64ms"


def test_parse_latency_arg() -> None:
    assert parse_latency_arg("7,1") == (7, 1, "columns")
    assert parse_latency_arg("ycsb") == (7, 1, "ycsb")
    assert parse_latency_arg("YCSB") == (7, 1, "ycsb")
    with pytest.raises(argparse.ArgumentTypeError):
        parse_latency_arg("1")


def test_parse_workload_variants() -> None:
    assert parse_workload_arg("I")[0] == WorkloadKind.INSERT
    k, r, ra, wa = parse_workload_arg("RU,80,60,30")
    assert k == WorkloadKind.READ_UPDATE and r == 80 and ra == 60 and wa == 30
    k2, r2, _, _ = parse_workload_arg("RR,25")
    assert k2 == WorkloadKind.READ_REPLACE and r2 == 25


def test_config_from_args_seed() -> None:
    p = build_arg_parser()
    ns = p.parse_args(["--seed", "0", "-d", "1"])
    cfg = config_from_args(ns)
    assert cfg.seed != 0
