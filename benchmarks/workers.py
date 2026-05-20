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

"""Async and sync benchmark worker loops."""

from __future__ import annotations

import asyncio
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, List, Tuple, Union

from aerospike_async import Key, ReadPolicy, WritePolicy, new_client, new_client_blocking
from aerospike_async.exceptions import RecordNotFound as _AsRecordNotFound

from aerospike_sdk import AsyncPool
from aerospike_sdk.aio.client import Client
from aerospike_sdk.aio.session import Session
from aerospike_sdk.dataset import DataSet
from aerospike_sdk.error_strategy import ErrorStrategy
from aerospike_sdk.exceptions import TimeoutError as AsTimeoutError
from aerospike_sdk.policy.behavior import Behavior
from aerospike_sdk.sync.client import SyncClient
from aerospike_sdk.sync.session import SyncSession

from ._env import client_policy_from_config
from .config import WorkloadConfig, WorkloadKind
from .record_spec import (
    BinField,
    first_integer_bin,
    full_bins,
    pick_bin_index,
    single_bin_put,
)
from .stats import StatsCollector


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, AsTimeoutError):
        return True
    return isinstance(exc, TimeoutError)


def _is_not_found(exc: BaseException) -> bool:
    """A cache miss on a point read — counted as success-with-no-record,
    not as an error. PAC/PSDK fast-path raise ``RecordNotFound``; the
    legacy ``aerospike`` C client raises its own ``RecordNotFound``
    (checked separately inside ``run_legacy_sync``).
    """
    return isinstance(exc, _AsRecordNotFound)


def _classify_exc(exc: BaseException) -> Tuple[bool, bool]:
    is_timeout = _is_timeout(exc)
    is_err = not is_timeout
    return is_timeout, is_err


def _make_keys(
    dataset: DataSet,
    key_count: int,
    rng: random.Random,
    batch_size: int,
) -> Union[Key, List[Key]]:
    if batch_size <= 1:
        return dataset.id(rng.randint(1, key_count))
    return [dataset.id(rng.randint(1, key_count)) for _ in range(batch_size)]


class _BenchState:
    __slots__ = ("insert_seq", "lock")

    def __init__(self) -> None:
        self.insert_seq = 0
        self.lock = threading.Lock()

    def next_insert_key(self) -> int:
        with self.lock:
            self.insert_seq += 1
            return self.insert_seq


async def _drain_async(stream: Any, batch: int) -> None:
    if batch > 1:
        await stream.collect()
    else:
        await stream.first()


def _drain_sync(stream: Any, batch: int) -> None:
    if batch > 1:
        stream.collect()
    else:
        stream.first()


async def _one_op_async(
    session: Session,
    cfg: WorkloadConfig,
    dataset: DataSet,
    fields: List[BinField],
    rng: random.Random,
    bench: _BenchState,
    decision: List[bool],
) -> None:
    keys = _make_keys(dataset, cfg.key_count, rng, cfg.batch_size)
    bsz = max(1, cfg.batch_size)

    if cfg.workload == WorkloadKind.INSERT:
        decision[0] = False
        kid = bench.next_insert_key()
        stream = await session.insert(dataset.id(kid)).put(full_bins(fields)).execute()
        await _drain_async(stream, bsz)
        return

    if cfg.workload == WorkloadKind.READ_UPDATE:
        is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
        decision[0] = is_read
        if is_read:
            if rng.randint(1, 100) <= cfg.read_all_bins_percent:
                if bsz > 1:
                    assert isinstance(keys, list)
                    stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
                    await _drain_async(stream, bsz)
                elif cfg.fast_path:
                    assert isinstance(keys, Key)
                    await session.get(keys)
                else:
                    assert isinstance(keys, Key)
                    stream = await session.query(keys).execute()
                    await _drain_async(stream, bsz)
            else:
                assert isinstance(keys, Key)
                bi = pick_bin_index(rng, len(fields))
                stream = await session.query(keys).bin(fields[bi].name).get().execute()
                await _drain_async(stream, 1)
        else:
            if rng.randint(1, 100) <= cfg.write_all_bins_percent:
                bins = full_bins(fields)
            else:
                bins = single_bin_put(fields, pick_bin_index(rng, len(fields)))
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur: Any = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = await cur.execute()
                await _drain_async(stream, bsz)
            elif cfg.fast_path:
                assert isinstance(keys, Key)
                await session.put(keys, bins)
            else:
                assert isinstance(keys, Key)
                stream = await session.upsert(keys).put(bins).execute()
                await _drain_async(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_REPLACE:
        is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = await session.query(keys).execute()
            await _drain_async(stream, bsz)
        else:
            bins = full_bins(fields)
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.replace_if_exists(k).put(bins)
                stream = await cur.execute()
                await _drain_async(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = await session.replace_if_exists(keys).put(bins).execute()
                await _drain_async(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_MODIFY_UPDATE:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = await session.query(keys).execute()
            await _drain_async(stream, bsz)
        else:
            bins = single_bin_put(fields, pick_bin_index(rng, len(fields)))
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = await cur.execute()
                await _drain_async(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = await session.upsert(keys).put(bins).execute()
                await _drain_async(stream, 1)
        return

    int_bin = first_integer_bin(fields)
    if cfg.workload == WorkloadKind.READ_MODIFY_INCREMENT:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = await session.query(keys).execute()
            await _drain_async(stream, bsz)
        else:
            assert isinstance(keys, Key)
            stream = await session.upsert(keys).add(int_bin, 1).execute()
            await _drain_async(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_MODIFY_DECREMENT:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = await session.query(keys).execute()
            await _drain_async(stream, bsz)
        else:
            assert isinstance(keys, Key)
            stream = await session.upsert(keys).add(int_bin, -1).execute()
            await _drain_async(stream, 1)
        return

    raise NotImplementedError(cfg.workload)


def _build_op_sync(
    session: SyncSession,
    cfg: WorkloadConfig,
    dataset: DataSet,
    fields: List[BinField],
    bench: _BenchState,
    decision: List[bool],
):
    """Build a per-op callable for the configured sync workload.

    Closes over ``session``, ``dataset``, ``fields``, etc. so the hot loop
    can call ``op(rng)`` without re-reading config per op. For single-bin
    record specs the write payload is ``{b0: kid}`` literal (no per-op
    rng for bin values).
    """
    kc = cfg.key_count
    bsz = max(1, cfg.batch_size)
    fp = cfg.fast_path
    read_pct = cfg.read_percent
    write_all_bins_pct = cfg.write_all_bins_percent
    fields_t = tuple(fields)
    field_count = len(fields_t)
    single_bin = field_count == 1
    b0_name = fields_t[0].name
    ds_id = dataset.id

    if cfg.workload == WorkloadKind.INSERT:
        if bsz <= 1:
            def op(rng):
                decision[0] = False
                kid = bench.next_insert_key()
                key = ds_id(str(kid))
                bins = {b0_name: kid} if single_bin else full_bins(fields_t)
                if fp:
                    session.put(key, bins)
                    return
                session.upsert(key).put(bins).execute()
            return op

        def op(rng):
            decision[0] = False
            keys = [ds_id(str(bench.next_insert_key())) for _ in range(bsz)]
            bins = full_bins(fields_t)
            b = session.batch()
            cur = b
            for k in keys:
                cur = cur.upsert(k).put(bins)
            stream = cur.execute()
            results = stream.collect()
            n_err = sum(
                1 for r in results
                if not r.is_ok and not isinstance(r.error, _AsRecordNotFound)
            )
            return len(results) - n_err, n_err
        return op

    if cfg.workload == WorkloadKind.READ_UPDATE:
        if bsz <= 1 and fp:
            if single_bin:
                def op(rng):
                    kid = rng.randint(1, kc)
                    key = ds_id(str(kid))
                    if rng.randint(1, 100) > (100 - read_pct):
                        decision[0] = True
                        session.get(key)
                    else:
                        decision[0] = False
                        session.put(key, {b0_name: kid})
                return op

            def op(rng):
                kid = rng.randint(1, kc)
                key = ds_id(str(kid))
                if rng.randint(1, 100) > (100 - read_pct):
                    decision[0] = True
                    session.get(key)
                else:
                    decision[0] = False
                    if rng.randint(1, 100) <= write_all_bins_pct:
                        bins = full_bins(fields_t)
                    else:
                        bins = single_bin_put(fields_t, pick_bin_index(rng, field_count))
                    session.put(key, bins)
            return op

        if bsz <= 1:
            if single_bin:
                def op(rng):
                    kid = rng.randint(1, kc)
                    key = ds_id(str(kid))
                    if rng.randint(1, 100) > (100 - read_pct):
                        decision[0] = True
                        stream = session.query(key).execute()
                        stream.first()
                    else:
                        decision[0] = False
                        session.upsert(key).put({b0_name: kid}).execute()
                return op

            def op(rng):
                kid = rng.randint(1, kc)
                key = ds_id(str(kid))
                if rng.randint(1, 100) > (100 - read_pct):
                    decision[0] = True
                    stream = session.query(key).execute()
                    stream.first()
                else:
                    decision[0] = False
                    if rng.randint(1, 100) <= write_all_bins_pct:
                        bins = full_bins(fields_t)
                    else:
                        bins = single_bin_put(fields_t, pick_bin_index(rng, field_count))
                    session.upsert(key).put(bins).execute()
            return op

        # batch RU
        def op(rng):
            keys = [ds_id(str(rng.randint(1, kc))) for _ in range(bsz)]
            if rng.randint(1, 100) > (100 - read_pct):
                decision[0] = True
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                decision[0] = False
                if single_bin:
                    bins = {b0_name: int(keys[0].user_key)} if hasattr(keys[0], "user_key") else {b0_name: 0}
                elif rng.randint(1, 100) <= write_all_bins_pct:
                    bins = full_bins(fields_t)
                else:
                    bins = single_bin_put(fields_t, pick_bin_index(rng, field_count))
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = cur.execute()
            results = stream.collect()
            n_err = sum(
                1 for r in results
                if not r.is_ok and not isinstance(r.error, _AsRecordNotFound)
            )
            return len(results) - n_err, n_err
        return op

    def op(rng):
        _one_op_sync(session, cfg, dataset, fields, rng, bench, decision)
    return op


def _build_op_async(
    session: Session,
    cfg: WorkloadConfig,
    dataset: DataSet,
    fields: List[BinField],
    bench: _BenchState,
    decision: List[bool],
):
    """Build an async per-op callable for the configured workload.

    Mirrors :func:`_build_op_sync`; the per-op closure awaits PAC's async
    surface (``session.get`` / ``session.put`` fast-path or
    ``session.query/upsert(...).execute()`` builder). Single-bin specs use
    the literal ``{b0: kid}`` payload — no per-op rng for values.
    """
    kc = cfg.key_count
    bsz = max(1, cfg.batch_size)
    fp = cfg.fast_path
    read_pct = cfg.read_percent
    write_all_bins_pct = cfg.write_all_bins_percent
    fields_t = tuple(fields)
    field_count = len(fields_t)
    single_bin = field_count == 1
    b0_name = fields_t[0].name
    ds_id = dataset.id

    if cfg.workload == WorkloadKind.INSERT:
        if bsz <= 1:
            async def op(rng):
                decision[0] = False
                kid = bench.next_insert_key()
                key = ds_id(str(kid))
                bins = {b0_name: kid} if single_bin else full_bins(fields_t)
                if fp:
                    await session.put(key, bins)
                    return
                stream = await session.upsert(key).put(bins).execute()
                await stream.first()
            return op

        async def op(rng):
            decision[0] = False
            keys = [ds_id(str(bench.next_insert_key())) for _ in range(bsz)]
            bins = full_bins(fields_t)
            b = session.batch()
            cur: Any = b
            for k in keys:
                cur = cur.upsert(k).put(bins)
            stream = await cur.execute()
            results = await stream.collect()
            n_err = sum(
                1 for r in results
                if not r.is_ok and not isinstance(r.error, _AsRecordNotFound)
            )
            return len(results) - n_err, n_err
        return op

    if cfg.workload == WorkloadKind.READ_UPDATE:
        if bsz <= 1 and fp:
            if single_bin:
                async def op(rng):
                    kid = rng.randint(1, kc)
                    key = ds_id(str(kid))
                    if rng.randint(1, 100) > (100 - read_pct):
                        decision[0] = True
                        await session.get(key)
                    else:
                        decision[0] = False
                        await session.put(key, {b0_name: kid})
                return op

            async def op(rng):
                kid = rng.randint(1, kc)
                key = ds_id(str(kid))
                if rng.randint(1, 100) > (100 - read_pct):
                    decision[0] = True
                    await session.get(key)
                else:
                    decision[0] = False
                    if rng.randint(1, 100) <= write_all_bins_pct:
                        bins = full_bins(fields_t)
                    else:
                        bins = single_bin_put(fields_t, pick_bin_index(rng, field_count))
                    await session.put(key, bins)
            return op

        if bsz <= 1:
            if single_bin:
                async def op(rng):
                    kid = rng.randint(1, kc)
                    key = ds_id(str(kid))
                    if rng.randint(1, 100) > (100 - read_pct):
                        decision[0] = True
                        stream = await session.query(key).execute()
                        await stream.first()
                    else:
                        decision[0] = False
                        stream = await session.upsert(key).put({b0_name: kid}).execute()
                        await stream.first()
                return op

            async def op(rng):
                kid = rng.randint(1, kc)
                key = ds_id(str(kid))
                if rng.randint(1, 100) > (100 - read_pct):
                    decision[0] = True
                    stream = await session.query(key).execute()
                    await stream.first()
                else:
                    decision[0] = False
                    if rng.randint(1, 100) <= write_all_bins_pct:
                        bins = full_bins(fields_t)
                    else:
                        bins = single_bin_put(fields_t, pick_bin_index(rng, field_count))
                    stream = await session.upsert(key).put(bins).execute()
                    await stream.first()
            return op

        # batch RU
        async def op(rng):
            keys = [ds_id(str(rng.randint(1, kc))) for _ in range(bsz)]
            if rng.randint(1, 100) > (100 - read_pct):
                decision[0] = True
                stream = await session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                decision[0] = False
                if single_bin:
                    bins = {b0_name: int(keys[0].user_key)} if hasattr(keys[0], "user_key") else {b0_name: 0}
                elif rng.randint(1, 100) <= write_all_bins_pct:
                    bins = full_bins(fields_t)
                else:
                    bins = single_bin_put(fields_t, pick_bin_index(rng, field_count))
                b = session.batch()
                cur: Any = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = await cur.execute()
            results = await stream.collect()
            n_err = sum(
                1 for r in results
                if not r.is_ok and not isinstance(r.error, _AsRecordNotFound)
            )
            return len(results) - n_err, n_err
        return op

    async def op(rng):
        await _one_op_async(session, cfg, dataset, list(fields_t), rng, bench, decision)
    return op


def _one_op_sync(
    session: SyncSession,
    cfg: WorkloadConfig,
    dataset: DataSet,
    fields: List[BinField],
    rng: random.Random,
    bench: _BenchState,
    decision: List[bool],
) -> None:
    keys = _make_keys(dataset, cfg.key_count, rng, cfg.batch_size)
    bsz = max(1, cfg.batch_size)

    if cfg.workload == WorkloadKind.INSERT:
        decision[0] = False
        kid = bench.next_insert_key()
        stream = session.insert(dataset.id(kid)).put(full_bins(fields)).execute()
        _drain_sync(stream, bsz)
        return

    if cfg.workload == WorkloadKind.READ_UPDATE:
        is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
        decision[0] = is_read
        if is_read:
            if rng.randint(1, 100) <= cfg.read_all_bins_percent:
                if bsz > 1:
                    assert isinstance(keys, list)
                    stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
                    _drain_sync(stream, bsz)
                elif cfg.fast_path:
                    assert isinstance(keys, Key)
                    session.get(keys)
                else:
                    assert isinstance(keys, Key)
                    stream = session.query(keys).execute()
                    _drain_sync(stream, bsz)
            else:
                assert isinstance(keys, Key)
                bi = pick_bin_index(rng, len(fields))
                stream = session.query(keys).bin(fields[bi].name).get().execute()
                _drain_sync(stream, 1)
        else:
            if rng.randint(1, 100) <= cfg.write_all_bins_percent:
                bins = full_bins(fields)
            else:
                bins = single_bin_put(fields, pick_bin_index(rng, len(fields)))
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur: Any = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = cur.execute()
                _drain_sync(stream, bsz)
            elif cfg.fast_path:
                assert isinstance(keys, Key)
                session.put(keys, bins)
            else:
                assert isinstance(keys, Key)
                stream = session.upsert(keys).put(bins).execute()
                _drain_sync(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_REPLACE:
        is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = session.query(keys).execute()
            _drain_sync(stream, bsz)
        else:
            bins = full_bins(fields)
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.replace_if_exists(k).put(bins)
                stream = cur.execute()
                _drain_sync(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = session.replace_if_exists(keys).put(bins).execute()
                _drain_sync(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_MODIFY_UPDATE:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = session.query(keys).execute()
            _drain_sync(stream, bsz)
        else:
            bins = single_bin_put(fields, pick_bin_index(rng, len(fields)))
            if bsz > 1:
                assert isinstance(keys, list)
                b = session.batch()
                cur = b
                for k in keys:
                    cur = cur.upsert(k).put(bins)
                stream = cur.execute()
                _drain_sync(stream, bsz)
            else:
                assert isinstance(keys, Key)
                stream = session.upsert(keys).put(bins).execute()
                _drain_sync(stream, 1)
        return

    int_bin = first_integer_bin(fields)
    if cfg.workload == WorkloadKind.READ_MODIFY_INCREMENT:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = session.query(keys).execute()
            _drain_sync(stream, bsz)
        else:
            assert isinstance(keys, Key)
            stream = session.upsert(keys).add(int_bin, 1).execute()
            _drain_sync(stream, 1)
        return

    if cfg.workload == WorkloadKind.READ_MODIFY_DECREMENT:
        is_read = rng.randint(1, 100) <= 50
        decision[0] = is_read
        if is_read:
            if bsz > 1:
                assert isinstance(keys, list)
                stream = session.query(keys).execute(on_error=ErrorStrategy.IN_STREAM)
            else:
                assert isinstance(keys, Key)
                stream = session.query(keys).execute()
            _drain_sync(stream, bsz)
        else:
            assert isinstance(keys, Key)
            stream = session.upsert(keys).add(int_bin, -1).execute()
            _drain_sync(stream, 1)
        return

    raise NotImplementedError(cfg.workload)


async def run_async(
    cfg: WorkloadConfig,
    stats: StatsCollector,
    stop: asyncio.Event,
    connected: asyncio.Event | None = None,
) -> None:
    if cfg.workload in (
        WorkloadKind.READ_MODIFY_INCREMENT,
        WorkloadKind.READ_MODIFY_DECREMENT,
    ):
        first_integer_bin(cfg.bin_fields)

    bench_state = _BenchState()
    policy = client_policy_from_config(cfg)
    async with Client(cfg.seeds, policy=policy) as client:
        # Signal that the connection succeeded so the caller can start the
        # ticker.  Without this, the ticker prints empty intervals while the
        # client is still trying to connect (or timing out).
        if connected is not None:
            connected.set()

        session = client.create_session(Behavior.DEFAULT)
        dataset = DataSet.of(cfg.namespace, cfg.set_name)
        fields = list(cfg.bin_fields)

        async def worker(worker_id: int) -> None:
            seed = (cfg.seed + worker_id + 1) % (2**32)
            rng = random.Random(seed)
            decision = [False]
            has_limit = cfg.max_ops is not None
            sample_every = cfg.lat_sample_every
            with_tel = cfg.with_telemetry
            op_func = _build_op_async(
                session, cfg, dataset, fields, bench_state, decision,
            )
            ws = stats.register_worker()
            local_count = 0
            while not stop.is_set():
                if has_limit and stats.total_ops() >= cfg.max_ops:
                    return
                sample = with_tel and (local_count % sample_every == 0)
                t0 = time.perf_counter() if sample else 0.0
                decision[0] = False
                try:
                    ret = await op_func(rng)
                except BaseException as exc:
                    dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                    if _is_not_found(exc):
                        ws.record(decision[0], False, False, dt)
                    else:
                        to, er = _classify_exc(exc)
                        ws.record(decision[0], to, er, dt)
                        if not isinstance(exc, Exception):
                            raise
                else:
                    dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                    if ret is None:
                        ws.record(decision[0], False, False, dt)
                    else:
                        ws.bulk_record(decision[0], ret[0], ret[1], dt)
                local_count += 1

        tasks = [asyncio.create_task(worker(i)) for i in range(cfg.async_tasks)]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def run_async_pool(
    cfg: WorkloadConfig,
    stats: StatsCollector,
    stop: asyncio.Event,
    connected: asyncio.Event | None = None,
) -> None:
    """Same work as :func:`run_async` but dispatched across an ``AsyncPool``.

    Each of the ``cfg.pool_loops`` event loops gets ``cfg.async_tasks``
    concurrent workers, so total concurrency = ``pool_loops × async_tasks``.
    The per-op work (``_one_op_async``) is identical to the single-client
    path, making the two modes directly comparable.
    """
    if cfg.workload in (
        WorkloadKind.READ_MODIFY_INCREMENT,
        WorkloadKind.READ_MODIFY_DECREMENT,
    ):
        first_integer_bin(cfg.bin_fields)

    n_loops = cfg.pool_loops
    bench_state = _BenchState()
    policy = client_policy_from_config(cfg)

    # threading.Event is safe to check from any OS thread / event loop.
    thread_stop = threading.Event()

    def factory() -> Client:
        return Client(cfg.seeds, policy=policy)

    async def _bridge_stop() -> None:
        await stop.wait()
        thread_stop.set()

    async with AsyncPool(factory, loop_count=n_loops) as pool:
        if connected is not None:
            connected.set()

        bridge_task = asyncio.create_task(_bridge_stop())

        async def loop_worker(client: Client, loop_idx: int) -> None:
            session = client.create_session(Behavior.DEFAULT)
            dataset = DataSet.of(cfg.namespace, cfg.set_name)
            fields = list(cfg.bin_fields)

            async def worker(worker_id: int) -> None:
                seed = (cfg.seed + loop_idx * cfg.async_tasks + worker_id + 1) % (2**32)
                rng = random.Random(seed)
                decision = [False]
                has_limit = cfg.max_ops is not None
                sample_every = cfg.lat_sample_every
                with_tel = cfg.with_telemetry
                op_func = _build_op_async(
                    session, cfg, dataset, fields, bench_state, decision,
                )
                ws = stats.register_worker()
                local_count = 0
                while not thread_stop.is_set():
                    if has_limit and stats.total_ops() >= cfg.max_ops:
                        return
                    sample = with_tel and (local_count % sample_every == 0)
                    t0 = time.perf_counter() if sample else 0.0
                    decision[0] = False
                    try:
                        ret = await op_func(rng)
                    except BaseException as exc:
                        dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                        if _is_not_found(exc):
                            ws.record(decision[0], False, False, dt)
                        else:
                            to, er = _classify_exc(exc)
                            ws.record(decision[0], to, er, dt)
                            if not isinstance(exc, Exception):
                                raise
                    else:
                        dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                        if ret is None:
                            ws.record(decision[0], False, False, dt)
                        else:
                            ws.bulk_record(decision[0], ret[0], ret[1], dt)
                    local_count += 1

            tasks = [
                asyncio.create_task(worker(i))
                for i in range(cfg.async_tasks)
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        try:
            await pool.map(loop_worker, list(range(n_loops)))
        finally:
            thread_stop.set()
            bridge_task.cancel()
            try:
                await bridge_task
            except asyncio.CancelledError:
                pass


def run_sync(
    cfg: WorkloadConfig,
    stats: StatsCollector,
    stop: threading.Event,
    connected: threading.Event | None = None,
) -> None:
    if cfg.workload in (
        WorkloadKind.READ_MODIFY_INCREMENT,
        WorkloadKind.READ_MODIFY_DECREMENT,
    ):
        first_integer_bin(cfg.bin_fields)

    bench_state = _BenchState()
    policy = client_policy_from_config(cfg)
    dataset = DataSet.of(cfg.namespace, cfg.set_name)

    # One shared SyncClient + session across all worker threads. Per-thread
    # clients would each spin up their own connection pool and Tokio
    # runtime state, multiplying overhead for no gain.
    with SyncClient(cfg.seeds, policy=policy) as shared_client:
        if connected is not None:
            connected.set()
        shared_session = shared_client.create_session(Behavior.DEFAULT)

        def thread_main(worker_id: int) -> None:
            seed = (cfg.seed + worker_id + 1) % (2**32)
            rng = random.Random(seed)
            fields = list(cfg.bin_fields)
            decision = [False]
            has_limit = cfg.max_ops is not None
            sample_every = cfg.lat_sample_every
            with_tel = cfg.with_telemetry
            op_func = _build_op_sync(
                shared_session, cfg, dataset, fields, bench_state, decision,
            )
            ws = stats.register_worker()
            local_count = 0
            while not stop.is_set():
                if has_limit and stats.total_ops() >= cfg.max_ops:
                    return
                sample = with_tel and (local_count % sample_every == 0)
                t0 = time.perf_counter() if sample else 0.0
                decision[0] = False
                try:
                    ret = op_func(rng)
                except BaseException as exc:
                    dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                    if _is_not_found(exc):
                        ws.record(decision[0], False, False, dt)
                    else:
                        to, er = _classify_exc(exc)
                        ws.record(decision[0], to, er, dt)
                        if not isinstance(exc, Exception):
                            raise
                else:
                    dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                    if ret is None:
                        ws.record(decision[0], False, False, dt)
                    else:
                        ws.bulk_record(decision[0], ret[0], ret[1], dt)
                local_count += 1

        with ThreadPoolExecutor(max_workers=cfg.threads) as pool:
            futures = [pool.submit(thread_main, i) for i in range(cfg.threads)]
            while not stop.is_set():
                time.sleep(0.05)
            wait(futures)
            for f in futures:
                f.result()


def run_pac_blocking(
    cfg: WorkloadConfig,
    stats: StatsCollector,
    stop: threading.Event,
    connected: threading.Event | None = None,
) -> None:
    """Worker for ``--mode pac-blocking`` — direct PAC sync.

    Each OS thread shares one PAC client built via ``new_client_blocking``
    and calls ``_blocking`` entries directly — no PSDK session involved.
    Single-bin specs use a literal ``{b0: kid}`` write payload.
    """
    if cfg.workload not in (WorkloadKind.READ_UPDATE, WorkloadKind.INSERT):
        raise NotImplementedError(
            f"pac-blocking mode currently supports only RU/I workloads "
            f"(got {cfg.workload.name})."
        )
    if cfg.batch_size > 1:
        raise NotImplementedError(
            "pac-blocking mode does not yet support --batch-size > 1."
        )

    policy = client_policy_from_config(cfg)
    seeds = cfg.seeds
    write_policy = WritePolicy()
    read_policy = ReadPolicy()
    bench_state = _BenchState()
    dataset = DataSet.of(cfg.namespace, cfg.set_name)
    fields_t = tuple(cfg.bin_fields)
    single_bin = len(fields_t) == 1
    b0_name = fields_t[0].name

    shared_client = new_client_blocking(policy, seeds)
    if connected is not None:
        connected.set()

    def thread_main(worker_id: int) -> None:
        seed = (cfg.seed + worker_id + 1) % (2**32)
        rng = random.Random(seed)
        has_limit = cfg.max_ops is not None
        sample_every = cfg.lat_sample_every
        with_tel = cfg.with_telemetry
        ws = stats.register_worker()
        local_count = 0
        while not stop.is_set():
            if has_limit and stats.total_ops() >= cfg.max_ops:
                return

            if cfg.workload == WorkloadKind.INSERT:
                is_read = False
                kid = bench_state.next_insert_key()
                key = dataset.id(kid)
                payload = {b0_name: kid} if single_bin else full_bins(fields_t)
                verb = "put"
            else:  # READ_UPDATE
                keys = _make_keys(dataset, cfg.key_count, rng, 1)
                assert isinstance(keys, Key)
                key = keys
                is_read = rng.randint(1, 100) > (100 - cfg.read_percent)
                if is_read:
                    verb = "get"
                    payload = None
                else:
                    verb = "put"
                    if single_bin:
                        payload = {b0_name: rng.randint(1, cfg.key_count)}
                    elif rng.randint(1, 100) <= cfg.write_all_bins_percent:
                        payload = full_bins(fields_t)
                    else:
                        payload = single_bin_put(
                            fields_t, pick_bin_index(rng, len(fields_t))
                        )

            sample = with_tel and (local_count % sample_every == 0)
            t0 = time.perf_counter() if sample else 0.0
            try:
                if verb == "get":
                    shared_client.get_blocking(key, policy=read_policy)
                else:
                    shared_client.put_blocking(key, payload, policy=write_policy)
            except BaseException as exc:
                dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                if _is_not_found(exc):
                    ws.record(is_read, False, False, dt)
                else:
                    to, er = _classify_exc(exc)
                    ws.record(is_read, to, er, dt)
                    if not isinstance(exc, Exception):
                        raise
            else:
                dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                ws.record(is_read, False, False, dt)
            local_count += 1

    try:
        with ThreadPoolExecutor(max_workers=cfg.threads) as pool:
            futures = [pool.submit(thread_main, i) for i in range(cfg.threads)]
            while not stop.is_set():
                time.sleep(0.05)
            wait(futures)
            for f in futures:
                f.result()
    finally:
        try:
            shared_client.close_blocking()
        except Exception:
            pass


async def run_pac_async(
    cfg: WorkloadConfig,
    stats: StatsCollector,
    stop: asyncio.Event,
    connected: asyncio.Event | None = None,
) -> None:
    """Worker for ``--mode pac-async`` — direct PAC async client.

    One shared ``aerospike_async`` client and N concurrent asyncio tasks.
    No PSDK session, no builder; calls ``client.get(k, policy=rp)`` /
    ``client.put(k, bins, policy=wp)`` directly. Single-bin specs use a
    literal ``{b0: kid}`` write payload.
    """
    if cfg.workload not in (WorkloadKind.READ_UPDATE, WorkloadKind.INSERT):
        raise NotImplementedError(
            f"pac-async mode currently supports only RU/I workloads "
            f"(got {cfg.workload.name})."
        )
    if cfg.batch_size > 1:
        raise NotImplementedError(
            "pac-async mode does not support --batch-size > 1."
        )

    policy = client_policy_from_config(cfg)
    read_policy = ReadPolicy()
    write_policy = WritePolicy()
    bench_state = _BenchState()
    fields_t = tuple(cfg.bin_fields)
    single_bin = len(fields_t) == 1
    b0_name = fields_t[0].name
    ns = cfg.namespace
    set_name = cfg.set_name
    kc = cfg.key_count
    read_pct = cfg.read_percent
    sample_every = cfg.lat_sample_every
    with_tel = cfg.with_telemetry

    client = await new_client(policy, cfg.seeds)
    try:
        if connected is not None:
            connected.set()

        async def worker(worker_id: int) -> None:
            seed = (cfg.seed + worker_id + 1) % (2**32)
            rng = random.Random(seed)
            ws = stats.register_worker()
            has_limit = cfg.max_ops is not None
            local_count = 0
            while not stop.is_set():
                if has_limit and stats.total_ops() >= cfg.max_ops:
                    return

                if cfg.workload == WorkloadKind.INSERT:
                    is_read = False
                    kid = bench_state.next_insert_key()
                    k = Key(ns, set_name, str(kid))
                    payload = {b0_name: kid} if single_bin else full_bins(fields_t)
                    verb = "put"
                else:  # READ_UPDATE
                    kid = rng.randint(1, kc)
                    k = Key(ns, set_name, str(kid))
                    is_read = rng.randint(1, 100) > (100 - read_pct)
                    if is_read:
                        verb = "get"
                        payload = None
                    else:
                        verb = "put"
                        payload = (
                            {b0_name: kid}
                            if single_bin
                            else full_bins(fields_t)
                        )

                sample = with_tel and (local_count % sample_every == 0)
                t0 = time.perf_counter() if sample else 0.0
                try:
                    if verb == "get":
                        await client.get(k, policy=read_policy)
                    else:
                        await client.put(k, payload, policy=write_policy)
                except BaseException as exc:
                    dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                    if _is_not_found(exc):
                        ws.record(is_read, False, False, dt)
                    else:
                        to, er = _classify_exc(exc)
                        ws.record(is_read, to, er, dt)
                        if not isinstance(exc, Exception):
                            raise
                else:
                    dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                    ws.record(is_read, False, False, dt)
                local_count += 1

        tasks = [asyncio.create_task(worker(i)) for i in range(cfg.async_tasks)]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        try:
            await client.close()
        except Exception:
            pass


def run_legacy_sync(
    cfg: WorkloadConfig,
    stats: StatsCollector,
    stop: threading.Event,
    connected: threading.Event | None = None,
) -> None:
    """Worker for ``--mode legacy-sync`` — the legacy C ``aerospike`` client.

    Shared ``aerospike.client(...).connect()`` instance across worker
    threads; each thread runs a tight get/put loop on tuple keys
    ``(ns, set, str(kid))``. Single-bin specs use a literal ``{b0: kid}``
    write payload. ``RecordNotFound`` on reads is treated as a successful
    empty result (not an error).
    """
    if cfg.workload not in (WorkloadKind.READ_UPDATE, WorkloadKind.INSERT):
        raise NotImplementedError(
            f"legacy-sync mode supports only RU/I workloads "
            f"(got {cfg.workload.name})."
        )
    if cfg.batch_size > 1:
        raise NotImplementedError(
            "legacy-sync mode does not support --batch-size > 1."
        )

    import os as _os

    try:
        import aerospike
    except ImportError as e:
        raise RuntimeError(
            "legacy `aerospike` client not installed in this environment. "
            "`pip install aerospike` first."
        ) from e

    hosts: list[tuple[str, int]] = []
    for seed in cfg.seeds.split(","):
        seed = seed.strip()
        if ":" in seed:
            h, p = seed.rsplit(":", 1)
            hosts.append((h, int(p)))
        else:
            hosts.append((seed, 3000))

    use_services_alt = (
        _os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").lower() == "true"
        or getattr(cfg, "services_alternate", False)
    )
    config: dict = {"hosts": hosts}
    if use_services_alt:
        config["use_services_alternate"] = True

    bench_state = _BenchState()
    fields_t = tuple(cfg.bin_fields)
    single_bin = len(fields_t) == 1
    b0_name = fields_t[0].name
    ns = cfg.namespace
    set_name = cfg.set_name
    kc = cfg.key_count
    read_pct = cfg.read_percent

    shared_client = aerospike.client(config).connect()
    try:
        record_not_found = getattr(
            getattr(aerospike, "exception", None), "RecordNotFound", None,
        )
    except Exception:
        record_not_found = None

    if connected is not None:
        connected.set()

    def thread_main(worker_id: int) -> None:
        seed = (cfg.seed + worker_id + 1) % (2**32)
        rng = random.Random(seed)
        has_limit = cfg.max_ops is not None
        sample_every = cfg.lat_sample_every
        with_tel = cfg.with_telemetry
        ws = stats.register_worker()
        local_count = 0
        while not stop.is_set():
            if has_limit and stats.total_ops() >= cfg.max_ops:
                return

            if cfg.workload == WorkloadKind.INSERT:
                is_read = False
                kid = bench_state.next_insert_key()
                key = (ns, set_name, str(kid))
                payload = {b0_name: kid} if single_bin else full_bins(fields_t)
                verb = "put"
            else:  # READ_UPDATE
                kid = rng.randint(1, kc)
                key = (ns, set_name, str(kid))
                is_read = rng.randint(1, 100) > (100 - read_pct)
                if is_read:
                    verb = "get"
                    payload = None
                else:
                    verb = "put"
                    payload = (
                        {b0_name: kid}
                        if single_bin
                        else full_bins(fields_t)
                    )

            sample = with_tel and (local_count % sample_every == 0)
            t0 = time.perf_counter() if sample else 0.0
            try:
                if verb == "get":
                    shared_client.get(key)
                else:
                    shared_client.put(key, payload)
            except BaseException as exc:
                if record_not_found is not None and isinstance(exc, record_not_found):
                    dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                    ws.record(is_read, False, False, dt)
                else:
                    dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                    to, er = _classify_exc(exc)
                    ws.record(is_read, to, er, dt)
                    if not isinstance(exc, Exception):
                        raise
            else:
                dt = (time.perf_counter() - t0) * 1000.0 if sample else None
                ws.record(is_read, False, False, dt)
            local_count += 1

    try:
        with ThreadPoolExecutor(max_workers=cfg.threads) as pool:
            futures = [pool.submit(thread_main, i) for i in range(cfg.threads)]
            while not stop.is_set():
                time.sleep(0.05)
            wait(futures)
            for f in futures:
                f.result()
    finally:
        try:
            shared_client.close()
        except Exception:
            pass
