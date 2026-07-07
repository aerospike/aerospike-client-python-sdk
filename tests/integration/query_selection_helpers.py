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

"""Shared constants and helpers for query-selection integration tests."""

from __future__ import annotations

NS = "test"
SET_NAME = "qselint"
INDEX_NAME = "qsel_age_idx"
SCORE_INDEX_NAME = "qsel_score_idx"
BIN_AGE = "age"
BIN_SCORE = "score"
BIN_COUNTRY = "country"
KEY_PREFIX = "qselkey"
SIZE = 50


def key_name(i: int) -> str:
    return f"{KEY_PREFIX}{i}"


async def collect_ages_async(stream) -> list[int]:
    ages: list[int] = []
    try:
        async for result in stream:
            rec = result.record_or_raise()
            ages.append(rec.bins[BIN_AGE])
    finally:
        stream.close()
    return sorted(ages)


def collect_ages_sync(stream) -> list[int]:
    ages: list[int] = []
    try:
        for result in stream:
            rec = result.record_or_raise()
            ages.append(rec.bins[BIN_AGE])
    finally:
        stream.close()
    return sorted(ages)


async def count_records_async(stream) -> int:
    count = 0
    try:
        async for result in stream:
            result.record_or_raise()
            count += 1
    finally:
        stream.close()
    return count


def count_records_sync(stream) -> int:
    count = 0
    try:
        for result in stream:
            result.record_or_raise()
            count += 1
    finally:
        stream.close()
    return count
