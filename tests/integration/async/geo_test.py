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

"""GeoJSON round-trip integration tests.

Seeds 15 AeroCircle (3 km) regions clustered around the San Francisco
Peninsula, builds a GEO2DSPHERE index on the regions set, and asserts that
a spatial filter for a query point in the cluster center returns the 5
regions whose radii contain that point.
"""

import asyncio

import pytest

from aerospike_sdk import Client, Exp
from aerospike_sdk.dataset import DataSet


REGION_SET = "georeg_psdk"
INDEX_NAME = "geoidx_psdk"
BIN_NAME = "loc"
NAMESPACE = "test"

# 3 km AeroCircles clustered around the San Francisco Peninsula.
STARBUCKS = [
    (-122.1708441, 37.4241193),
    (-122.1492040, 37.4273569),
    (-122.1441078, 37.4268202),
    (-122.1251714, 37.4130590),
    (-122.0964289, 37.4218102),
    (-122.0776641, 37.4158199),
    (-122.0943475, 37.4114654),
    (-122.1122861, 37.4028493),
    (-122.0947230, 37.3909250),
    (-122.0831037, 37.3876090),
    (-122.0707119, 37.3787855),
    (-122.0303178, 37.3882739),
    (-122.0464861, 37.3786236),
    (-122.0582128, 37.3726980),
    (-122.0365083, 37.3676930),
]

QUERY_POINT = '{"type":"Point","coordinates":[-122.0986857,37.4214209]}'


def _aero_circle(lng: float, lat: float, radius_m: float = 3000.0) -> str:
    return f'{{"type":"AeroCircle","coordinates":[[{lng},{lat}],{radius_m}]}}'


@pytest.fixture
async def geo_seeded_client(aerospike_host, client_policy, enterprise):
    """Set up a GEO2DSPHERE index plus 15 AeroCircle regions. Tear down on exit."""
    async with Client(seeds=aerospike_host, policy=client_policy) as client:
        session = client.create_session()
        regions = DataSet.of(NAMESPACE, REGION_SET)

        # Clean any leftover data from a previous run.
        for i in range(len(STARBUCKS)):
            try:
                await session.delete(regions.id(i)).execute()
            except Exception:
                pass
        try:
            await client.index(NAMESPACE, REGION_SET).named(INDEX_NAME).drop()
        except Exception:
            pass

        # Create the GEO2DSPHERE index.
        try:
            await (
                client.index(NAMESPACE, REGION_SET)
                .named(INDEX_NAME)
                .on_bin(BIN_NAME)
                .geo2dsphere()
                .create()
            )
        except Exception:
            pass  # index may already exist if a prior run failed mid-teardown

        # Insert AeroCircle regions via the new set_to_geo_json builder method.
        for i, (lng, lat) in enumerate(STARBUCKS):
            await (
                session.upsert(regions.id(i))
                .bin(BIN_NAME).set_to_geo_json(_aero_circle(lng, lat))
                .execute()
            )

        # Give the secondary index a moment to populate on community edition.
        await asyncio.sleep(0.5 if not enterprise else 0.05)

        yield client

        for i in range(len(STARBUCKS)):
            try:
                await session.delete(regions.id(i)).execute()
            except Exception:
                pass
        try:
            await client.index(NAMESPACE, REGION_SET).named(INDEX_NAME).drop()
        except Exception:
            pass


class TestGeoQuery:
    """``geoCompare(...)`` over a GEO2DSPHERE index returns the expected hits."""

    async def test_ael_geo_compare_returns_5_intersecting_regions(self, geo_seeded_client):
        """AEL ``geoCompare($.loc, geoJson('...'))`` matches 5 of the 15 regions."""
        stream = await (
            geo_seeded_client.query(NAMESPACE, REGION_SET)
            .where(f"geoCompare($.{BIN_NAME}, geoJson('{QUERY_POINT}'))")
            .execute()
        )
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        assert count == 5

    async def test_ael_with_explicit_get_type_geo(self, geo_seeded_client):
        """Same query expressed with explicit ``.get(type: GEO)`` cast on the bin."""
        stream = await (
            geo_seeded_client.query(NAMESPACE, REGION_SET)
            .where(f"geoCompare($.{BIN_NAME}.get(type: GEO), geoJson('{QUERY_POINT}'))")
            .execute()
        )
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        assert count == 5

    async def test_programmatic_exp_geo_compare_returns_5(self, geo_seeded_client):
        """Programmatic ``Exp.geo_compare(...)`` via ``.where(FilterExpression)``.

        Bypasses the AEL parser so the underlying FilterExpression path is
        exercised end-to-end against a live cluster. Equivalent in effect to
        the AEL form above, but proves both surfaces independently.
        """
        filter_exp = Exp.geo_compare(
            Exp.geo_bin(BIN_NAME),
            Exp.geo_val(QUERY_POINT),
        )
        stream = await (
            geo_seeded_client.query(NAMESPACE, REGION_SET)
            .where(filter_exp)
            .execute()
        )
        count = 0
        async for _ in stream:
            count += 1
        stream.close()
        assert count == 5
