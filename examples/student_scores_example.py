#!/usr/bin/env python3
"""Querying map-valued bins with an AEL path predicate (server 8.2.0+).

Each student record holds a ``scores`` map of subject -> grade. A single
dataset query with an AEL path predicate filters and aggregates inside the map
*on the server* — the honor-roll query below scans the set and returns only the
students with at least one score of 90 or above, without reading every record
back to the client.
"""

import asyncio
import random

import _env
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.aio.operations.query import QueryHint

SUBJECTS = ("math", "english", "science", "history", "art")

CLASS_10A = DataSet.of("test", "class10a")


def generate_scores(rng: random.Random) -> dict[str, int]:
    """A grade in 55..100 for each subject."""
    return {subject: 55 + rng.randrange(46) for subject in SUBJECTS}


async def run_examples(session) -> None:
    try:
        if not await _env.server_at_least(session, (8, 2, 0)):
            print("Skipped: AEL path queries require Aerospike 8.2.0+.")
            return

        await session.truncate(CLASS_10A)

        # --- Write 30 student records ---
        # Scores are seeded from a fixed RNG so runs are reproducible.
        rng = random.Random(42)
        for i in range(1, 31):
            await (
                session.upsert(CLASS_10A.id(f"student-{i}"))
                .bin("name").set_to(f"Student {i}")
                .bin("scores").set_to(generate_scores(rng))
                .execute()
            )

        # --- Query: students with any score >= 90 ---
        # One server-side pass. $.scores.{=90:} selects map values >= 90;
        # .count() > 0 is the filter.
        #
        # A filtered set-wide query normally requires a secondary index, and
        # is rejected otherwise so a full scan can never be entered by
        # accident. No index can serve a count over a map's values, so this
        # query opts into the scan deliberately.
        async with await (
            session.query(CLASS_10A)
            .where("$.scores.{=90:}.count() > 0")
            .with_hint(QueryHint(allow_scans_with_where=True))
            .execute()
        ) as stream:
            async for result in stream:
                record = result.record_or_raise()
                print(f"{record.bins['name']}: {record.bins['scores']}")

    finally:
        await session.truncate(CLASS_10A)


async def main() -> None:
    async with _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)

        await run_examples(session)


if __name__ == "__main__":
    asyncio.run(main())
