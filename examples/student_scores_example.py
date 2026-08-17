#!/usr/bin/env python3
"""Querying map-valued bins with an AEL path predicate (server 8.1.3+).

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

SUBJECTS = ("math", "english", "science", "history", "art")


def generate_scores(rng: random.Random) -> dict[str, int]:
    """A grade in 55..100 for each subject."""
    return {subject: 55 + rng.randrange(46) for subject in SUBJECTS}


async def main() -> None:
    async with await _env.connect().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        class10a = DataSet.of("test", "class10a")

        try:
            if not await _env.server_at_least(session, (8, 1, 3)):
                print("Skipped: AEL path queries require Aerospike 8.1.3+.")
                return

            await session.truncate(class10a)

            # Write 30 student records with reproducible random scores.
            rng = random.Random(42)
            for i in range(1, 31):
                await (
                    session.upsert(class10a.id(f"student-{i}"))
                    .bin("name").set_to(f"Student {i}")
                    .bin("scores").set_to(generate_scores(rng))
                    .execute()
                )

            # One server-side scan: keep students with any score >= 90.
            # $.scores.{=90:} selects map values >= 90; .count() > 0 is the filter.
            stream = await (
                session.query(class10a)
                .where("$.scores.{=90:}.count() > 0")
                .execute()
            )
            async for result in stream:
                record = result.record_or_raise()
                print(f"{record.bins['name']}: {record.bins['scores']}")

        finally:
            await session.truncate(class10a)


if __name__ == "__main__":
    asyncio.run(main())
