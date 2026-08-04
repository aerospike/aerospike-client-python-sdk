#!/usr/bin/env python3
"""Inspecting a strong-consistency namespace roster via info commands.

Strong-consistency (SC) namespaces track a *roster* — the authoritative set of
nodes that own data. This reads the current, pending, and observed rosters for
an SC namespace. Requires an Aerospike Enterprise cluster running an SC
namespace (``AEROSPIKE_HOST_SC`` + ``AEROSPIKE_SC_NAMESPACE``).

Initializing/changing a roster (``roster-set`` + ``recluster``) is a deliberate
cluster-admin action and is intentionally *not* performed here — this example
only reads.
"""

import asyncio

import _env
from aerospike_sdk import Behavior
from aerospike_sdk.aio.info import InfoCommands


def _parse_roster(raw: str) -> dict[str, str]:
    """`roster=A,B:pending_roster=C:observed_nodes=D` -> {field: value}."""
    fields = {}
    for part in raw.split(":"):
        if "=" in part:
            name, value = part.split("=", 1)
            fields[name] = value
    return fields


async def main() -> None:
    async with await _env.connect_sc().connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        info = InfoCommands(session)
        ns = _env.sc_namespace()

        details = await info.namespace_details(ns)
        if details is None:
            print(f"Skipped: namespace {ns!r} not found "
                  "(set AEROSPIKE_HOST_SC / AEROSPIKE_SC_NAMESPACE to an SC cluster).")
            return

        print(f"Namespace: {ns}")
        response = await info.info(f"roster:namespace={ns}")
        raw = next(iter(response.values()))
        roster = _parse_roster(raw)

        current = roster.get("roster", "null")
        if current in ("", "null"):
            print("  Roster is empty — this namespace has not been rostered yet.")
            print("  To initialize: send `roster-set:namespace=<ns>;nodes=<node-ids>`")
            print("  then `recluster:` (a cluster-admin action, not shown here).")
            return

        def show(label: str, field: str) -> None:
            value = roster.get(field, "null")
            nodes = value.split(",") if value not in ("", "null") else []
            print(f"  {label:16s} {len(nodes)} node(s): {', '.join(nodes) or '(none)'}")

        show("roster", "roster")
        show("pending_roster", "pending_roster")
        show("observed_nodes", "observed_nodes")

        if roster.get("roster") == roster.get("observed_nodes"):
            print("  Roster matches observed nodes — cluster is fully rostered.")



if __name__ == "__main__":
    asyncio.run(main())
