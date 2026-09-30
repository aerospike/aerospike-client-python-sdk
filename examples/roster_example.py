#!/usr/bin/env python3
"""Reading and initializing a strong-consistency namespace roster.

Strong-consistency (SC) namespaces track a *roster* — the authoritative set of
nodes that own data. This reads the current, pending, and observed rosters for
an SC namespace, then sets the roster to the observed nodes on every node and
triggers a recluster so it takes effect. Requires an Aerospike Enterprise
cluster running an SC namespace (``AEROSPIKE_HOST_SC`` +
``AEROSPIKE_SC_NAMESPACE``).

Setting the roster is a cluster-admin action: it replaces whatever roster was
there with the nodes observed right now, so a node that is down at the time is
left out.
"""

import asyncio

import _env


async def run_examples(cluster) -> None:
    ns = _env.sc_namespace()
    nodes = cluster.nodes()

    # --- 1) Read the current roster ---
    print(f"Namespace: {ns}")
    command = f"roster:namespace={ns}"
    raw = (await nodes[0].info(command))[command]
    # An info failure answers in the ERROR:<code>:<message> form.
    if raw.startswith("ERROR") or "observed_nodes=" not in raw:
        print(f"Skipped: roster info unavailable for {ns!r} "
              f"(is it a strong-consistency namespace?): {raw}")
        return

    # --- 2) Parse and report the roster fields ---
    roster = _parse_roster(raw)

    def show(label: str, field: str) -> None:
        value = roster.get(field, "null")
        members = value.split(",") if value not in ("", "null") else []
        print(f"  {label:16s} {len(members)} node(s): {', '.join(members) or '(none)'}")

    show("roster", "roster")
    show("pending_roster", "pending_roster")
    show("observed_nodes", "observed_nodes")

    # --- 3) Set the roster to the observed nodes, on every node ---
    command = f"roster-set:namespace={ns};nodes={roster['observed_nodes']}"
    for node in nodes:
        response = (await node.info(command))[command]
        print(f"  roster-set on {node.address}: {response}")

    # --- 4) Recluster so the new roster takes effect ---
    # Only the principal node acts; the rest answer `ignored-by-non-principal`.
    for node in nodes:
        response = (await node.info("recluster:"))["recluster:"]
        print(f"  recluster on {node.address}: {response}")

    await asyncio.sleep(3)
    print("Roster initialization complete.")


def _parse_roster(raw: str) -> dict[str, str]:
    """`roster=A,B:pending_roster=C:observed_nodes=D` -> {field: value}."""
    fields = {}
    for part in raw.split(":"):
        if "=" in part:
            name, value = part.split("=", 1)
            fields[name] = value
    return fields


async def main() -> None:
    async with _env.connect_sc().connect() as cluster:
        await run_examples(cluster)


if __name__ == "__main__":
    asyncio.run(main())
