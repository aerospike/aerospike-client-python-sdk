#!/usr/bin/env python3
"""Roster and recluster test_sc on the single-node SC cluster.

Simpler than the three-node sibling: security is off here, so no role grants
are needed, and there is exactly one node, so it is always the principal and
the roster is just its own id.

An SC namespace serves nothing until its roster is set -- without this the
node answers every read and write with "partition unavailable". Safe to rerun
against a restarted container: dead partitions are revived.
"""
import asyncio
import sys

from aerospike_native import ClientPolicy, new_client

SEED = "127.0.0.1:3130"
NAMESPACE = "test_sc"


def _fields(body: str, sep: str) -> dict:
    return dict(kv.split("=", 1) for kv in body.split(sep) if "=" in kv)


async def _namespace_stats(client) -> dict:
    # A recluster rebalances asynchronously; stats read straight after it can
    # still describe the previous partition state.
    await asyncio.sleep(2)
    return _fields(next(iter((await client.info(
        f"namespace/{NAMESPACE}")).values())), ";")


async def main() -> int:
    client = await new_client(ClientPolicy(), SEED)
    try:
        observed = await client.info(f"roster:namespace={NAMESPACE}")
        body = next(iter(observed.values()))
        nodes = _fields(body, ":").get("observed_nodes", "")
        if not nodes:
            print(f"error: no observed nodes for {NAMESPACE}", file=sys.stderr)
            return 1

        await client.info(f"roster-set:namespace={NAMESPACE};nodes={nodes}")
        await client.info("recluster:")

        # A restarted container keeps its roster but not its in-memory data,
        # so the node cold-starts with every partition dead and refuses all
        # writes while reporting zero unavailable partitions.
        ns = await _namespace_stats(client)
        if ns.get("dead_partitions", "0") != "0":
            await client.info(f"revive:namespace={NAMESPACE}")
            await client.info("recluster:")
            ns = await _namespace_stats(client)

        after = _fields(next(iter((await client.info(
            f"roster:namespace={NAMESPACE}")).values())), ":")
        print(f"  roster:   {after.get('roster', '?')}")
        print(f"  pending:  {after.get('pending_roster', '?')}")
        print(f"  unavailable_partitions: {ns.get('unavailable_partitions', '?')}")
        print(f"  dead_partitions:        {ns.get('dead_partitions', '?')}")
        print(f"  strong-consistency:     {ns.get('strong-consistency', '?')}")
        if ns.get("dead_partitions", "0") != "0":
            print(f"error: {NAMESPACE} still has dead partitions", file=sys.stderr)
            return 1
        return 0
    finally:
        await client.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
