#!/usr/bin/env python3
"""Read the observed roster, set it, and recluster.

Defaults match the local single-node rig: ``127.0.0.1:3130``, namespace
``test_sc``, no credentials. ``AEROSPIKE_HOST``, ``AEROSPIKE_SC_NAMESPACE``,
and ``AEROSPIKE_AUTH_*`` override those. This prepares the cluster for the
gated commit tests; it is not a test itself.

An SC namespace serves nothing until its roster is set -- without this the
node answers every read and write with "partition unavailable".
"""
import asyncio
import os
import sys

from aerospike_async import ClientPolicy, new_client

SEED = os.environ.get("AEROSPIKE_HOST", "127.0.0.1:3130").strip() or "127.0.0.1:3130"
NAMESPACE = os.environ.get("AEROSPIKE_SC_NAMESPACE", "test_sc").strip() or "test_sc"


def _fields(body: str, sep: str) -> dict:
    return dict(kv.split("=", 1) for kv in body.split(sep) if "=" in kv)


def _policy() -> ClientPolicy:
    policy = ClientPolicy()
    alternate = os.environ.get("AEROSPIKE_USE_SERVICES_ALTERNATE", "").strip().lower()
    policy.use_services_alternate = alternate in ("true", "1", "yes")
    user = os.environ.get("AEROSPIKE_AUTH_USER", "").strip()
    if user:
        policy.user = user
        policy.password = os.environ.get("AEROSPIKE_AUTH_PASSWORD", "")
    return policy


async def main() -> int:
    client = await new_client(_policy(), SEED)
    try:
        observed = await client.info(f"roster:namespace={NAMESPACE}")
        body = next(iter(observed.values()))
        nodes = _fields(body, ":").get("observed_nodes", "")
        if not nodes:
            print(f"error: no observed nodes for {NAMESPACE}", file=sys.stderr)
            return 1

        await client.info(f"roster-set:namespace={NAMESPACE};nodes={nodes}")
        await client.info("recluster:")

        after = _fields(next(iter((await client.info(
            f"roster:namespace={NAMESPACE}")).values())), ":")
        print(f"  roster:   {after.get('roster', '?')}")
        print(f"  pending:  {after.get('pending_roster', '?')}")
        ns = _fields(next(iter((await client.info(
            f"namespace/{NAMESPACE}")).values())), ";")
        print(f"  unavailable_partitions: {ns.get('unavailable_partitions', '?')}")
        print(f"  strong-consistency:     {ns.get('strong-consistency', '?')}")
        return 0
    finally:
        await client.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
