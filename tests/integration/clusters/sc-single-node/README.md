# Single-node strong-consistency cluster

Starts one Aerospike node on `127.0.0.1:3130` with `test_sc` at
`replication-factor 1, strong-consistency true`, so a single node owns all 4096
partitions.

```bash
tests/integration/clusters/sc-single-node/create-sc-single
```

Then `tests/integration/async/txn_commit_status_test.py` runs. Without it those
tests skip: the `aerospike_host_sc_single` fixture probes the seed and skips
cleanly when nothing answers.

## Why a second SC cluster

The gated commit tests route the client through a TCP proxy
(`tests/integration/tcp_gate.py`) so a chosen commit command can be made to
fail. For the proxy to be the client's only route, the client is restricted with
`force_single_node()` — and that restricts the *node set* without rewriting the
partition map. The map still names the real owners, so on the
replication-factor-2 SC cluster most partitions are unroutable and the tests
fail for that reason rather than the one under test. One node owning every
partition removes the inconsistency.

The Java and C clients rewrite the map to point every partition at the seed, so
their equivalent test runs against whatever cluster the suite is already using.
If that capability ever lands below us, this directory and the
`aerospike_host_sc_single` fixture both become unnecessary.

## Feature key

Not in this repo — it is licensed, and `.gitignore` excludes it so it cannot be
committed by accident. The script fetches it into this directory on first run:

```bash
gh api repos/citrusleaf/aerospike-server-enterprise/contents/etc/features.conf \
  -H "Accept: application/vnd.github.raw"
```

That repo is private, so this relies on `gh auth status` being good. Overrides:
`FEATURES=/path/to/features.conf` to use one you already have,
`FEATURES_REPO` / `FEATURES_PATH` to fetch from elsewhere, `IMAGE=...` for a
different server build.

## Security is off deliberately

The gate counts the client's data messages to decide which commit command to
fail. An auth handshake would spend allowances the tests are counting, so this
node has no `security` stanza — unlike the three-node SC cluster.
