# Test suites

`tests/` is the authoritative source for SDK behavior. At this stage of the
project the tests track the API more closely than the prose does, so when a
guide and a test disagree, trust the test and report the guide.

- `tests/unit/` — no server required. Authoritative for API shape, builder
  semantics, policy resolution, and validation behavior.
- `tests/integration/` — requires a running Aerospike server (environment
  comes from `aerospike.env`, falling back to `aerospike.env.example`).
  Authoritative for server-observable behavior. `async/` and `sync/` are
  parallel suites: the two runtimes are independent implementations, so
  behavior is exercised on both surfaces.

Run `make test-unit`, `make test-int`, or `make test` for everything.
`make coverage` reports unit-only line coverage (the figure CI enforces);
`make coverage-all` adds the integration suite (the figure the coverage
target is held against). On macOS raise the file-descriptor limit first:
`ulimit -n 4096`.
