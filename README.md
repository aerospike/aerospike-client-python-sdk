# Aerospike Python SDK

Ultra-High-performance, developer-friendly interface for Aerospike. A dual first-class
sync/async, Pythonic API — both high-performance — with a chainable session model,
fluent query builder, and AEL string filters layered over the
[Aerospike Python Async Client](https://pypi.org/project/aerospike-async/)
(PAC) — with first-class free-threaded Python (`cp314t`) support for parallel-
thread throughput well past what GIL-bound clients can sustain.

> **Status:** Public preview (alpha). Not yet production-ready; feedback welcome
> via [GitHub Issues](https://github.com/aerospike/aerospike-client-python-sdk/issues).

## Resources

- **PyPI:** https://pypi.org/project/aerospike-sdk/
- **Documentation:** https://aerospike.com/docs/develop/client/sdk/
- **API Reference:** https://aerospike-python-sdk.readthedocs.io/
- **Source:** https://github.com/aerospike/aerospike-client-python-sdk
- **Issues:** https://github.com/aerospike/aerospike-client-python-sdk/issues

## Installation

```bash
pip install aerospike-sdk
```

Pin to a specific release if you need reproducible builds:

```bash
pip install aerospike-sdk==0.9.0a2
```

This installs the SDK plus its dependency on the Aerospike Python Async Client
(`aerospike-async`). No Rust toolchain or git checkout required for ordinary
use — pre-built wheels are available for Linux, macOS, and Windows on Python
3.10–3.14.

## Quick start

```python
import asyncio
from aerospike_sdk import Behavior, ClusterDefinition, DataSet


async def main():
    async with await ClusterDefinition("localhost", 3000).connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")

        # High-level key-value writes
        await session.upsert(users.id(1)).put({"name": "Alice", "age": 28, "country": "UK"}).execute()
        await session.upsert(users.id(2)).put({"name": "Bob", "age": 35, "country": "US"}).execute()

        # Filtered query with AEL — streams results memory-efficiently
        results = await (
            session.query(users)
            .where("$.age > 25 and $.country == 'US'")
            .execute()
        )
        async for row in results:
            if row.is_ok and row.record is not None:
                print(row.record.bins)

        # Or drain the entire stream into a list
        all_users = await session.query(users).execute()
        rows = await all_users.collect()


asyncio.run(main())
```

### Sync

The same surface is available without asyncio — no `async`/`await`, no event
loop — useful for sync codebases or when a dependency forbids asyncio. Connect
through the sync `ClusterDefinition`.

```python
from aerospike_sdk import Behavior, DataSet
from aerospike_sdk.sync import ClusterDefinition


def main():
    with ClusterDefinition("localhost", 3000).connect() as cluster:
        session = cluster.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")

        # High-level key-value writes
        session.upsert(users.id(1)).put({"name": "Alice", "age": 28, "country": "UK"}).execute()
        session.upsert(users.id(2)).put({"name": "Bob", "age": 35, "country": "US"}).execute()

        # Filtered query with AEL — same builder API as async
        results = (
            session.query(users)
            .where("$.age > 25 and $.country == 'US'")
            .execute()
        )
        for row in results:
            if row.is_ok and row.record is not None:
                print(row.record.bins)


main()
```

`SyncClient` is a thin façade over the PAC `_blocking` surface — there is no per-call
event loop and no per-thread loop runner. Sessions, behaviors, builders, and AEL
filters are identical to the async path.

See the [Quick Start guide](https://aerospike.com/docs/develop/client/sdk/) for
a deeper walkthrough; the [API reference](https://aerospike-python-sdk.readthedocs.io/)
covers every public class and method in detail.

## Performance modes

PSDK offers two API shapes — pick based on what your code needs.

| API | Use when | Trade-off |
|---|---|---|
| **Chained builder** (`session.query(k).execute()`, `session.upsert(k).put(...).execute()`) | You need filters (`where(...)`), batch ops, error handlers, secondary-index queries, TTL overrides, generation checks, etc. Same shape as the Aerospike Java SDK. | Builder + stream wrapping costs ~60 µs/op of Python overhead. |
| **Fast-path** (`session.get(key)`, `session.put(key, bins)`) | Single-key reads/writes where you want the lowest per-op overhead. | Single-key only; no filters, no error-handler callbacks, no batch semantics. Errors raise directly. |

Both shapes work in sync and async modes. Use whichever fits each call site — they share the same `Session` and `Behavior`.

### Free-threaded Python

For high-throughput multi-threaded workloads, run PSDK on the free-threaded build with the GIL disabled:

```bash
# uv example — install once, then always launch with PYTHON_GIL=0
uv python install 3.14.5+freethreaded
PYTHON_GIL=0 python my_app.py
```

Verify with `sys._is_gil_enabled() == False` after imports — if any non-FT-safe C extension is imported, the interpreter silently re-enables the GIL and your multi-threaded perf collapses 4-6×.

`AsyncPool` (multi-loop async) is a free-threading feature — **don't use it on regular Python**, it's slower than a single-client setup there. Each pool spawns N event loops on N OS threads, each with its own `Client`; coroutines submitted via `pool.run(...)` round-robin across loops:

```python
from aerospike_sdk import AsyncPool, Behavior, Client, DataSet


async def main():
    pool = AsyncPool(
        client_factory=lambda: Client("localhost:3000"),
        loop_count=4,
    )
    async with pool:
        users = DataSet.of("test", "users")

        # Submit a coroutine — picks an idle loop round-robin
        await pool.run(
            lambda client: client.create_session(Behavior.DEFAULT)
                                 .upsert(users.id(1))
                                 .put({"name": "Alice"})
                                 .execute()
        )
```

Tune `loop_count` based on your workload — `os.cpu_count()` is the default. Cluster-wide index metadata is shared via a single `IndexesMonitor` across the pool, so `sindex-list` load doesn't scale with `loop_count`.

For the full decision guide, the trade-offs, and measured TPS/latency across all modes, see [`docs/guide/performance.md`](docs/guide/performance.md). For the raw bench data and methodology, see [`docs/guide/benchmarking.md`](docs/guide/benchmarking.md).

## Documentation

- **Guides and tutorials** (user-facing, hand-curated):
  https://aerospike.com/docs/develop/client/sdk/
- **API reference** (auto-generated from docstrings, hosted on Read the Docs):
  https://aerospike-python-sdk.readthedocs.io/

The two complement each other: the guide site introduces concepts and works
through realistic examples, while the API reference is the exhaustive source
for `Client`, `Session`, query/update builders, AEL, behavior policies, and
every public symbol.

## Versioning

PSDK follows [SemVer](https://semver.org/). Pre-releases use the
`MAJOR.MINOR.PATCH-{alpha,beta,rc}.N` form (e.g. `0.9.0-alpha.1`). PyPI
normalizes these on upload to the equivalent PEP 440 spelling (`0.9.0a1`).

The top-level `VERSION` file is the single source of truth; `pyproject.toml`
reads it dynamically, so the wheel and the working tree are guaranteed to
match. See the [Development](#development--contributing) section below for the
bump procedure.

### Pre-release builds (Aerospike internal)

Every merge to `dev` publishes a wheel and an sdist to Aerospike's internal
package index, versioned as a dev release leading toward the next
pre-release — `0.9.0a6.dev123`, where `123` is the publishing workflow's run
number. This is for Aerospike test teams and internal consumers who need a
specific `dev` build; external users should use the public PyPI releases.

These builds are **not** on public PyPI, so installing one requires
credentials for the internal index. Generate an identity token in the JFrog UI
(avatar → *Edit Profile* → *Identity Tokens*); your username is your Aerospike
**email address**, and the `@` in it must be URL-encoded as `%40`:

```bash
export PIP_EXTRA_INDEX_URL="https://<you>%40aerospike.com:<identity-token>@artifact.aerospike.io/artifactory/api/pypi/database-pypi-dev-local/simple/"
```

Persist it in `~/.config/pip/pip.conf` under `[global] extra-index-url`, or put
the credentials in `~/.netrc` for `artifact.aerospike.io`, if you'd rather not
set it per shell.

With that in place, installing needs no repository checkout, no Java, and no
ANTLR generation step — the parser ships inside the package:

```bash
pip index versions aerospike-sdk --pre        # what's available
pip install "aerospike-sdk==0.9.0a6.dev123"   # a specific build
```

Pin the exact dev version rather than reaching for `--pre --upgrade`. If the
index is unconfigured or the token has expired, `--pre` quietly resolves the
newest *public* pre-release instead and looks like it worked; an exact dev
version fails loudly with "no matching distribution".

The same index also serves the pinned `aerospike-async` (PAC) pre-release, so
one credential setup resolves both. Adding `--only-binary aerospike-async` is
worth it on unusual platforms: it turns a missing PAC wheel into a clear
resolution error instead of a slow source build that needs a Rust toolchain.
Report bugs against the exact `aerospike_sdk.__version__` you installed.

### Benchmarking a dev build (Aerospike internal)

The benchmark tools live in `benchmarks/` in this repository and are not part of
the published package — they are development tooling, with their own shell
scripts and a Rust helper project. To benchmark a published dev build, install
the package from the index (per
[Pre-release builds](#pre-release-builds-aerospike-internal) above) and check
out *only* the tools:

```bash
git clone --depth 1 --branch dev --filter=blob:none --sparse \
  https://github.com/aerospike/aerospike-client-python-sdk.git psdk-bench
cd psdk-bench
git sparse-checkout set benchmarks           # directories only; root files come free

pip install "aerospike-sdk==0.9.0a6.dev123"

export AEROSPIKE_HOST=10.0.0.5:3000
export AEROSPIKE_USE_SERVICES_ALTERNATE=false
python -m benchmarks.benchmark -w RU,50 -k 100000 -z 32 -d 10
```

Nothing here needs Java, Rust, or `make generate-ael`: the tools are plain
scripts, and the generated parser arrives inside the installed package.

Connection settings come from the environment. `benchmarks/_env.py` loads
`aerospike.env` if you made one and otherwise the committed
`aerospike.env.example`, skipping any key already exported — so exported values
win and there is no file to edit. Set `AEROSPIKE_USE_SERVICES_ALTERNATE`
explicitly rather than inheriting it: the example file ships `true` for
container setups, and using alternate access addresses against a cluster that
doesn't publish them strands the client on a single node, where most reads then
fail to route.

Take the tools from the same branch the build came from. `--depth 1` otherwise
clones the default branch (`main`), and dev builds are published from `dev`, so
bench flags introduced alongside a new SDK feature — `--mode async-many`, for
instance — may not exist in `main`'s copy of the tools yet,
and the run dies on an unknown flag. For a build published by a manual dispatch
from a feature branch, or when you want exact parity, use the git revision
recorded in that build's JFrog build-info.

Use a sparse checkout rather than a full clone. `benchmarks/benchmark.py`
prepends its parent directory to `sys.path`, so in a full checkout the
repository's own `aerospike_sdk/` shadows the installed package — at best the
run fails because the generated parser is absent from a fresh clone, and at
worst it silently measures your working tree instead of the build you pinned.
With only `benchmarks/` checked out there is nothing to shadow.

`python -m benchmarks.compare` is a maintainer tool rather than a tester one: it
drives several client repositories side by side and expects a pyenv environment
per repository. See [`benchmarks/README.md`](benchmarks/README.md) for the full
flag reference and [`docs/guide/benchmarking.md`](docs/guide/benchmarking.md)
for methodology.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## Development / Contributing

The sections below are for SDK *contributors*. Downstream users do **not** need
any of this — `pip install aerospike-sdk` is sufficient to use the package.

### Prerequisites

- **Python** 3.10 - 3.14, **or** 3.14t (free-threaded) for high-throughput / `AsyncPool` work.
  Recommended installer: [`uv`](https://docs.astral.sh/uv/) (`uv python install 3.14.5+freethreaded`)
  or [`pyenv`](https://github.com/pyenv/pyenv) with a dedicated environment.
  Free-threaded wheels (`cp314t`) ship across the same platform matrix as
  the regular CPython wheels. (PyO3 0.29 dropped 3.13t support; PSDK's
  free-threaded build starts at 3.14t.)
- **Aerospike server** — required for integration tests
- **Rust toolchain** (`rustc` + `cargo`) — required only when building the Aerospike Python Async Client from source (e.g. for an unreleased PAC feature)
- **Java 11+** — required for the one-time AEL parser build (`make generate-ael`)

### Setting up a dev environment

```bash
make generate-ael          # one-time: build the ANTLR AEL parser (requires Java 11+)
pip install -e ".[dev]"    # install with dev extras
```

`make generate-ael` only needs to be re-run if `aerospike_sdk/ael/antlr4/Condition.g4` changes.

On the `dev` branch, the pinned `aerospike-async` (PAC) version is a
pre-release build published to Aerospike's internal package index rather
than public PyPI, so the plain install above needs one extra step that
depends on who you are:

**External contributors:** the internal index requires Aerospike
credentials, but PAC's source is public — build it locally per
[Local PAC checkout](#local-pac-checkout) below (requires a Rust
toolchain), then install this SDK with `--no-deps`. Released versions of
`aerospike-sdk` on public PyPI depend only on public PyPI packages and
need none of this.

**Aerospike engineers:** configure the internal index once, per
[Pre-release builds](#pre-release-builds-aerospike-internal) above, then:

```bash
pip install -e ".[dev]"
```

CI does the equivalent with short-lived OIDC credentials; ReadTheDocs builds
need `PIP_EXTRA_INDEX_URL` set as an environment variable in the RTD project
dashboard.

### Local PAC checkout

To build against a local Aerospike Python Async Client working tree —
whether because you're changing PAC itself or because you don't have
access to the internal index — install it editable first and pass
`--no-deps` to this SDK so pip doesn't try to resolve the exact PAC pin
from an index:

```bash
pip install -e /path/to/aerospike-client-python-async
pip install -e ".[dev]" --no-deps
```

Or use `requirements-local.txt` (edit the `file:` path for your machine).

### Configuration

Copy `aerospike.env.example` to `aerospike.env` in the repo root and adjust
hosts or ports. `aerospike.env` is not committed.

```bash
cp aerospike.env.example aerospike.env
source aerospike.env
```

Pytest loads `aerospike.env` when present; otherwise `conftest.py` loads
`aerospike.env.example` for unset variables only (so CI env vars still win).

### Running tests

```bash
make test          # all tests
make test-unit     # unit tests only
make test-int      # integration tests only (requires running Aerospike server)
```

**macOS file descriptor limit.** On macOS, you may encounter `OSError: [Errno
24] Too many open files` when running the full test suite. The default limit
(256) is not enough for the concurrent async connections created during
testing.

```bash
ulimit -n 4096
```

To make this permanent, add it to your shell profile (`~/.zshrc` or
`~/.bash_profile`).

### Building docs locally

API docs are built with [Sphinx](https://www.sphinx-doc.org/) (Furo theme,
MyST-Parser for Markdown). The same Sphinx config is what
[Read the Docs](https://aerospike-python-sdk.readthedocs.io/) builds from.

```bash
pip install -e ".[docs]"   # one-time: install Sphinx toolchain
make docs                  # build static HTML to docs/_build/html/
make docs-serve            # live-reloading local preview
```

Docstrings use Google style with Sphinx cross-references (`:meth:`, `:class:`,
etc.).

### Lint

```bash
ruff check .
```

### Bumping the version

Bumps are manual and happen in PRs against `dev`. Promotion workflows
(`dev → stage → main`) do not mutate the version.

```bash
# 1. Edit VERSION:
#    e.g. 0.9.0-alpha.1  →  0.9.0-alpha.2
echo '0.9.0-alpha.2' > VERSION

# 2. Confirm:
bin/get-version    # prints 0.9.0-alpha.2

# 3. Open a PR against dev with just this change.
```

### Bumping the PAC pin

PSDK depends on a published release of the
[Aerospike Python Async Client](https://github.com/aerospike/aerospike-client-python-async)
on PyPI as `aerospike-async`. On `dev` and downstream release branches the pin
**must** be a published PyPI version. The pin lives in `pyproject.toml` under
`[project] dependencies`:

```toml
[project]
dependencies = [
    "aerospike-async==0.5.0a1",
    # ...other deps
]
```

To bump: change the version to the new release on PyPI, then reinstall:

```bash
pip install --upgrade "aerospike-async==0.5.0aN"
```

Open the PR against `dev`. PSDK's own `VERSION` does not need to change for a
PAC pin bump unless the underlying API contract has shifted enough to warrant
it.

#### Mid-cycle: pinning a tagged but unpublished PAC

During a breaking-change cycle, the new PAC may be git-tagged on GitHub before
its PyPI wheel is published. Feature branches (not `dev`) may temporarily pin
to the git ref to validate against the new PAC before publish:

```toml
[project]
dependencies = [
    #"aerospike-async==0.5.0a1",   # ← restore this form before merging to dev
    "aerospike-async @ git+ssh://git@github.com/aerospike/aerospike-client-python-async.git@v0.5.0-alpha.1",
    # ...other deps
]
```

Use git refs only on feature branches; switch back to the `==X.Y.ZaN` form
before opening a PR against `dev`. CI reads the PAC ref out of `pyproject.toml`
at job start, so both forms work transparently for the build matrix.

### Reading the version programmatically

Anywhere a build script, CI step, or release tool needs the version:

```bash
bin/get-version    # → 0.9.0-alpha.1
```

The script reads `VERSION` and trims trailing whitespace. No Python or
setuptools runtime dependency — usable from any shell, container, or CI
environment.
