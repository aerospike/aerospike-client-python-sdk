# Aerospike Python SDK

A high-level API on the Aerospike Python Async Client, providing an intuitive and chainable interface for database operations.

> **Status:** Pre-alpha -- internal development.

## Prerequisites

- **Python** 3.10 - 3.14 (recommended: [pyenv](https://github.com/pyenv/pyenv) with a dedicated environment for this repo)
- **Aerospike server** -- required for integration tests
- **Rust toolchain** (rustc + cargo) -- only needed if building the Aerospike Python Async Client from source
- **Java 11+** -- only needed if regenerating the ANTLR AEL parser

## Install the Aerospike Python Async Client

This SDK depends on the [Aerospike Python Async Client](https://github.com/aerospike/aerospike-client-python-async).
The version is pinned in `pyproject.toml` (git tag). `pip install -e ".[dev]"` installs PAC from Git (requires Rust to build unless you pre-install a wheel).

### Option 1: Pre-built wheel (no Rust)

Download the wheel for your platform and Python version from the
[GitHub Releases page](https://github.com/aerospike/aerospike-client-python-async/releases),
then install it with the **same pyenv-backed Python** you use for this SDK **before** installing this package:

```bash
# e.g. pyenv activate sdk_client_3_14_2_crsr   # your env name
pip install aerospike_async-0.3.0a10-cp313-cp313-macosx_11_0_arm64.whl  # example; match your Python and platform
pip install -e ".[dev]" --no-deps   # if PAC is already satisfied
```

### Option 2: Build PAC from source (requires Rust)

```bash
git clone git@github.com:aerospike/aerospike-client-python-async.git
cd aerospike-client-python-async
git checkout v0.3.0-alpha.10
pip install -r requirements.txt
make dev
```

See the [Aerospike Python Async Client README](https://github.com/aerospike/aerospike-client-python-async/blob/rust-async/README.md) for detailed Rust setup instructions.

### Local PAC checkout (temporary)

To test against an **unreleased** sibling PAC tree, install it explicitly, then install this SDK without re-resolving PAC from git:

```bash
pip install -e /path/to/aerospike-client-python-async
pip install -e ".[dev]" --no-deps
```

Or adjust and use `requirements-local.txt` (gitignored path example).

## Install this package

Use the interpreter from your pyenv environment (see `.cursor/rules/guiding-principles.mdc` for the usual env name), then:

```bash
pip install -e ".[dev]"
```

## Configuration

Copy `aerospike.env.example` to `aerospike.env` in the repo root and adjust hosts or ports. `aerospike.env` is not committed.

```bash
cp aerospike.env.example aerospike.env
source aerospike.env
```

Pytest loads `aerospike.env` when present; otherwise `conftest.py` loads `aerospike.env.example` for unset variables only (so CI env vars still win).

## Running Tests

```bash
make test          # all tests
make test-unit     # unit tests only
make test-int      # integration tests only (requires running Aerospike server)
```

### macOS File Descriptor Limit

On macOS, you may encounter `OSError: [Errno 24] Too many open files` when running the full test suite. The default limit (256) is not enough for the concurrent async connections created during testing.

```bash
ulimit -n 4096
```

To make this permanent, add it to your shell profile (`~/.zshrc` or `~/.bash_profile`).

## Documentation

API docs are built with [Sphinx](https://www.sphinx-doc.org/) (Furo theme, MyST-Parser for Markdown).

```bash
pip install -e ".[docs]"   # one-time: install Sphinx toolchain

make docs                  # build static HTML to docs/_build/html/
make docs-serve            # live-reloading local preview
```

Docstrings use Google style with Sphinx cross-references (`:meth:`, `:class:`, etc.).

## Development

```bash
# Regenerate the ANTLR AEL parser (requires Java 11+)
make generate-ael

# Lint
ruff check .
```

## Usage

```python
import asyncio
from aerospike_sdk import Behavior, DataSet, Client

async def main():
    async with Client("localhost:3100") as client:
        session = client.create_session(Behavior.DEFAULT)
        users = DataSet.of("test", "users")

        # High-level key-value operations
        await session.upsert(key=users.id(1)).put({"name": "Alice", "age": 28, "country": "UK"})
        await session.upsert(key=users.id(2)).put({"name": "Bob", "age": 35, "country": "US"})

        # Query with string AEL -- stream results one at a time (memory-efficient)
        results = await (
            session.query(users)
            .where("$.age > %s and $.country == '%s'", 25, "US")
            .execute()
        )
        async for rec in results:
            print(rec.bins)

        # execute() returns a lazy async stream
        all_users = await session.query(users).execute()
        # collect() drains the stream into a list
        user_list = await all_users.collect()

asyncio.run(main())
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
