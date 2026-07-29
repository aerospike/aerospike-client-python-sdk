#!/usr/bin/env python3
"""Compute the next dev-channel version from the committed VERSION file.

Usage: next_dev_version.py <version> <dev-number>

Prints GitHub-output-style lines:

    version=<string to stamp into the VERSION file>
    pep440-version=<the version pip will see>

The committed version is the *last released* one, so dev builds lead toward
the next release: a prerelease base bumps its prerelease number
(``0.9.0-alpha.5`` -> ``0.9.0-alpha.6.dev.N``), a stable base bumps the
patch (``0.9.0`` -> ``0.9.1-dev.N``). PEP 440 orders dev releases before the
release they lead to, so the bump is required for dev builds to sort after
the released version.

Only the stamped form is written anywhere; setuptools normalizes it to the
PEP 440 form when building, which is what lands in artifact filenames and
what pip resolves. This script reproduces that normalization so callers can
address the published artifact without parsing filenames.
"""

import re
import sys

_PRERELEASE_PEP440 = {"alpha": "a", "beta": "b", "rc": "rc"}


def next_dev_version(base: str, dev_number: int) -> tuple[str, str]:
    """Return ``(stamped_version, pep440_version)`` for the next dev build."""
    if ".dev." in base or base.endswith("-dev") or "-dev." in base:
        raise ValueError(f"committed version {base!r} already carries a dev segment")

    prerelease = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)-(alpha|beta|rc)\.(\d+)", base)
    if prerelease:
        major, minor, patch, kind, num = prerelease.groups()
        bumped = int(num) + 1
        stamped = f"{major}.{minor}.{patch}-{kind}.{bumped}.dev.{dev_number}"
        pep440 = (
            f"{major}.{minor}.{patch}"
            f"{_PRERELEASE_PEP440[kind]}{bumped}.dev{dev_number}"
        )
        return stamped, pep440

    stable = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", base)
    if stable:
        major, minor, patch = stable.groups()
        bumped = int(patch) + 1
        stamped = f"{major}.{minor}.{bumped}-dev.{dev_number}"
        pep440 = f"{major}.{minor}.{bumped}.dev{dev_number}"
        return stamped, pep440

    raise ValueError(f"unrecognized version shape: {base!r}")


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    stamped, pep440 = next_dev_version(sys.argv[1], int(sys.argv[2]))
    print(f"version={stamped}")
    print(f"pep440-version={pep440}")


if __name__ == "__main__":
    main()
