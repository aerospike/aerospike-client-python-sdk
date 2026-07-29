#!/usr/bin/env python3
"""Stamp a version into the VERSION file in place.

Usage: stamp_version.py <version>

Used by CI to apply an ephemeral dev-channel version to the build workspace
without committing it. setuptools reads the version dynamically from this
file, so writing it is the whole of the override.

This lives in a script rather than an inline shell one-liner because the
inline build-script path does not preserve backslash escapes, which silently
corrupts the stamped version.
"""

import pathlib
import sys


def stamp(version: str) -> None:
    path = pathlib.Path("VERSION")
    if not path.exists():
        raise SystemExit("VERSION not found; stamp from the repository root")
    path.write_text(f"{version}\n")
    print(f"Stamped version {version}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    stamp(sys.argv[1])
