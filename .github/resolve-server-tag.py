#!/usr/bin/env python3
"""Pick the newest Aerospike Server release or RC tag from a Docker tag list.

Reads a registry ``tags/list`` JSON document on stdin
(``{"tags": ["8.2.0.0", "8.2.0.1-rc1", ...]}``) and prints one tag.
Release tags are ``X.Y.Z.W``. RC tags are that plus ``rc`` and a number
(``8.2.0.1-rc1``, ``8.2.0.1-rc.1``, ``8.2.0.1_rc1``, ``8.2.0.1rc1``).
Nightly, moving, and other tags are ignored. The floor is 8.2.0.0.
"""

from __future__ import annotations

import json
import re
import sys

_RELEASE = re.compile(r"^(\d+)\.(\d+)\.(\d+)\.(\d+)$")
_RC = re.compile(r"^(\d+)\.(\d+)\.(\d+)\.(\d+)(?:[-_.]?rc\.?)(\d+)$", re.IGNORECASE)
_FLOOR = (8, 2, 0, 0)


def _parsed(tag: str) -> tuple[tuple[int, int, int, int], bool, int] | None:
    release = _RELEASE.fullmatch(tag)
    if release:
        version = tuple(int(part) for part in release.groups())
        return version, True, 0  # type: ignore[return-value]
    candidate = _RC.fullmatch(tag)
    if candidate:
        version = tuple(int(part) for part in candidate.groups()[:4])
        return version, False, int(candidate.group(5))  # type: ignore[return-value]
    return None


def choose_tag(tags: list[str], floor: tuple[int, int, int, int] = _FLOOR) -> str:
    """Newest release or RC at or above ``floor``. A release beats an RC of the same build."""
    parsed: list[tuple[tuple[int, int, int, int], bool, int, str]] = []
    for tag in tags:
        got = _parsed(tag)
        if got is None:
            continue
        version, is_release, rc_number = got
        if version < floor:
            continue
        parsed.append((version, is_release, rc_number, tag))
    if not parsed:
        raise ValueError(f"no release or RC tag at or above {'.'.join(map(str, floor))}")
    parsed.sort()
    return parsed[-1][3]


def main() -> None:
    document = json.load(sys.stdin)
    tags = document.get("tags") if isinstance(document, dict) else document
    if not isinstance(tags, list):
        sys.exit("::error::tag list JSON has no tags array")
    try:
        print(choose_tag([str(tag) for tag in tags]))
    except ValueError as exc:
        sys.exit(f"::error::{exc}")


if __name__ == "__main__":
    main()
