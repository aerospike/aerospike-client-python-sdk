#!/usr/bin/env python3
"""Fail unless named modules actually executed.

Nightly jobs exist to run suites that skip on the pull-request cluster. Pytest
exits 0 when every test skipped, which would turn a missing cluster into a
green run. This reads a JUnit XML report and requires each named module to
have at least one non-skipped test.

``--forbid-skips`` also fails when any test in those modules was skipped. Use
it for a job whose only purpose is that module.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET


def _module_name(path: str) -> str:
    return path.removesuffix(".py").replace("\\", "/").replace("/", ".")


def _matches(case: ET.Element, path: str) -> bool:
    wanted = path.replace("\\", "/").removeprefix("./")
    reported = (case.get("file") or "").replace("\\", "/")
    if reported == wanted or reported.endswith("/" + wanted):
        return True
    module = _module_name(wanted)
    classname = case.get("classname") or ""
    return classname == module or classname.startswith(module + ".")


def _is_skipped(case: ET.Element) -> bool:
    return any(child.tag.endswith("skipped") for child in case)


def _skip_message(case: ET.Element) -> str:
    for child in case:
        if child.tag.endswith("skipped"):
            return child.get("message") or (child.text or "").strip()
    return ""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("junit", help="pytest --junitxml report")
    parser.add_argument("modules", nargs="+", help="test module paths")
    parser.add_argument(
        "--forbid-skips",
        action="store_true",
        help="fail if any test in the named modules was skipped",
    )
    args = parser.parse_args()

    testcases = [
        el for el in ET.parse(args.junit).iter() if el.tag.endswith("testcase")
    ]
    failed = False
    for module in args.modules:
        matched = [case for case in testcases if _matches(case, module)]
        skipped = [case for case in matched if _is_skipped(case)]
        executed = len(matched) - len(skipped)
        print(f"{module}: {executed} ran, {len(skipped)} skipped")
        if executed == 0:
            print(f"::error::{module} did not run (skipped or not collected)")
            failed = True
            continue
        if args.forbid_skips and skipped:
            print(f"::error::{module} skipped {len(skipped)} test(s)")
            for case in skipped:
                name = f"{case.get('classname')}::{case.get('name')}"
                print(f"  skipped {name}: {_skip_message(case)}")
            failed = True
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
