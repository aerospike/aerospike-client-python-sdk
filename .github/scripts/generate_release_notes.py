#!/usr/bin/env python3
# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""
Transform GitHub-generated release notes into a custom format:
- Release date at top
- Sections: Breaking Changes, New Features, Improvements, Bug Fixes, Security (omitted if empty)
- Style normalization: sentence case start, trailing period, expanded contractions
- JIRA/ticket identifiers (e.g. [CLIENT-1234] or CLIENT-1234:) moved to the end of each line (after PR link)
- PR links preserved as ([#N](url))
- Internal-only items (CI, tests-only, etc.) filtered out
- Full Changelog link at bottom

Reads raw markdown from stdin; writes formatted markdown to stdout.
"""

from __future__ import annotations

import argparse
import re
import sys


TICKET_RE = re.compile(r"\[([A-Z][A-Z0-9]*-[0-9]+)\]")
COLON_TICKET_PREFIX_RE = re.compile(
    r"^([A-Z][A-Z0-9]*-[0-9]+)\s*:\s*",
)
PR_SUFFIX_RE = re.compile(r"\s+by\s+@[\w-]+\s+in\s+(https?://\S+)", re.IGNORECASE)

# Longer phrases first where relevant; word-boundary case-insensitive replacement.
_CONTRACTION_PAIRS: tuple[tuple[str, str], ...] = (
    (r"shouldn't\b", "should not"),
    (r"couldn't\b", "could not"),
    (r"wouldn't\b", "would not"),
    (r"doesn't\b", "does not"),
    (r"haven't\b", "have not"),
    (r"hasn't\b", "has not"),
    (r"weren't\b", "were not"),
    (r"wasn't\b", "was not"),
    (r"aren't\b", "are not"),
    (r"isn't\b", "is not"),
    (r"won't\b", "will not"),
    (r"can't\b", "cannot"),
    (r"don't\b", "do not"),
    (r"there's\b", "there is"),
    (r"that's\b", "that is"),
    (r"it's\b", "it is"),
)

INTERNAL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bbump\b.*\btest\b", re.IGNORECASE),
    re.compile(r"\bskip\b.*\btest\b", re.IGNORECASE),
    re.compile(r"\bstub\s+pipeline\b", re.IGNORECASE),
    re.compile(r"\brefactor\b", re.IGNORECASE),
    re.compile(r"\bversion\s+bump\b", re.IGNORECASE),
    re.compile(r"\bCI\b", re.IGNORECASE),
    re.compile(r"\bpipeline\b", re.IGNORECASE),
    re.compile(r"\btest(?:s)?\s+(?:only|fix|update|cleanup)\b", re.IGNORECASE),
)


def normalize_summary(desc: str) -> str:
    """Capitalize first letter, ensure trailing sentence end, expand common contractions."""
    s = desc.strip()
    for pattern, repl in _CONTRACTION_PAIRS:
        s = re.sub(pattern, repl, s, flags=re.IGNORECASE)
    s = s.strip()
    if not s:
        return s
    for i, c in enumerate(s):
        if c.isalpha():
            s = s[:i] + c.upper() + s[i + 1 :]
            break
    if s[-1] not in ".!?":
        s = f"{s}."
    return s


def is_internal(desc: str) -> bool:
    """True if the item should be omitted from user-facing release notes."""
    for pat in INTERNAL_PATTERNS:
        if pat.search(desc):
            return True
    return False


def is_bug_fix(title: str) -> bool:
    t = title.strip().lower()
    return (
        t.startswith("fix ")
        or t.startswith("fixes ")
        or t.startswith("bug")
        or t.startswith("fix:")
    )


def is_security(desc: str) -> bool:
    d = desc.lower()
    return "cve-" in d or "security" in d or "vulnerability" in d


def is_breaking(desc: str) -> bool:
    d = desc.lower()
    return (
        "breaking change" in d
        or bool(re.search(r"\bbreaking\b", d))
        or "drop support" in d
        or "remove deprecated" in d
        or "incompatible" in d
    )


def is_new_feature(desc: str) -> bool:
    if is_bug_fix(desc):
        return False
    d = desc.strip().lower()
    return (
        d.startswith("add ")
        or d.startswith("new ")
        or d.startswith("implement ")
        or d.startswith("introduce ")
    )


_BREAKING_PREFIX_RE = re.compile(
    r"^(?:breaking\s*(?:change\s*)?[:=-]+\s*|breaking\s+)", re.IGNORECASE
)
_SECURITY_PREFIX_RE = re.compile(r"^(?:security\s*[:=-]+\s*)", re.IGNORECASE)


def strip_section_prefix(desc: str, section: str) -> str:
    """Remove redundant leading keywords that duplicate the section header."""
    if section == "breaking":
        return _BREAKING_PREFIX_RE.sub("", desc).strip()
    if section == "security":
        return _SECURITY_PREFIX_RE.sub("", desc).strip()
    return desc


def classify_section(desc: str) -> str:
    """Return bucket key: breaking, feature, improvement, bug, security."""
    if is_security(desc):
        return "security"
    if is_breaking(desc):
        return "breaking"
    if is_bug_fix(desc):
        return "bug"
    if is_new_feature(desc):
        return "feature"
    return "improvement"


def warn_if_compound_summary(desc: str) -> None:
    if " + " in desc:
        print(
            f'WARNING: Compound summary detected, consider splitting: "{desc}"',
            file=sys.stderr,
        )
        return
    if desc.count(",") > 2:
        print(
            f'WARNING: Compound summary detected, consider splitting: "{desc}"',
            file=sys.stderr,
        )


def split_description_and_tickets(text: str) -> tuple[str, list[str]]:
    """Strip ticket brackets from text; return (clean description, ticket keys)."""
    tickets = TICKET_RE.findall(text)
    rest = TICKET_RE.sub("", text).strip()
    rest = re.sub(r"  +", " ", rest).strip()
    return rest, tickets


def extract_leading_colon_ticket(text: str) -> tuple[str, list[str]]:
    """If text starts with CLIENT-1234:, strip it and return extra ticket keys."""
    m = COLON_TICKET_PREFIX_RE.match(text)
    if not m:
        return text, []
    return text[m.end() :].strip(), [m.group(1)]


def pr_link_markdown(url: str) -> str:
    m = re.search(r"/pull/(\d+)", url, re.IGNORECASE)
    if m:
        return f"([#{m.group(1)}]({url}))"
    return f"([pull]({url}))"


def parse_bullet(line: str) -> tuple[str, str] | None:
    """
    Parse a line like '* Fix something by @user in https://.../pull/99'.

    Returns (formatted_bullet, description_for_classification) or None.
    """
    line = line.strip()
    if not line.startswith("* ") and not line.startswith("- "):
        return None
    text = line[2:].strip()
    pr_url = ""
    by_match = PR_SUFFIX_RE.search(text)
    if by_match:
        pr_url = by_match.group(1).rstrip(").,;")
        text = text[: by_match.start()].strip()
    if not text:
        return None

    text, colon_tickets = extract_leading_colon_ticket(text)
    desc, bracket_tickets = split_description_and_tickets(text)
    tickets = colon_tickets + bracket_tickets

    if not desc:
        return None

    warn_if_compound_summary(desc)

    if is_internal(desc):
        return None

    desc_norm = normalize_summary(desc)
    if not desc_norm:
        return None

    parts = [desc_norm]
    if pr_url:
        parts.append(pr_link_markdown(pr_url))
    if tickets:
        parts.append(f"[{tickets[0]}]")
    return " ".join(parts), desc_norm


def run_self_tests() -> None:
    # normalize_summary: capitalize, period, contractions
    assert normalize_summary("fix the bug") == "Fix the bug."
    assert normalize_summary("already done.") == "Already done."
    assert normalize_summary("doesn't work") == "Does not work."
    assert normalize_summary("don't panic!") == "Do not panic!"

    # is_internal: broadened patterns
    assert is_internal("Bump test dependencies")
    assert is_internal("skip test on windows")
    assert is_internal("Bump timeout on large put test")
    assert is_internal("Skip timeout test in case the timeout does not happen")
    assert is_internal("Refactor lib.rs - split into submodules")
    assert is_internal("Tests only fix for flaky suite")
    assert is_internal("test cleanup for CI")
    assert not is_internal("Fix query timeout for users")
    assert not is_internal("Add batch API support")

    # extract_leading_colon_ticket: Jira colon format
    t2, k2 = extract_leading_colon_ticket("CLIENT-99: hello [CLIENT-100]")
    desc2, bracket_keys = split_description_and_tickets(t2)
    assert desc2 == "hello" and k2 == ["CLIENT-99"] and bracket_keys == ["CLIENT-100"]
    t, keys = extract_leading_colon_ticket("CLIENT-99: hello world")
    assert t == "hello world" and keys == ["CLIENT-99"]

    # classify_section
    assert classify_section("Security patch for CVE-2024-1") == "security"
    assert classify_section("Breaking change: drop Python 3.8") == "breaking"
    assert classify_section("Fix null pointer in client") == "bug"
    assert classify_section("Add batch API support") == "feature"
    assert classify_section("Polish error messages") == "improvement"

    # strip_section_prefix: remove redundant keywords
    assert strip_section_prefix("BREAKING drop Python 3.8.", "breaking") == "drop Python 3.8."
    assert strip_section_prefix("Breaking change: drop Python 3.8.", "breaking") == "drop Python 3.8."
    assert strip_section_prefix("Security patch for CVE-2024-1.", "security") == "Security patch for CVE-2024-1."
    assert strip_section_prefix("Security: fix for CVE-2024-1.", "security") == "fix for CVE-2024-1."
    assert strip_section_prefix("Add batch API support.", "feature") == "Add batch API support."

    # parse_bullet: bracket ticket preserved at end
    out = parse_bullet("* fix foo by @x in https://github.com/o/r/pull/1")
    assert out is not None
    assert out[0].startswith("Fix foo.")

    # parse_bullet: internal items filtered
    assert parse_bullet("* Bump timeout on large put test by @x in https://github.com/o/r/pull/1") is None
    assert parse_bullet("* Refactor lib.rs by @x in https://github.com/o/r/pull/2") is None

    print("generate_release_notes self-tests passed.", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Format release notes.")
    parser.add_argument("--date", default="", help="Release date (e.g. 'March 17, 2026')")
    parser.add_argument(
        "--changelog-url",
        default="",
        help="Full Changelog URL (optional; may be parsed from input)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run inline self-tests and exit",
    )
    args = parser.parse_args()

    if args.test:
        run_self_tests()
        return

    if not args.date:
        parser.error("--date is required unless --test is set")

    raw = sys.stdin.read()
    breaking: list[str] = []
    features: list[str] = []
    improvements: list[str] = []
    bug_fixes: list[str] = []
    security: list[str] = []

    changelog_url = args.changelog_url
    for line in raw.splitlines():
        if "Full Changelog" in line or "full changelog" in line.lower():
            url_match = re.search(r"https://[^\s\)]+", line)
            if url_match and not changelog_url:
                changelog_url = url_match.group(0)
            continue
        parsed = parse_bullet(line)
        if parsed is None:
            continue
        formatted, desc_for_category = parsed
        bucket = classify_section(desc_for_category)
        stripped = strip_section_prefix(desc_for_category, bucket)
        if stripped and stripped != desc_for_category:
            stripped = normalize_summary(stripped)
            formatted = formatted.replace(desc_for_category, stripped, 1)
        if bucket == "security":
            security.append(formatted)
        elif bucket == "breaking":
            breaking.append(formatted)
        elif bucket == "bug":
            bug_fixes.append(formatted)
        elif bucket == "feature":
            features.append(formatted)
        else:
            improvements.append(formatted)

    out = [f"Release Date: {args.date}", ""]
    section_blocks: list[tuple[str, list[str]]] = [
        ("## Breaking Changes", breaking),
        ("## New Features", features),
        ("## Improvements", improvements),
        ("## Bug Fixes", bug_fixes),
        ("## Security", security),
    ]
    for heading, items in section_blocks:
        if items:
            out.append(heading)
            for item in items:
                out.append(f"- {item}")
            out.append("")
    if changelog_url:
        out.append(f"**Full Changelog**: {changelog_url}")

    sys.stdout.write("\n".join(out))
    if out and not out[-1].endswith("\n"):
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
