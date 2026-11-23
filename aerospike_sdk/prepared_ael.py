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

"""Typed parameter binding for AEL templates."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

# One alternation, tried left to right: a quoted literal (backslash escapes
# honored), a block comment, a placeholder (group 1), or a ``?`` followed by a
# digit (group 2), which is refused. A value substituted inside a literal or a
# comment would land outside its own quotes, so those spans are skipped rather
# than searched.
_TOKEN = re.compile(
    r"""'(?:[^'\\]|\\.)*'?|"(?:[^"\\]|\\.)*"?|/\*.*?(?:\*/|\Z)"""
    r"""|\?([A-Za-z_][A-Za-z0-9_]*)|(\?[0-9]+)""",
    re.DOTALL,
)

_WORD = re.compile(r"\w+")

_STRING_ESCAPES = {
    "\\": "\\\\",
    "'": "\\'",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
    "\0": "\\0",
}
_STRING_UNSAFE = re.compile(r"[\\'\x00-\x1f\x7f]")

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

# The server rejects a numeric literal of 64 or more characters.
_MAX_NUMBER_CHARS = 63


def _escape_char(match: re.Match[str]) -> str:
    char = match.group()
    return _STRING_ESCAPES.get(char) or f"\\x{ord(char):02x}"


def _literal(value: Any) -> str:
    """Return the AEL literal for *value*, or raise if it has none."""
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError(
                "a string holding an unpaired surrogate has no AEL literal",
            ) from None
        return "'" + _STRING_UNSAFE.sub(_escape_char, value) + "'"
    if isinstance(value, int):
        if not _INT64_MIN <= value <= _INT64_MAX:
            raise ValueError(
                f"integer {value} is outside the signed 64-bit range",
            )
        return str(int(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"float {value!r} has no AEL literal")
        text = format(Decimal(repr(float(value))), "f")
        if "." not in text:
            text += ".0"
        if len(text) > _MAX_NUMBER_CHARS:
            raise ValueError(
                f"float {value!r} needs {len(text)} characters as a decimal "
                f"literal and the server accepts at most {_MAX_NUMBER_CHARS}; "
                "build the expression with Exp instead",
            )
        return text
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "X'" + bytes(value).hex() + "'"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join([_literal(item) for item in value]) + "]"
    if isinstance(value, dict):
        entries = []
        for key, item in value.items():
            if isinstance(key, bool) or not isinstance(key, (str, int, bytes)):
                raise TypeError(
                    f"a map key of type {type(key).__name__} has no AEL "
                    "literal; keys must be str, int or bytes",
                )
            entries.append(f"{_literal(key)}: {_literal(item)}")
        return "{" + ", ".join(entries) + "}"
    raise TypeError(f"a value of type {type(value).__name__} has no AEL literal")


class PreparedAel:
    """An AEL template whose parameters bind as correctly formed AEL literals.

    Placeholders are named: ``?name``, ``?min_age``. Pass the template
    wherever an AEL string is accepted, followed by one mapping that holds a
    value for each name; every value is quoted and escaped for its type, so it
    can never change the shape of the expression. This is the form to use for
    any value the application did not write itself.

    =====================================  ===========================
    Python value                           AEL literal
    =====================================  ===========================
    ``bool``                               ``true`` / ``false``
    ``int``                                decimal integer
    ``float``                              plain decimal, no exponent
    ``str``                                single-quoted, escaped
    ``bytes``, ``bytearray``,              ``X'<hex>'``
    ``memoryview``
    ``list``, ``tuple``                    ``[a, b]``
    ``dict``                               ``{key: value}``
    =====================================  ===========================

    The server compares strictly by type, so a ``float`` always binds with a
    decimal point and an ``int`` never does. Map keys must be ``str``, ``int``
    or ``bytes``. ``None`` and every other type have no literal.

    A name is an ASCII letter or underscore followed by letters, digits or
    underscores, and is case-sensitive. A name may repeat. Text inside a
    quoted literal or a ``/* ... */`` comment in the template is never treated
    as a placeholder: a bound value brings its own quotes, so write
    ``$.name == ?name``, not ``$.name == '?name'``.

    The template is scanned once, here; reuse one instance across calls.

    Args:
        template: AEL text with named placeholders.

    Raises:
        TypeError: If *template* is not a string.
        ValueError: If a ``?`` outside quotes and comments is followed by a
            digit, such as ``?0``, or a name runs into a non-ASCII letter or
            digit, such as ``?año``.

    Example::

        from aerospike_sdk import PreparedAel

        by_name_and_age = PreparedAel("$.name == ?name and $.age > ?min_age")

        stream = await (
            session.query(users)
            .where(by_name_and_age, {"name": customer_name, "min_age": 21})
            .execute()
        )

    See Also:
        :meth:`bind`: The bound text, for APIs that take a plain AEL string.
    """

    __slots__ = ("_template", "_format", "_required")

    def __init__(self, template: str) -> None:
        if not isinstance(template, str):
            raise TypeError(
                f"an AEL template must be a str, got {type(template).__name__}",
            )
        # The template becomes a %-format string keyed by placeholder name, so
        # binding is one C-level substitution.
        parts: list[str] = []
        names: list[str] = []
        last = 0
        for match in _TOKEN.finditer(template):
            name = match.group(1)
            if name is not None:
                following = template[match.end():match.end() + 1]
                if following.isalnum() and not following.isascii():
                    rest = _WORD.match(template, match.end()).group()
                    raise ValueError(
                        "placeholder names are ASCII letters, digits and "
                        f"underscores; got ?{name}{rest} in AEL template "
                        f"{template!r}",
                    )
                parts.append(template[last:match.start()].replace("%", "%%"))
                parts.append(f"%({name})s")
                names.append(name)
                last = match.end()
            elif match.group(2) is not None:
                raise ValueError(
                    "placeholders are named, like ?min_age; got "
                    f"{match.group(2)} in AEL template {template!r}",
                )
        parts.append(template[last:].replace("%", "%%"))
        self._template = template
        self._format = "".join(parts)
        self._required = frozenset(names)

    @property
    def template(self) -> str:
        """The template text this instance was built from."""
        return self._template

    def bind(self, /, **values: Any) -> str:
        """Return the AEL text with each placeholder replaced by a literal.

        The builder methods that accept a ``PreparedAel`` call this for you
        with the mapping they are given. Call it directly for an API that
        takes a plain AEL string, or to log or test the text a filter
        resolves to.

        Args:
            **values: One value for each placeholder name.

        Returns:
            The bound AEL text.

        Raises:
            TypeError: If a value's type has no AEL literal, including
                ``None`` and a map key that is not ``str``, ``int`` or
                ``bytes``.
            ValueError: If a placeholder has no value, a value names no
                placeholder, or a value has no literal the server accepts:
                NaN or infinity, an integer outside the signed 64-bit range, a
                float whose decimal form is longer than 63 characters, or a
                string holding an unpaired surrogate.

        Example::

            in_region = PreparedAel("geoCompare($.loc, geoJson(?region))")
            log.debug("store filter: %s", in_region.bind(region=region.value))
        """
        return self._bind(values)

    def _bind(self, values: Mapping[str, Any]) -> str:
        if values.keys() != self._required:
            raise self._name_mismatch(values)
        name = ""
        try:
            literals = {}
            for name, value in values.items():
                literals[name] = _literal(value)
        except (TypeError, ValueError) as exc:
            raise type(exc)(f"cannot bind ?{name}: {exc}") from None
        return self._format % literals

    def _name_mismatch(self, values: Mapping[str, Any]) -> Exception:
        if not all(isinstance(name, str) for name in values):
            return TypeError(
                f"placeholder names for AEL template {self._template!r} must be str",
            )
        problems = []
        missing = sorted(self._required.difference(values))
        if missing:
            problems.append("no value for " + ", ".join(["?" + n for n in missing]))
        extra = sorted(set(values).difference(self._required))
        if extra:
            problems.append(
                "no placeholder outside quotes and comments named "
                + ", ".join(["?" + n for n in extra]),
            )
        return ValueError(
            f"AEL template {self._template!r} has " + " and ".join(problems),
        )

    def __str__(self) -> str:
        return self._template

    def __repr__(self) -> str:
        return f"PreparedAel({self._template!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PreparedAel):
            return NotImplemented
        return self._template == other._template

    def __hash__(self) -> int:
        return hash(self._template)
