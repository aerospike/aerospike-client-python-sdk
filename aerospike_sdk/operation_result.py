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

"""OperationResult — typed-accessor wrapper over a single value.

Wraps any value (bin contents, projected sub-result, etc.) and exposes
type-safe accessors that raise on type mismatch and return idiomatic defaults
on ``None``

Note:
    A positional :attr:`Record.results` array indexed in operation order is
    not yet exposed by the underlying async client. Until the async client
    surfaces ordered per-op results, ``OperationResult`` is most useful as a
    typed wrapper over values retrieved by bin name (or projected via the
    ops projection / ``get(return: ...)`` machinery).
"""

from __future__ import annotations

from typing import Any


class OperationResult:
    """Typed-accessor wrapper around a single value from a write or operate command.

    The wrapped value can be any type the server returns: integer, float,
    string, bytes, list, map, GeoJSON, HLL, boolean, or ``None``. Each
    ``get_*`` method coerces or rejects the underlying type explicitly so
    silent miscasts surface as :class:`TypeError` rather than mysterious
    runtime failures elsewhere.

    Numeric and boolean accessors return idiomatic defaults (``0``,
    ``0.0``, ``False``) when the wrapped value is ``None``, matching how
    other SDKs model an absent result for a write-then-read sequence on a
    bin that did not exist.

    Example::

        result = OperationResult(42)
        result.get_int()       # 42
        result.get_string()    # raises TypeError — value is int, not str
        result.value           # 42 (raw)

    See Also:
        :meth:`OperationResult.value`: Raw access without coercion.
    """

    __slots__ = ("_value",)

    def __init__(self, value: Any) -> None:
        self._value = value

    @property
    def value(self) -> Any:
        """The raw underlying value, with no coercion."""
        return self._value

    # -- Numeric accessors ----------------------------------------------------

    def get_long(self) -> int:
        """Return the value as an integer; ``0`` when the value is ``None``.

        Booleans are treated as integers per Python semantics (``True`` → 1).

        Raises:
            TypeError: When the wrapped value is neither ``int`` nor ``None``.
        """
        v = self._value
        if v is None:
            return 0
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return v
        raise TypeError(
            f"OperationResult value is {type(v).__name__}, not int"
        )

    def get_int(self) -> int:
        """Alias for :meth:`get_long` — Python ``int`` is unbounded so the
        ``int`` / ``long`` distinction is purely for naming parity."""
        return self.get_long()

    def get_double(self) -> float:
        """Return the value as a float; ``0.0`` when the value is ``None``.

        Integers are widened to float for convenience; strings and other
        types raise.

        Raises:
            TypeError: When the wrapped value is neither numeric nor ``None``.
        """
        v = self._value
        if v is None:
            return 0.0
        if isinstance(v, bool):
            return float(v)
        if isinstance(v, (int, float)):
            return float(v)
        raise TypeError(
            f"OperationResult value is {type(v).__name__}, not float"
        )

    def get_float(self) -> float:
        """Alias for :meth:`get_double`."""
        return self.get_double()

    def get_bool(self) -> bool:
        """Return the value as a boolean; ``False`` when the value is ``None``.

        Accepts both native booleans and integers so values written by older
        clients (which encoded booleans as longs) still decode correctly.

        Raises:
            TypeError: When the wrapped value is not a bool, int, or ``None``.
        """
        v = self._value
        if v is None:
            return False
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return v != 0
        raise TypeError(
            f"OperationResult value is {type(v).__name__}, not bool"
        )

    # -- String / bytes accessors --------------------------------------------

    def get_string(self) -> str | None:
        """Return the value as ``str``; ``None`` is propagated.

        Raises:
            TypeError: When the wrapped value is non-``None`` and not a string.
        """
        v = self._value
        if v is None:
            return None
        if isinstance(v, str):
            return v
        raise TypeError(
            f"OperationResult value is {type(v).__name__}, not str"
        )

    def get_bytes(self) -> bytes | None:
        """Return the value as ``bytes``; ``None`` is propagated.

        Bytearrays are accepted and converted; other types raise.
        """
        v = self._value
        if v is None:
            return None
        if isinstance(v, bytes):
            return v
        if isinstance(v, bytearray):
            return bytes(v)
        raise TypeError(
            f"OperationResult value is {type(v).__name__}, not bytes"
        )

    # -- Collection accessors -------------------------------------------------

    def get_list(self) -> list[Any] | None:
        """Return the value as ``list``; ``None`` is propagated.

        Raises:
            TypeError: When the wrapped value is non-``None`` and not a list.
        """
        v = self._value
        if v is None:
            return None
        if isinstance(v, list):
            return v
        raise TypeError(
            f"OperationResult value is {type(v).__name__}, not list"
        )

    def get_map(self) -> dict[Any, Any] | None:
        """Return the value as ``dict``; ``None`` is propagated.

        Raises:
            TypeError: When the wrapped value is non-``None`` and not a dict.
        """
        v = self._value
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        raise TypeError(
            f"OperationResult value is {type(v).__name__}, not dict"
        )

    def __repr__(self) -> str:
        return f"OperationResult({self._value!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OperationResult):
            return NotImplemented
        return self._value == other._value

    def __hash__(self) -> int:
        try:
            return hash(self._value)
        except TypeError:
            # Lists, dicts, etc. — fall back to identity to keep hashability
            # consistent with Python's general convention.
            return id(self)
