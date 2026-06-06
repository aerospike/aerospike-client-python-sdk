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

"""AEL parser — parse Aerospike Expression Language text into FilterExpression objects.

This module provides parsing of text-based AEL expressions like:
    "$.country == 'US' and $.order_total > 500"

into Aerospike FilterExpression objects.

Supports parameterized queries with placeholders:
    parse_ael("$.age > ?0", 30)

Also supports parsing paths into CTX arrays:
    parse_ctx("$.listBin.[0].[1]")  # Returns [CTX.list_index(0), CTX.list_index(1)]

Also supports filter generation with index context:
    parse_ael_with_index("$.intBin1 > 100 and $.intBin2 < 50", index_context)
    # Returns ParseResult with optimal Filter + remaining Exp
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener

from aerospike_async import CTX, FilterExpression

from aerospike_sdk.ael.antlr4.generated.ConditionLexer import ConditionLexer
from aerospike_sdk.ael.antlr4.generated.ConditionParser import ConditionParser
from aerospike_sdk.ael.exceptions import AelParseException
from aerospike_sdk.ael.exp_visitor import (
    CDTPath,
    DeferredBin,
    ExpressionConditionVisitor,
    ListIndexPart,
    ListRankPart,
    ListValuePart,
    MapIndexPart,
    MapKeyPart,
    MapRankPart,
    MapValuePart,
)
from aerospike_sdk.ael.filter_gen import FilterGenerator, IndexContext, ParseResult


class _AELParseErrorListener(ErrorListener):
    """Error listener that raises on first syntax error (no recovery)."""

    def syntaxError(self, recognizer, offending_symbol, line, column, msg, e):
        raise AelParseException(f"line {line}:{column} {msg}")


class PlaceholderValues:
    """Internal container for placeholder values (?0, ?1, ...) in AEL queries.

    Not part of the public API — use parse_ael() with \\*args instead.
    """

    def __init__(self, *values: Any):
        self._values: list[Any] = list(values)

    def get(self, index: int) -> Any:
        if index < 0 or index >= len(self._values):
            raise AelParseException(f"Missing value for placeholder ?{index}")
        return self._values[index]

    def __len__(self) -> int:
        return len(self._values)


class AELParser:
    """Parser for Aerospike Expression Language (AEL) text."""

    @staticmethod
    def parse(
        expression: str,
        placeholder_values: Optional[PlaceholderValues] = None,
    ) -> FilterExpression:
        """Parse an AEL expression string into a FilterExpression.

        Args:
            expression: The AEL expression string (e.g., "$.country == 'US' and $.order_total > 500")
            placeholder_values: Optional values for placeholders (?0, ?1, etc.)

        Returns:
            A FilterExpression object that can be used in queries.

        Raises:
            AelParseException: If the expression cannot be parsed.
        """
        try:
            # 1. Create input stream from string
            input_stream = InputStream(expression)

            # 2. Create lexer with custom error listener
            lexer = ConditionLexer(input_stream)
            lexer.removeErrorListeners()
            error_listener = _AELParseErrorListener()
            lexer.addErrorListener(error_listener)

            # 3. Create token stream
            token_stream = CommonTokenStream(lexer)

            # 4. Create parser with same error listener
            parser = ConditionParser(token_stream)
            parser.removeErrorListeners()
            parser.addErrorListener(error_listener)

            # 5. Parse to get parse tree
            parse_tree = parser.parse()

            # 6. Visit parse tree with visitor to build FilterExpression
            visitor = ExpressionConditionVisitor(placeholder_values=placeholder_values)
            result = visitor.visit(parse_tree)

            if result is None:
                raise AelParseException("Failed to parse AEL expression: visitor returned None")

            return result

        except Exception as e:
            if isinstance(e, AelParseException):
                raise
            raise AelParseException(f"Failed to parse AEL expression: {e}") from e


# Global parser instance
_parser: Optional[AELParser] = None


def parse_ael(expression: str, *args: Any) -> FilterExpression:
    """Parse an AEL expression string into a FilterExpression.

    Convenience function that uses a global parser instance.

    Args:
        expression: The AEL expression string.
        *args: Values for placeholders ?0, ?1, ?2, etc.

    Returns:
        A FilterExpression object.

    Example:
        expr = parse_ael("$.age > 30")
        expr = parse_ael("$.age > ?0 and $.name == ?1", 30, "John")
    """
    global _parser
    if _parser is None:
        _parser = AELParser()
    placeholder_values = PlaceholderValues(*args) if args else None
    return _parser.parse(expression, placeholder_values)


def parse_ctx(path: str) -> List[CTX]:
    """Parse an AEL path into a list of CTX objects.

    Converts an AEL path like "$.listBin.[0].[1]" into a CTX array
    for use with secondary index context operations.

    Args:
        path: The AEL path string (e.g., "$.listBin.[0]", "$.mapBin.key.subkey")

    Returns:
        A list of CTX objects representing the path context.

    Raises:
        AelParseException: If the path is invalid or unsupported.

    Example:
        ctx = parse_ctx("$.listBin.[0].[1]")
        # Returns [CTX.list_index(0), CTX.list_index(1)]

        ctx = parse_ctx("$.mapBin.a.bb")
        # Returns [CTX.map_key("a"), CTX.map_key("bb")]
    """
    if not path:
        raise AelParseException("Path must not be null or empty")

    try:
        input_stream = InputStream(path)
        lexer = ConditionLexer(input_stream)
        lexer.removeErrorListeners()
        error_listener = _AELParseErrorListener()
        lexer.addErrorListener(error_listener)
        token_stream = CommonTokenStream(lexer)
        parser = ConditionParser(token_stream)
        parser.removeErrorListeners()
        parser.addErrorListener(error_listener)
        parse_tree = parser.parse()

        # Use visitor in ctx_only mode to preserve CDTPath without finalization
        visitor = ExpressionConditionVisitor(ctx_only=True)
        result = visitor.visit(parse_tree)

        # Check if result is a full expression (has comparison operator)
        if isinstance(result, FilterExpression):
            raise AelParseException(
                "Unsupported input expression type 'EXPRESSION_CONTAINER', "
                "please provide only path to convert to CTX[]"
            )

        # Handle DeferredBin (bare bin name like $.listBin)
        if isinstance(result, DeferredBin):
            raise AelParseException("CDT context is not provided")

        # Check if result is a CDTPath
        if not isinstance(result, CDTPath):
            raise AelParseException("Could not parse the given AEL path input")

        # Check for path functions (get, asInt, etc.)
        if result.explicit_type is not None or result.has_path_function:
            raise AelParseException(
                "Path function is unsupported, please provide only path to convert to CTX[]"
            )

        # Check that CDT context is provided (not just a bin name)
        if not result.parts:
            raise AelParseException("CDT context is not provided")

        # Convert CDTPath parts to CTX list
        ctx_list: List[CTX] = []
        for part in result.parts:
            if isinstance(part, ListIndexPart):
                ctx_list.append(CTX.list_index(part.index))
            elif isinstance(part, ListRankPart):
                ctx_list.append(CTX.list_rank(part.rank))
            elif isinstance(part, ListValuePart):
                ctx_list.append(CTX.list_value(part.value))
            elif isinstance(part, MapKeyPart):
                ctx_list.append(CTX.map_key(part.key))
            elif isinstance(part, MapIndexPart):
                ctx_list.append(CTX.map_index(part.index))
            elif isinstance(part, MapRankPart):
                ctx_list.append(CTX.map_rank(part.rank))
            elif isinstance(part, MapValuePart):
                ctx_list.append(CTX.map_value(part.value))
            else:
                raise AelParseException(f"Unsupported CDT part type: {type(part).__name__}")

        return ctx_list

    except AelParseException:
        raise
    except Exception as e:
        raise AelParseException(f"Could not parse the given AEL path input: {e}") from e


def parse_ael_with_index(
    expression: str,
    index_context: Optional[IndexContext] = None,
    placeholder_values: Optional[Sequence[Any]] = None,
    *,
    hint_index_name: Optional[str] = None,
    hint_bin_name: Optional[str] = None,
) -> ParseResult:
    """Parse an AEL expression and generate optimal Filter + Exp based on available indexes.

    This function analyzes the AEL expression and available secondary indexes to:
    1. Extract parts that can use secondary index Filters (more efficient)
    2. Return remaining parts as filter Exp (for post-filtering)

    The algorithm:
    1. Build an expression tree that tracks filter eligibility
    2. Mark nodes under OR as "excluded from filter" (can't use secondary index)
    3. Collect all filterable expressions grouped by cardinality
    4. Choose the best by cardinality (or alphabetically if tied)
    5. Generate complementary Exp, skipping the part used for Filter

    Rules for filter generation:
    - AND expressions: One part can become Filter, rest becomes Exp
    - OR expressions: Cannot use Filter (need to evaluate both branches)
    - Nested AND inside OR: The AND parts are still excluded
    - AND(a, OR(b, c)): 'a' can become Filter, OR(b,c) becomes Exp
    - Only simple comparisons (==, >, <, >=, <=) on indexed bins can become Filters
    - String comparisons (>, <, etc.) are not supported by secondary index

    Args:
        expression: The AEL expression string.
        index_context: IndexContext with available indexes. If None, only Exp is returned.
        placeholder_values: Optional sequence of values for placeholders (?0, ?1, etc.)
        hint_index_name: Force the generated Filter to target this named
            secondary index via ``Filter.\\*_by_index()``.
        hint_bin_name: Override the bin name used for the generated Filter.

    Returns:
        ParseResult containing:

        - filter: Secondary index Filter (or None if not applicable)
        - exp: Filter expression for remaining parts (or None if fully covered)

    Example::

        Build an :class:`~aerospike_sdk.ael.filter_gen.IndexContext` with
        :class:`~aerospike_sdk.ael.filter_gen.Index` entries (see ``filter_gen``),
        then pass it as *index_context*::

            ctx = IndexContext.of("test", [...])
            result = parse_ael_with_index("$.intBin1 > 100 and $.intBin2 < 1000", ctx)
            result = parse_ael_with_index("$.intBin1 > ?0", ctx, (100,))
    """
    generator = FilterGenerator(index_context)
    pv = PlaceholderValues(*placeholder_values) if placeholder_values else None
    return generator.generate(
        expression,
        pv,
        hint_index_name=hint_index_name,
        hint_bin_name=hint_bin_name,
    )
