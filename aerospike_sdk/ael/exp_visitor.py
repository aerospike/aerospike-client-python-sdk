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

"""AEL expression visitor: converts ANTLR parse tree to FilterExpression objects.

This module contains the visitor that walks the ANTLR parse tree and converts
it into Aerospike FilterExpression objects.

Type Inference:
    Bin types are inferred from the comparison operand. For example,
    `$.A == 1` will use int_bin("A") because 1 is an integer. Explicit
    casts like `$.A.asInt()` override inference.
"""

from __future__ import annotations

import base64
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, List, Optional, Protocol, Sequence, Union, cast

from aerospike_async import (
    CTX,
    ExpType,
    FilterExpression,
    ListReturnType,
    MapReturnType,
)

from aerospike_sdk.ael.antlr4.generated.ConditionParser import ConditionParser
from aerospike_sdk.ael.antlr4.generated.ConditionVisitor import ConditionVisitor
from aerospike_sdk.ael.exceptions import AelParseException


class _AntlrText(Protocol):
    """ANTLR parse nodes that expose ``getText()`` (parser or terminal)."""

    def getText(self) -> str: ...


class InferredType(Enum):
    """Type hints for expression inference."""
    INT = auto()
    FLOAT = auto()
    STRING = auto()
    BOOL = auto()
    BLOB = auto()
    LIST = auto()
    MAP = auto()
    GEO = auto()
    HLL = auto()
    UNKNOWN = auto()


_INFERRED_TO_EXP_TYPE: dict[InferredType, ExpType] = {
    InferredType.INT: ExpType.INT,
    InferredType.FLOAT: ExpType.FLOAT,
    InferredType.STRING: ExpType.STRING,
    InferredType.BOOL: ExpType.BOOL,
    InferredType.LIST: ExpType.LIST,
    InferredType.MAP: ExpType.MAP,
    InferredType.BLOB: ExpType.BLOB,
    InferredType.GEO: ExpType.GEO,
    InferredType.HLL: ExpType.HLL,
}


@dataclass
class DeferredBin:
    """Represents a bin reference that hasn't been typed yet.
    
    The bin type will be inferred from context (comparison operand) or
    defaulted to INT.
    """
    name: str
    explicit_type: Optional[InferredType] = None
    
    def to_expression(self, inferred_type: InferredType = InferredType.INT) -> FilterExpression:
        """Convert to FilterExpression with the given type.

        If explicit_type is set (via .asInt(), .asFloat(), etc.), use that.
        Otherwise use the inferred type.
        """
        type_to_use = self.explicit_type if self.explicit_type else inferred_type

        if type_to_use == InferredType.INT:
            return FilterExpression.int_bin(self.name)
        elif type_to_use == InferredType.FLOAT:
            return FilterExpression.float_bin(self.name)
        elif type_to_use == InferredType.BOOL:
            return FilterExpression.bool_bin(self.name)
        elif type_to_use == InferredType.STRING:
            return FilterExpression.string_bin(self.name)
        elif type_to_use == InferredType.BLOB:
            return FilterExpression.blob_bin(self.name)
        elif type_to_use == InferredType.LIST:
            return FilterExpression.list_bin(self.name)
        elif type_to_use == InferredType.MAP:
            return FilterExpression.map_bin(self.name)
        elif type_to_use == InferredType.GEO:
            return FilterExpression.geo_bin(self.name)
        elif type_to_use == InferredType.HLL:
            return FilterExpression.hll_bin(self.name)
        else:
            return FilterExpression.int_bin(self.name)


@dataclass
class TypedExpr:
    """Wrapper for FilterExpression with type hint for inference.

    Since FilterExpression is a PyO3 object that doesn't allow attribute setting,
    we wrap it with type information for inference during parsing.
    Optional value stores the raw Python value for constants (e.g. string for
    base64 decoding when comparing to BLOB).
    """
    expr: FilterExpression
    type_hint: InferredType
    value: Optional[Any] = None


class ArithOp(Enum):
    """Arithmetic operation types."""
    ADD = auto()
    SUB = auto()
    MUL = auto()
    DIV = auto()
    MOD = auto()
    ABS = auto()
    MAX = auto()
    MIN = auto()


@dataclass
class DeferredArithmetic:
    """Represents an arithmetic expression with deferred type resolution.

    The type is determined from context (comparison operand) at the point
    where the expression is used, allowing float inference to propagate.
    """
    op: ArithOp
    operands: List[Any]  # Can contain DeferredBin, DeferredArithmetic, or FilterExpression

    def to_expression(self, inferred_type: InferredType = InferredType.INT) -> FilterExpression:
        """Convert to FilterExpression with the given type."""
        # Promote to FLOAT if any resolved child is already FLOAT-typed,
        # so that deferred bins are read with float_bin instead of int_bin.
        effective_type = inferred_type
        for operand in self.operands:
            if isinstance(operand, TypedExpr) and operand.type_hint == InferredType.FLOAT:
                effective_type = InferredType.FLOAT
                break

        resolved_operands = []
        for operand in self.operands:
            if isinstance(operand, DeferredBin):
                resolved_operands.append(operand.to_expression(effective_type))
            elif isinstance(operand, DeferredArithmetic):
                resolved_operands.append(operand.to_expression(effective_type))
            elif isinstance(operand, TypedExpr):
                resolved_operands.append(operand.expr)
            else:
                resolved_operands.append(operand)

        if self.op == ArithOp.ADD:
            return FilterExpression.num_add(resolved_operands)
        elif self.op == ArithOp.SUB:
            return FilterExpression.num_sub(resolved_operands)
        elif self.op == ArithOp.MUL:
            return FilterExpression.num_mul(resolved_operands)
        elif self.op == ArithOp.DIV:
            return FilterExpression.num_div(resolved_operands)
        elif self.op == ArithOp.MOD:
            return FilterExpression.num_mod(resolved_operands[0], resolved_operands[1])
        elif self.op == ArithOp.ABS:
            return FilterExpression.num_abs(resolved_operands[0])
        elif self.op == ArithOp.MAX:
            return FilterExpression.max(resolved_operands)
        elif self.op == ArithOp.MIN:
            return FilterExpression.min(resolved_operands)
        else:
            raise AelParseException(f"Unknown arithmetic operation: {self.op}")


# =============================================================================
# CDT Path Parts - Represent list/map access in paths like $.bin[0] or $.bin.key
# =============================================================================

class CDTPart(ABC):
    """Abstract base class for CDT path parts."""
    
    @abstractmethod
    def get_context(self) -> CTX:
        """Get the CTX for this path part (used for nested operations)."""
        pass
    
    @abstractmethod
    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        """Construct the final FilterExpression for this CDT access.
        
        Args:
            bin_name: The name of the bin containing the CDT
            value_type: The expected type of the value being accessed
            return_type: The return type for the CDT operation
            ctx: Context array for nested operations (preceding parts)
            bin_expr: Optional pre-built bin expression (for mixed CDT paths)
        """
        pass


@dataclass
class ListIndexPart(CDTPart):
    """List access by index: $.myList[0] or $.myList[-1]"""
    index: int
    
    def get_context(self) -> CTX:
        return CTX.list_index(self.index)
    
    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if bin_expr is None:
            bin_expr = FilterExpression.list_bin(bin_name)
        return FilterExpression.list_get_by_index(
            return_type,
            value_type,
            FilterExpression.int_val(self.index),
            bin_expr,
            list(ctx),
        )


@dataclass
class ListRankPart(CDTPart):
    """List access by rank: $.myList[#0] (smallest) or $.myList[#-1] (largest)"""
    rank: int

    def get_context(self) -> CTX:
        return CTX.list_rank(self.rank)

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if bin_expr is None:
            bin_expr = FilterExpression.list_bin(bin_name)
        return FilterExpression.list_get_by_rank(
            return_type,
            value_type,
            FilterExpression.int_val(self.rank),
            bin_expr,
            list(ctx),
        )


@dataclass
class ListValuePart(CDTPart):
    """List access by value: $.myList.[=100]"""
    value: Any
    inverted: bool = False

    def get_context(self) -> CTX:
        # Value-based access doesn't have a simple context
        raise NotImplementedError("ListValuePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.list_bin(bin_name)
        if isinstance(self.value, int):
            val_expr = FilterExpression.int_val(self.value)
        elif isinstance(self.value, float):
            val_expr = FilterExpression.float_val(self.value)
        elif isinstance(self.value, str):
            val_expr = FilterExpression.string_val(self.value)
        elif isinstance(self.value, bool):
            val_expr = FilterExpression.bool_val(self.value)
        else:
            val_expr = FilterExpression.int_val(int(self.value))
        return FilterExpression.list_get_by_value(
            return_type,
            val_expr,
            base_bin,
            list(ctx),
        )


@dataclass
class ListIndexRangePart(CDTPart):
    """List access by index range: $.myList.[1:3] or $.myList.[1:]"""
    start: int
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListIndexRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        if self.count is not None:
            return FilterExpression.list_get_by_index_range_count(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.int_val(self.count),
                FilterExpression.list_bin(bin_name),
                list(ctx),
            )
        else:
            return FilterExpression.list_get_by_index_range(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.list_bin(bin_name),
                list(ctx),
            )


@dataclass
class ListValueRangePart(CDTPart):
    """List access by value range: $.myList.[=10:20] or $.myList.[=10:]"""
    value_begin: Optional[Any] = None
    value_end: Optional[Any] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListValueRangePart cannot be used as context")

    @staticmethod
    def _make_val_expr(value: Any) -> Optional[FilterExpression]:
        if value is None:
            return None
        if isinstance(value, int):
            return FilterExpression.int_val(value)
        elif isinstance(value, float):
            return FilterExpression.float_val(value)
        elif isinstance(value, str):
            return FilterExpression.string_val(value)
        else:
            return FilterExpression.int_val(int(value))

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        return FilterExpression.list_get_by_value_range(
            return_type,
            self._make_val_expr(self.value_begin),
            self._make_val_expr(self.value_end),
            bin_expr if bin_expr else FilterExpression.list_bin(bin_name),
            list(ctx),
        )


@dataclass
class ListValueListPart(CDTPart):
    """List access by value list: $.myList.[=a,b,c]"""
    values: List[Any] = field(default_factory=list)
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListValueListPart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        # Create a list value expression
        return FilterExpression.list_get_by_value_list(
            return_type,
            FilterExpression.list_val(self.values),
            bin_expr if bin_expr else FilterExpression.list_bin(bin_name),
            list(ctx),
        )


@dataclass
class ListRankRangePart(CDTPart):
    """List access by rank range: $.myList.[#0:3] or $.myList.[#-3:]"""
    start: int
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListRankRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.list_bin(bin_name)
        if self.count is not None:
            return FilterExpression.list_get_by_rank_range_count(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.list_get_by_rank_range(
                return_type,
                FilterExpression.int_val(self.start),
                base_bin,
                list(ctx),
            )


@dataclass
class MapKeyPart(CDTPart):
    """Map access by key: $.myMap.key, $.myMap.42, or $.myMap.\"my key\"."""
    key: Any  # str | int | bytes — typed at parse time

    def get_context(self) -> CTX:
        return CTX.map_key(self.key)

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if bin_expr is None:
            bin_expr = FilterExpression.map_bin(bin_name)
        return FilterExpression.map_get_by_key(
            return_type,
            value_type,
            _object_to_exp(self.key),
            bin_expr,
            list(ctx),
        )


@dataclass
class MapIndexPart(CDTPart):
    """Map access by index: $.myMap{0}"""
    index: int
    
    def get_context(self) -> CTX:
        return CTX.map_index(self.index)
    
    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        return FilterExpression.map_get_by_index(
            return_type,
            value_type,
            FilterExpression.int_val(self.index),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapRankPart(CDTPart):
    """Map access by rank: $.myMap{#0}"""
    rank: int

    def get_context(self) -> CTX:
        return CTX.map_rank(self.rank)

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        return FilterExpression.map_get_by_rank(
            return_type,
            value_type,
            FilterExpression.int_val(self.rank),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapValuePart(CDTPart):
    """Map access by value: $.myMap.{=100}"""
    value: Any
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapValuePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        if isinstance(self.value, int):
            val_expr = FilterExpression.int_val(self.value)
        elif isinstance(self.value, float):
            val_expr = FilterExpression.float_val(self.value)
        elif isinstance(self.value, str):
            val_expr = FilterExpression.string_val(self.value)
        elif isinstance(self.value, bool):
            val_expr = FilterExpression.bool_val(self.value)
        else:
            val_expr = FilterExpression.int_val(int(self.value))
        return FilterExpression.map_get_by_value(
            return_type,
            val_expr,
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapKeyRangePart(CDTPart):
    """Map access by key range: $.myMap.{a-c} or $.myMap.{1-5}."""
    key_begin: Optional[Any] = None  # str | int | bytes | None
    key_end: Optional[Any] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapKeyRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        begin_expr = _object_to_exp(self.key_begin) if self.key_begin is not None else None
        end_expr = _object_to_exp(self.key_end) if self.key_end is not None else None
        return FilterExpression.map_get_by_key_range(
            return_type,
            begin_expr,
            end_expr,
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapKeyListPart(CDTPart):
    """Map access by key list: $.myMap.{a,b,c} or $.myMap.{1,2,3}."""
    keys: List[Any] = field(default_factory=list)  # each str | int | bytes
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapKeyListPart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        return FilterExpression.map_get_by_key_list(
            return_type,
            FilterExpression.list_val(list(self.keys)),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapIndexRangePart(CDTPart):
    """Map access by index range: $.myMap.{1:3}"""
    start: int
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapIndexRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.map_bin(bin_name)
        if self.count is not None:
            return FilterExpression.map_get_by_index_range_count(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.map_get_by_index_range(
                return_type,
                FilterExpression.int_val(self.start),
                base_bin,
                list(ctx),
            )


@dataclass
class MapValueRangePart(CDTPart):
    """Map access by value range: $.myMap.{=10:20}"""
    value_begin: Optional[Any] = None
    value_end: Optional[Any] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapValueRangePart cannot be used as context")

    @staticmethod
    def _make_val_expr(value: Any) -> Optional[FilterExpression]:
        if value is None:
            return None
        if isinstance(value, int):
            return FilterExpression.int_val(value)
        elif isinstance(value, float):
            return FilterExpression.float_val(value)
        elif isinstance(value, str):
            return FilterExpression.string_val(value)
        else:
            return FilterExpression.int_val(int(value))

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        return FilterExpression.map_get_by_value_range(
            return_type,
            self._make_val_expr(self.value_begin),
            self._make_val_expr(self.value_end),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapValueListPart(CDTPart):
    """Map access by value list: $.myMap.{=a,b,c}"""
    values: List[Any] = field(default_factory=list)
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapValueListPart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        return FilterExpression.map_get_by_value_list(
            return_type,
            FilterExpression.list_val(self.values),
            bin_expr if bin_expr else FilterExpression.map_bin(bin_name),
            list(ctx),
        )


@dataclass
class MapRankRangePart(CDTPart):
    """Map access by rank range: $.myMap.{#0:3}"""
    start: int
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapRankRangePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.map_bin(bin_name)
        if self.count is not None:
            return FilterExpression.map_get_by_rank_range_count(
                return_type,
                FilterExpression.int_val(self.start),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.map_get_by_rank_range(
                return_type,
                FilterExpression.int_val(self.start),
                base_bin,
                list(ctx),
            )


@dataclass
class ListRankRangeRelativePart(CDTPart):
    """List access by value-relative rank range: $.list.[#-3:-1~b] (rank -3 to -1 relative to value b)"""
    rank: int
    value: Any
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("ListRankRangeRelativePart cannot be used as context")

    @staticmethod
    def _make_val_expr(value: Any) -> FilterExpression:
        if isinstance(value, int):
            return FilterExpression.int_val(value)
        elif isinstance(value, float):
            return FilterExpression.float_val(value)
        elif isinstance(value, str):
            return FilterExpression.string_val(value)
        else:
            return FilterExpression.string_val(str(value))

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, ListReturnType):
            return_type = ListReturnType.VALUE
        if self.inverted:
            return_type = return_type | ListReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.list_bin(bin_name)
        if self.count is not None:
            return FilterExpression.list_get_by_value_relative_rank_range_count(
                return_type,
                self._make_val_expr(self.value),
                FilterExpression.int_val(self.rank),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.list_get_by_value_relative_rank_range(
                return_type,
                self._make_val_expr(self.value),
                FilterExpression.int_val(self.rank),
                base_bin,
                list(ctx),
            )


@dataclass
class MapRankRangeRelativePart(CDTPart):
    """Map access by value-relative rank range: $.map.{#-1:1~10} (rank -1 to 1 relative to value 10)"""
    rank: int
    value: Any
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapRankRangeRelativePart cannot be used as context")

    @staticmethod
    def _make_val_expr(value: Any) -> FilterExpression:
        if isinstance(value, int):
            return FilterExpression.int_val(value)
        elif isinstance(value, float):
            return FilterExpression.float_val(value)
        elif isinstance(value, str):
            return FilterExpression.string_val(value)
        else:
            return FilterExpression.string_val(str(value))

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.map_bin(bin_name)
        if self.count is not None:
            return FilterExpression.map_get_by_value_relative_rank_range_count(
                return_type,
                self._make_val_expr(self.value),
                FilterExpression.int_val(self.rank),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.map_get_by_value_relative_rank_range(
                return_type,
                self._make_val_expr(self.value),
                FilterExpression.int_val(self.rank),
                base_bin,
                list(ctx),
            )


@dataclass
class MapIndexRangeRelativePart(CDTPart):
    """Map access by key-relative index range: $.map.{0:1~a} (index 0 to 1 relative to key a)."""
    index: int
    key: Any  # str | int | bytes
    count: Optional[int] = None
    inverted: bool = False

    def get_context(self) -> CTX:
        raise NotImplementedError("MapIndexRangeRelativePart cannot be used as context")

    def construct_expr(
        self,
        bin_name: str,
        value_type: ExpType,
        return_type: Union[ListReturnType, MapReturnType],
        ctx: Sequence[CTX],
        bin_expr: Optional[FilterExpression] = None,
    ) -> FilterExpression:
        if not isinstance(return_type, MapReturnType):
            return_type = MapReturnType.VALUE
        if self.inverted:
            return_type = return_type | MapReturnType.INVERTED
        base_bin = bin_expr if bin_expr else FilterExpression.map_bin(bin_name)
        if self.count is not None:
            return FilterExpression.map_get_by_key_relative_index_range_count(
                return_type,
                _object_to_exp(self.key),
                FilterExpression.int_val(self.index),
                FilterExpression.int_val(self.count),
                base_bin,
                list(ctx),
            )
        else:
            return FilterExpression.map_get_by_key_relative_index_range(
                return_type,
                _object_to_exp(self.key),
                FilterExpression.int_val(self.index),
                base_bin,
                list(ctx),
            )


@dataclass
class CDTPath:
    """Represents a complete CDT path: bin name + list of CDT parts.
    
    Examples:
        $.myList[0] -> CDTPath("myList", [ListIndexPart(0)])
        $.myMap.key -> CDTPath("myMap", [MapKeyPart("key")])
        $.nested[0].key -> CDTPath("nested", [ListIndexPart(0), MapKeyPart("key")])
    """
    bin_name: str
    parts: List[CDTPart] = field(default_factory=list)
    value_type: ExpType = ExpType.INT  # Default, can be overridden by .get(type:...)
    explicit_type: Optional[InferredType] = None  # For .get(type: X) type override
    has_path_function: bool = False  # True if .get(), .asInt(), etc. was applied
    cast_wrap: Optional[str] = None  # "to_int" or "to_float" — post-read conversion
    list_return_type: Optional[ListReturnType] = None  # From .get(return: COUNT|EXISTS|INDEX)
    map_return_type: Optional[MapReturnType] = None

    def to_expression(self, inferred_type: InferredType = InferredType.INT) -> FilterExpression:
        """Convert the CDT path to a FilterExpression.
        
        If no CDT parts, returns a simple bin expression.
        If CDT parts exist, returns a CDT get expression.
        """
        if not self.parts:
            # No CDT parts - just a bin reference
            type_to_use = self.explicit_type if self.explicit_type else inferred_type
            if type_to_use == InferredType.INT:
                return FilterExpression.int_bin(self.bin_name)
            elif type_to_use == InferredType.FLOAT:
                return FilterExpression.float_bin(self.bin_name)
            elif type_to_use == InferredType.BOOL:
                return FilterExpression.bool_bin(self.bin_name)
            elif type_to_use == InferredType.STRING:
                return FilterExpression.string_bin(self.bin_name)
            else:
                return FilterExpression.int_bin(self.bin_name)
        
        # Has CDT parts - build the expression
        # Build context from all parts except the last one
        ctx: List[CTX] = []
        for part in self.parts[:-1]:
            ctx.append(part.get_context())
        
        # When get(return: COUNT|EXISTS|INDEX) was used, value_type was set in _apply_get_params
        if self.list_return_type is not None or self.map_return_type is not None:
            exp_type = self.value_type
        else:
            type_to_use = self.explicit_type if self.explicit_type else inferred_type
            exp_type = _INFERRED_TO_EXP_TYPE.get(type_to_use, self.value_type)
        
        # Determine bin type from FIRST part (not last)
        # If first part is a map access, base bin is map_bin; if list, base bin is list_bin
        first_part = self.parts[0]
        is_map_base = isinstance(first_part, (MapKeyPart, MapIndexPart, MapRankPart,
                                               MapValuePart, MapKeyRangePart, MapKeyListPart,
                                               MapIndexRangePart, MapValueRangePart, MapValueListPart,
                                               MapRankRangePart, MapRankRangeRelativePart,
                                               MapIndexRangeRelativePart))
        
        # Create the appropriate bin expression
        bin_expr = FilterExpression.map_bin(self.bin_name) if is_map_base else FilterExpression.list_bin(self.bin_name)
        
        # Use the last part to construct the expression
        last_part = self.parts[-1]
        list_ret = self.list_return_type if self.list_return_type is not None else ListReturnType.VALUE
        map_ret = self.map_return_type if self.map_return_type is not None else MapReturnType.VALUE
        if isinstance(last_part, (ListIndexPart, ListRankPart, ListValuePart,
                                   ListIndexRangePart, ListValueRangePart, ListValueListPart,
                                   ListRankRangePart, ListRankRangeRelativePart)):
            result = last_part.construct_expr(
                self.bin_name, exp_type, list_ret, ctx,
                bin_expr=bin_expr
            )
        else:
            result = last_part.construct_expr(
                self.bin_name, exp_type, map_ret, ctx,
                bin_expr=bin_expr
            )

        if self.cast_wrap == "to_int":
            return FilterExpression.to_int(result)
        elif self.cast_wrap == "to_float":
            return FilterExpression.to_float(result)
        return result


# Type alias for visitor return types (can be deferred, typed, CDT path, or raw expression)
ExprOrDeferred = Union[
    FilterExpression,
    DeferredBin,
    DeferredArithmetic,
    TypedExpr,
    CDTPath,
    None,
]


def _unquote(text: str) -> str:
    """Remove quotes from a quoted string."""
    if (text.startswith("'") and text.endswith("'")) or (text.startswith('"') and text.endswith('"')):
        return text[1:-1]
    return text


def _reject_bin_name_containing_null(bin_name: str) -> None:
    """Bin names containing ``null`` (case-insensitive) are reserved."""
    if "null" in bin_name.lower():
        raise AelParseException(
            f"Bin name must not contain the reserved word 'null': {bin_name}"
        )


def _extract_bin_name(ctx) -> str:
    """Resolve a ``binPart`` parser context to its bin-name string.

    BIN_IDENTIFIER and NAME_IDENTIFIER pass through; QUOTED_STRING is unquoted
    (and rejected if empty); IN is lowercased to preserve historical behavior;
    a reservedWord alternative falls through to ``ctx.getText()``. The
    result is checked for the reserved 'null' substring.
    """
    if ctx.BIN_IDENTIFIER() is not None:
        bin_name = ctx.BIN_IDENTIFIER().getText()
    elif ctx.NAME_IDENTIFIER() is not None:
        bin_name = ctx.NAME_IDENTIFIER().getText()
    elif ctx.QUOTED_STRING() is not None:
        quoted = ctx.QUOTED_STRING().getText()
        if len(quoted) <= 2:
            raise AelParseException("Bin name must not be empty")
        bin_name = _unquote(quoted)
    elif ctx.IN() is not None:
        bin_name = ctx.IN().getText().lower()
    else:
        bin_name = ctx.getText()
    _reject_bin_name_containing_null(bin_name)
    return bin_name


def _parse_int_literal(text: str) -> int:
    """Parse a decimal, hex (``0x...``), or binary (``0b...``) integer token.

    Handles a leading ``+`` / ``-`` sign as may appear in ``LEADING_DOT_SIGNED_INT``.
    """
    sign = 1
    body = text
    if body and body[0] in "+-":
        if body[0] == "-":
            sign = -1
        body = body[1:]
    try:
        if body[:2] in ("0x", "0X"):
            return sign * int(body[2:], 16)
        if body[:2] in ("0b", "0B"):
            return sign * int(body[2:], 2)
        return sign * int(body, 10)
    except ValueError as e:
        raise AelParseException(f"Invalid integer literal: {text}") from e


def _object_to_exp(value: Any) -> FilterExpression:
    """Convert a parsed Python value into a ``FilterExpression`` value node.

    Supports the primitive types AEL currently lifts from the grammar:
    ``str``, ``int``, ``bool``, and ``bytes``. Floats use the canonical
    ``float_val`` constructor. Anything else is rejected.
    """
    if isinstance(value, bool):
        return FilterExpression.bool_val(value)
    if isinstance(value, int):
        return FilterExpression.int_val(value)
    if isinstance(value, str):
        return FilterExpression.string_val(value)
    if isinstance(value, float):
        return FilterExpression.float_val(value)
    if isinstance(value, (bytes, bytearray)):
        return FilterExpression.blob_val(list(value))
    raise AelParseException(
        f"Unsupported value type for Exp conversion: {type(value).__name__}"
    )


def _get_type_hint(expr: ExprOrDeferred) -> InferredType:
    """Get the type hint from an expression for type inference.
    
    Returns the explicit_type if expr is a DeferredBin/CDTPath with one set.
    Returns the type if expr is a TypedExpr with a known type.
    Returns UNKNOWN for raw FilterExpression or None.
    """
    if isinstance(expr, CDTPath):
        if expr.cast_wrap is not None:
            return InferredType.INT if expr.cast_wrap == "to_int" else InferredType.FLOAT
        if expr.explicit_type is not None:
            return expr.explicit_type
        return InferredType.UNKNOWN
    if isinstance(expr, DeferredBin):
        if expr.explicit_type is not None:
            return expr.explicit_type
        return InferredType.UNKNOWN
    if isinstance(expr, TypedExpr):
        return expr.type_hint
    # Raw FilterExpression - no type info available
    return InferredType.UNKNOWN


def _is_float_context(expr: ExprOrDeferred) -> bool:
    """Check if an expression carries float type information."""
    return _get_type_hint(expr) == InferredType.FLOAT


def _unwrap_expr(expr: ExprOrDeferred) -> Optional[FilterExpression]:
    """Unwrap a TypedExpr to get the underlying FilterExpression."""
    if isinstance(expr, TypedExpr):
        return expr.expr
    if isinstance(expr, FilterExpression):
        return expr
    return None


def _resolve_deferred(expr: ExprOrDeferred, inferred_type: InferredType) -> FilterExpression:
    """Resolve a DeferredBin, CDTPath, or DeferredArithmetic to a FilterExpression."""
    if isinstance(expr, DeferredBin):
        return expr.to_expression(inferred_type)
    if isinstance(expr, CDTPath):
        return expr.to_expression(inferred_type)
    if isinstance(expr, DeferredArithmetic):
        return expr.to_expression(inferred_type)
    if isinstance(expr, TypedExpr):
        return expr.expr
    if isinstance(expr, FilterExpression):
        return expr
    raise AelParseException("Cannot resolve None expression")


def _resolve_for_comparison(left: ExprOrDeferred, right: ExprOrDeferred) -> tuple[FilterExpression, FilterExpression]:
    """Resolve deferred bins/CDT paths based on comparison operand types.

    If one side is a DeferredBin/CDTPath/DeferredArithmetic and the other has
    a known type, use that type for resolution.
    """
    left_hint = _get_type_hint(left)
    right_hint = _get_type_hint(right)

    # Reject incompatible type pairs for comparison
    if left_hint != InferredType.UNKNOWN and right_hint != InferredType.UNKNOWN:
        _validate_comparison_types(left_hint, right_hint)

    # Resolve left side
    if isinstance(left, (DeferredBin, CDTPath, DeferredArithmetic)):
        # Infer from right side, default to INT
        inferred = right_hint if right_hint != InferredType.UNKNOWN else InferredType.INT
        resolved_left = _resolve_deferred(left, inferred)
    elif left is None:
        raise AelParseException("Left operand cannot be None")
    else:
        resolved_left = _unwrap_expr(left)
        if resolved_left is None:
            raise AelParseException("Failed to unwrap left operand")

    # Resolve right side
    if isinstance(right, (DeferredBin, CDTPath, DeferredArithmetic)):
        # Infer from left side, default to INT
        inferred = left_hint if left_hint != InferredType.UNKNOWN else InferredType.INT
        resolved_right = _resolve_deferred(right, inferred)
    elif right is None:
        raise AelParseException("Right operand cannot be None")
    else:
        resolved_right = _unwrap_expr(right)
        if resolved_right is None:
            raise AelParseException("Failed to unwrap right operand")

    # BLOB vs string constant: decode quoted string as base64 for blob_val
    if left_hint == InferredType.BLOB and isinstance(right, TypedExpr) and right.type_hint == InferredType.STRING and right.value is not None:
        try:
            decoded = base64.b64decode(right.value)
            resolved_right = FilterExpression.blob_val(list(decoded))
        except Exception:
            raise AelParseException("Invalid base64 for BLOB comparison")
    if right_hint == InferredType.BLOB and isinstance(left, TypedExpr) and left.type_hint == InferredType.STRING and left.value is not None:
        try:
            decoded = base64.b64decode(left.value)
            resolved_left = FilterExpression.blob_val(list(decoded))
        except Exception:
            raise AelParseException("Invalid base64 for BLOB comparison")

    return resolved_left, resolved_right


def _infer_element_type(values: list) -> InferredType:
    """Infer element type from a list of Python values.

    Returns the type of the first non-None element, or UNKNOWN if empty.
    Raises AelParseException if elements have mixed types.
    """
    inferred = InferredType.UNKNOWN
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            current = InferredType.BOOL
        elif isinstance(v, int):
            current = InferredType.INT
        elif isinstance(v, float):
            current = InferredType.FLOAT
        elif isinstance(v, str):
            current = InferredType.STRING
        elif isinstance(v, list):
            current = InferredType.LIST
        elif isinstance(v, dict):
            current = InferredType.MAP
        else:
            current = InferredType.UNKNOWN
        if inferred == InferredType.UNKNOWN:
            inferred = current
        elif inferred != current:
            raise AelParseException(
                f"IN list elements must all be of the same type; "
                f"found {inferred.name} and {current.name}"
            )
    return inferred


def _resolve_for_in_value(value: ExprOrDeferred, list_expr: ExprOrDeferred) -> FilterExpression:
    """Resolve the left operand of an IN expression (the value to search for).

    Type inference priority:
      1. Left operand's own type (if explicit/known)
      2. Element type inferred from a right-side list constant
      3. Reject if left is a bin/path with unknown type (ambiguous)
      4. Default to STRING for already-resolved expressions
    """
    if value is None:
        raise AelParseException("Left operand of 'in' cannot be None")

    hint = _get_type_hint(value)
    if hint != InferredType.UNKNOWN:
        return _resolve_deferred(value, hint)

    if isinstance(list_expr, TypedExpr) and list_expr.type_hint == InferredType.LIST:
        if isinstance(list_expr.value, list) and list_expr.value:
            element_type = _infer_element_type(list_expr.value)
            if element_type != InferredType.UNKNOWN:
                return _resolve_deferred(value, element_type)

    if isinstance(value, (DeferredBin, CDTPath)):
        raise AelParseException(
            "cannot infer the type of the left operand for IN operation; "
            "use .get(type: ...) or .asInt()/.asFloat() to specify the type"
        )

    return _resolve_deferred(value, InferredType.STRING)


def _resolve_for_hll(expr: ExprOrDeferred) -> FilterExpression:
    """Resolve a bare path / typed expression as an HLL-typed FilterExpression.

    Used as the receiver of an HLL path function (the ``$.h`` in
    ``$.h.hllCount()``). Bare bin references are wrapped as ``hll_bin(name)``;
    already-resolved expressions are passed through unchanged.
    """
    if isinstance(expr, DeferredBin):
        return FilterExpression.hll_bin(expr.name)
    if isinstance(expr, TypedExpr):
        return expr.expr
    if isinstance(expr, FilterExpression):
        return expr
    resolved = _finalize_result(expr, InferredType.HLL)
    if resolved is None:
        raise AelParseException("Failed to resolve HLL receiver expression")
    return resolved


def _resolve_for_list(expr: ExprOrDeferred) -> FilterExpression:
    """Resolve any expression as a LIST-typed FilterExpression.

    Used for AEL arguments that must be a list — e.g. the values argument to
    ``hllMayContain([...])``. List literals come through as ``TypedExpr`` with
    ``InferredType.LIST``; bin references are resolved via ``list_bin``.
    """
    if isinstance(expr, DeferredBin):
        return FilterExpression.list_bin(expr.name)
    if isinstance(expr, TypedExpr):
        return expr.expr
    if isinstance(expr, FilterExpression):
        return expr
    resolved = _finalize_result(expr, InferredType.LIST)
    if resolved is None:
        raise AelParseException("Failed to resolve list expression")
    return resolved


def _resolve_for_hll_list(expr: ExprOrDeferred) -> FilterExpression:
    """Resolve the ``hlls`` argument of a multi-HLL AEL function.

    Acceptable shapes:

    - **Single HLL bin reference** — ``$.a``: passed through as
      ``hll_bin('a')``. The server's HLL ops treat a bare HLL value as an
      implicit single-element list, so ``HLLExp.getUnion(hllBin('a'), hllBin('b'))``-style
      composition works without an outer list wrapper.
    - **Literal byte-blob list** — ``[?0, ?1]`` or an inline list of bytes,
      packed by :meth:`visitListConstant` into ``list_val(...)``.
    - **Placeholder** bound to a Python list of bytes (``?0``).

    Not supported: ``[$.a, $.b]`` (list of bin references). The Aerospike
    server expression VM does not recursively dereference scalar bin
    expressions inside a composed list when that list is fed to an HLL op.
    The workaround is to pre-fetch HLL blobs via a separate read and pass
    them as a literal value-list.
    """
    if isinstance(expr, TypedExpr):
        if expr.type_hint in (InferredType.LIST, InferredType.UNKNOWN):
            return expr.expr
        raise AelParseException(
            f"HLL multi-sketch argument must resolve to a list expression "
            f"or a single HLL bin reference; got {expr.type_hint.name}",
        )
    if isinstance(expr, DeferredBin):
        return FilterExpression.hll_bin(expr.name)
    if isinstance(expr, FilterExpression):
        return expr
    resolved = _finalize_result(expr, InferredType.LIST)
    if resolved is None:
        raise AelParseException("Failed to resolve HLL list expression")
    return resolved


def _resolve_for_in_list(expr: ExprOrDeferred) -> FilterExpression:
    """Resolve the right operand of an IN expression (must be a list)."""
    if expr is None:
        raise AelParseException("Right operand of 'in' cannot be None")
    if isinstance(expr, (DeferredBin, CDTPath)):
        if expr.explicit_type is not None and expr.explicit_type != InferredType.LIST:
            raise AelParseException(
                f"IN operation requires a List as the right operand, "
                f"got {expr.explicit_type.name}"
            )
        if isinstance(expr, CDTPath) and expr.parts:
            return expr.to_expression(InferredType.STRING)
        return expr.to_expression(InferredType.LIST)
    if isinstance(expr, TypedExpr):
        if expr.type_hint not in (InferredType.LIST, InferredType.UNKNOWN):
            raise AelParseException(
                f"IN operation requires a List as the right operand, "
                f"got {expr.type_hint.name}"
            )
        return expr.expr
    if isinstance(expr, FilterExpression):
        return expr
    raise AelParseException("Right operand of 'in' must be a list expression")


_NUMERIC_TYPES = frozenset({InferredType.INT, InferredType.FLOAT})


_BLOB_STRING_PAIR = frozenset({InferredType.BLOB, InferredType.STRING})


def _validate_comparison_types(left: InferredType, right: InferredType) -> None:
    """Reject incompatible type pairs in a comparison expression.

    Same types are always compatible. INT and FLOAT are numeric-compatible.
    BLOB and STRING are compatible (string is base64-decoded).
    All other cross-type pairs are rejected.
    """
    if left == right:
        return
    if left in _NUMERIC_TYPES and right in _NUMERIC_TYPES:
        return
    if {left, right} == _BLOB_STRING_PAIR:
        return
    raise AelParseException(f"Cannot compare {left.name} to {right.name}")


def _validate_in_type_compatibility(
    left_type: InferredType, element_type: InferredType
) -> None:
    """Reject incompatible type pairs in an IN expression.

    INT and FLOAT are numeric-compatible; all other cross-type pairs are rejected.
    """
    if left_type == element_type:
        return
    if left_type in _NUMERIC_TYPES and element_type in _NUMERIC_TYPES:
        return
    raise AelParseException(
        f"Cannot compare {left_type.name} to {element_type.name} in IN expression"
    )


def _resolve_for_arithmetic(expr: ExprOrDeferred, has_float: bool = False) -> FilterExpression:
    """Resolve deferred bin/CDT path for arithmetic operations.

    Arithmetic operations default to INT unless there's a float in the expression.
    """
    if isinstance(expr, (DeferredBin, CDTPath, DeferredArithmetic)):
        inferred = InferredType.FLOAT if has_float else InferredType.INT
        return _resolve_deferred(expr, inferred)
    elif expr is None:
        raise AelParseException("Operand cannot be None in arithmetic expression")

    unwrapped = _unwrap_expr(expr)
    if unwrapped is None:
        raise AelParseException("Failed to unwrap operand in arithmetic expression")
    return unwrapped


def _contains_deferred(expr: ExprOrDeferred) -> bool:
    """Check if expression contains deferred types that need later resolution."""
    if isinstance(expr, (DeferredBin, CDTPath)):
        return True
    if isinstance(expr, DeferredArithmetic):
        return True
    return False


def _validate_arg_count(func_name: str, args: list, expected: int) -> None:
    if len(args) != expected:
        raise AelParseException(
            f"Function '{func_name}' expects {expected} argument(s), got {len(args)}"
        )


def _validate_min_arg_count(func_name: str, args: list, min_count: int) -> None:
    if len(args) < min_count:
        raise AelParseException(
            f"Function '{func_name}' expects at least {min_count} arguments, got {len(args)}"
        )


def _require_numeric_operands(_op: ArithOp, left: ExprOrDeferred, right: ExprOrDeferred) -> None:
    """Raise AelParseException if either operand has a known non-numeric type."""
    non_numeric = (InferredType.STRING, InferredType.BOOL, InferredType.LIST, InferredType.MAP, InferredType.BLOB)
    left_hint = _get_type_hint(left)
    right_hint = _get_type_hint(right)
    if left_hint in non_numeric or right_hint in non_numeric:
        raise AelParseException(
            "Arithmetic requires numeric operands (INT or FLOAT); got non-numeric type"
        )


def _build_arithmetic(op: ArithOp, left: ExprOrDeferred, right: ExprOrDeferred) -> ExprOrDeferred:
    """Build arithmetic expression, deferring if operands need type inference.

    If either operand contains deferred types (bins without known type),
    returns DeferredArithmetic so the type can be inferred from comparison context.

    If both operands are resolved and one is FLOAT, resolves as FLOAT.
    Otherwise resolves as INT.
    """
    # Check if we need to defer
    if _contains_deferred(left) or _contains_deferred(right):
        return DeferredArithmetic(op, [left, right])

    # Both resolved - require numeric types (reject string, bool, list, map, blob)
    _require_numeric_operands(op, left, right)

    has_float = (_get_type_hint(left) == InferredType.FLOAT or
                 _get_type_hint(right) == InferredType.FLOAT)
    result_type = InferredType.FLOAT if has_float else InferredType.INT
    resolved_left = _resolve_for_arithmetic(left, has_float)
    resolved_right = _resolve_for_arithmetic(right, has_float)

    if op == ArithOp.ADD:
        result = FilterExpression.num_add([resolved_left, resolved_right])
    elif op == ArithOp.SUB:
        result = FilterExpression.num_sub([resolved_left, resolved_right])
    elif op == ArithOp.MUL:
        result = FilterExpression.num_mul([resolved_left, resolved_right])
    elif op == ArithOp.DIV:
        result = FilterExpression.num_div([resolved_left, resolved_right])
    elif op == ArithOp.MOD:
        result = FilterExpression.num_mod(resolved_left, resolved_right)
    else:
        raise AelParseException(f"Unknown arithmetic operation: {op}")
    return TypedExpr(result, result_type)


def _finalize_result(result: ExprOrDeferred, default_type: InferredType = InferredType.INT) -> Optional[FilterExpression]:
    """Finalize a visitor result into a FilterExpression.

    Handles DeferredBin, CDTPath, DeferredArithmetic (all default to default_type),
    TypedExpr (unwraps), and raw FilterExpression (pass-through).

    Args:
        result: The visitor result to finalize.
        default_type: Type to use if not explicitly set (default INT).
    """
    if result is None:
        return None
    if isinstance(result, DeferredBin):
        # Use explicit_type if set (via .get(type:X) or .asX()), else use default
        inferred = result.explicit_type if result.explicit_type else default_type
        return result.to_expression(inferred)
    if isinstance(result, CDTPath):
        # Use explicit_type if set, else use default
        inferred = result.explicit_type if result.explicit_type else default_type
        return result.to_expression(inferred)
    if isinstance(result, DeferredArithmetic):
        return result.to_expression(default_type)
    if isinstance(result, TypedExpr):
        return result.expr
    return result


class ExpressionConditionVisitor(ConditionVisitor):
    """Visitor that converts ANTLR parse tree nodes to FilterExpression objects."""
    
    def __init__(self, placeholder_values: Optional[Any] = None, ctx_only: bool = False):
        """Initialize the visitor.

        Args:
            placeholder_values: Optional PlaceholderValues for resolving ?0, ?1, etc.
            ctx_only: If True, don't finalize CDTPath to FilterExpression (for parse_ctx).
        """
        super().__init__()
        self._placeholder_values = placeholder_values
        self._ctx_only = ctx_only
        self._var_types: list[dict[str, InferredType]] = [{}]

    def _push_var_scope(self) -> None:
        self._var_types.append({})

    def _pop_var_scope(self) -> None:
        self._var_types.pop()

    def _set_var_type(self, name: str, var_type: InferredType) -> None:
        self._var_types[-1][name] = var_type

    def _get_var_type(self, name: str) -> InferredType:
        for scope in reversed(self._var_types):
            if name in scope:
                return scope[name]
        return InferredType.UNKNOWN

    def visitParse(self, ctx: ConditionParser.ParseContext) -> ExprOrDeferred:
        """Visit the root parse node."""
        result = self.visit(ctx.expression())
        if self._ctx_only:
            return result
        return _finalize_result(result)

    def visitExpression(self, ctx: ConditionParser.ExpressionContext) -> ExprOrDeferred:
        """Visit expression node."""
        return self.visit(ctx.logicalOrExpression())

    def visitOrExpression(self, ctx: ConditionParser.OrExpressionContext) -> ExprOrDeferred:
        """Visit OR expression: expr1 or expr2 or expr3 ...

        Bare bins in logical expressions are inferred as bool_bin.
        """
        if len(ctx.logicalAndExpression()) == 1:
            return self.visit(ctx.logicalAndExpression(0))

        expressions: List[FilterExpression] = []
        for expr_ctx in ctx.logicalAndExpression():
            expr = _finalize_result(self.visit(expr_ctx), InferredType.BOOL)
            if expr is None:
                raise AelParseException("Failed to parse expression in OR clause")
            expressions.append(expr)

        if not expressions:
            raise AelParseException("OR expression requires at least one expression")
        return FilterExpression.or_(expressions)

    def visitAndExpression(self, ctx: ConditionParser.AndExpressionContext) -> ExprOrDeferred:
        """Visit AND expression: expr1 and expr2 and expr3 ...

        Bare bins in logical expressions are inferred as bool_bin.
        """
        if len(ctx.comparisonExpression()) == 1:
            return self.visit(ctx.comparisonExpression(0))

        expressions: List[FilterExpression] = []
        for expr_ctx in ctx.comparisonExpression():
            expr = _finalize_result(self.visit(expr_ctx), InferredType.BOOL)
            if expr is None:
                raise AelParseException("Failed to parse expression in AND clause")
            expressions.append(expr)

        if not expressions:
            raise AelParseException("AND expression requires at least one expression")
        return FilterExpression.and_(expressions)

    def visitNotExpression(self, ctx: ConditionParser.NotExpressionContext) -> ExprOrDeferred:
        """Visit NOT expression: not (expr)

        Bare bins in logical expressions are inferred as bool_bin.
        """
        expr = _finalize_result(self.visit(ctx.expression()), InferredType.BOOL)
        if expr is None:
            raise AelParseException("Failed to parse expression in NOT clause")
        return FilterExpression.not_(expr)

    def visitEqualityExpression(self, ctx: ConditionParser.EqualityExpressionContext) -> Optional[FilterExpression]:
        """Visit equality expression: left == right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.bitwiseExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.eq(resolved_left, resolved_right)

    def visitInequalityExpression(self, ctx: ConditionParser.InequalityExpressionContext) -> Optional[FilterExpression]:
        """Visit inequality expression: left != right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.bitwiseExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.ne(resolved_left, resolved_right)

    def visitGreaterThanExpression(self, ctx: ConditionParser.GreaterThanExpressionContext) -> Optional[FilterExpression]:
        """Visit greater than expression: left > right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.bitwiseExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.gt(resolved_left, resolved_right)

    def visitGreaterThanOrEqualExpression(self, ctx: ConditionParser.GreaterThanOrEqualExpressionContext) -> Optional[FilterExpression]:
        """Visit greater than or equal expression: left >= right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.bitwiseExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.ge(resolved_left, resolved_right)

    def visitLessThanExpression(self, ctx: ConditionParser.LessThanExpressionContext) -> Optional[FilterExpression]:
        """Visit less than expression: left < right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.bitwiseExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.lt(resolved_left, resolved_right)

    def visitLessThanOrEqualExpression(self, ctx: ConditionParser.LessThanOrEqualExpressionContext) -> Optional[FilterExpression]:
        """Visit less than or equal expression: left <= right
        
        Type inference: If one side is a bin and the other is a literal,
        the bin type is inferred from the literal type.
        """
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.bitwiseExpression(1))
        resolved_left, resolved_right = _resolve_for_comparison(left, right)
        return FilterExpression.le(resolved_left, resolved_right)

    def visitInExpression(self, ctx: ConditionParser.InExpressionContext) -> Optional[FilterExpression]:
        """Visit IN expression: left in right → boolean.

        Translates to list_get_by_value(ListReturnType.EXISTS, left, right).
        The right side must be a list; the left can be any supported type.

        Validation order: right-must-be-list → type-compatibility → resolve.
        """
        left = self.visit(ctx.bitwiseExpression(0))
        right = self.visit(ctx.bitwiseExpression(1))

        # 1. Reject right operand if it has a known non-LIST type
        right_hint = _get_type_hint(right)
        if right_hint not in (InferredType.UNKNOWN, InferredType.LIST):
            raise AelParseException(
                f"IN operation requires a List as the right operand, got {right_hint.name}"
            )
        if isinstance(right, TypedExpr) and right.type_hint not in (InferredType.LIST, InferredType.UNKNOWN):
            raise AelParseException(
                f"IN operation requires a List as the right operand, got {right.type_hint.name}"
            )

        # 2. Reject incompatible explicit-left-type vs list-element-type
        left_hint = _get_type_hint(left)
        if left_hint != InferredType.UNKNOWN:
            if isinstance(right, TypedExpr) and right.type_hint == InferredType.LIST:
                if isinstance(right.value, list) and right.value:
                    element_type = _infer_element_type(right.value)
                    if element_type != InferredType.UNKNOWN:
                        _validate_in_type_compatibility(left_hint, element_type)

        # 3. Resolve operands
        resolved_value = _resolve_for_in_value(left, right)
        resolved_list = _resolve_for_in_list(right)
        return FilterExpression.list_get_by_value(
            ListReturnType.EXISTS,
            resolved_value,
            resolved_list,
            [],
        )

    def visitBinPart(self, ctx: ConditionParser.BinPartContext) -> ExprOrDeferred:
        """Visit bin part: bin name identifier.

        Returns a DeferredBin that will be resolved to the correct bin type
        based on context (comparison operand type) or explicit cast.
        Type defaults to INT.

        Accepts BIN_IDENTIFIER (allows ``@``), NAME_IDENTIFIER, QUOTED_STRING
        (single- or double-quoted bin name with otherwise-illegal characters),
        IN (lowercased to match historical PSDK behavior), and any
        ``reservedWord`` (a keyword used as a bin name). Bin names containing
        ``null`` (case-insensitive) are rejected — that token is reserved.
        """
        bin_name = _extract_bin_name(ctx)
        return DeferredBin(bin_name)

    def visitPath(self, ctx: ConditionParser.PathContext) -> ExprOrDeferred:
        """Visit path: $.binName or $.binName.pathFunction() or $.binName[0].get(...)"""
        base_path = self.visit(ctx.basePath())
        if base_path is None:
            raise AelParseException("Failed to parse base path")
        
        # Handle path functions like asInt(), asFloat(), exists(), count(), get()
        if ctx.pathFunction() is not None:
            path_func_ctx = ctx.pathFunction()
            
            # Check for cast functions (asInt, asFloat)
            # Casting always generates a conversion wrapper: asInt reads as FLOAT
            # then converts to INT, asFloat reads as INT then converts to FLOAT.
            if hasattr(path_func_ctx, 'pathFunctionCast') and path_func_ctx.pathFunctionCast() is not None:
                cast_ctx = path_func_ctx.pathFunctionCast()
                cast_text = cast_ctx.getText().lower()

                if isinstance(base_path, DeferredBin):
                    if cast_text == "asint()":
                        resolved = FilterExpression.float_bin(base_path.name)
                        return TypedExpr(FilterExpression.to_int(resolved), InferredType.INT)
                    elif cast_text == "asfloat()":
                        resolved = FilterExpression.int_bin(base_path.name)
                        return TypedExpr(FilterExpression.to_float(resolved), InferredType.FLOAT)
                elif isinstance(base_path, CDTPath):
                    base_path.has_path_function = True
                    if cast_text == "asint()":
                        base_path.explicit_type = InferredType.FLOAT
                        base_path.cast_wrap = "to_int"
                        return base_path
                    elif cast_text == "asfloat()":
                        base_path.explicit_type = InferredType.INT
                        base_path.cast_wrap = "to_float"
                        return base_path
                else:
                    resolved = _finalize_result(base_path)
                    if resolved is None:
                        raise AelParseException("Failed to resolve path for cast")
                    if cast_text == "asint()":
                        return FilterExpression.to_int(resolved)
                    elif cast_text == "asfloat()":
                        return FilterExpression.to_float(resolved)
            
            # Check for get(type:..., return:...)
            if hasattr(path_func_ctx, 'pathFunctionGet') and path_func_ctx.pathFunctionGet() is not None:
                get_ctx = path_func_ctx.pathFunctionGet()
                # Parse get() parameters - extract type for simple bins
                if isinstance(base_path, DeferredBin):
                    self._apply_get_params_to_deferred(base_path, get_ctx)
                elif isinstance(base_path, CDTPath):
                    self._apply_get_params(base_path, get_ctx)
                    base_path.has_path_function = True
            
            # Check for exists()
            if hasattr(path_func_ctx, 'pathFunctionExists') and path_func_ctx.pathFunctionExists() is not None:
                # exists() returns a boolean indicating whether the path is
                # present. For a bare bin path (no CDT parts) this is just
                # bin_exists. For a CDT path we lower it to the underlying
                # *_get_by_* op with EXISTS return-type and BOOL value-type,
                # which is the same shape produced by ``.get(return: EXISTS)``.
                if isinstance(base_path, CDTPath) and base_path.parts:
                    last_part = base_path.parts[-1]
                    if isinstance(last_part, (ListIndexPart, ListRankPart, ListValuePart,
                                              ListIndexRangePart, ListValueRangePart,
                                              ListValueListPart, ListRankRangePart,
                                              ListRankRangeRelativePart)):
                        base_path.list_return_type = ListReturnType.EXISTS
                    else:
                        base_path.map_return_type = MapReturnType.EXISTS
                    base_path.value_type = ExpType.BOOL
                    base_path.has_path_function = True
                    return base_path.to_expression(InferredType.BOOL)
                if isinstance(base_path, CDTPath):
                    return FilterExpression.bin_exists(base_path.bin_name)
                if isinstance(base_path, DeferredBin):
                    return FilterExpression.bin_exists(base_path.name)
            
            # Check for count()
            if hasattr(path_func_ctx, 'pathFunctionCount') and path_func_ctx.pathFunctionCount() is not None:
                count_expr = self._build_count(base_path)
                if count_expr is not None:
                    return TypedExpr(count_expr, InferredType.INT)

            # Check for HLL read-side path functions.
            if hasattr(path_func_ctx, 'pathFunctionHllCount') and path_func_ctx.pathFunctionHllCount() is not None:
                bin_expr = _resolve_for_hll(base_path)
                return TypedExpr(FilterExpression.hll_get_count(bin_expr), InferredType.INT)
            if hasattr(path_func_ctx, 'pathFunctionHllDescribe') and path_func_ctx.pathFunctionHllDescribe() is not None:
                bin_expr = _resolve_for_hll(base_path)
                return TypedExpr(FilterExpression.hll_describe(bin_expr), InferredType.LIST)
            if hasattr(path_func_ctx, 'pathFunctionHllMayContain') and path_func_ctx.pathFunctionHllMayContain() is not None:
                bin_expr = _resolve_for_hll(base_path)
                values_arg = self.visit(path_func_ctx.pathFunctionHllMayContain().expression())
                values_expr = _resolve_for_list(values_arg)
                return TypedExpr(
                    FilterExpression.hll_may_contain(values_expr, bin_expr), InferredType.INT,
                )
            if hasattr(path_func_ctx, 'pathFunctionHllUnion') and path_func_ctx.pathFunctionHllUnion() is not None:
                bin_expr = _resolve_for_hll(base_path)
                hlls_arg = self.visit(path_func_ctx.pathFunctionHllUnion().expression())
                hlls_expr = _resolve_for_hll_list(hlls_arg)
                return TypedExpr(
                    FilterExpression.hll_get_union(hlls_expr, bin_expr), InferredType.UNKNOWN,
                )
            if hasattr(path_func_ctx, 'pathFunctionHllUnionCount') and path_func_ctx.pathFunctionHllUnionCount() is not None:
                bin_expr = _resolve_for_hll(base_path)
                hlls_arg = self.visit(path_func_ctx.pathFunctionHllUnionCount().expression())
                hlls_expr = _resolve_for_hll_list(hlls_arg)
                return TypedExpr(
                    FilterExpression.hll_get_union_count(hlls_expr, bin_expr), InferredType.INT,
                )
            if hasattr(path_func_ctx, 'pathFunctionHllIntersectCount') and path_func_ctx.pathFunctionHllIntersectCount() is not None:
                bin_expr = _resolve_for_hll(base_path)
                hlls_arg = self.visit(path_func_ctx.pathFunctionHllIntersectCount().expression())
                hlls_expr = _resolve_for_hll_list(hlls_arg)
                return TypedExpr(
                    FilterExpression.hll_get_intersect_count(hlls_expr, bin_expr), InferredType.INT,
                )
            if hasattr(path_func_ctx, 'pathFunctionHllSimilarity') and path_func_ctx.pathFunctionHllSimilarity() is not None:
                bin_expr = _resolve_for_hll(base_path)
                hlls_arg = self.visit(path_func_ctx.pathFunctionHllSimilarity().expression())
                hlls_expr = _resolve_for_hll_list(hlls_arg)
                return TypedExpr(
                    FilterExpression.hll_get_similarity(hlls_expr, bin_expr), InferredType.FLOAT,
                )

        return base_path
    
    @staticmethod
    def _build_count(base_path: ExprOrDeferred) -> Optional[FilterExpression]:
        """Build a count/size expression for a path with .count()."""
        if isinstance(base_path, CDTPath) and base_path.parts:
            last_part = base_path.parts[-1]

            if isinstance(last_part, (ListValuePart, ListIndexRangePart,
                                      ListValueRangePart, ListValueListPart, ListRankRangePart,
                                      ListRankRangeRelativePart)):
                ctx_list: List[CTX] = []
                for p in base_path.parts[:-1]:
                    try:
                        ctx_list.append(p.get_context())
                    except NotImplementedError:
                        pass
                return last_part.construct_expr(
                    base_path.bin_name, ExpType.INT, ListReturnType.COUNT, ctx_list,
                )

            if isinstance(last_part, (MapValuePart, MapKeyRangePart, MapKeyListPart,
                                      MapIndexRangePart, MapValueRangePart,
                                      MapValueListPart, MapRankRangePart,
                                      MapRankRangeRelativePart, MapIndexRangeRelativePart)):
                ctx_list: List[CTX] = []
                for p in base_path.parts[:-1]:
                    try:
                        ctx_list.append(p.get_context())
                    except NotImplementedError:
                        pass
                return last_part.construct_expr(
                    base_path.bin_name, ExpType.INT, MapReturnType.COUNT, ctx_list,
                )

            ctx_list: List[CTX] = []
            for p in base_path.parts[:-1]:
                try:
                    ctx_list.append(p.get_context())
                except NotImplementedError:
                    pass

            first_part = base_path.parts[0]
            is_map_base = isinstance(first_part, (MapKeyPart, MapIndexPart, MapRankPart,
                                                   MapValuePart, MapKeyRangePart, MapKeyListPart,
                                                   MapIndexRangePart, MapValueRangePart, MapValueListPart,
                                                   MapRankRangePart, MapRankRangeRelativePart,
                                                   MapIndexRangeRelativePart))
            bin_expr = (FilterExpression.map_bin(base_path.bin_name)
                        if is_map_base else FilterExpression.list_bin(base_path.bin_name))

            is_map_result = (hasattr(base_path, 'explicit_type')
                            and base_path.explicit_type == InferredType.MAP)
            inner_type = ExpType.MAP if is_map_result else ExpType.LIST
            size_fn = FilterExpression.map_size if is_map_result else FilterExpression.list_size

            if isinstance(last_part, ListIndexPart):
                inner = FilterExpression.list_get_by_index(
                    ListReturnType.VALUE, inner_type,
                    FilterExpression.int_val(last_part.index), bin_expr, ctx_list,
                )
                return size_fn(inner, [])
            elif isinstance(last_part, ListRankPart):
                inner = FilterExpression.list_get_by_rank(
                    ListReturnType.VALUE, inner_type,
                    FilterExpression.int_val(last_part.rank), bin_expr, ctx_list,
                )
                return size_fn(inner, [])
            elif isinstance(last_part, MapKeyPart):
                inner = FilterExpression.map_get_by_key(
                    MapReturnType.VALUE, inner_type,
                    FilterExpression.string_val(last_part.key), bin_expr, ctx_list,
                )
                return size_fn(inner, [])
            elif isinstance(last_part, MapIndexPart):
                inner = FilterExpression.map_get_by_index(
                    MapReturnType.VALUE, inner_type,
                    FilterExpression.int_val(last_part.index), bin_expr, ctx_list,
                )
                return size_fn(inner, [])
            elif isinstance(last_part, MapRankPart):
                inner = FilterExpression.map_get_by_rank(
                    MapReturnType.VALUE, inner_type,
                    FilterExpression.int_val(last_part.rank), bin_expr, ctx_list,
                )
                return size_fn(inner, [])
        elif isinstance(base_path, CDTPath):
            if base_path.explicit_type == InferredType.MAP:
                return FilterExpression.map_size(
                    FilterExpression.map_bin(base_path.bin_name), [],
                )
            return FilterExpression.list_size(
                FilterExpression.list_bin(base_path.bin_name), [],
            )
        elif isinstance(base_path, DeferredBin):
            if base_path.explicit_type == InferredType.MAP:
                return FilterExpression.map_size(
                    FilterExpression.map_bin(base_path.name), [],
                )
            return FilterExpression.list_size(
                FilterExpression.list_bin(base_path.name), [],
            )
        return None

    @staticmethod
    def _apply_get_params(cdt_path: CDTPath, get_ctx) -> None:
        """Apply get() function parameters to a CDTPath."""
        if hasattr(get_ctx, 'pathFunctionParams') and get_ctx.pathFunctionParams() is not None:
            params_ctx = get_ctx.pathFunctionParams()
            if hasattr(params_ctx, 'pathFunctionParam'):
                for param_ctx in params_ctx.pathFunctionParam():
                    if hasattr(param_ctx, 'pathFunctionParamName') and hasattr(param_ctx, 'pathFunctionParamValue'):
                        name_ctx = param_ctx.pathFunctionParamName()
                        value_ctx = param_ctx.pathFunctionParamValue()
                        if name_ctx and value_ctx:
                            param_name = name_ctx.getText().lower()
                            param_value = value_ctx.getText().upper()
                            
                            if param_name == "type":
                                # Set value type for CDT expression
                                type_map = {
                                    "INT": ExpType.INT,
                                    "STRING": ExpType.STRING,
                                    "FLOAT": ExpType.FLOAT,
                                    "BOOL": ExpType.BOOL,
                                    "LIST": ExpType.LIST,
                                    "MAP": ExpType.MAP,
                                    "BLOB": ExpType.BLOB,
                                    "GEO": ExpType.GEO,
                                    "HLL": ExpType.HLL,
                                }
                                if param_value in type_map:
                                    cdt_path.value_type = type_map[param_value]
                                # Also set explicit_type for type inference
                                inferred_map = {
                                    "INT": InferredType.INT,
                                    "STRING": InferredType.STRING,
                                    "FLOAT": InferredType.FLOAT,
                                    "BOOL": InferredType.BOOL,
                                    "LIST": InferredType.LIST,
                                    "MAP": InferredType.MAP,
                                    "BLOB": InferredType.BLOB,
                                }
                                if param_value in inferred_map:
                                    cdt_path.explicit_type = inferred_map[param_value]
                            elif param_name == "return":
                                ret_map = {
                                    "COUNT": (ListReturnType.COUNT, MapReturnType.COUNT, None),
                                    "EXISTS": (ListReturnType.EXISTS, MapReturnType.EXISTS, ExpType.BOOL),
                                    "INDEX": (ListReturnType.INDEX, MapReturnType.INDEX, ExpType.INT),
                                    "RANK": (ListReturnType.VALUE, MapReturnType.RANK, ExpType.INT),
                                    "ORDERED_MAP": (ListReturnType.VALUE, MapReturnType.ORDERED_MAP, ExpType.STRING),
                                    "UNORDERED_MAP": (ListReturnType.VALUE, MapReturnType.UNORDERED_MAP, ExpType.STRING),
                                }
                                if param_value in ret_map:
                                    list_ret, map_ret, val_type = ret_map[param_value]
                                    last = cdt_path.parts[-1] if cdt_path.parts else None
                                    if isinstance(last, (ListIndexPart, ListRankPart, ListValuePart,
                                                         ListIndexRangePart, ListValueRangePart,
                                                         ListValueListPart, ListRankRangePart,
                                                         ListRankRangeRelativePart)):
                                        cdt_path.list_return_type = list_ret
                                        if val_type is not None:
                                            cdt_path.value_type = val_type
                                        elif param_value == "COUNT":
                                            cdt_path.value_type = ExpType.LIST
                                    elif isinstance(last, (MapKeyPart, MapIndexPart, MapRankPart,
                                                           MapValuePart, MapKeyRangePart, MapKeyListPart,
                                                           MapIndexRangePart, MapValueRangePart,
                                                           MapValueListPart, MapRankRangePart,
                                                           MapRankRangeRelativePart, MapIndexRangeRelativePart)):
                                        cdt_path.map_return_type = map_ret
                                        if val_type is not None:
                                            cdt_path.value_type = val_type
                                        elif param_value == "COUNT":
                                            cdt_path.value_type = ExpType.INT

    @staticmethod
    def _apply_get_params_to_deferred(deferred: DeferredBin, get_ctx) -> None:
        """Apply get() function type parameter to a DeferredBin.

        For simple bins like $.name.get(type: STRING), this sets the explicit_type.
        """
        if hasattr(get_ctx, 'pathFunctionParams') and get_ctx.pathFunctionParams() is not None:
            params_ctx = get_ctx.pathFunctionParams()
            if hasattr(params_ctx, 'pathFunctionParam'):
                for param_ctx in params_ctx.pathFunctionParam():
                    if hasattr(param_ctx, 'pathFunctionParamName') and hasattr(param_ctx, 'pathFunctionParamValue'):
                        name_ctx = param_ctx.pathFunctionParamName()
                        value_ctx = param_ctx.pathFunctionParamValue()
                        if name_ctx and value_ctx:
                            param_name = name_ctx.getText().lower()
                            param_value = value_ctx.getText().upper()

                            if param_name == "type":
                                # Set explicit type for bin resolution
                                type_map = {
                                    "INT": InferredType.INT,
                                    "STRING": InferredType.STRING,
                                    "FLOAT": InferredType.FLOAT,
                                    "BOOL": InferredType.BOOL,
                                    "BLOB": InferredType.BLOB,
                                    "LIST": InferredType.LIST,
                                    "MAP": InferredType.MAP,
                                    "GEO": InferredType.GEO,
                                    "HLL": InferredType.HLL,
                                }
                                if param_value in type_map:
                                    deferred.explicit_type = type_map[param_value]

    def visitBasePath(self, ctx: ConditionParser.BasePathContext) -> ExprOrDeferred:
        """Visit base path: binPart with optional CDT parts (list/map access).

        ``binPart`` is resolved through :func:`_extract_bin_name` so quoted /
        reserved-keyword bin names and BIN_IDENTIFIER (with ``@``) all
        normalize to a plain Python string. Any embedded ``.<int>`` or
        ``.0xff`` segments produce integer-keyed ``MapKeyPart`` entries via
        the dedicated ``pathIntMapKey`` / ``pathHexBinaryMapKey`` rules.
        """
        bin_name: Optional[str] = None
        cdt_parts: List[CDTPart] = []
        designator_type: Optional[InferredType] = None

        for child in ctx.children:
            if not hasattr(child, 'getRuleIndex'):
                continue
            rule_index = child.getRuleIndex()
            if rule_index == ConditionParser.RULE_binPart:
                bin_name = _extract_bin_name(child)
            elif rule_index == ConditionParser.RULE_listPart:
                if self._is_list_type_designator(child):
                    designator_type = InferredType.LIST
                else:
                    part = self._parse_list_part(child)
                    if part:
                        cdt_parts.append(part)
            elif rule_index == ConditionParser.RULE_mapPart:
                if self._is_map_type_designator(child):
                    designator_type = InferredType.MAP
                else:
                    part = self._parse_map_part(child)
                    if part:
                        cdt_parts.append(part)
            elif rule_index == ConditionParser.RULE_pathIntMapKey:
                # ``.42`` or ``.+5`` / ``.-3`` after a bin or another part.
                key_text = child.getText()[1:]  # strip leading dot
                cdt_parts.append(MapKeyPart(_parse_int_literal(key_text)))
            elif rule_index == ConditionParser.RULE_pathHexBinaryMapKey:
                # ``.0xff`` or ``.0b101`` — leading dot, then a numeric token.
                key_text = child.getText()[1:]
                cdt_parts.append(MapKeyPart(_parse_int_literal(key_text)))

        if bin_name is None:
            raise AelParseException("Base path must start with a bin name")

        if not cdt_parts:
            return DeferredBin(bin_name, explicit_type=designator_type)

        path = CDTPath(bin_name=bin_name, parts=cdt_parts)
        if designator_type is not None:
            path.explicit_type = designator_type
        return path

    @staticmethod
    def _is_list_type_designator(ctx) -> bool:
        return (hasattr(ctx, 'LIST_TYPE_DESIGNATOR')
                and ctx.LIST_TYPE_DESIGNATOR() is not None)

    @staticmethod
    def _is_map_type_designator(ctx) -> bool:
        return (hasattr(ctx, 'MAP_TYPE_DESIGNATOR')
                and ctx.MAP_TYPE_DESIGNATOR() is not None)
    
    def _parse_list_part(self, ctx) -> Optional[CDTPart]:
        """Parse a list part from the parse tree."""
        if hasattr(ctx, 'listIndex') and ctx.listIndex() is not None:
            idx_ctx = ctx.listIndex()
            if idx_ctx.signedInt() is not None:
                index = int(idx_ctx.signedInt().getText(), 0)
                return ListIndexPart(index)

        if hasattr(ctx, 'listRank') and ctx.listRank() is not None:
            rank_ctx = ctx.listRank()
            if rank_ctx.signedInt() is not None:
                rank = int(rank_ctx.signedInt().getText(), 0)
                return ListRankPart(rank)

        # Check for listValue: [=value]
        if hasattr(ctx, 'listValue') and ctx.listValue() is not None:
            val_ctx = ctx.listValue()
            value = self._parse_value_identifier(val_ctx.valueIdentifier())
            return ListValuePart(value=value)

        # Check for listIndexRange: [start:end] or [!start:end]
        if hasattr(ctx, 'listIndexRange') and ctx.listIndexRange() is not None:
            range_ctx = ctx.listIndexRange()
            inverted = False
            if hasattr(range_ctx, 'standardListIndexRange') and range_ctx.standardListIndexRange() is not None:
                idx_range = range_ctx.standardListIndexRange().indexRangeIdentifier()
            elif hasattr(range_ctx, 'invertedListIndexRange') and range_ctx.invertedListIndexRange() is not None:
                idx_range = range_ctx.invertedListIndexRange().indexRangeIdentifier()
                inverted = True
            else:
                return None
            start, count = self._parse_index_range(idx_range)
            return ListIndexRangePart(start=start, count=count, inverted=inverted)

        # Check for listValueList: [=a,b,c] or [!=a,b,c]
        if hasattr(ctx, 'listValueList') and ctx.listValueList() is not None:
            list_ctx = ctx.listValueList()
            inverted = False
            if hasattr(list_ctx, 'standardListValueList') and list_ctx.standardListValueList() is not None:
                val_list = list_ctx.standardListValueList().valueListIdentifier()
            elif hasattr(list_ctx, 'invertedListValueList') and list_ctx.invertedListValueList() is not None:
                val_list = list_ctx.invertedListValueList().valueListIdentifier()
                inverted = True
            else:
                return None
            values = self._parse_value_list(val_list)
            return ListValueListPart(values=values, inverted=inverted)

        # Check for listValueRange: [=start:end] or [!=start:end]
        if hasattr(ctx, 'listValueRange') and ctx.listValueRange() is not None:
            range_ctx = ctx.listValueRange()
            inverted = False
            if hasattr(range_ctx, 'standardListValueRange') and range_ctx.standardListValueRange() is not None:
                val_range = range_ctx.standardListValueRange().valueRangeIdentifier()
            elif hasattr(range_ctx, 'invertedListValueRange') and range_ctx.invertedListValueRange() is not None:
                val_range = range_ctx.invertedListValueRange().valueRangeIdentifier()
                inverted = True
            else:
                return None
            begin, end = self._parse_value_range(val_range)
            return ListValueRangePart(value_begin=begin, value_end=end, inverted=inverted)

        # Check for listRankRange: [#start:count] or [!#start:count]
        if hasattr(ctx, 'listRankRange') and ctx.listRankRange() is not None:
            range_ctx = ctx.listRankRange()
            inverted = False
            if hasattr(range_ctx, 'standardListRankRange') and range_ctx.standardListRankRange() is not None:
                rank_range = range_ctx.standardListRankRange().rankRangeIdentifier()
            elif hasattr(range_ctx, 'invertedListRankRange') and range_ctx.invertedListRankRange() is not None:
                rank_range = range_ctx.invertedListRankRange().rankRangeIdentifier()
                inverted = True
            else:
                return None
            start, count = self._parse_rank_range(rank_range)
            return ListRankRangePart(start=start, count=count, inverted=inverted)

        # Check for listRankRangeRelative: [#start:end~value] or [!#start:end~value]
        if hasattr(ctx, 'listRankRangeRelative') and ctx.listRankRangeRelative() is not None:
            range_ctx = ctx.listRankRangeRelative()
            inverted = False
            if hasattr(range_ctx, 'standardListRankRangeRelative') and range_ctx.standardListRankRangeRelative() is not None:
                rel_range = range_ctx.standardListRankRangeRelative().rankRangeRelativeIdentifier()
            elif hasattr(range_ctx, 'invertedListRankRangeRelative') and range_ctx.invertedListRankRangeRelative() is not None:
                rel_range = range_ctx.invertedListRankRangeRelative().rankRangeRelativeIdentifier()
                inverted = True
            else:
                return None
            rank, count, value = self._parse_rank_range_relative(rel_range)
            return ListRankRangeRelativePart(rank=rank, value=value, count=count, inverted=inverted)

        # Check for LIST_TYPE_DESIGNATOR: []
        if hasattr(ctx, 'LIST_TYPE_DESIGNATOR') and ctx.LIST_TYPE_DESIGNATOR() is not None:
            return None

        return None

    @staticmethod
    def _parse_value_identifier(ctx) -> Any:
        """Parse a valueIdentifier into a Python value."""
        if ctx is None:
            return None
        if ctx.signedInt() is not None:
            return int(ctx.signedInt().getText(), 0)
        if ctx.QUOTED_STRING() is not None:
            return _unquote(ctx.QUOTED_STRING().getText())
        if ctx.NAME_IDENTIFIER() is not None:
            return ctx.NAME_IDENTIFIER().getText()
        if ctx.IN() is not None:
            return ctx.IN().getText()
        return ctx.getText()

    @staticmethod
    def _parse_index_range(ctx) -> tuple[int, Optional[int]]:
        """Parse indexRangeIdentifier into (start, count)."""
        if ctx is None:
            return 0, None
        start_ctx = ctx.getTypedRuleContext(ConditionParser.StartContext, 0)
        end_ctx = ctx.end() if hasattr(ctx, 'end') and callable(ctx.end) else None

        start = (
            int(cast(_AntlrText, start_ctx).getText(), 0) if start_ctx else 0
        )
        if end_ctx:
            end = int(cast(_AntlrText, end_ctx).getText(), 0)
            count = end - start
        else:
            count = None
        return start, count

    @staticmethod
    def _parse_rank_range(ctx) -> tuple[int, Optional[int]]:
        """Parse rankRangeIdentifier into (start, count)."""
        if ctx is None:
            return 0, None
        start_ctx = ctx.getTypedRuleContext(ConditionParser.StartContext, 0)
        end_ctx = ctx.end() if hasattr(ctx, 'end') and callable(ctx.end) else None

        start = (
            int(cast(_AntlrText, start_ctx).getText(), 0) if start_ctx else 0
        )
        if end_ctx:
            end = int(cast(_AntlrText, end_ctx).getText(), 0)
            count = end - start
        else:
            count = None
        return start, count

    def _parse_value_range(self, ctx) -> tuple[Optional[Any], Optional[Any]]:
        """Parse valueRangeIdentifier into (begin, end)."""
        if ctx is None:
            return None, None
        val_ids = ctx.valueIdentifier()
        if len(val_ids) >= 2:
            begin = self._parse_value_identifier(val_ids[0])
            end = self._parse_value_identifier(val_ids[1])
        elif len(val_ids) == 1:
            begin = self._parse_value_identifier(val_ids[0])
            end = None
        else:
            begin = None
            end = None
        return begin, end

    def _parse_value_list(self, ctx) -> List[Any]:
        """Parse valueListIdentifier into a list of values."""
        if ctx is None:
            return []
        values = []
        for val_id in ctx.valueIdentifier():
            values.append(self._parse_value_identifier(val_id))
        return values

    def _parse_rank_range_relative(self, ctx) -> tuple[int, Optional[int], Any]:
        """Parse rankRangeRelativeIdentifier into (rank, count, value).
        
        Format: start ':' relativeRankEnd
        where relativeRankEnd is: end relativeValue | relativeValue
        and relativeValue is: '~' valueIdentifier
        """
        if ctx is None:
            return 0, None, None
        
        # Get start
        start_ctx = ctx.getTypedRuleContext(ConditionParser.StartContext, 0)
        rank = (
            int(cast(_AntlrText, start_ctx).getText(), 0) if start_ctx else 0
        )

        # Get relativeRankEnd
        rel_end = ctx.relativeRankEnd() if hasattr(ctx, 'relativeRankEnd') else None
        if rel_end is None:
            return rank, None, None

        # Check for end (count)
        end_ctx = rel_end.end() if hasattr(rel_end, 'end') and callable(rel_end.end) else None
        if end_ctx:
            end = int(cast(_AntlrText, end_ctx).getText(), 0)
            count = end - rank
        else:
            count = None

        # Get relativeValue (~value)
        rel_val = rel_end.relativeValue() if hasattr(rel_end, 'relativeValue') else None
        if rel_val:
            val_id = rel_val.valueIdentifier() if hasattr(rel_val, 'valueIdentifier') else None
            value = self._parse_value_identifier(val_id)
        else:
            value = None

        return rank, count, value

    def _parse_index_range_relative(self, ctx) -> tuple[int, Optional[int], str]:
        """Parse indexRangeRelativeIdentifier into (index, count, key).
        
        Format: start ':' relativeKeyEnd
        where relativeKeyEnd is: end '~' mapKey | '~' mapKey
        """
        if ctx is None:
            return 0, None, ""

        # Get start
        start_ctx = ctx.getTypedRuleContext(ConditionParser.StartContext, 0)
        index = (
            int(cast(_AntlrText, start_ctx).getText(), 0) if start_ctx else 0
        )

        # Get relativeKeyEnd
        rel_end = ctx.relativeKeyEnd() if hasattr(ctx, 'relativeKeyEnd') else None
        if rel_end is None:
            return index, None, ""

        # Check for end (count)
        end_ctx = rel_end.end() if hasattr(rel_end, 'end') and callable(rel_end.end) else None
        if end_ctx:
            end = int(cast(_AntlrText, end_ctx).getText(), 0)
            count = end - index
        else:
            count = None
        
        # Get key
        key_ctx = rel_end.mapKey() if hasattr(rel_end, 'mapKey') else None
        if key_ctx:
            key = self._parse_map_key(key_ctx)
        else:
            key = ""
        
        return index, count, key
    
    def _parse_map_part(self, ctx) -> Optional[CDTPart]:
        """Parse a map part from the parse tree."""
        if hasattr(ctx, 'mapKey') and ctx.mapKey() is not None:
            return MapKeyPart(self._parse_map_key(ctx.mapKey()))

        if hasattr(ctx, 'mapIndex') and ctx.mapIndex() is not None:
            idx_ctx = ctx.mapIndex()
            if idx_ctx.signedInt() is not None:
                index = int(idx_ctx.signedInt().getText(), 0)
                return MapIndexPart(index)

        if hasattr(ctx, 'mapRank') and ctx.mapRank() is not None:
            rank_ctx = ctx.mapRank()
            if rank_ctx.signedInt() is not None:
                rank = int(rank_ctx.signedInt().getText(), 0)
                return MapRankPart(rank)

        # Check for mapValue: {=value}
        if hasattr(ctx, 'mapValue') and ctx.mapValue() is not None:
            val_ctx = ctx.mapValue()
            value = self._parse_value_identifier(val_ctx.valueIdentifier())
            return MapValuePart(value=value)

        # Check for mapKeyRange: {key-key} or {!key-key}
        if hasattr(ctx, 'mapKeyRange') and ctx.mapKeyRange() is not None:
            range_ctx = ctx.mapKeyRange()
            inverted = False
            if hasattr(range_ctx, 'standardMapKeyRange') and range_ctx.standardMapKeyRange() is not None:
                key_range = range_ctx.standardMapKeyRange().keyRangeIdentifier()
            elif hasattr(range_ctx, 'invertedMapKeyRange') and range_ctx.invertedMapKeyRange() is not None:
                key_range = range_ctx.invertedMapKeyRange().keyRangeIdentifier()
                inverted = True
            else:
                return None
            begin, end = self._parse_key_range(key_range)
            return MapKeyRangePart(key_begin=begin, key_end=end, inverted=inverted)

        # Check for mapKeyList: {a,b,c} or {!a,b,c}
        if hasattr(ctx, 'mapKeyList') and ctx.mapKeyList() is not None:
            list_ctx = ctx.mapKeyList()
            inverted = False
            if hasattr(list_ctx, 'standardMapKeyList') and list_ctx.standardMapKeyList() is not None:
                key_list = list_ctx.standardMapKeyList().keyListIdentifier()
            elif hasattr(list_ctx, 'invertedMapKeyList') and list_ctx.invertedMapKeyList() is not None:
                key_list = list_ctx.invertedMapKeyList().keyListIdentifier()
                inverted = True
            else:
                return None
            keys = self._parse_key_list(key_list)
            return MapKeyListPart(keys=keys, inverted=inverted)

        # Check for mapIndexRange: {start:end} or {!start:end}
        if hasattr(ctx, 'mapIndexRange') and ctx.mapIndexRange() is not None:
            range_ctx = ctx.mapIndexRange()
            inverted = False
            if hasattr(range_ctx, 'standardMapIndexRange') and range_ctx.standardMapIndexRange() is not None:
                idx_range = range_ctx.standardMapIndexRange().indexRangeIdentifier()
            elif hasattr(range_ctx, 'invertedMapIndexRange') and range_ctx.invertedMapIndexRange() is not None:
                idx_range = range_ctx.invertedMapIndexRange().indexRangeIdentifier()
                inverted = True
            else:
                return None
            start, count = self._parse_index_range(idx_range)
            return MapIndexRangePart(start=start, count=count, inverted=inverted)

        # Check for mapValueList: {=a,b,c} or {!=a,b,c}
        if hasattr(ctx, 'mapValueList') and ctx.mapValueList() is not None:
            list_ctx = ctx.mapValueList()
            inverted = False
            if hasattr(list_ctx, 'standardMapValueList') and list_ctx.standardMapValueList() is not None:
                val_list = list_ctx.standardMapValueList().valueListIdentifier()
            elif hasattr(list_ctx, 'invertedMapValueList') and list_ctx.invertedMapValueList() is not None:
                val_list = list_ctx.invertedMapValueList().valueListIdentifier()
                inverted = True
            else:
                return None
            values = self._parse_value_list(val_list)
            return MapValueListPart(values=values, inverted=inverted)

        # Check for mapValueRange: {=start:end} or {!=start:end}
        if hasattr(ctx, 'mapValueRange') and ctx.mapValueRange() is not None:
            range_ctx = ctx.mapValueRange()
            inverted = False
            if hasattr(range_ctx, 'standardMapValueRange') and range_ctx.standardMapValueRange() is not None:
                val_range = range_ctx.standardMapValueRange().valueRangeIdentifier()
            elif hasattr(range_ctx, 'invertedMapValueRange') and range_ctx.invertedMapValueRange() is not None:
                val_range = range_ctx.invertedMapValueRange().valueRangeIdentifier()
                inverted = True
            else:
                return None
            begin, end = self._parse_value_range(val_range)
            return MapValueRangePart(value_begin=begin, value_end=end, inverted=inverted)

        # Check for mapRankRange: {#start:count} or {!#start:count}
        if hasattr(ctx, 'mapRankRange') and ctx.mapRankRange() is not None:
            range_ctx = ctx.mapRankRange()
            inverted = False
            if hasattr(range_ctx, 'standardMapRankRange') and range_ctx.standardMapRankRange() is not None:
                rank_range = range_ctx.standardMapRankRange().rankRangeIdentifier()
            elif hasattr(range_ctx, 'invertedMapRankRange') and range_ctx.invertedMapRankRange() is not None:
                rank_range = range_ctx.invertedMapRankRange().rankRangeIdentifier()
                inverted = True
            else:
                return None
            start, count = self._parse_rank_range(rank_range)
            return MapRankRangePart(start=start, count=count, inverted=inverted)

        # Check for mapRankRangeRelative: {#start:end~value} or {!#start:end~value}
        if hasattr(ctx, 'mapRankRangeRelative') and ctx.mapRankRangeRelative() is not None:
            range_ctx = ctx.mapRankRangeRelative()
            inverted = False
            if hasattr(range_ctx, 'standardMapRankRangeRelative') and range_ctx.standardMapRankRangeRelative() is not None:
                rel_range = range_ctx.standardMapRankRangeRelative().rankRangeRelativeIdentifier()
            elif hasattr(range_ctx, 'invertedMapRankRangeRelative') and range_ctx.invertedMapRankRangeRelative() is not None:
                rel_range = range_ctx.invertedMapRankRangeRelative().rankRangeRelativeIdentifier()
                inverted = True
            else:
                return None
            rank, count, value = self._parse_rank_range_relative(rel_range)
            return MapRankRangeRelativePart(rank=rank, value=value, count=count, inverted=inverted)

        # Check for mapIndexRangeRelative: {start:end~key} or {!start:end~key}
        if hasattr(ctx, 'mapIndexRangeRelative') and ctx.mapIndexRangeRelative() is not None:
            range_ctx = ctx.mapIndexRangeRelative()
            inverted = False
            if hasattr(range_ctx, 'standardMapIndexRangeRelative') and range_ctx.standardMapIndexRangeRelative() is not None:
                rel_range = range_ctx.standardMapIndexRangeRelative().indexRangeRelativeIdentifier()
            elif hasattr(range_ctx, 'invertedMapIndexRangeRelative') and range_ctx.invertedMapIndexRangeRelative() is not None:
                rel_range = range_ctx.invertedMapIndexRangeRelative().indexRangeRelativeIdentifier()
                inverted = True
            else:
                return None
            index, count, key = self._parse_index_range_relative(rel_range)
            return MapIndexRangeRelativePart(index=index, key=key, count=count, inverted=inverted)

        # Check for MAP_TYPE_DESIGNATOR: {}
        if hasattr(ctx, 'MAP_TYPE_DESIGNATOR') and ctx.MAP_TYPE_DESIGNATOR() is not None:
            return None

        return None

    def _parse_key_range(self, ctx) -> tuple[Optional[str], Optional[str]]:
        """Parse keyRangeIdentifier into (begin, end)."""
        if ctx is None:
            return None, None
        # keyRangeIdentifier has mapKey children
        key_ids = ctx.mapKey()
        if len(key_ids) >= 2:
            begin = self._parse_map_key(key_ids[0])
            end = self._parse_map_key(key_ids[1])
        elif len(key_ids) == 1:
            begin = self._parse_map_key(key_ids[0])
            end = None
        else:
            begin = None
            end = None
        return begin, end

    def _parse_key_list(self, ctx) -> List[str]:
        """Parse keyListIdentifier into a list of keys."""
        if ctx is None:
            return []
        keys = []
        # keyListIdentifier has mapKey children
        for key_ctx in ctx.mapKey():
            keys.append(self._parse_map_key(key_ctx))
        return keys

    @staticmethod
    def _parse_map_key(ctx) -> Any:
        """Parse a ``mapKey`` parser context into its typed value.

        Returns ``str`` for NAME_IDENTIFIER / QUOTED_STRING / IN / reservedWord,
        and ``int`` for an INT literal (decimal, ``0x...``, or ``0b...``). An
        empty context resolves to ``""`` so callers can branch cheaply.
        """
        if ctx is None:
            return ""
        if hasattr(ctx, 'NAME_IDENTIFIER') and ctx.NAME_IDENTIFIER() is not None:
            return ctx.NAME_IDENTIFIER().getText()
        if hasattr(ctx, 'QUOTED_STRING') and ctx.QUOTED_STRING() is not None:
            return _unquote(ctx.QUOTED_STRING().getText())
        if hasattr(ctx, 'INT') and ctx.INT() is not None:
            return _parse_int_literal(ctx.INT().getText())
        # IN keyword and reservedWord alternatives — fall through to literal text.
        return ctx.getText()

    def visitStringOperand(self, ctx: ConditionParser.StringOperandContext) -> ExprOrDeferred:
        """Visit string operand: 'value' or "value" """
        text = ctx.getText()
        unquoted = _unquote(text)
        expr = FilterExpression.string_val(unquoted)
        return TypedExpr(expr, InferredType.STRING, value=unquoted)

    def visitIntOperand(self, ctx: ConditionParser.IntOperandContext) -> ExprOrDeferred:
        """Visit integer operand: 123, 0xFF, 0b1010"""
        text = ctx.INT().getText()
        value = int(text, 0)
        expr = FilterExpression.int_val(value)
        return TypedExpr(expr, InferredType.INT)

    def visitFloatOperand(self, ctx: ConditionParser.FloatOperandContext) -> ExprOrDeferred:
        """Visit float operand: 123.45 or .5"""
        if ctx.FLOAT() is not None:
            text = ctx.FLOAT().getText()
        else:
            text = ctx.LEADING_DOT_FLOAT().getText()
        value = float(text)
        expr = FilterExpression.float_val(value)
        return TypedExpr(expr, InferredType.FLOAT)

    def visitBooleanOperand(self, ctx: ConditionParser.BooleanOperandContext) -> ExprOrDeferred:
        """Visit boolean operand: true or false"""
        text = ctx.getText().lower()
        value = text == "true"
        expr = FilterExpression.bool_val(value)
        return TypedExpr(expr, InferredType.BOOL)

    def visitNumberOperand(self, ctx: ConditionParser.NumberOperandContext) -> Optional[FilterExpression]:
        """Visit number operand - delegates to int or float."""
        return self.visitChildren(ctx)

    def visitOperand(self, ctx: ConditionParser.OperandContext) -> Optional[FilterExpression]:
        """Visit operand - can be number, string, boolean, path, variable, etc."""
        if ctx.operandCast() is not None:
            return self.visit(ctx.operandCast())

        if ctx.functionCall() is not None:
            return self.visit(ctx.functionCall())

        if ctx.pathOrMetadata() is not None:
            return self.visit(ctx.pathOrMetadata())

        if ctx.numberOperand() is not None:
            return self.visit(ctx.numberOperand())

        if ctx.booleanOperand() is not None:
            return self.visit(ctx.booleanOperand())

        if ctx.stringOperand() is not None:
            return self.visit(ctx.stringOperand())

        if ctx.expression() is not None:
            return self.visit(ctx.expression())

        if ctx.variable() is not None:
            return self.visit(ctx.variable())

        if ctx.listConstant() is not None:
            return self.visit(ctx.listConstant())

        if ctx.orderedMapConstant() is not None:
            return self.visit(ctx.orderedMapConstant())

        if ctx.placeholder() is not None:
            return self.visit(ctx.placeholder())

        if ctx.notExpression() is not None:
            return self.visit(ctx.notExpression())

        if ctx.exclusiveExpression() is not None:
            return self.visit(ctx.exclusiveExpression())

        if ctx.letExpression() is not None:
            return self.visit(ctx.letExpression())

        if ctx.whenExpression() is not None:
            return self.visit(ctx.whenExpression())

        if ctx.unknownExpression() is not None:
            return self.visit(ctx.unknownExpression())

        raise AelParseException(f"Unsupported operand type: {ctx.getText()}")

    def visitPathOrMetadata(
        self, ctx: ConditionParser.PathOrMetadataContext,
    ) -> Optional[ExprOrDeferred]:
        """Visit path or metadata: $.binName or metadata function."""
        if ctx.path() is not None:
            return self.visit(ctx.path())
        if ctx.metadata() is not None:
            return self.visit(ctx.metadata())
        raise AelParseException("PathOrMetadata must contain either path or metadata")

    def visitMetadata(
        self, ctx: ConditionParser.MetadataContext,
    ) -> Optional[Union[FilterExpression, TypedExpr]]:
        """Visit metadata function: deviceSize(), ttl(), etc."""
        text = ctx.METADATA_FUNCTION().getText()
        
        # Map metadata functions to FilterExpression methods
        if text == "deviceSize()":
            return TypedExpr(FilterExpression.device_size(), InferredType.INT)
        elif text == "memorySize()":
            return TypedExpr(FilterExpression.device_size(), InferredType.INT)
        elif text == "recordSize()":
            return TypedExpr(FilterExpression.device_size(), InferredType.INT)
        elif text == "isTombstone()":
            return TypedExpr(FilterExpression.is_tombstone(), InferredType.BOOL)
        elif text == "keyExists()":
            return TypedExpr(FilterExpression.key_exists(), InferredType.BOOL)
        elif text == "lastUpdate()":
            return TypedExpr(FilterExpression.last_update(), InferredType.INT)
        elif text == "sinceUpdate()":
            return TypedExpr(FilterExpression.since_update(), InferredType.INT)
        elif text == "setName()":
            return TypedExpr(FilterExpression.set_name(), InferredType.STRING)
        elif text == "ttl()":
            return TypedExpr(FilterExpression.ttl(), InferredType.INT)
        elif text == "voidTime()":
            return TypedExpr(FilterExpression.void_time(), InferredType.INT)
        elif text.startswith("digestModulo(") and text.endswith(")"):
            match = re.search(r"digestModulo\((-?(?:0[xX][0-9a-fA-F]+|0[bB][01]+|\d+))\)", text)
            if match:
                value = int(match.group(1), 0)
                return TypedExpr(FilterExpression.digest_modulo(value), InferredType.INT)
        
        raise AelParseException(f"Unsupported metadata function: {text}")

    def visitExclusiveExpression(self, ctx: ConditionParser.ExclusiveExpressionContext) -> Optional[FilterExpression]:
        """Visit exclusive expression: exclusive(expr1, expr2, ...)

        Bare bins in logical context are inferred as bool_bin.
        """
        if len(ctx.expression()) < 2:
            raise AelParseException("Exclusive expression requires at least 2 expressions")

        expressions: List[FilterExpression] = []
        for expr_ctx in ctx.expression():
            expr = _finalize_result(self.visit(expr_ctx), InferredType.BOOL)
            if expr is None:
                raise AelParseException("Failed to parse expression in exclusive clause")
            expressions.append(expr)

        # Exclusive is equivalent to XOR of all expressions
        result = expressions[0]
        for expr in expressions[1:]:
            result = FilterExpression.xor([result, expr])
        return result

    def visitLetExpression(self, ctx: ConditionParser.LetExpressionContext) -> Optional[FilterExpression]:
        """Visit let expression: let(var1=expr1, var2=expr2) then (action)

        Translates to: FilterExpression.exp_let([def1, def2, ..., action])

        Example:
            let (x = 1, y = ${x} + 1) then (${x} + ${y})
            ->
            FilterExpression.exp_let([
                FilterExpression.def_("x", FilterExpression.int_val(1)),
                FilterExpression.def_("y", ...),
                FilterExpression.num_add([FilterExpression.var("x"), FilterExpression.var("y")])
            ])
        """
        self._push_var_scope()
        try:
            definitions: List[FilterExpression] = []
            for var_def_ctx in ctx.variableDefinition():
                var_name = var_def_ctx.NAME_IDENTIFIER().getText()
                value_expr = self.visit(var_def_ctx.expression())
                self._set_var_type(var_name, _get_type_hint(value_expr))
                value_expr = _resolve_for_arithmetic(value_expr)
                definitions.append(FilterExpression.def_(var_name, value_expr))

            action_expr = self.visit(ctx.expression())
            action_expr = _resolve_for_arithmetic(action_expr)
        finally:
            self._pop_var_scope()

        all_exprs = definitions + [action_expr]
        return FilterExpression.exp_let(all_exprs)

    def visitWhenExpression(self, ctx: ConditionParser.WhenExpressionContext) -> Optional[FilterExpression]:
        """Visit when expression: when(cond1=>action1, cond2=>action2, default=>action)
        
        Translates to: FilterExpression.cond([bool1, action1, bool2, action2, ..., default_action])
        
        Example:
            when ($.who == 1 => "bob", $.who == 2 => "fred", default => "other")
            ->
            FilterExpression.cond([
                FilterExpression.eq(FilterExpression.int_bin("who"), FilterExpression.int_val(1)),
                FilterExpression.string_val("bob"),
                FilterExpression.eq(FilterExpression.int_bin("who"), FilterExpression.int_val(2)),
                FilterExpression.string_val("fred"),
                FilterExpression.string_val("other")
            ])
        """
        cond_exprs: List[FilterExpression] = []
        
        # Process each condition => action mapping
        for mapping_ctx in ctx.expressionMapping():
            # expression(0) is the condition, expression(1) is the action
            condition = self.visit(mapping_ctx.expression(0))
            condition = _resolve_for_arithmetic(condition)
            
            action = self.visit(mapping_ctx.expression(1))
            action = _resolve_for_arithmetic(action)
            
            cond_exprs.append(condition)
            cond_exprs.append(action)
        
        # Get the default action (the expression after 'default =>')
        default_action = self.visit(ctx.expression())
        default_action = _resolve_for_arithmetic(default_action)
        cond_exprs.append(default_action)
        
        return FilterExpression.cond(cond_exprs)

    def visitBitwiseExpressionWrapper(self, ctx: ConditionParser.BitwiseExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.bitwiseExpression())

    def visitShiftExpressionWrapper(self, ctx: ConditionParser.ShiftExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.shiftExpression())

    def visitAdditiveExpressionWrapper(self, ctx: ConditionParser.AdditiveExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.additiveExpression())

    def visitMultiplicativeExpressionWrapper(self, ctx: ConditionParser.MultiplicativeExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.multiplicativeExpression())

    def visitPowerExpressionWrapper(self, ctx: ConditionParser.PowerExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.powerExpression())

    def visitUnaryExpressionWrapper(self, ctx: ConditionParser.UnaryExpressionWrapperContext) -> Optional[FilterExpression]:
        """Pass through wrapper."""
        return self.visit(ctx.unaryExpression())

    def visitOperandExpression(self, ctx: ConditionParser.OperandExpressionContext) -> Optional[FilterExpression]:
        """Visit operand expression."""
        return self.visit(ctx.operand())

    def visitUnknownExpression(self, ctx: ConditionParser.UnknownExpressionContext) -> ExprOrDeferred:
        """Visit ``unknown`` / ``error`` keyword.

        Both produce ``Exp.unknown()`` — at evaluation the server raises an
        evaluator-unknown error, which short-circuits enclosing expressions
        the same way as a CDT path that fails to resolve.
        """
        return TypedExpr(FilterExpression.unknown(), InferredType.UNKNOWN)

    def visitUnaryMinusExpression(self, ctx: ConditionParser.UnaryMinusExpressionContext) -> ExprOrDeferred:
        """Visit unary minus: -expr"""
        neg = self._try_negate_number(ctx.unaryExpression())
        if neg is not None:
            return neg
        inner = self.visit(ctx.unaryExpression())
        resolved = _resolve_for_arithmetic(inner, has_float=False)
        return FilterExpression.num_mul([resolved, FilterExpression.int_val(-1)])

    def _try_negate_number(self, unary_ctx) -> Optional[ExprOrDeferred]:
        """Optimise literal negation through unary chains (e.g. -5, --5, -+5)."""
        result = self._try_extract_signed_number(unary_ctx)
        if result is None:
            return None
        value, typ = result
        value = -value
        if typ == InferredType.INT:
            return TypedExpr(FilterExpression.int_val(int(value)), typ)
        return TypedExpr(FilterExpression.float_val(float(value)), typ)

    def _try_extract_signed_number(self, unary_ctx) -> Optional[tuple]:
        """Resolve a chain of unary +/- to a signed number literal, or None."""
        if isinstance(unary_ctx, ConditionParser.OperandExpressionContext):
            operand = unary_ctx.operand()
            if operand is None:
                return None
            if operand.operandCast() is not None:
                return self._extract_cast_number(operand.operandCast())
            if operand.numberOperand() is None:
                return None
            return self._extract_plain_number(operand.numberOperand())
        if isinstance(unary_ctx, ConditionParser.UnaryMinusExpressionContext):
            inner = self._try_extract_signed_number(unary_ctx.unaryExpression())
            return (-inner[0], inner[1]) if inner is not None else None
        if isinstance(unary_ctx, ConditionParser.UnaryPlusExpressionContext):
            return self._try_extract_signed_number(unary_ctx.unaryExpression())
        return None

    @staticmethod
    def _extract_plain_number(num: ConditionParser.NumberOperandContext) -> Optional[tuple]:
        if num.intOperand() is not None:
            return int(num.intOperand().INT().getText(), 0), InferredType.INT
        if num.floatOperand() is not None:
            fctx = num.floatOperand()
            text = fctx.FLOAT().getText() if fctx.FLOAT() is not None else fctx.LEADING_DOT_FLOAT().getText()
            return float(text), InferredType.FLOAT
        return None

    @staticmethod
    def _extract_cast_number(cast_ctx: ConditionParser.OperandCastContext) -> Optional[tuple]:
        num = cast_ctx.numberOperand()
        text = num.getText()
        raw = int(text, 0) if num.intOperand() is not None else float(text)
        cast_fn = cast_ctx.pathFunctionCast().PATH_FUNCTION_CAST().getText()
        if cast_fn == "asInt()":
            return int(raw), InferredType.INT
        if cast_fn == "asFloat()":
            return float(raw), InferredType.FLOAT
        return None

    def visitUnaryPlusExpression(self, ctx: ConditionParser.UnaryPlusExpressionContext) -> ExprOrDeferred:
        """Visit unary plus: +expr (identity)"""
        return self.visit(ctx.unaryExpression())

    def visitAddExpression(self, ctx: ConditionParser.AddExpressionContext) -> ExprOrDeferred:
        """Visit add expression: left + right

        If operands contain deferred types, returns DeferredArithmetic for
        later type inference from comparison context.
        """
        left = self.visit(ctx.additiveExpression())
        right = self.visit(ctx.multiplicativeExpression())
        return _build_arithmetic(ArithOp.ADD, left, right)

    def visitSubExpression(self, ctx: ConditionParser.SubExpressionContext) -> ExprOrDeferred:
        """Visit subtract expression: left - right

        If operands contain deferred types, returns DeferredArithmetic for
        later type inference from comparison context.
        """
        left = self.visit(ctx.additiveExpression())
        right = self.visit(ctx.multiplicativeExpression())
        return _build_arithmetic(ArithOp.SUB, left, right)

    def visitMulExpression(self, ctx: ConditionParser.MulExpressionContext) -> ExprOrDeferred:
        """Visit multiply expression: left * right

        If operands contain deferred types, returns DeferredArithmetic for
        later type inference from comparison context.
        """
        left = self.visit(ctx.multiplicativeExpression())
        right = self.visit(ctx.powerExpression())
        return _build_arithmetic(ArithOp.MUL, left, right)

    def visitDivExpression(self, ctx: ConditionParser.DivExpressionContext) -> ExprOrDeferred:
        """Visit divide expression: left / right

        If operands contain deferred types, returns DeferredArithmetic for
        later type inference from comparison context.
        """
        left = self.visit(ctx.multiplicativeExpression())
        right = self.visit(ctx.powerExpression())
        return _build_arithmetic(ArithOp.DIV, left, right)

    def visitModExpression(self, ctx: ConditionParser.ModExpressionContext) -> ExprOrDeferred:
        """Visit modulo expression: left % right

        Modulo always uses INT (no float support).
        """
        left = self.visit(ctx.multiplicativeExpression())
        right = self.visit(ctx.powerExpression())
        resolved_left = _resolve_for_arithmetic(left, has_float=False)
        resolved_right = _resolve_for_arithmetic(right, has_float=False)
        return FilterExpression.num_mod(resolved_left, resolved_right)

    def visitPowExpression(self, ctx: ConditionParser.PowExpressionContext) -> ExprOrDeferred:
        """Visit power infix: left ** right (right-associative, float-only)"""
        left = self.visit(ctx.powerExpression(0))
        right = self.visit(ctx.powerExpression(1))
        resolved_left = _resolve_for_arithmetic(left, has_float=True)
        resolved_right = _resolve_for_arithmetic(right, has_float=True)
        return FilterExpression.num_pow(resolved_left, resolved_right)

    def visitIntAndExpression(self, ctx: ConditionParser.IntAndExpressionContext) -> Optional[FilterExpression]:
        """Visit integer AND expression: left & right
        
        Grammar: bitwiseExpression '&' shiftExpression
        """
        left = self.visit(ctx.bitwiseExpression())
        right = self.visit(ctx.shiftExpression())
        resolved_left = _resolve_for_arithmetic(left, has_float=False)
        resolved_right = _resolve_for_arithmetic(right, has_float=False)
        return FilterExpression.int_and([resolved_left, resolved_right])

    def visitIntOrExpression(self, ctx: ConditionParser.IntOrExpressionContext) -> Optional[FilterExpression]:
        """Visit integer OR expression: left | right
        
        Grammar: bitwiseExpression '|' shiftExpression
        """
        left = self.visit(ctx.bitwiseExpression())
        right = self.visit(ctx.shiftExpression())
        resolved_left = _resolve_for_arithmetic(left, has_float=False)
        resolved_right = _resolve_for_arithmetic(right, has_float=False)
        return FilterExpression.int_or([resolved_left, resolved_right])

    def visitIntXorExpression(self, ctx: ConditionParser.IntXorExpressionContext) -> Optional[FilterExpression]:
        """Visit integer XOR expression: left ^ right
        
        Grammar: bitwiseExpression '^' shiftExpression
        """
        left = self.visit(ctx.bitwiseExpression())
        right = self.visit(ctx.shiftExpression())
        resolved_left = _resolve_for_arithmetic(left, has_float=False)
        resolved_right = _resolve_for_arithmetic(right, has_float=False)
        return FilterExpression.int_xor([resolved_left, resolved_right])

    def visitIntNotExpression(self, ctx: ConditionParser.IntNotExpressionContext) -> Optional[FilterExpression]:
        """Visit integer NOT expression: ~expr

        Grammar: '~' unaryExpression
        """
        expr = self.visit(ctx.unaryExpression())
        resolved = _resolve_for_arithmetic(expr, has_float=False)
        return FilterExpression.int_not(resolved)

    def visitIntLShiftExpression(self, ctx: ConditionParser.IntLShiftExpressionContext) -> Optional[FilterExpression]:
        """Visit left shift expression: left << right

        Grammar: shiftExpression '<<' additiveExpression
        """
        value = self.visit(ctx.shiftExpression())
        shift = self.visit(ctx.additiveExpression())
        resolved_value = _resolve_for_arithmetic(value, has_float=False)
        resolved_shift = _resolve_for_arithmetic(shift, has_float=False)
        return FilterExpression.int_lshift(resolved_value, resolved_shift)

    def visitIntArithmeticRShiftExpression(self, ctx: ConditionParser.IntArithmeticRShiftExpressionContext) -> Optional[FilterExpression]:
        """Visit arithmetic right shift: left >> right (sign-preserving)"""
        value = self.visit(ctx.shiftExpression())
        shift = self.visit(ctx.additiveExpression())
        resolved_value = _resolve_for_arithmetic(value, has_float=False)
        resolved_shift = _resolve_for_arithmetic(shift, has_float=False)
        return FilterExpression.int_arshift(resolved_value, resolved_shift)

    def visitIntLogicalRShiftExpression(self, ctx: ConditionParser.IntLogicalRShiftExpressionContext) -> Optional[FilterExpression]:
        """Visit logical right shift: left >>> right (zero-fill)"""
        value = self.visit(ctx.shiftExpression())
        shift = self.visit(ctx.additiveExpression())
        resolved_value = _resolve_for_arithmetic(value, has_float=False)
        resolved_shift = _resolve_for_arithmetic(shift, has_float=False)
        return FilterExpression.int_rshift(resolved_value, resolved_shift)

    # --- arithmetic function visitors ---

    def visitFunctionCall(self, ctx: ConditionParser.FunctionCallContext) -> ExprOrDeferred:
        if ctx.getChildCount() < 3 or ctx.getChild(1).getText() != "(":
            name = ctx.getChild(0).getText() if ctx.getChildCount() > 0 else "<unknown>"
            raise AelParseException(f"Unexpected identifier: {name}")

        name = ctx.NAME_IDENTIFIER().getText()
        args = [self.visit(e) for e in ctx.expression()]

        match name:
            case "abs":
                _validate_arg_count(name, args, 1)
                if _contains_deferred(args[0]):
                    return DeferredArithmetic(ArithOp.ABS, args)
                abs_type = _get_type_hint(args[0])
                abs_type = abs_type if abs_type in _NUMERIC_TYPES else InferredType.INT
                return TypedExpr(
                    FilterExpression.num_abs(_resolve_for_arithmetic(args[0])), abs_type,
                )
            case "ceil":
                _validate_arg_count(name, args, 1)
                return TypedExpr(
                    FilterExpression.num_ceil(_resolve_for_arithmetic(args[0], has_float=True)),
                    InferredType.FLOAT,
                )
            case "floor":
                _validate_arg_count(name, args, 1)
                return TypedExpr(
                    FilterExpression.num_floor(_resolve_for_arithmetic(args[0], has_float=True)),
                    InferredType.FLOAT,
                )
            case "log":
                _validate_arg_count(name, args, 2)
                return TypedExpr(FilterExpression.num_log(
                    _resolve_for_arithmetic(args[0], has_float=True),
                    _resolve_for_arithmetic(args[1], has_float=True),
                ), InferredType.FLOAT)
            case "pow":
                _validate_arg_count(name, args, 2)
                return TypedExpr(FilterExpression.num_pow(
                    _resolve_for_arithmetic(args[0], has_float=True),
                    _resolve_for_arithmetic(args[1], has_float=True),
                ), InferredType.FLOAT)
            case "max":
                _validate_min_arg_count(name, args, 2)
                if any(_contains_deferred(v) for v in args):
                    return DeferredArithmetic(ArithOp.MAX, args)
                has_float = any(_is_float_context(v) for v in args)
                result_type = InferredType.FLOAT if has_float else InferredType.INT
                return TypedExpr(FilterExpression.max(
                    [_resolve_for_arithmetic(v, has_float=has_float) for v in args]
                ), result_type)
            case "min":
                _validate_min_arg_count(name, args, 2)
                if any(_contains_deferred(v) for v in args):
                    return DeferredArithmetic(ArithOp.MIN, args)
                has_float = any(_is_float_context(v) for v in args)
                result_type = InferredType.FLOAT if has_float else InferredType.INT
                return TypedExpr(FilterExpression.min(
                    [_resolve_for_arithmetic(v, has_float=has_float) for v in args]
                ), result_type)
            case "countOneBits":
                _validate_arg_count(name, args, 1)
                return TypedExpr(
                    FilterExpression.int_count(_resolve_for_arithmetic(args[0], has_float=False)),
                    InferredType.INT,
                )
            case "findBitLeft":
                _validate_arg_count(name, args, 2)
                return TypedExpr(FilterExpression.int_lscan(
                    _resolve_for_arithmetic(args[0], has_float=False),
                    _resolve_for_arithmetic(args[1], has_float=False),
                ), InferredType.INT)
            case "findBitRight":
                _validate_arg_count(name, args, 2)
                return TypedExpr(FilterExpression.int_rscan(
                    _resolve_for_arithmetic(args[0], has_float=False),
                    _resolve_for_arithmetic(args[1], has_float=False),
                ), InferredType.INT)
            case "geoJson":
                _validate_arg_count(name, args, 1)
                arg = args[0]
                if not (isinstance(arg, TypedExpr) and arg.type_hint == InferredType.STRING
                        and isinstance(arg.value, str)):
                    raise AelParseException(
                        "geoJson() requires a single string literal argument",
                    )
                return TypedExpr(FilterExpression.geo_val(arg.value), InferredType.GEO)
            case "geoCompare":
                _validate_arg_count(name, args, 2)
                left = _finalize_result(args[0], InferredType.GEO)
                right = _finalize_result(args[1], InferredType.GEO)
                if left is None or right is None:
                    raise AelParseException(
                        "geoCompare() requires two GEO-typed arguments",
                    )
                return TypedExpr(
                    FilterExpression.geo_compare(left, right), InferredType.BOOL,
                )
            case _:
                raise AelParseException(f"Unknown function: {name}")

    def visitOperandCast(self, ctx: ConditionParser.OperandCastContext) -> ExprOrDeferred:
        num = ctx.numberOperand()
        text = num.getText()
        cast_fn = ctx.pathFunctionCast().PATH_FUNCTION_CAST().getText()

        if num.intOperand() is not None:
            raw = int(text, 0)
        else:
            raw = float(text)

        if cast_fn == "asInt()":
            return TypedExpr(FilterExpression.int_val(int(raw)), InferredType.INT)
        if cast_fn == "asFloat()":
            return TypedExpr(FilterExpression.float_val(float(raw)), InferredType.FLOAT)
        raise AelParseException(f"Unknown cast function: {cast_fn}")

    def _unary_expr_to_python_value(self, unary_ctx) -> Any:
        """Extract a Python value from a unaryExpression (for list/map literals).

        Handles unary minus/plus on numeric operands, and delegates plain
        operands to _operand_to_python_value.
        """
        if isinstance(unary_ctx, ConditionParser.UnaryMinusExpressionContext):
            inner = self._unary_expr_to_python_value(unary_ctx.unaryExpression())
            if not isinstance(inner, (int, float)):
                raise AelParseException("Unary minus in list/map constant requires a numeric value")
            return -inner
        if isinstance(unary_ctx, ConditionParser.UnaryPlusExpressionContext):
            return self._unary_expr_to_python_value(unary_ctx.unaryExpression())
        if isinstance(unary_ctx, ConditionParser.OperandExpressionContext):
            return self._operand_to_python_value(unary_ctx.operand())
        raise AelParseException(
            "List and map constants may only contain constants (number, string, boolean, list, map)"
        )

    def _operand_to_python_value(self, oper_ctx: ConditionParser.OperandContext) -> Any:
        """Extract a Python value from an operand that must be a constant (for list/map literals)."""
        if oper_ctx.operandCast() is not None:
            cast_ctx = oper_ctx.operandCast()
            num = cast_ctx.numberOperand()
            text = num.getText()
            raw = int(text, 0) if num.intOperand() is not None else float(text)
            cast_fn = cast_ctx.pathFunctionCast().PATH_FUNCTION_CAST().getText()
            if cast_fn == "asInt()":
                return int(raw)
            if cast_fn == "asFloat()":
                return float(raw)
            raise AelParseException(f"Unknown cast function: {cast_fn}")
        if oper_ctx.numberOperand() is not None:
            num = oper_ctx.numberOperand()
            text = num.getText()
            if num.floatOperand() is not None:
                return float(text)
            return int(text, 0)
        if oper_ctx.stringOperand() is not None:
            return _unquote(oper_ctx.stringOperand().getText())
        if oper_ctx.booleanOperand() is not None:
            return oper_ctx.booleanOperand().getText().lower() == "true"
        if oper_ctx.listConstant() is not None:
            lc = oper_ctx.listConstant()
            if lc.LIST_TYPE_DESIGNATOR() is not None:
                return []
            return [
                self._unary_expr_to_python_value(u)
                for u in lc.unaryExpression()
            ]
        if oper_ctx.orderedMapConstant() is not None:
            omc = oper_ctx.orderedMapConstant()
            if omc.MAP_TYPE_DESIGNATOR() is not None:
                return {}
            result: dict = {}
            for pair in omc.mapPairConstant():
                key_oper = pair.mapKeyOperand()
                if key_oper.intOperand() is not None:
                    key = int(key_oper.intOperand().getText(), 0)
                else:
                    key = _unquote(key_oper.stringOperand().getText())
                value = self._unary_expr_to_python_value(pair.unaryExpression())
                result[key] = value
            return result
        raise AelParseException(
            "List and map constants may only contain constants (number, string, boolean, list, map)"
        )

    def visitListConstant(self, ctx: ConditionParser.ListConstantContext) -> ExprOrDeferred:
        """Visit list constant: [val1, val2, ...] or [] (empty)"""
        if ctx.LIST_TYPE_DESIGNATOR() is not None:
            return TypedExpr(FilterExpression.list_val([]), InferredType.LIST, [])
        values: List[Any] = []
        for unary_ctx in ctx.unaryExpression():
            values.append(self._unary_expr_to_python_value(unary_ctx))
        expr = FilterExpression.list_val(values)
        return TypedExpr(expr, InferredType.LIST, values)

    def visitOrderedMapConstant(self, ctx: ConditionParser.OrderedMapConstantContext) -> ExprOrDeferred:
        """Visit ordered map constant: {key1: val1, key2: val2, ...} or {} (empty)"""
        if ctx.MAP_TYPE_DESIGNATOR() is not None:
            return TypedExpr(FilterExpression.map_val({}), InferredType.MAP)
        result: dict = {}
        for pair in ctx.mapPairConstant():
            key_oper = pair.mapKeyOperand()
            if key_oper.intOperand() is not None:
                key = int(key_oper.intOperand().getText(), 0)
            else:
                key = _unquote(key_oper.stringOperand().getText())
            value = self._unary_expr_to_python_value(pair.unaryExpression())
            result[key] = value
        expr = FilterExpression.map_val(result)
        return TypedExpr(expr, InferredType.MAP)

    def visitVariable(self, ctx: ConditionParser.VariableContext) -> ExprOrDeferred:
        """Visit variable: ${varName}"""
        var_name = ctx.getText()
        if var_name.startswith("${") and var_name.endswith("}"):
            var_name = var_name[2:-1]
        expr = FilterExpression.var(var_name)
        var_type = self._get_var_type(var_name)
        if var_type != InferredType.UNKNOWN:
            return TypedExpr(expr, var_type)
        return expr

    def visitPlaceholder(self, ctx: ConditionParser.PlaceholderContext) -> ExprOrDeferred:
        """Visit placeholder: ?0, ?1, etc.
        
        Resolves the placeholder to a value expression using the provided PlaceholderValues.
        The type of expression created depends on the Python type of the value:
            - int -> int_val
            - float -> float_val
            - str -> string_val
            - bool -> bool_val
            - bytes -> blob_val
            - list -> list_val
            - dict -> map_val
        """
        if self._placeholder_values is None:
            raise AelParseException("Placeholder used but no placeholder values provided")
        
        # Extract the index from ?0, ?1, etc.
        text = ctx.getText()  # e.g., "?0"
        try:
            index = int(text[1:])  # Remove the '?' prefix
        except ValueError:
            raise AelParseException(f"Invalid placeholder format: {text}")
        
        # Get the value from PlaceholderValues
        value = self._placeholder_values.get(index)
        
        # Convert Python value to FilterExpression based on type
        if isinstance(value, bool):
            # Check bool before int since bool is subclass of int
            return TypedExpr(FilterExpression.bool_val(value), InferredType.BOOL)
        elif isinstance(value, int):
            return TypedExpr(FilterExpression.int_val(value), InferredType.INT)
        elif isinstance(value, float):
            return TypedExpr(FilterExpression.float_val(value), InferredType.FLOAT)
        elif isinstance(value, str):
            return TypedExpr(FilterExpression.string_val(value), InferredType.STRING)
        elif isinstance(value, bytes):
            return TypedExpr(FilterExpression.blob_val(list(value)), InferredType.UNKNOWN)
        elif isinstance(value, (list, tuple)):
            vals = list(value)
            return TypedExpr(FilterExpression.list_val(vals), InferredType.LIST, value=vals)
        elif isinstance(value, dict):
            return TypedExpr(FilterExpression.map_val(value), InferredType.UNKNOWN)
        else:
            raise AelParseException(f"Unsupported placeholder value type: {type(value).__name__}")

    def visitPathFunction(self, ctx: ConditionParser.PathFunctionContext) -> Optional[FilterExpression]:
        """Visit path function: asInt(), asFloat(), get(), etc."""
        # Delegate to specific path function visitors
        return self.visitChildren(ctx)

    def visitPathFunctionCast(self, ctx: ConditionParser.PathFunctionCastContext) -> Optional[FilterExpression]:
        """Visit path function cast: asInt(), asFloat(), etc.
        
        These functions are applied to the base path in visitPath().
        For now, we return a marker that visitPath() can use.
        """
        # Path function cast is handled in visitPath() by applying to_int or to_float
        # to the base path. We return None here and handle it in visitPath().
        return None

    def visitPathFunctionExists(self, ctx: ConditionParser.PathFunctionExistsContext) -> Optional[FilterExpression]:
        """Visit path function exists: exists()

        The expression is built in :meth:`visitPath` so the bin name and any
        preceding CDT parts are in scope; this hook just acknowledges the
        token in the parse tree.
        """
        return None

    def visitPathFunctionCount(self, ctx: ConditionParser.PathFunctionCountContext) -> Optional[FilterExpression]:
        """Visit path function count: count()"""
        # Count() returns the size of a list/map
        # This needs to be applied to the base path in visitPath()
        return None

    def visitPathFunctionGet(self, ctx: ConditionParser.PathFunctionGetContext) -> Optional[FilterExpression]:
        """Visit path function get: get(type:INT, return:VALUE)"""
        # Get() is used for CDT operations
        # This is complex and may need special handling
        return None
