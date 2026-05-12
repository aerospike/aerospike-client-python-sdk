grammar Condition;

parse: expression EOF;

expression
    : logicalOrExpression
    ;

logicalOrExpression
    : logicalAndExpression ('or' logicalAndExpression)*                         # OrExpression
    ;

logicalAndExpression
    : comparisonExpression ('and' comparisonExpression)*                        # AndExpression
    ;

comparisonExpression
    : bitwiseExpression '>' bitwiseExpression                                   # GreaterThanExpression
    | bitwiseExpression '>=' bitwiseExpression                                  # GreaterThanOrEqualExpression
    | bitwiseExpression '<' bitwiseExpression                                   # LessThanExpression
    | bitwiseExpression '<=' bitwiseExpression                                  # LessThanOrEqualExpression
    | bitwiseExpression '==' bitwiseExpression                                  # EqualityExpression
    | bitwiseExpression '!=' bitwiseExpression                                  # InequalityExpression
    | bitwiseExpression IN bitwiseExpression                                    # InExpression
    | bitwiseExpression                                                         # BitwiseExpressionWrapper
    ;

bitwiseExpression
    : shiftExpression                                                           # ShiftExpressionWrapper
    | bitwiseExpression '&' shiftExpression                                     # IntAndExpression
    | bitwiseExpression '|' shiftExpression                                     # IntOrExpression
    | bitwiseExpression '^' shiftExpression                                     # IntXorExpression
    ;

shiftExpression
    : additiveExpression                                                        # AdditiveExpressionWrapper
    | shiftExpression '<<' additiveExpression                                   # IntLShiftExpression
    | shiftExpression '>>>' additiveExpression                                  # IntLogicalRShiftExpression
    | shiftExpression '>>' additiveExpression                                   # IntArithmeticRShiftExpression
    ;

additiveExpression
    : multiplicativeExpression                                                  # MultiplicativeExpressionWrapper
    | additiveExpression '+' multiplicativeExpression                           # AddExpression
    | additiveExpression '-' multiplicativeExpression                           # SubExpression
    ;

multiplicativeExpression
    : powerExpression                                                            # PowerExpressionWrapper
    | multiplicativeExpression '*' powerExpression                               # MulExpression
    | multiplicativeExpression '/' powerExpression                               # DivExpression
    | multiplicativeExpression '%' powerExpression                               # ModExpression
    ;

powerExpression
    : unaryExpression                                                            # UnaryExpressionWrapper
    | <assoc=right> powerExpression '**' powerExpression                         # PowExpression
    ;

unaryExpression
    : operand                                                                   # OperandExpression
    | '-' unaryExpression                                                       # UnaryMinusExpression
    | '+' unaryExpression                                                       # UnaryPlusExpression
    | '~' unaryExpression                                                       # IntNotExpression
    ;

variableDefinition
    : NAME_IDENTIFIER '=' expression
    ;

expressionMapping
    : expression '=>' expression
    ;

operand
    : operandCast
    | functionCall
    | numberOperand
    | booleanOperand
    | stringOperand
    | listConstant
    | orderedMapConstant
    | variable
    | placeholder
    | '$.' pathOrMetadata
    | '(' expression ')'
    | notExpression
    | exclusiveExpression
    | letExpression
    | whenExpression
    | unknownExpression
    ;

notExpression: 'not' '(' expression ')';

exclusiveExpression: 'exclusive' '(' expression (',' expression)+ ')';

letExpression: 'let' '(' variableDefinition (',' variableDefinition)* ')' 'then' '(' expression ')';

whenExpression: 'when' '(' expressionMapping (',' expressionMapping)* ',' 'default' '=>' expression ')';

unknownExpression: 'unknown' | 'error';

functionCall
    : NAME_IDENTIFIER '(' expression (',' expression)* ')'
    ;

operandCast
    : numberOperand '.' pathFunctionCast
    ;

numberOperand: intOperand | floatOperand;

intOperand: INT;
floatOperand: FLOAT | LEADING_DOT_FLOAT;

INT: ('0' [xX] [0-9a-fA-F]+ | '0' [bB] [01]+ | [0-9]+);
FLOAT: [0-9]+ '.' [0-9]+;

// Precedes LEADING_DOT_FLOAT so the lexer greedily captures .0xff and .0b101 as one token.
LEADING_DOT_FLOAT_HEX_OR_BINARY: '.' '0' ([xX] [0-9a-fA-F]+ | [bB] [01]+);

LEADING_DOT_SIGNED_INT: '.' [+-] [0-9]+;

// Supports the .N path syntax with predictable lexing.
LEADING_DOT_FLOAT: '.' [0-9]+;

booleanOperand: TRUE | FALSE;

TRUE: 'true';
FALSE: 'false';

stringOperand: QUOTED_STRING;

QUOTED_STRING: ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

listConstant: '[' unaryExpression? (',' unaryExpression)* ']' | LIST_TYPE_DESIGNATOR;

orderedMapConstant: '{' mapPairConstant? (',' mapPairConstant)* '}' | MAP_TYPE_DESIGNATOR;

mapPairConstant: mapKeyOperand ':' unaryExpression;

mapKeyOperand: intOperand | stringOperand;

variable: VARIABLE_REFERENCE;

VARIABLE_REFERENCE
    : '${' STRING_VARIABLE_NAME '}'
    ;

fragment STRING_VARIABLE_NAME
    : [a-zA-Z_][a-zA-Z0-9_]*
    ;

placeholder: PLACEHOLDER;

PLACEHOLDER: '?' [0-9]+;

pathOrMetadata: path | metadata;

path: basePath ('.' pathFunction)?;

basePath: binPart (('.' (mapPart | listPart)) | pathIntMapKey | pathHexBinaryMapKey)*?;

pathIntMapKey: LEADING_DOT_FLOAT | LEADING_DOT_SIGNED_INT;
pathHexBinaryMapKey: LEADING_DOT_FLOAT_HEX_OR_BINARY;

metadata: METADATA_FUNCTION;

METADATA_FUNCTION
    : 'deviceSize()'
    | 'memorySize()'
    | 'recordSize()'
    | 'isTombstone()'
    | 'keyExists()'
    | 'lastUpdate()'
    | 'sinceUpdate()'
    | 'setName()'
    | 'ttl()'
    | 'voidTime()'
    | 'digestModulo(' INT ')'
    ;

PATH_FUNCTION_GET: 'get';

pathFunctionParamName
    : PATH_FUNCTION_PARAM_TYPE
    | PATH_FUNCTION_PARAM_RETURN
    ;

PATH_FUNCTION_PARAM_TYPE: 'type';

PATH_FUNCTION_PARAM_RETURN: 'return';

pathFunctionParamValue: pathFunctionGetType | pathFunctionReturnType;

pathFunctionGetType
    : 'INT'
    | 'STRING'
    | 'HLL'
    | 'BLOB'
    | 'FLOAT'
    | 'BOOL'
    | 'LIST'
    | 'MAP'
    | 'GEO'
    ;

pathFunctionReturnType: PATH_FUNCTION_CDT_RETURN_TYPE;

PATH_FUNCTION_CDT_RETURN_TYPE
    : 'VALUE'
    | 'KEY_VALUE'
    | 'UNORDERED_MAP'
    | 'ORDERED_MAP'
    | 'KEY'
    | 'INDEX'
    | 'RANK'
    | 'COUNT'
    | 'NONE'
    | 'EXISTS'
    | 'REVERSE_INDEX'
    | 'REVERSE_RANK'
    ;

reservedWord
    : TRUE
    | FALSE
    | PATH_FUNCTION_GET
    | PATH_FUNCTION_PARAM_TYPE
    | PATH_FUNCTION_PARAM_RETURN
    | 'and'
    | 'or'
    | 'not'
    | 'exclusive'
    | 'let'
    | 'then'
    | 'when'
    | 'default'
    | 'remove'
    | 'insert'
    | 'set'
    | 'append'
    | 'increment'
    | 'clear'
    | 'sort'
    | 'unknown'
    | 'error'
    ;

binPart
    : BIN_IDENTIFIER
    | NAME_IDENTIFIER
    | QUOTED_STRING
    | IN
    | reservedWord
    ;

mapPart
    : MAP_TYPE_DESIGNATOR
    | mapKey
    | mapValue
    | mapRank
    | mapIndex
    | mapKeyRange
    | mapKeyList
    | mapIndexRange
    | mapValueList
    | mapValueRange
    | mapRankRange
    | mapRankRangeRelative
    | mapIndexRangeRelative
    ;

MAP_TYPE_DESIGNATOR: '{}';

mapKey
    : NAME_IDENTIFIER
    | QUOTED_STRING
    | IN
    | INT
    | reservedWord
    ;

mapValue: '{=' valueIdentifier '}';

mapRank: '{#' signedInt '}';

mapIndex: '{' signedInt '}';

mapKeyRange
    : standardMapKeyRange
    | invertedMapKeyRange
    ;

standardMapKeyRange
    : '{' keyRangeIdentifier '}'
    ;

invertedMapKeyRange
    : '{!' keyRangeIdentifier '}'
    ;

keyRangeIdentifier
    : mapKey '-' mapKey
    | mapKey '-'
    ;

mapKeyList
    : standardMapKeyList
    | invertedMapKeyList
    ;

standardMapKeyList
    : '{' keyListIdentifier '}'
    ;

invertedMapKeyList
    : '{!' keyListIdentifier '}'
    ;

keyListIdentifier
    : mapKey (',' mapKey)*
    ;

mapIndexRange
    : standardMapIndexRange
    | invertedMapIndexRange
    ;

standardMapIndexRange
    : '{' indexRangeIdentifier '}'
    ;

invertedMapIndexRange
    : '{!' indexRangeIdentifier '}'
    ;

indexRangeIdentifier
    : start ':' end
    | start ':'
    ;

signedInt: '-'? INT;

start: signedInt;
end: signedInt;

mapValueList
    : standardMapValueList
    | invertedMapValueList
    ;

standardMapValueList
    : '{=' valueListIdentifier '}'
    ;

invertedMapValueList
    : '{!=' valueListIdentifier '}'
    ;

mapValueRange
    : standardMapValueRange
    | invertedMapValueRange
    ;

standardMapValueRange
    : '{=' valueRangeIdentifier '}'
    ;

invertedMapValueRange
    : '{!=' valueRangeIdentifier '}'
    ;

valueRangeIdentifier
    : valueIdentifier ':' valueIdentifier
    | valueIdentifier ':'
    ;

mapRankRange
    : standardMapRankRange
    | invertedMapRankRange
    ;

standardMapRankRange
    : '{#' rankRangeIdentifier '}'
    ;

invertedMapRankRange
    : '{!#' rankRangeIdentifier '}'
    ;

rankRangeIdentifier
    : start ':' end
    | start ':'
    ;

mapRankRangeRelative
    : standardMapRankRangeRelative
    | invertedMapRankRangeRelative
    ;

standardMapRankRangeRelative
    : '{#' rankRangeRelativeIdentifier '}'
    ;

invertedMapRankRangeRelative
    : '{!#' rankRangeRelativeIdentifier '}'
    ;

rankRangeRelativeIdentifier
    : start ':' relativeRankEnd
    ;

relativeRankEnd
    : end relativeValue
    | relativeValue
    ;

relativeValue
    : '~' valueIdentifier
    ;

mapIndexRangeRelative
    : standardMapIndexRangeRelative
    | invertedMapIndexRangeRelative
    ;

standardMapIndexRangeRelative
    : '{' indexRangeRelativeIdentifier '}'
    ;

invertedMapIndexRangeRelative
    : '{!' indexRangeRelativeIdentifier '}'
    ;

indexRangeRelativeIdentifier
    : start ':' relativeKeyEnd
    ;

relativeKeyEnd
    : end '~' mapKey
    | '~' mapKey
    ;

listPart
    : LIST_TYPE_DESIGNATOR
    | listIndex
    | listValue
    | listRank
    | listIndexRange
    | listValueList
    | listValueRange
    | listRankRange
    | listRankRangeRelative
    ;

LIST_TYPE_DESIGNATOR: '[]';

listIndex: '[' signedInt ']';

listValue: '[=' valueIdentifier ']';

listRank: '[#' signedInt ']';

listIndexRange
    : standardListIndexRange
    | invertedListIndexRange
    ;

standardListIndexRange
    : '[' indexRangeIdentifier ']'
    ;

invertedListIndexRange
    : '[!' indexRangeIdentifier ']'
    ;

listValueList
    : standardListValueList
    | invertedListValueList
    ;

standardListValueList
    : '[=' valueListIdentifier ']'
    ;

invertedListValueList
    : '[!=' valueListIdentifier ']'
    ;

listValueRange
    : standardListValueRange
    | invertedListValueRange
    ;

standardListValueRange
    : '[=' valueRangeIdentifier ']'
    ;

invertedListValueRange
    : '[!=' valueRangeIdentifier ']'
    ;

listRankRange
    : standardListRankRange
    | invertedListRankRange
    ;

standardListRankRange
    : '[#' rankRangeIdentifier ']'
    ;

invertedListRankRange
    : '[!#' rankRangeIdentifier ']'
    ;

listRankRangeRelative
    : standardListRankRangeRelative
    | invertedListRankRangeRelative
    ;

standardListRankRangeRelative
    : '[#' rankRangeRelativeIdentifier ']'
    ;

invertedListRankRangeRelative
    : '[!#' rankRangeRelativeIdentifier ']'
    ;

valueIdentifier
    : NAME_IDENTIFIER
    | QUOTED_STRING
    | signedInt
    | IN
    | reservedWord
    ;

valueListIdentifier: valueIdentifier ',' valueIdentifier (',' valueIdentifier)*;

pathFunction
    : pathFunctionCast
    | pathFunctionExists
    | pathFunctionGet
    | pathFunctionCount
    | pathFunctionHllCount
    | pathFunctionHllDescribe
    | pathFunctionHllMayContain
    | pathFunctionHllUnion
    | pathFunctionHllUnionCount
    | pathFunctionHllIntersectCount
    | pathFunctionHllSimilarity
    | 'remove' '()'
    | 'insert' '()'
    | 'set' '()'
    | 'append' '()'
    | 'increment' '()'
    | 'clear' '()'
    | 'sort' '()'
    ;

pathFunctionHllCount: PATH_FUNCTION_HLL_COUNT;
pathFunctionHllDescribe: PATH_FUNCTION_HLL_DESCRIBE;
pathFunctionHllMayContain: 'hllMayContain' '(' expression ')';
pathFunctionHllUnion: 'hllUnion' '(' expression ')';
pathFunctionHllUnionCount: 'hllUnionCount' '(' expression ')';
pathFunctionHllIntersectCount: 'hllIntersectCount' '(' expression ')';
pathFunctionHllSimilarity: 'hllSimilarity' '(' expression ')';

PATH_FUNCTION_HLL_COUNT: 'hllCount' '()';
PATH_FUNCTION_HLL_DESCRIBE: 'hllDescribe' '()';

pathFunctionCast: PATH_FUNCTION_CAST;

PATH_FUNCTION_CAST
    : 'asInt()'
    | 'asFloat()'
    ;

pathFunctionExists: PATH_FUNCTION_EXISTS;

PATH_FUNCTION_EXISTS: 'exists' '()';

pathFunctionCount: PATH_FUNCTION_COUNT;

PATH_FUNCTION_COUNT: 'count' '()';

pathFunctionGet
    : PATH_FUNCTION_GET '(' pathFunctionParams ')'
    | PATH_FUNCTION_GET '()'
    ;

pathFunctionParams: pathFunctionParam (',' pathFunctionParam)*?;

pathFunctionParam: pathFunctionParamName ':' pathFunctionParamValue;

IN: [iI][nN];

NAME_IDENTIFIER: [a-zA-Z0-9_]+;

// Must come AFTER NAME_IDENTIFIER. Inputs containing '@' match this token only
// (longer, exclusive match); inputs without '@' resolve to NAME_IDENTIFIER first
// by ANTLR's first-match priority on equal-length matches.
BIN_IDENTIFIER: [a-zA-Z0-9_@]+;

WS: [ \t\r\n]+ -> skip;
