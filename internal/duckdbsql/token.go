// Package duckdbsql provides a DuckDB-native SQL parser, AST, and formatter.
//
// It replaces the previous pg_query_go (PostgreSQL parser) dependency with
// a purpose-built parser that understands the full DuckDB SQL dialect including
// QUALIFY, PIVOT/UNPIVOT, struct/list/lambda literals, star modifiers
// (EXCLUDE/REPLACE/RENAME), and all DuckDB-specific join types.
//
// The parser is designed for the security pipeline: statement classification,
// table name extraction, RLS filter injection, and column masking.
package duckdbsql

import "fmt"

// TokenType represents the type of a lexical token.
type TokenType int

// TOKEN_EOF and friends enumerate all token types produced by the lexer.
const (
	TOKEN_EOF     TokenType = iota // end of input
	TOKEN_ILLEGAL                  // unexpected character

	TOKEN_IDENT  // identifier
	TOKEN_NUMBER // 123, 45.67, 1e10
	TOKEN_STRING // 'hello'

	TOKEN_PLUS      // +
	TOKEN_MINUS     // -
	TOKEN_STAR      // *
	TOKEN_SLASH     // /
	TOKEN_DSLASH    // // (integer division)
	TOKEN_MOD       // %
	TOKEN_DPIPE     // ||
	TOKEN_EQ        // =
	TOKEN_NE        // != or <>
	TOKEN_LT        // <
	TOKEN_GT        // >
	TOKEN_LE        // <=
	TOKEN_GE        // >=
	TOKEN_DOT       // .
	TOKEN_COMMA     // ,
	TOKEN_SEMICOLON // ;
	TOKEN_LPAREN    // (
	TOKEN_RPAREN    // )
	TOKEN_LBRACKET  // [
	TOKEN_RBRACKET  // ]
	TOKEN_LBRACE    // {
	TOKEN_RBRACE    // }
	TOKEN_COLON     // :
	TOKEN_DCOLON    // :: (DuckDB cast)
	TOKEN_ARROW     // -> (lambda)

	// TOKEN_ALL and below are SQL keywords (alphabetical).
	TOKEN_ALL
	TOKEN_ALTER
	TOKEN_AND
	TOKEN_AS
	TOKEN_ASC
	TOKEN_ATTACH
	TOKEN_BEGIN
	TOKEN_BETWEEN
	TOKEN_BY
	TOKEN_CALL
	TOKEN_CASCADE
	TOKEN_CASE
	TOKEN_CAST
	TOKEN_CHECKPOINT
	TOKEN_COMMIT
	TOKEN_CONFLICT
	TOKEN_COPY
	TOKEN_CREATE
	TOKEN_CROSS
	TOKEN_CURRENT
	TOKEN_DEALLOCATE
	TOKEN_DEFAULT
	TOKEN_DELETE
	TOKEN_DESC
	TOKEN_DESCRIBE
	TOKEN_DETACH
	TOKEN_DISTINCT
	TOKEN_DO
	TOKEN_DROP
	TOKEN_ELSE
	TOKEN_END
	TOKEN_EXCEPT
	TOKEN_EXECUTE
	TOKEN_EXISTS
	TOKEN_EXPLAIN
	TOKEN_EXPORT
	TOKEN_EXTRACT
	TOKEN_FALSE
	TOKEN_FETCH
	TOKEN_FILTER
	TOKEN_FIRST
	TOKEN_FOLLOWING
	TOKEN_FOR
	TOKEN_FROM
	TOKEN_FULL
	TOKEN_FUNCTION
	TOKEN_GLOB
	TOKEN_GRANT
	TOKEN_GROUP
	TOKEN_GROUPS
	TOKEN_HAVING
	TOKEN_IF
	TOKEN_IMPORT
	TOKEN_IN
	TOKEN_INDEX
	TOKEN_INNER
	TOKEN_INSERT
	TOKEN_INSTALL
	TOKEN_INTERVAL
	TOKEN_INTERSECT
	TOKEN_INTO
	TOKEN_IS
	TOKEN_JOIN
	TOKEN_LAST
	TOKEN_LATERAL
	TOKEN_LEFT
	TOKEN_LIKE
	TOKEN_LIMIT
	TOKEN_LOAD
	TOKEN_MACRO
	TOKEN_NATURAL
	TOKEN_NEXT
	TOKEN_NOT
	TOKEN_NOTHING
	TOKEN_NULL
	TOKEN_NULLS
	TOKEN_OFFSET
	TOKEN_ON
	TOKEN_ONLY
	TOKEN_OR
	TOKEN_ORDER
	TOKEN_OUTER
	TOKEN_OVER
	TOKEN_PARTITION
	TOKEN_PERCENT
	TOKEN_PRAGMA
	TOKEN_PRECEDING
	TOKEN_PREPARE
	TOKEN_RANGE
	TOKEN_RECURSIVE
	TOKEN_REINDEX
	TOKEN_RENAME
	TOKEN_REPLACE
	TOKEN_RESET
	TOKEN_RESTRICT
	TOKEN_RETURNING
	TOKEN_REVOKE
	TOKEN_RIGHT
	TOKEN_ROLLBACK
	TOKEN_ROW
	TOKEN_ROWS
	TOKEN_SCHEMA
	TOKEN_SECRET
	TOKEN_SELECT
	TOKEN_SET
	TOKEN_SHOW
	TOKEN_SIMILAR
	TOKEN_SUMMARIZE
	TOKEN_TABLE
	TOKEN_TEMPORARY
	TOKEN_THEN
	TOKEN_TIES
	TOKEN_TRUE
	TOKEN_TRUNCATE
	TOKEN_TRY_CAST
	TOKEN_TYPE
	TOKEN_UNBOUNDED
	TOKEN_UNION
	TOKEN_UPDATE
	TOKEN_USE
	TOKEN_USING
	TOKEN_VACUUM
	TOKEN_VALUES
	TOKEN_VIEW
	TOKEN_WHEN
	TOKEN_WHERE
	TOKEN_WINDOW
	TOKEN_WITH
	TOKEN_WITHIN

	// TOKEN_ANTI and below are DuckDB-specific keywords.
	TOKEN_ANTI
	TOKEN_ASOF
	TOKEN_COLUMNS
	TOKEN_EXCLUDE
	TOKEN_ILIKE
	TOKEN_POSITIONAL
	TOKEN_PIVOT
	TOKEN_QUALIFY
	TOKEN_SEMI
	TOKEN_UNPIVOT
)

// String returns a human-readable representation of the token type.
func (t TokenType) String() string {
	if name, ok := tokenNames[t]; ok {
		return name
	}
	return fmt.Sprintf("TOKEN(%d)", t)
}

// tokenNames maps token types to their string representations.
var tokenNames = map[TokenType]string{
	TOKEN_EOF:     "EOF",
	TOKEN_ILLEGAL: "ILLEGAL",
	TOKEN_IDENT:   "IDENT",
	TOKEN_NUMBER:  "NUMBER",
	TOKEN_STRING:  "STRING",

	TOKEN_PLUS:      "+",
	TOKEN_MINUS:     "-",
	TOKEN_STAR:      "*",
	TOKEN_SLASH:     "/",
	TOKEN_DSLASH:    "//",
	TOKEN_MOD:       "%",
	TOKEN_DPIPE:     "||",
	TOKEN_EQ:        "=",
	TOKEN_NE:        "!=",
	TOKEN_LT:        "<",
	TOKEN_GT:        ">",
	TOKEN_LE:        "<=",
	TOKEN_GE:        ">=",
	TOKEN_DOT:       ".",
	TOKEN_COMMA:     ",",
	TOKEN_SEMICOLON: ";",
	TOKEN_LPAREN:    "(",
	TOKEN_RPAREN:    ")",
	TOKEN_LBRACKET:  "[",
	TOKEN_RBRACKET:  "]",
	TOKEN_LBRACE:    "{",
	TOKEN_RBRACE:    "}",
	TOKEN_COLON:     ":",
	TOKEN_DCOLON:    "::",
	TOKEN_ARROW:     "->",

	TOKEN_ALL:        "ALL",
	TOKEN_ALTER:      "ALTER",
	TOKEN_AND:        "AND",
	TOKEN_AS:         "AS",
	TOKEN_ASC:        "ASC",
	TOKEN_ATTACH:     "ATTACH",
	TOKEN_BEGIN:      "BEGIN",
	TOKEN_BETWEEN:    "BETWEEN",
	TOKEN_BY:         "BY",
	TOKEN_CALL:       "CALL",
	TOKEN_CASCADE:    "CASCADE",
	TOKEN_CASE:       "CASE",
	TOKEN_CAST:       "CAST",
	TOKEN_CHECKPOINT: "CHECKPOINT",
	TOKEN_COMMIT:     "COMMIT",
	TOKEN_CONFLICT:   "CONFLICT",
	TOKEN_COPY:       "COPY",
	TOKEN_CREATE:     "CREATE",
	TOKEN_CROSS:      "CROSS",
	TOKEN_CURRENT:    "CURRENT",
	TOKEN_DEALLOCATE: "DEALLOCATE",
	TOKEN_DEFAULT:    "DEFAULT",
	TOKEN_DELETE:     "DELETE",
	TOKEN_DESC:       "DESC",
	TOKEN_DESCRIBE:   "DESCRIBE",
	TOKEN_DETACH:     "DETACH",
	TOKEN_DISTINCT:   "DISTINCT",
	TOKEN_DO:         "DO",
	TOKEN_DROP:       "DROP",
	TOKEN_ELSE:       "ELSE",
	TOKEN_END:        "END",
	TOKEN_EXCEPT:     "EXCEPT",
	TOKEN_EXECUTE:    "EXECUTE",
	TOKEN_EXISTS:     "EXISTS",
	TOKEN_EXPLAIN:    "EXPLAIN",
	TOKEN_EXPORT:     "EXPORT",
	TOKEN_EXTRACT:    "EXTRACT",
	TOKEN_FALSE:      "FALSE",
	TOKEN_FETCH:      "FETCH",
	TOKEN_FILTER:     "FILTER",
	TOKEN_FIRST:      "FIRST",
	TOKEN_FOLLOWING:  "FOLLOWING",
	TOKEN_FOR:        "FOR",
	TOKEN_FROM:       "FROM",
	TOKEN_FULL:       "FULL",
	TOKEN_FUNCTION:   "FUNCTION",
	TOKEN_GLOB:       "GLOB",
	TOKEN_GRANT:      "GRANT",
	TOKEN_GROUP:      "GROUP",
	TOKEN_GROUPS:     "GROUPS",
	TOKEN_HAVING:     "HAVING",
	TOKEN_IF:         "IF",
	TOKEN_IMPORT:     "IMPORT",
	TOKEN_IN:         "IN",
	TOKEN_INDEX:      "INDEX",
	TOKEN_INNER:      "INNER",
	TOKEN_INSERT:     "INSERT",
	TOKEN_INSTALL:    "INSTALL",
	TOKEN_INTERVAL:   "INTERVAL",
	TOKEN_INTERSECT:  "INTERSECT",
	TOKEN_INTO:       "INTO",
	TOKEN_IS:         "IS",
	TOKEN_JOIN:       "JOIN",
	TOKEN_LAST:       "LAST",
	TOKEN_LATERAL:    "LATERAL",
	TOKEN_LEFT:       "LEFT",
	TOKEN_LIKE:       "LIKE",
	TOKEN_LIMIT:      "LIMIT",
	TOKEN_LOAD:       "LOAD",
	TOKEN_MACRO:      "MACRO",
	TOKEN_NATURAL:    "NATURAL",
	TOKEN_NEXT:       "NEXT",
	TOKEN_NOT:        "NOT",
	TOKEN_NOTHING:    "NOTHING",
	TOKEN_NULL:       "NULL",
	TOKEN_NULLS:      "NULLS",
	TOKEN_OFFSET:     "OFFSET",
	TOKEN_ON:         "ON",
	TOKEN_ONLY:       "ONLY",
	TOKEN_OR:         "OR",
	TOKEN_ORDER:      "ORDER",
	TOKEN_OUTER:      "OUTER",
	TOKEN_OVER:       "OVER",
	TOKEN_PARTITION:  "PARTITION",
	TOKEN_PERCENT:    "PERCENT",
	TOKEN_PRAGMA:     "PRAGMA",
	TOKEN_PRECEDING:  "PRECEDING",
	TOKEN_PREPARE:    "PREPARE",
	TOKEN_RANGE:      "RANGE",
	TOKEN_RECURSIVE:  "RECURSIVE",
	TOKEN_REINDEX:    "REINDEX",
	TOKEN_RENAME:     "RENAME",
	TOKEN_REPLACE:    "REPLACE",
	TOKEN_RESET:      "RESET",
	TOKEN_RESTRICT:   "RESTRICT",
	TOKEN_RETURNING:  "RETURNING",
	TOKEN_REVOKE:     "REVOKE",
	TOKEN_RIGHT:      "RIGHT",
	TOKEN_ROLLBACK:   "ROLLBACK",
	TOKEN_ROW:        "ROW",
	TOKEN_ROWS:       "ROWS",
	TOKEN_SCHEMA:     "SCHEMA",
	TOKEN_SECRET:     "SECRET",
	TOKEN_SELECT:     "SELECT",
	TOKEN_SET:        "SET",
	TOKEN_SHOW:       "SHOW",
	TOKEN_SIMILAR:    "SIMILAR",
	TOKEN_SUMMARIZE:  "SUMMARIZE",
	TOKEN_TABLE:      "TABLE",
	TOKEN_TEMPORARY:  "TEMPORARY",
	TOKEN_THEN:       "THEN",
	TOKEN_TIES:       "TIES",
	TOKEN_TRUE:       "TRUE",
	TOKEN_TRUNCATE:   "TRUNCATE",
	TOKEN_TRY_CAST:   "TRY_CAST",
	TOKEN_TYPE:       "TYPE",
	TOKEN_UNBOUNDED:  "UNBOUNDED",
	TOKEN_UNION:      "UNION",
	TOKEN_UPDATE:     "UPDATE",
	TOKEN_USE:        "USE",
	TOKEN_USING:      "USING",
	TOKEN_VACUUM:     "VACUUM",
	TOKEN_VALUES:     "VALUES",
	TOKEN_VIEW:       "VIEW",
	TOKEN_WHEN:       "WHEN",
	TOKEN_WHERE:      "WHERE",
	TOKEN_WINDOW:     "WINDOW",
	TOKEN_WITH:       "WITH",
	TOKEN_WITHIN:     "WITHIN",

	// DuckDB-specific
	TOKEN_ANTI:       "ANTI",
	TOKEN_ASOF:       "ASOF",
	TOKEN_COLUMNS:    "COLUMNS",
	TOKEN_EXCLUDE:    "EXCLUDE",
	TOKEN_ILIKE:      "ILIKE",
	TOKEN_POSITIONAL: "POSITIONAL",
	TOKEN_PIVOT:      "PIVOT",
	TOKEN_QUALIFY:    "QUALIFY",
	TOKEN_SEMI:       "SEMI",
	TOKEN_UNPIVOT:    "UNPIVOT",
}

// keywords maps lowercase keyword strings to their token types.
var keywords = map[string]TokenType{
	"all":        TOKEN_ALL,
	"alter":      TOKEN_ALTER,
	"and":        TOKEN_AND,
	"as":         TOKEN_AS,
	"asc":        TOKEN_ASC,
	"attach":     TOKEN_ATTACH,
	"begin":      TOKEN_BEGIN,
	"between":    TOKEN_BETWEEN,
	"by":         TOKEN_BY,
	"call":       TOKEN_CALL,
	"cascade":    TOKEN_CASCADE,
	"case":       TOKEN_CASE,
	"cast":       TOKEN_CAST,
	"checkpoint": TOKEN_CHECKPOINT,
	"commit":     TOKEN_COMMIT,
	"conflict":   TOKEN_CONFLICT,
	"copy":       TOKEN_COPY,
	"create":     TOKEN_CREATE,
	"cross":      TOKEN_CROSS,
	"current":    TOKEN_CURRENT,
	"deallocate": TOKEN_DEALLOCATE,
	"default":    TOKEN_DEFAULT,
	"delete":     TOKEN_DELETE,
	"desc":       TOKEN_DESC,
	"describe":   TOKEN_DESCRIBE,
	"detach":     TOKEN_DETACH,
	"distinct":   TOKEN_DISTINCT,
	"do":         TOKEN_DO,
	"drop":       TOKEN_DROP,
	"else":       TOKEN_ELSE,
	"end":        TOKEN_END,
	"except":     TOKEN_EXCEPT,
	"execute":    TOKEN_EXECUTE,
	"exists":     TOKEN_EXISTS,
	"explain":    TOKEN_EXPLAIN,
	"export":     TOKEN_EXPORT,
	"extract":    TOKEN_EXTRACT,
	"false":      TOKEN_FALSE,
	"fetch":      TOKEN_FETCH,
	"filter":     TOKEN_FILTER,
	"first":      TOKEN_FIRST,
	"following":  TOKEN_FOLLOWING,
	"for":        TOKEN_FOR,
	"from":       TOKEN_FROM,
	"full":       TOKEN_FULL,
	"function":   TOKEN_FUNCTION,
	"glob":       TOKEN_GLOB,
	"grant":      TOKEN_GRANT,
	"group":      TOKEN_GROUP,
	"groups":     TOKEN_GROUPS,
	"having":     TOKEN_HAVING,
	"if":         TOKEN_IF,
	"import":     TOKEN_IMPORT,
	"in":         TOKEN_IN,
	"index":      TOKEN_INDEX,
	"inner":      TOKEN_INNER,
	"insert":     TOKEN_INSERT,
	"install":    TOKEN_INSTALL,
	"interval":   TOKEN_INTERVAL,
	"intersect":  TOKEN_INTERSECT,
	"into":       TOKEN_INTO,
	"is":         TOKEN_IS,
	"join":       TOKEN_JOIN,
	"last":       TOKEN_LAST,
	"lateral":    TOKEN_LATERAL,
	"left":       TOKEN_LEFT,
	"like":       TOKEN_LIKE,
	"limit":      TOKEN_LIMIT,
	"load":       TOKEN_LOAD,
	"macro":      TOKEN_MACRO,
	"natural":    TOKEN_NATURAL,
	"next":       TOKEN_NEXT,
	"not":        TOKEN_NOT,
	"nothing":    TOKEN_NOTHING,
	"null":       TOKEN_NULL,
	"nulls":      TOKEN_NULLS,
	"offset":     TOKEN_OFFSET,
	"on":         TOKEN_ON,
	"only":       TOKEN_ONLY,
	"or":         TOKEN_OR,
	"order":      TOKEN_ORDER,
	"outer":      TOKEN_OUTER,
	"over":       TOKEN_OVER,
	"partition":  TOKEN_PARTITION,
	"percent":    TOKEN_PERCENT,
	"pragma":     TOKEN_PRAGMA,
	"preceding":  TOKEN_PRECEDING,
	"prepare":    TOKEN_PREPARE,
	"range":      TOKEN_RANGE,
	"recursive":  TOKEN_RECURSIVE,
	"reindex":    TOKEN_REINDEX,
	"rename":     TOKEN_RENAME,
	"replace":    TOKEN_REPLACE,
	"reset":      TOKEN_RESET,
	"restrict":   TOKEN_RESTRICT,
	"returning":  TOKEN_RETURNING,
	"revoke":     TOKEN_REVOKE,
	"right":      TOKEN_RIGHT,
	"rollback":   TOKEN_ROLLBACK,
	"row":        TOKEN_ROW,
	"rows":       TOKEN_ROWS,
	"schema":     TOKEN_SCHEMA,
	"secret":     TOKEN_SECRET,
	"select":     TOKEN_SELECT,
	"set":        TOKEN_SET,
	"show":       TOKEN_SHOW,
	"similar":    TOKEN_SIMILAR,
	"summarize":  TOKEN_SUMMARIZE,
	"table":      TOKEN_TABLE,
	"temporary":  TOKEN_TEMPORARY,
	"temp":       TOKEN_TEMPORARY,
	"then":       TOKEN_THEN,
	"ties":       TOKEN_TIES,
	"true":       TOKEN_TRUE,
	"truncate":   TOKEN_TRUNCATE,
	"try_cast":   TOKEN_TRY_CAST,
	"type":       TOKEN_TYPE,
	"unbounded":  TOKEN_UNBOUNDED,
	"union":      TOKEN_UNION,
	"update":     TOKEN_UPDATE,
	"use":        TOKEN_USE,
	"using":      TOKEN_USING,
	"vacuum":     TOKEN_VACUUM,
	"values":     TOKEN_VALUES,
	"view":       TOKEN_VIEW,
	"when":       TOKEN_WHEN,
	"where":      TOKEN_WHERE,
	"window":     TOKEN_WINDOW,
	"with":       TOKEN_WITH,
	"within":     TOKEN_WITHIN,

	// DuckDB-specific
	"anti":       TOKEN_ANTI,
	"asof":       TOKEN_ASOF,
	"columns":    TOKEN_COLUMNS,
	"exclude":    TOKEN_EXCLUDE,
	"ilike":      TOKEN_ILIKE,
	"positional": TOKEN_POSITIONAL,
	"pivot":      TOKEN_PIVOT,
	"qualify":    TOKEN_QUALIFY,
	"semi":       TOKEN_SEMI,
	"unpivot":    TOKEN_UNPIVOT,
}

// lookupKeyword returns the token type for the given lowercase identifier.
// Returns TOKEN_IDENT if it's not a keyword.
func lookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TOKEN_IDENT
}

// Token represents a lexical token with its literal value.
type Token struct {
	Type    TokenType
	Literal string
}

// Precedence constants for operator precedence parsing (Pratt parser).
const (
	PrecedenceNone       = 0
	PrecedenceOr         = 1
	PrecedenceAnd        = 2
	PrecedenceNot        = 3
	PrecedenceComparison = 4 // =, <>, <, >, <=, >=, LIKE, ILIKE, IN, BETWEEN, IS
	PrecedenceAddition   = 5 // +, -, ||
	PrecedenceMultiply   = 6 // *, /, %, //
	PrecedenceUnary      = 7 // -, +, NOT (prefix)
	PrecedencePostfix    = 8 // ::, [], ()
)
