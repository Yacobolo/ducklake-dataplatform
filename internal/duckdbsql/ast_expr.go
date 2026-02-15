package duckdbsql

// === Expression Nodes ===

// ColumnRef represents a column reference, optionally qualified with table name.
type ColumnRef struct {
	Table  string // optional table/alias qualifier
	Column string // column name
	Quoted bool   // true if the column was double-quoted in the original SQL
}

func (*ColumnRef) node()     {}
func (*ColumnRef) exprNode() {}

// Literal represents a literal value (number, string, bool, null).
type Literal struct {
	Type  LiteralType
	Value string
}

func (*Literal) node()     {}
func (*Literal) exprNode() {}

// LiteralType represents the type of a literal.
type LiteralType int

const (
	LiteralNumber LiteralType = iota
	LiteralString
	LiteralBool
	LiteralNull
)

// BinaryExpr represents a binary expression (left op right).
type BinaryExpr struct {
	Left  Expr
	Op    TokenType
	Right Expr
}

func (*BinaryExpr) node()     {}
func (*BinaryExpr) exprNode() {}

// UnaryExpr represents a unary expression (NOT x, -x, +x).
type UnaryExpr struct {
	Op   TokenType
	Expr Expr
}

func (*UnaryExpr) node()     {}
func (*UnaryExpr) exprNode() {}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

func (*ParenExpr) node()     {}
func (*ParenExpr) exprNode() {}

// FuncCall represents a function call.
type FuncCall struct {
	Schema   string        // optional schema qualifier
	Name     string        // function name (stored in original case)
	Distinct bool          // COUNT(DISTINCT ...)
	Args     []Expr        // arguments
	Star     bool          // COUNT(*)
	OrderBy  []OrderByItem // array_agg(x ORDER BY y)
	Filter   Expr          // FILTER (WHERE ...) clause
	Window   *WindowSpec   // OVER clause
}

func (*FuncCall) node()     {}
func (*FuncCall) exprNode() {}

// WindowSpec represents a window specification (OVER clause).
type WindowSpec struct {
	Name        string // named window reference
	PartitionBy []Expr
	OrderBy     []OrderByItem
	Frame       *FrameSpec
}

// FrameSpec represents a window frame specification.
type FrameSpec struct {
	Type  FrameType
	Start *FrameBound
	End   *FrameBound
}

// FrameType represents the type of window frame.
type FrameType string

const (
	FrameRows   FrameType = "ROWS"
	FrameRange  FrameType = "RANGE"
	FrameGroups FrameType = "GROUPS"
)

// FrameBound represents a window frame bound.
type FrameBound struct {
	Type   FrameBoundType
	Offset Expr // for N PRECEDING/FOLLOWING
}

// FrameBoundType represents the type of frame bound.
type FrameBoundType string

const (
	FrameUnboundedPreceding FrameBoundType = "UNBOUNDED PRECEDING"
	FrameUnboundedFollowing FrameBoundType = "UNBOUNDED FOLLOWING"
	FrameCurrentRow         FrameBoundType = "CURRENT ROW"
	FrameExprPreceding      FrameBoundType = "EXPR PRECEDING"
	FrameExprFollowing      FrameBoundType = "EXPR FOLLOWING"
)

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Operand Expr // CASE operand WHEN... (optional, nil for searched CASE)
	Whens   []WhenClause
	Else    Expr
}

func (*CaseExpr) node()     {}
func (*CaseExpr) exprNode() {}

// WhenClause represents a WHEN clause in a CASE expression.
type WhenClause struct {
	Condition Expr
	Result    Expr
}

// CastExpr represents a CAST(expr AS type) or TRY_CAST(expr AS type) expression.
type CastExpr struct {
	Expr     Expr
	TypeName string
	TryCast  bool // true for TRY_CAST
}

func (*CastExpr) node()     {}
func (*CastExpr) exprNode() {}

// TypeCastExpr represents a DuckDB :: cast expression (expr::type).
type TypeCastExpr struct {
	Expr     Expr
	TypeName string
}

func (*TypeCastExpr) node()     {}
func (*TypeCastExpr) exprNode() {}

// InExpr represents an IN expression.
type InExpr struct {
	Expr   Expr
	Not    bool
	Values []Expr      // IN (1, 2, 3)
	Query  *SelectStmt // IN (SELECT ...)
}

func (*InExpr) node()     {}
func (*InExpr) exprNode() {}

// BetweenExpr represents a BETWEEN expression.
type BetweenExpr struct {
	Expr Expr
	Not  bool
	Low  Expr
	High Expr
}

func (*BetweenExpr) node()     {}
func (*BetweenExpr) exprNode() {}

// IsNullExpr represents IS [NOT] NULL.
type IsNullExpr struct {
	Expr Expr
	Not  bool
}

func (*IsNullExpr) node()     {}
func (*IsNullExpr) exprNode() {}

// IsBoolExpr represents IS [NOT] TRUE/FALSE.
type IsBoolExpr struct {
	Expr  Expr
	Not   bool
	Value bool // true for IS TRUE, false for IS FALSE
}

func (*IsBoolExpr) node()     {}
func (*IsBoolExpr) exprNode() {}

// LikeExpr represents a LIKE or ILIKE expression.
type LikeExpr struct {
	Expr    Expr
	Not     bool
	Pattern Expr
	ILike   bool // true for ILIKE, false for LIKE
}

func (*LikeExpr) node()     {}
func (*LikeExpr) exprNode() {}

// ExistsExpr represents [NOT] EXISTS (subquery).
type ExistsExpr struct {
	Not    bool
	Select *SelectStmt
}

func (*ExistsExpr) node()     {}
func (*ExistsExpr) exprNode() {}

// SubqueryExpr represents a scalar subquery used as an expression.
type SubqueryExpr struct {
	Select *SelectStmt
}

func (*SubqueryExpr) node()     {}
func (*SubqueryExpr) exprNode() {}

// StarExpr represents a * or table.* expression.
type StarExpr struct {
	Table     string         // optional table qualifier
	Modifiers []StarModifier // DuckDB: EXCLUDE, REPLACE, RENAME
}

func (*StarExpr) node()     {}
func (*StarExpr) exprNode() {}

// IntervalExpr represents INTERVAL 'value' unit.
type IntervalExpr struct {
	Value Expr
	Unit  string // DAY, HOUR, etc.
}

func (*IntervalExpr) node()     {}
func (*IntervalExpr) exprNode() {}

// ExtractExpr represents EXTRACT(field FROM expr).
type ExtractExpr struct {
	Field string // YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, EPOCH, etc.
	Expr  Expr
}

func (*ExtractExpr) node()     {}
func (*ExtractExpr) exprNode() {}

// GlobExpr represents expr [NOT] GLOB pattern.
type GlobExpr struct {
	Expr    Expr
	Not     bool
	Pattern Expr
}

func (*GlobExpr) node()     {}
func (*GlobExpr) exprNode() {}

// SimilarToExpr represents expr [NOT] SIMILAR TO pattern.
type SimilarToExpr struct {
	Expr    Expr
	Not     bool
	Pattern Expr
}

func (*SimilarToExpr) node()     {}
func (*SimilarToExpr) exprNode() {}

// ColumnsExpr represents DuckDB COLUMNS(regex_or_lambda) expression.
type ColumnsExpr struct {
	Pattern Expr
}

func (*ColumnsExpr) node()     {}
func (*ColumnsExpr) exprNode() {}

// RawExpr is an escape hatch for unparseable sub-expressions, preserved verbatim.
type RawExpr struct {
	SQL string
}

func (*RawExpr) node()     {}
func (*RawExpr) exprNode() {}

// === DuckDB Expression Extensions ===

// LambdaExpr represents a lambda: x -> expr or (x, y) -> expr.
type LambdaExpr struct {
	Params []string
	Body   Expr
}

func (*LambdaExpr) node()     {}
func (*LambdaExpr) exprNode() {}

// StructLiteral represents a DuckDB struct literal: {'key': value, ...}.
type StructLiteral struct {
	Fields []StructField
}

func (*StructLiteral) node()     {}
func (*StructLiteral) exprNode() {}

// StructField represents a field in a struct literal.
type StructField struct {
	Key   string
	Value Expr
}

// ListLiteral represents a DuckDB list/array literal: [expr, ...].
type ListLiteral struct {
	Elements []Expr
}

func (*ListLiteral) node()     {}
func (*ListLiteral) exprNode() {}

// IndexExpr represents array indexing or slicing: arr[i] or arr[start:stop].
type IndexExpr struct {
	Expr    Expr // the expression being indexed
	Index   Expr // simple index (non-nil if not a slice)
	IsSlice bool
	Start   Expr // nil means from beginning
	Stop    Expr // nil means to end
}

func (*IndexExpr) node()     {}
func (*IndexExpr) exprNode() {}

// === Star Modifiers (DuckDB) ===

// StarModifier is the interface for star expression modifiers.
type StarModifier interface {
	starModifier()
}

// ExcludeModifier represents * EXCLUDE (col1, col2, ...).
type ExcludeModifier struct {
	Columns []string
}

func (*ExcludeModifier) starModifier() {}

// ReplaceItem represents a single replacement in REPLACE modifier.
type ReplaceItem struct {
	Expr  Expr
	Alias string
}

// ReplaceModifier represents * REPLACE (expr AS col, ...).
type ReplaceModifier struct {
	Items []ReplaceItem
}

func (*ReplaceModifier) starModifier() {}

// RenameItem represents a single rename in RENAME modifier.
type RenameItem struct {
	OldName string
	NewName string
}

// RenameModifier represents * RENAME (old AS new, ...).
type RenameModifier struct {
	Items []RenameItem
}

func (*RenameModifier) starModifier() {}
