package duckdbsql

// === Statement Nodes ===

// SelectStmt represents a complete SELECT statement with optional WITH clause.
type SelectStmt struct {
	With *WithClause
	Body *SelectBody
}

func (*SelectStmt) node()     {}
func (*SelectStmt) stmtNode() {}

// WithClause represents a WITH clause with CTEs.
type WithClause struct {
	Recursive bool
	CTEs      []*CTE
}

// CTE represents a Common Table Expression.
type CTE struct {
	Name   string
	Select *SelectStmt
}

// SelectBody represents the body of a SELECT with possible set operations.
type SelectBody struct {
	Left   *SelectCore
	Op     SetOpType   // UNION, INTERSECT, EXCEPT, or empty
	All    bool        // UNION ALL
	ByName bool        // DuckDB: BY NAME
	Right  *SelectBody // for chained set operations
}

// SetOpType represents the type of set operation.
type SetOpType string

// SetOpNone and friends classify set operations (UNION, INTERSECT, EXCEPT).
const (
	SetOpNone      SetOpType = ""
	SetOpUnion     SetOpType = "UNION"
	SetOpUnionAll  SetOpType = "UNION ALL"
	SetOpIntersect SetOpType = "INTERSECT"
	SetOpExcept    SetOpType = "EXCEPT"
)

// SelectCore represents the core SELECT clause with all optional clauses.
type SelectCore struct {
	Distinct       bool
	Columns        []SelectItem
	From           *FromClause
	Where          Expr
	GroupBy        []Expr
	GroupByAll     bool // DuckDB: GROUP BY ALL
	Having         Expr
	Windows        []WindowDef
	Qualify        Expr // DuckDB: QUALIFY
	OrderBy        []OrderByItem
	OrderByAll     bool // DuckDB: ORDER BY ALL
	OrderByAllDesc bool // DuckDB: direction for ORDER BY ALL
	Limit          Expr
	Offset         Expr
	Fetch          *FetchClause
}

// FetchClause represents FETCH FIRST/NEXT n ROWS ONLY/WITH TIES.
type FetchClause struct {
	First    bool
	Count    Expr
	Percent  bool
	WithTies bool
}

// WindowDef represents a named window definition.
type WindowDef struct {
	Name string
	Spec *WindowSpec
}

// SelectItem represents an item in the SELECT list.
type SelectItem struct {
	Star      bool           // SELECT *
	TableStar string         // SELECT t.*
	Expr      Expr           // expression
	Alias     string         // AS alias
	Modifiers []StarModifier // DuckDB: EXCLUDE, REPLACE, RENAME
}

// FromClause represents the FROM clause.
type FromClause struct {
	Source TableRef
	Joins  []*Join
}

// Join represents a JOIN clause.
type Join struct {
	Type      JoinType
	Natural   bool
	Right     TableRef
	Condition Expr     // ON clause
	Using     []string // USING (col1, col2)
}

// JoinType represents the type of join.
type JoinType string

// JoinInner and friends classify SQL JOIN types including DuckDB extensions.
const (
	JoinInner JoinType = "INNER"
	JoinLeft  JoinType = "LEFT"
	JoinRight JoinType = "RIGHT"
	JoinFull  JoinType = "FULL"
	JoinCross JoinType = "CROSS"
	JoinComma JoinType = ","
	// JoinLeftSemi and below are DuckDB extensions.
	JoinLeftSemi   JoinType = "LEFT SEMI"
	JoinRightSemi  JoinType = "RIGHT SEMI"
	JoinSemi       JoinType = "SEMI"
	JoinLeftAnti   JoinType = "LEFT ANTI"
	JoinRightAnti  JoinType = "RIGHT ANTI"
	JoinAnti       JoinType = "ANTI"
	JoinAsOf       JoinType = "ASOF"
	JoinPositional JoinType = "POSITIONAL"
)

// OrderByItem represents an item in ORDER BY clause.
type OrderByItem struct {
	Expr       Expr
	Desc       bool
	NullsFirst *bool // nil = default, true = NULLS FIRST, false = NULLS LAST
}

// === DML Statement Nodes ===

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Table      *TableName
	Columns    []string
	Values     [][]Expr    // VALUES rows
	Query      *SelectStmt // INSERT ... SELECT
	OnConflict *OnConflictClause
	Returning  []SelectItem
}

func (*InsertStmt) node()     {}
func (*InsertStmt) stmtNode() {}

// OnConflictClause represents ON CONFLICT handling.
type OnConflictClause struct {
	Columns   []string    // ON CONFLICT (col1, col2)
	DoUpdate  []SetClause // DO UPDATE SET ...
	DoNothing bool        // DO NOTHING
}

// SetClause represents column = value in UPDATE or ON CONFLICT DO UPDATE.
type SetClause struct {
	Column string
	Value  Expr
}

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	Table     *TableName
	Sets      []SetClause
	From      *FromClause // UPDATE ... FROM
	Where     Expr
	Returning []SelectItem
}

func (*UpdateStmt) node()     {}
func (*UpdateStmt) stmtNode() {}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Table     *TableName
	Using     *FromClause // USING clause
	Where     Expr
	Returning []SelectItem
}

func (*DeleteStmt) node()     {}
func (*DeleteStmt) stmtNode() {}

// === DDL and Utility Statement Nodes ===

// DDLType classifies the type of DDL statement.
type DDLType int

// DDLCreateTable and friends classify the type of DDL statement.
const (
	DDLCreateTable DDLType = iota
	DDLCreateView
	DDLCreateSchema
	DDLCreateIndex
	DDLCreateMacro
	DDLCreateType
	DDLCreateSecret
	DDLCreateFunction
	DDLDrop
	DDLAlter
	DDLTruncate
	DDLRename
	DDLOther
)

// DDLStmt represents a DDL statement (classification only, not deeply parsed).
type DDLStmt struct {
	Type DDLType
	Raw  string // the original SQL for re-emission
}

func (*DDLStmt) node()     {}
func (*DDLStmt) stmtNode() {}

// UtilityType classifies the type of utility statement.
type UtilityType int

// UtilityCall and friends classify the type of utility statement.
const (
	UtilityCall UtilityType = iota
	UtilityCopy
	UtilityExport
	UtilityPragma
	UtilitySet
	UtilityInstall
	UtilityLoad
	UtilityAttach
	UtilityDetach
	UtilityUse
	UtilityDescribe
	UtilityBegin
	UtilityCheckpoint
	UtilityCommit
	UtilityDeallocate
	UtilityExecute
	UtilityExplain
	UtilityGrant
	UtilityImport
	UtilityPrepare
	UtilityReindex
	UtilityReset
	UtilityRevoke
	UtilityRollback
	UtilityShow
	UtilitySummarize
	UtilityVacuum
	UtilityOther
)

// UtilityStmt represents a utility statement (classification only).
type UtilityStmt struct {
	Type UtilityType
	Raw  string // the original SQL for re-emission
}

func (*UtilityStmt) node()     {}
func (*UtilityStmt) stmtNode() {}
