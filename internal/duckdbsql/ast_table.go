package duckdbsql

// === Table Reference Nodes ===

// TableName represents a table name reference (up to 3-part: catalog.schema.name).
type TableName struct {
	Catalog       string
	Schema        string
	Name          string
	Alias         string
	ColumnAliases []string // e.g., t(a, b) → ColumnAliases: ["a", "b"]
}

func (*TableName) node()         {}
func (*TableName) tableRefNode() {}

// DerivedTable represents a subquery in FROM clause.
type DerivedTable struct {
	Select        *SelectStmt
	Alias         string
	ColumnAliases []string
}

func (*DerivedTable) node()         {}
func (*DerivedTable) tableRefNode() {}

// LateralTable represents a LATERAL subquery.
type LateralTable struct {
	Select        *SelectStmt
	Alias         string
	ColumnAliases []string
}

func (*LateralTable) node()         {}
func (*LateralTable) tableRefNode() {}

// FuncTable represents a table-valued function in FROM (e.g., read_parquet()).
type FuncTable struct {
	Func           *FuncCall
	Alias          string
	ColumnAliases  []string // e.g., range(5) t(i) → ColumnAliases: ["i"]
	WithOrdinality bool
}

func (*FuncTable) node()         {}
func (*FuncTable) tableRefNode() {}

// === DuckDB PIVOT/UNPIVOT ===

// PivotTable represents a PIVOT operation in FROM clause.
type PivotTable struct {
	Source     TableRef
	Aggregates []PivotAggregate
	ForColumn  string
	InValues   []PivotInValue
	InStar     bool
	Alias      string
	GroupBy    []Expr
}

func (*PivotTable) node()         {}
func (*PivotTable) tableRefNode() {}

// PivotAggregate represents an aggregate in PIVOT.
type PivotAggregate struct {
	Func  *FuncCall
	Alias string
}

// PivotInValue represents a value in PIVOT ... IN (...).
type PivotInValue struct {
	Value Expr
	Alias string
}

// UnpivotTable represents an UNPIVOT operation in FROM clause.
type UnpivotTable struct {
	Source       TableRef
	ValueColumns []string
	NameColumn   string
	InColumns    []UnpivotInGroup
	Alias        string
}

func (*UnpivotTable) node()         {}
func (*UnpivotTable) tableRefNode() {}

// UnpivotInGroup represents a group of columns in UNPIVOT ... IN (...).
type UnpivotInGroup struct {
	Columns []string
	Alias   string
}

// StringTable represents a string literal used as a table source (e.g., 'file.csv').
type StringTable struct {
	Path  string
	Alias string
}

func (*StringTable) node()         {}
func (*StringTable) tableRefNode() {}
