// Package cuesql defines the CUE-decoded query model used by the cue-sql generator.
package cuesql

import "fmt"

// Kind describes the generated method shape for a query.
type Kind string

const (
	// KindOne returns a single scalar or row.
	KindOne Kind = "one"
	// KindMany returns a slice of rows.
	KindMany Kind = "many"
	// KindExec returns only an execution error.
	KindExec Kind = "exec"
	// KindExecResult returns a raw sql.Result.
	KindExecResult Kind = "execresult"
	// KindExecRows returns the number of affected rows.
	KindExecRows Kind = "execrows"
)

// Query is the top-level query definition decoded from CUE.
type Query struct {
	Name      string  `json:"name"`
	Doc       string  `json:"doc,omitempty"`
	Kind      Kind    `json:"kind"`
	ParamMode string  `json:"paramMode,omitempty"`
	Params    []Param `json:"params,omitempty"`
	Result    Result  `json:"result,omitempty"`

	Select *Select `json:"select,omitempty"`
	Insert *Insert `json:"insert,omitempty"`
	Update *Update `json:"update,omitempty"`
	Delete *Delete `json:"delete,omitempty"`
	Raw    *Raw    `json:"raw,omitempty"`
}

// Param describes an input parameter for a generated query method.
type Param struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Doc  string `json:"doc,omitempty"`
}

// Result describes the output type for a generated query method.
type Result struct {
	Table  string  `json:"table,omitempty"`
	Row    string  `json:"row,omitempty"`
	Scalar string  `json:"scalar,omitempty"`
	Fields []Field `json:"fields,omitempty"`
}

// Field describes a field on a generated row type.
type Field struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Column string `json:"column,omitempty"`
}

// Select models a SELECT statement.
type Select struct {
	From        string      `json:"from"`
	Alias       string      `json:"alias,omitempty"`
	Distinct    bool        `json:"distinct,omitempty"`
	Columns     []Column    `json:"columns,omitempty"`
	Joins       []Join      `json:"joins,omitempty"`
	Where       []Predicate `json:"where,omitempty"`
	OrderBy     []OrderBy   `json:"orderBy,omitempty"`
	LimitSQL    string      `json:"limitSQL,omitempty"`
	LimitParam  string      `json:"limitParam,omitempty"`
	OffsetParam string      `json:"offsetParam,omitempty"`
}

// Column describes a selected SQL expression.
type Column struct {
	Expr  string `json:"expr"`
	Alias string `json:"alias,omitempty"`
}

// Join describes a SQL JOIN clause.
type Join struct {
	Type  string `json:"type,omitempty"`
	Table string `json:"table"`
	Alias string `json:"alias,omitempty"`
	On    string `json:"on"`
}

// Predicate describes a WHERE-clause predicate.
type Predicate struct {
	Column   string `json:"column,omitempty"`
	Expr     string `json:"expr,omitempty"`
	Op       string `json:"op,omitempty"`
	Param    string `json:"param,omitempty"`
	ValueSQL string `json:"valueSQL,omitempty"`
	Optional bool   `json:"optional,omitempty"`
	Slice    bool   `json:"slice,omitempty"`
	RawSQL   string `json:"rawSQL,omitempty"`
	All      []Predicate `json:"all,omitempty"`
	Any      []Predicate `json:"any,omitempty"`
}

// Insert models an INSERT statement.
type Insert struct {
	Modifier         string      `json:"modifier,omitempty"`
	Into             string      `json:"into"`
	Columns          []string    `json:"columns"`
	Values           []ValueExpr `json:"values"`
	Conflict         *Conflict   `json:"conflict,omitempty"`
	Returning        bool        `json:"returning,omitempty"`
	ReturningColumns []Column    `json:"returningColumns,omitempty"`
}

// Conflict describes an INSERT conflict handler.
type Conflict struct {
	Targets  []string     `json:"targets"`
	DoUpdate []Assignment `json:"doUpdate,omitempty"`
}

// Update models an UPDATE statement.
type Update struct {
	Table            string       `json:"table"`
	Set              []Assignment `json:"set"`
	Where            []Predicate  `json:"where,omitempty"`
	Returning        bool         `json:"returning,omitempty"`
	ReturningColumns []Column     `json:"returningColumns,omitempty"`
}

// Delete models a DELETE statement.
type Delete struct {
	From  string      `json:"from"`
	Where []Predicate `json:"where,omitempty"`
}

// Assignment describes a column assignment within an UPDATE.
type Assignment struct {
	Column       string    `json:"column"`
	Value        ValueExpr `json:"value"`
	CoalesceWith bool      `json:"coalesceWith,omitempty"`
}

// ValueExpr describes a parameterized or raw SQL value expression.
type ValueExpr struct {
	Param string `json:"param,omitempty"`
	SQL   string `json:"sql,omitempty"`
}

// Raw describes a raw SQL statement plus its parameter bind order.
type Raw struct {
	SQL  string   `json:"sql"`
	Bind []string `json:"bind,omitempty"`
}

// Rendered is the rendered SQL form of a query definition.
type Rendered struct {
	SQL     string
	Dynamic bool
}

// StatementCount returns the number of configured SQL statement variants on the query.
func (q Query) StatementCount() int {
	count := 0
	if q.Select != nil {
		count++
	}
	if q.Insert != nil {
		count++
	}
	if q.Update != nil {
		count++
	}
	if q.Delete != nil {
		count++
	}
	if q.Raw != nil {
		count++
	}
	return count
}

// ValidateShape validates the query's top-level structure before code generation.
func (q Query) ValidateShape() error {
	if q.Name == "" {
		return fmt.Errorf("query missing name")
	}
	switch q.Kind {
	case KindOne, KindMany, KindExec, KindExecResult, KindExecRows:
	default:
		return fmt.Errorf("query %s: unsupported kind %q", q.Name, q.Kind)
	}
	switch q.ParamMode {
	case "", "struct", "single":
	default:
		return fmt.Errorf("query %s: unsupported paramMode %q", q.Name, q.ParamMode)
	}
	if q.StatementCount() != 1 {
		return fmt.Errorf("query %s: expected exactly one statement, got %d", q.Name, q.StatementCount())
	}
	return nil
}
