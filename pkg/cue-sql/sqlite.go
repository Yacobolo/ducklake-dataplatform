package cuesql

import (
	"fmt"
	"strings"
)

// ResultColumns provides result-column lookup for table-backed row generation.
type ResultColumns interface {
	ColumnsForTable(table string) ([]string, error)
}

// RenderSQLite renders a query definition into SQLite SQL.
func RenderSQLite(q Query, catalog ResultColumns) (Rendered, error) {
	if err := q.ValidateShape(); err != nil {
		return Rendered{}, err
	}

	switch {
	case q.Raw != nil:
		return Rendered{SQL: strings.TrimSpace(q.Raw.SQL)}, nil
	case q.Select != nil:
		return renderSelect(q, catalog)
	case q.Insert != nil:
		return renderInsert(q, catalog)
	case q.Update != nil:
		return renderUpdate(q, catalog)
	case q.Delete != nil:
		return renderDelete(q)
	default:
		return Rendered{}, fmt.Errorf("query %s: no statement configured", q.Name)
	}
}

func renderSelect(q Query, catalog ResultColumns) (Rendered, error) {
	stmt := q.Select
	columns := stmt.Columns
	if len(columns) == 0 {
		if q.Result.Table == "" {
			return Rendered{}, fmt.Errorf("query %s: select requires columns or result.table", q.Name)
		}
		tableColumns, err := catalog.ColumnsForTable(q.Result.Table)
		if err != nil {
			return Rendered{}, fmt.Errorf("query %s: %w", q.Name, err)
		}
		qualifier := stmt.From
		if stmt.Alias != "" {
			qualifier = stmt.Alias
		}
		columns = make([]Column, 0, len(tableColumns))
		for _, name := range tableColumns {
			columns = append(columns, Column{Expr: qualifier + "." + name})
		}
	}

	var sb strings.Builder
	sb.WriteString("SELECT ")
	if stmt.Distinct {
		sb.WriteString("DISTINCT ")
	}
	sb.WriteString(JoinColumns(columns))
	sb.WriteString("\nFROM ")
	sb.WriteString(stmt.From)
	if stmt.Alias != "" {
		sb.WriteString(" ")
		sb.WriteString(stmt.Alias)
	}
	for _, join := range stmt.Joins {
		sb.WriteString("\n")
		joinType := strings.TrimSpace(join.Type)
		if joinType == "" {
			joinType = "JOIN"
		}
		sb.WriteString(joinType)
		sb.WriteString(" ")
		sb.WriteString(join.Table)
		if join.Alias != "" {
			sb.WriteString(" ")
			sb.WriteString(join.Alias)
		}
		sb.WriteString(" ON ")
		sb.WriteString(join.On)
	}

	dynamic := false
	staticWhere := make([]string, 0, len(stmt.Where))
	for _, predicate := range stmt.Where {
		if predicate.Optional || predicate.Slice {
			dynamic = true
			continue
		}
		sqlPart, err := RenderPredicateSQL(predicate)
		if err != nil {
			return Rendered{}, fmt.Errorf("query %s: %w", q.Name, err)
		}
		if sqlPart != "" {
			staticWhere = append(staticWhere, sqlPart)
		}
	}
	if len(staticWhere) > 0 {
		sb.WriteString("\nWHERE ")
		sb.WriteString(strings.Join(staticWhere, "\n  AND "))
	}
	if len(stmt.OrderBy) > 0 {
		sb.WriteString("\nORDER BY ")
		sb.WriteString(JoinOrderBy(stmt.OrderBy))
	}
	if stmt.LimitParam != "" {
		sb.WriteString("\nLIMIT ?")
	}
	if stmt.OffsetParam != "" {
		sb.WriteString(" OFFSET ?")
	}
	return Rendered{SQL: sb.String(), Dynamic: dynamic}, nil
}

func renderInsert(q Query, catalog ResultColumns) (Rendered, error) {
	stmt := q.Insert
	if len(stmt.Columns) == 0 || len(stmt.Columns) != len(stmt.Values) {
		return Rendered{}, fmt.Errorf("query %s: insert columns/values mismatch", q.Name)
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(stmt.Into)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(stmt.Columns, ", "))
	sb.WriteString(")\nVALUES (")
	valueSQL := make([]string, 0, len(stmt.Values))
	for _, value := range stmt.Values {
		valueSQL = append(valueSQL, renderValue(value))
	}
	sb.WriteString(strings.Join(valueSQL, ", "))
	sb.WriteString(")")
	if stmt.Returning {
		cols, err := returningColumns(q.Result, catalog)
		if err != nil {
			return Rendered{}, fmt.Errorf("query %s: %w", q.Name, err)
		}
		sb.WriteString("\nRETURNING ")
		sb.WriteString(strings.Join(cols, ", "))
	}
	return Rendered{SQL: sb.String()}, nil
}

func renderUpdate(q Query, catalog ResultColumns) (Rendered, error) {
	stmt := q.Update
	if len(stmt.Set) == 0 {
		return Rendered{}, fmt.Errorf("query %s: update requires set clauses", q.Name)
	}
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(stmt.Table)
	sb.WriteString("\nSET ")
	assignments := make([]string, 0, len(stmt.Set))
	for _, assignment := range stmt.Set {
		assignments = append(assignments, assignment.Column+" = "+renderValue(assignment.Value))
	}
	sb.WriteString(strings.Join(assignments, ",\n    "))
	if len(stmt.Where) > 0 {
		where := make([]string, 0, len(stmt.Where))
		for _, predicate := range stmt.Where {
			sqlPart, err := RenderPredicateSQL(predicate)
			if err != nil {
				return Rendered{}, fmt.Errorf("query %s: %w", q.Name, err)
			}
			if sqlPart != "" {
				where = append(where, sqlPart)
			}
		}
		if len(where) > 0 {
			sb.WriteString("\nWHERE ")
			sb.WriteString(strings.Join(where, "\n  AND "))
		}
	}
	if stmt.Returning {
		cols, err := returningColumns(q.Result, catalog)
		if err != nil {
			return Rendered{}, fmt.Errorf("query %s: %w", q.Name, err)
		}
		sb.WriteString("\nRETURNING ")
		sb.WriteString(strings.Join(cols, ", "))
	}
	return Rendered{SQL: sb.String()}, nil
}

func renderDelete(q Query) (Rendered, error) {
	stmt := q.Delete
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(stmt.From)
	if len(stmt.Where) > 0 {
		where := make([]string, 0, len(stmt.Where))
		for _, predicate := range stmt.Where {
			sqlPart, err := RenderPredicateSQL(predicate)
			if err != nil {
				return Rendered{}, fmt.Errorf("query %s: %w", q.Name, err)
			}
			if sqlPart != "" {
				where = append(where, sqlPart)
			}
		}
		if len(where) > 0 {
			sb.WriteString("\nWHERE ")
			sb.WriteString(strings.Join(where, "\n  AND "))
		}
	}
	return Rendered{SQL: sb.String()}, nil
}

// RenderPredicateSQL renders a predicate into SQLite SQL.
func RenderPredicateSQL(predicate Predicate) (string, error) {
	if predicate.RawSQL != "" {
		return predicate.RawSQL, nil
	}
	lhs := predicate.Column
	if lhs == "" {
		lhs = predicate.Expr
	}
	if lhs == "" || predicate.Op == "" {
		return "", fmt.Errorf("predicate missing expression or op")
	}
	if predicate.Slice {
		return lhs + " IN (?)", nil
	}
	return lhs + " " + predicate.Op + " ?", nil
}

func renderValue(value ValueExpr) string {
	if value.SQL != "" {
		return value.SQL
	}
	return "?"
}

func returningColumns(result Result, catalog ResultColumns) ([]string, error) {
	if result.Table == "" {
		return nil, fmt.Errorf("returning requires result.table")
	}
	return catalog.ColumnsForTable(result.Table)
}

// JoinColumns joins selected columns into a SQL select list.
func JoinColumns(columns []Column) string {
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		part := column.Expr
		if column.Alias != "" {
			part += " AS " + column.Alias
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ", ")
}

// JoinOrderBy joins order-by items into a SQL ORDER BY clause.
func JoinOrderBy(orderBy []OrderBy) string {
	parts := make([]string, 0, len(orderBy))
	for _, item := range orderBy {
		part := item.Expr
		if item.Desc {
			part += " DESC"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ", ")
}

// OrderBy describes one ORDER BY expression.
type OrderBy struct {
	Expr string `json:"expr"`
	Desc bool   `json:"desc,omitempty"`
}
