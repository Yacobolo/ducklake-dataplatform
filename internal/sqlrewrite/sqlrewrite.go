// Package sqlrewrite provides SQL-level RBAC and RLS enforcement.
//
// It parses SQL queries using the PostgreSQL parser (pg_query_go), extracts
// table names, checks access control, and injects WHERE clause filters for
// row-level security. This replaces the previous Substrait-based approach,
// removing the dependency on the DuckDB substrait extension.
package sqlrewrite

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Operator constants for RLS rule conditions (migrated from policy package).
const (
	OpEqual        = "eq"
	OpNotEqual     = "neq"
	OpLessThan     = "lt"
	OpLessEqual    = "lte"
	OpGreaterThan  = "gt"
	OpGreaterEqual = "gte"
)

// RLSRule defines a row-level security filter applied to a specific table.
type RLSRule struct {
	Table    string      // table name this rule applies to
	Column   string      // column name to filter on
	Operator string      // comparison operator (use Op* constants)
	Value    interface{} // literal value to compare against
}

// ExtractTableNames parses a SQL query and returns the deduplicated list
// of table names referenced in FROM clauses and JOINs.
// Handles compound names (e.g., "lake.main.titanic") by using the last element.
func ExtractTableNames(sql string) ([]string, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse SQL: %w", err)
	}

	seen := make(map[string]bool)
	var tables []string

	for _, stmt := range result.Stmts {
		collectTablesFromNode(stmt.Stmt, seen, &tables)
	}

	return tables, nil
}

// RewriteQuery parses the SQL query, injects WHERE clause conditions based on
// the RLS rules, and returns the rewritten SQL string.
// If rulesByTable is empty, the original query is returned unchanged.
func RewriteQuery(sql string, rulesByTable map[string][]RLSRule) (string, error) {
	if len(rulesByTable) == 0 {
		return sql, nil
	}

	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	for _, stmt := range result.Stmts {
		if err := injectFiltersIntoNode(stmt.Stmt, rulesByTable); err != nil {
			return "", err
		}
	}

	output, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("deparse SQL: %w", err)
	}

	return output, nil
}

// StatementType represents the kind of SQL statement.
type StatementType int

// SQL statement types identified during query classification.
const (
	StmtSelect StatementType = iota
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtDDL
	StmtOther
)

func (t StatementType) String() string {
	switch t {
	case StmtSelect:
		return "SELECT"
	case StmtInsert:
		return "INSERT"
	case StmtUpdate:
		return "UPDATE"
	case StmtDelete:
		return "DELETE"
	case StmtDDL:
		return "DDL"
	default:
		return "OTHER"
	}
}

// ClassifyStatement parses the SQL and returns the statement type.
// It rejects multi-statement input to prevent piggy-backed SQL injection
// (e.g., "SELECT 1; DROP TABLE foo").
func ClassifyStatement(sql string) (StatementType, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return StmtOther, fmt.Errorf("parse SQL: %w", err)
	}

	if len(result.Stmts) == 0 {
		return StmtOther, nil
	}

	if len(result.Stmts) > 1 {
		return StmtOther, fmt.Errorf("multi-statement queries are not allowed")
	}

	switch result.Stmts[0].Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		return StmtSelect, nil
	case *pg_query.Node_InsertStmt:
		return StmtInsert, nil
	case *pg_query.Node_UpdateStmt:
		return StmtUpdate, nil
	case *pg_query.Node_DeleteStmt:
		return StmtDelete, nil
	case *pg_query.Node_CreateStmt, *pg_query.Node_AlterTableStmt, *pg_query.Node_DropStmt,
		*pg_query.Node_IndexStmt, *pg_query.Node_ViewStmt, *pg_query.Node_CreateSchemaStmt,
		*pg_query.Node_TruncateStmt, *pg_query.Node_RenameStmt:
		return StmtDDL, nil
	default:
		return StmtOther, nil
	}
}

// ExtractTargetTable parses a SQL DML statement and returns the target table name
// for INSERT, UPDATE, and DELETE statements. Returns empty string for SELECT/DDL/other.
func ExtractTargetTable(sqlStr string) (string, error) {
	result, err := pg_query.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	if len(result.Stmts) == 0 {
		return "", nil
	}

	switch n := result.Stmts[0].Stmt.Node.(type) {
	case *pg_query.Node_InsertStmt:
		if n.InsertStmt.Relation != nil {
			return n.InsertStmt.Relation.Relname, nil
		}
	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt.Relation != nil {
			return n.UpdateStmt.Relation.Relname, nil
		}
	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt.Relation != nil {
			return n.DeleteStmt.Relation.Relname, nil
		}
	}

	return "", nil
}

// InjectRowFilterSQL injects a raw SQL WHERE clause expression into all SELECT
// statements that reference the given table. The filterSQL is a raw expression
// string like `"Pclass" = 1`.
func InjectRowFilterSQL(sqlStr string, tableName string, filterSQL string) (string, error) {
	if filterSQL == "" {
		return sqlStr, nil
	}

	// Parse the filter expression into an AST node
	filterResult, err := pg_query.Parse("SELECT 1 WHERE " + filterSQL)
	if err != nil {
		return "", fmt.Errorf("parse row filter %q: %w", filterSQL, err)
	}
	sel := filterResult.Stmts[0].Stmt.GetSelectStmt()
	if sel == nil || sel.WhereClause == nil {
		return "", fmt.Errorf("row filter %q did not produce a WHERE clause", filterSQL)
	}
	filterNode := sel.WhereClause

	// Parse the original query
	result, err := pg_query.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	for _, stmt := range result.Stmts {
		injectRawFilterIntoNode(stmt.Stmt, tableName, filterNode)
	}

	output, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("deparse SQL: %w", err)
	}
	return output, nil
}

// InjectMultipleRowFilters injects multiple row filter expressions into a SQL
// query for a given table. Multiple filters are combined with OR (each filter
// represents a separate visibility window), then ANDed with any existing WHERE.
func InjectMultipleRowFilters(sqlStr string, tableName string, filters []string) (string, error) {
	if len(filters) == 0 {
		return sqlStr, nil
	}
	if len(filters) == 1 {
		return InjectRowFilterSQL(sqlStr, tableName, filters[0])
	}

	// Combine filters with OR: (filter1) OR (filter2) OR ...
	parts := make([]string, len(filters))
	for i, f := range filters {
		parts[i] = "(" + f + ")"
	}
	combined := strings.Join(parts, " OR ")
	return InjectRowFilterSQL(sqlStr, tableName, combined)
}
