// Package sqlrewrite provides SQL-level RBAC and RLS enforcement.
//
// It parses SQL queries using a DuckDB-native parser, extracts table names,
// checks access control, and injects WHERE clause filters for row-level
// security.
package sqlrewrite

import (
	"fmt"
	"strings"

	"duck-demo/internal/duckdbsql"
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
func ExtractTableNames(sql string) ([]string, error) {
	stmt, err := duckdbsql.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse SQL: %w", err)
	}
	return duckdbsql.CollectTableNames(stmt), nil
}

// RewriteQuery parses the SQL query, injects WHERE clause conditions based on
// the RLS rules, and returns the rewritten SQL string.
// If rulesByTable is empty, the original query is returned unchanged.
func RewriteQuery(sql string, rulesByTable map[string][]RLSRule) (string, error) {
	if len(rulesByTable) == 0 {
		return sql, nil
	}

	stmt, err := duckdbsql.Parse(sql)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	// Build and inject filter expressions for each table
	if err := injectRLSRules(stmt, rulesByTable); err != nil {
		return "", err
	}

	return duckdbsql.Format(stmt), nil
}

// injectRLSRules injects RLS rule-based WHERE clauses into a statement.
func injectRLSRules(stmt duckdbsql.Stmt, rulesByTable map[string][]RLSRule) error {
	// For SELECT statements, we need to match table names in FROM clauses
	// and inject filters. Build filter expressions and inject.
	tables := duckdbsql.CollectTableNames(stmt)
	for _, tableName := range tables {
		rules, ok := rulesByTable[tableName]
		if !ok || len(rules) == 0 {
			continue
		}

		for _, rule := range rules {
			filter, err := buildRuleFilter(rule)
			if err != nil {
				return err
			}
			duckdbsql.InjectFilter(stmt, tableName, filter)
		}
	}
	return nil
}

// buildRuleFilter creates an expression node from an RLS rule.
func buildRuleFilter(rule RLSRule) (duckdbsql.Expr, error) {
	opToken, err := operatorToToken(rule.Operator)
	if err != nil {
		return nil, err
	}

	literal, err := makeLiteralExpr(rule.Value)
	if err != nil {
		return nil, fmt.Errorf("RLS rule for %s.%s: %w", rule.Table, rule.Column, err)
	}

	return &duckdbsql.BinaryExpr{
		Left:  &duckdbsql.ColumnRef{Column: rule.Column},
		Op:    opToken,
		Right: literal,
	}, nil
}

// operatorToToken converts a policy operator constant to a duckdbsql token type.
func operatorToToken(op string) (duckdbsql.TokenType, error) {
	switch op {
	case OpEqual:
		return duckdbsql.TOKEN_EQ, nil
	case OpNotEqual:
		return duckdbsql.TOKEN_NE, nil
	case OpLessThan:
		return duckdbsql.TOKEN_LT, nil
	case OpLessEqual:
		return duckdbsql.TOKEN_LE, nil
	case OpGreaterThan:
		return duckdbsql.TOKEN_GT, nil
	case OpGreaterEqual:
		return duckdbsql.TOKEN_GE, nil
	default:
		return 0, fmt.Errorf("unsupported operator: %q", op)
	}
}

// makeLiteralExpr creates a Literal expression from a Go value.
func makeLiteralExpr(v interface{}) (duckdbsql.Expr, error) {
	switch val := v.(type) {
	case int:
		return &duckdbsql.Literal{Type: duckdbsql.LiteralNumber, Value: fmt.Sprintf("%d", val)}, nil
	case int8:
		return &duckdbsql.Literal{Type: duckdbsql.LiteralNumber, Value: fmt.Sprintf("%d", val)}, nil
	case int16:
		return &duckdbsql.Literal{Type: duckdbsql.LiteralNumber, Value: fmt.Sprintf("%d", val)}, nil
	case int32:
		return &duckdbsql.Literal{Type: duckdbsql.LiteralNumber, Value: fmt.Sprintf("%d", val)}, nil
	case int64:
		return &duckdbsql.Literal{Type: duckdbsql.LiteralNumber, Value: fmt.Sprintf("%d", val)}, nil
	case float32:
		return &duckdbsql.Literal{Type: duckdbsql.LiteralNumber, Value: fmt.Sprintf("%g", val)}, nil
	case float64:
		return &duckdbsql.Literal{Type: duckdbsql.LiteralNumber, Value: fmt.Sprintf("%g", val)}, nil
	case string:
		return &duckdbsql.Literal{Type: duckdbsql.LiteralString, Value: val}, nil
	case bool:
		if val {
			return &duckdbsql.Literal{Type: duckdbsql.LiteralString, Value: "true"}, nil
		}
		return &duckdbsql.Literal{Type: duckdbsql.LiteralString, Value: "false"}, nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", v)
	}
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
	stmt, err := duckdbsql.Parse(sql)
	if err != nil {
		return StmtOther, fmt.Errorf("parse SQL: %w", err)
	}

	st := duckdbsql.Classify(stmt)
	switch st {
	case duckdbsql.StmtTypeSelect:
		if name, found := duckdbsql.ContainsDangerousFunction(stmt, dangerousFunctions); found {
			return StmtOther, fmt.Errorf("prohibited function: %s", name)
		}
		return StmtSelect, nil
	case duckdbsql.StmtTypeInsert:
		return StmtInsert, nil
	case duckdbsql.StmtTypeUpdate:
		return StmtUpdate, nil
	case duckdbsql.StmtTypeDelete:
		return StmtDelete, nil
	case duckdbsql.StmtTypeDDL:
		return StmtDDL, nil
	default:
		return StmtOther, nil
	}
}

// ExtractTargetTable parses a SQL DML statement and returns the target table name
// for INSERT, UPDATE, and DELETE statements. Returns empty string for SELECT/DDL/other.
func ExtractTargetTable(sqlStr string) (string, error) {
	if sqlStr == "" {
		return "", nil
	}
	stmt, err := duckdbsql.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}
	return duckdbsql.TargetTable(stmt), nil
}

// InjectRowFilterSQL injects a raw SQL WHERE clause expression into all SELECT
// statements that reference the given table. The filterSQL is a raw expression
// string like `"Pclass" = 1`.
func InjectRowFilterSQL(sqlStr string, tableName string, filterSQL string) (string, error) {
	if filterSQL == "" {
		return sqlStr, nil
	}

	// Parse the filter expression
	filterExpr, err := duckdbsql.ParseExpr(filterSQL)
	if err != nil {
		return "", fmt.Errorf("parse row filter %q: %w", filterSQL, err)
	}

	// Parse the original query
	stmt, err := duckdbsql.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	duckdbsql.InjectFilter(stmt, tableName, filterExpr)

	return duckdbsql.Format(stmt), nil
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

// ApplyColumnMasks rewrites SELECT target columns to apply mask expressions.
// masks is a map of column_name -> mask_expression (e.g., {"Name": "'***'"}).
// allColumns is the full list of column names for the table, used to expand
// SELECT * into explicit column references so masks can be applied.
// Returns an error if any mask expression cannot be parsed.
func ApplyColumnMasks(sqlStr string, tableName string, masks map[string]string, allColumns []string) (string, error) {
	if len(masks) == 0 {
		return sqlStr, nil
	}

	stmt, err := duckdbsql.Parse(sqlStr)
	if err != nil {
		return "", fmt.Errorf("parse SQL: %w", err)
	}

	if err := duckdbsql.ApplyColumnMasks(stmt, tableName, masks, allColumns); err != nil {
		return "", err
	}

	return duckdbsql.Format(stmt), nil
}

// QuoteIdentifier unconditionally quotes a SQL identifier using double quotes.
// Internal double quotes are escaped by doubling them ("" → ").
func QuoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// dangerousFunctions is the blocklist of DuckDB functions that can read the
// filesystem, leak internal metadata, or escape the query sandbox.
var dangerousFunctions = map[string]bool{
	"read_csv":             true,
	"read_csv_auto":        true,
	"read_parquet":         true,
	"read_json":            true,
	"read_json_auto":       true,
	"read_text":            true,
	"read_blob":            true,
	"glob":                 true,
	"sqlite_scan":          true,
	"query_table":          true,
	"duckdb_extensions":    true,
	"duckdb_settings":      true,
	"duckdb_databases":     true,
	"duckdb_secrets":       true,
	"pragma_database_list": true,
}
