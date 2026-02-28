package governance

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	// Register DuckDB SQL driver.
	_ "github.com/duckdb/duckdb-go/v2"
)

// Rule defines a persisted governance query rule.
type Rule struct {
	ID          string
	Category    string
	Severity    string
	Description string
	QuerySQL    string
	Enabled     bool
}

// Violation is a normalized finding emitted by a governance rule.
type Violation struct {
	RuleID    string
	Category  string
	Severity  string
	FilePath  string
	Symbol    string
	Detail    string
	Line      int
	RawValues map[string]any
}

// Row is a generic row map returned by ad-hoc query execution.
type Row map[string]any

// RunOptions controls which rules are executed.
type RunOptions struct {
	RuleIDs []string
}

// Runner executes governance rules against an AST database.
type Runner struct {
	duckDBPath string
}

// NewRunner returns a governance runner bound to a DuckDB file.
func NewRunner(duckDBPath string) *Runner {
	return &Runner{duckDBPath: duckDBPath}
}

// EnsureDefaultRules creates and upserts built-in governance rules.
func (r *Runner) EnsureDefaultRules(ctx context.Context) error {
	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS governance_rules (
	rule_id TEXT PRIMARY KEY,
	category TEXT NOT NULL,
	severity TEXT NOT NULL,
	description TEXT NOT NULL,
	query_sql TEXT NOT NULL,
	enabled BOOLEAN NOT NULL DEFAULT true,
	updated_unix BIGINT NOT NULL
)`); err != nil {
		return fmt.Errorf("ensure governance_rules table: %w", err)
	}

	stmt, err := db.PrepareContext(ctx, `
INSERT INTO governance_rules (rule_id, category, severity, description, query_sql, enabled, updated_unix)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(rule_id) DO UPDATE SET
	category=excluded.category,
	severity=excluded.severity,
	description=excluded.description,
	query_sql=excluded.query_sql,
	enabled=excluded.enabled,
	updated_unix=excluded.updated_unix`)
	if err != nil {
		return fmt.Errorf("prepare governance rule upsert: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	now := time.Now().Unix()
	for _, rule := range defaultRules() {
		if _, err := stmt.ExecContext(ctx, rule.ID, rule.Category, rule.Severity, rule.Description, rule.QuerySQL, rule.Enabled, now); err != nil {
			return fmt.Errorf("upsert governance rule %s: %w", rule.ID, err)
		}
	}

	return nil
}

// ListRules returns all governance rules ordered by id.
func (r *Runner) ListRules(ctx context.Context) ([]Rule, error) {
	if err := r.EnsureDefaultRules(ctx); err != nil {
		return nil, err
	}

	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `
SELECT rule_id, category, severity, description, query_sql, enabled
FROM governance_rules
ORDER BY rule_id`)
	if err != nil {
		return nil, fmt.Errorf("query governance rules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	rules := make([]Rule, 0)
	for rows.Next() {
		var rule Rule
		if err := rows.Scan(&rule.ID, &rule.Category, &rule.Severity, &rule.Description, &rule.QuerySQL, &rule.Enabled); err != nil {
			return nil, fmt.Errorf("scan governance rule: %w", err)
		}
		rules = append(rules, rule)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate governance rules: %w", err)
	}

	return rules, nil
}

// Run executes selected rules and returns normalized violations.
func (r *Runner) Run(ctx context.Context, opts RunOptions) ([]Violation, error) {
	rules, err := r.ListRules(ctx)
	if err != nil {
		return nil, err
	}

	selected := filterRules(rules, opts.RuleIDs)

	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	violations := make([]Violation, 0)
	for _, rule := range selected {
		if !rule.Enabled {
			continue
		}

		rows, err := db.QueryContext(ctx, rule.QuerySQL)
		if err != nil {
			return nil, fmt.Errorf("run rule %s: %w", rule.ID, err)
		}
		if err := func() error {
			defer func() { _ = rows.Close() }()

			columns, err := rows.Columns()
			if err != nil {
				return fmt.Errorf("columns for rule %s: %w", rule.ID, err)
			}

			for rows.Next() {
				values := make([]any, len(columns))
				ptrs := make([]any, len(columns))
				for i := range values {
					ptrs[i] = &values[i]
				}

				if err := rows.Scan(ptrs...); err != nil {
					return fmt.Errorf("scan row for rule %s: %w", rule.ID, err)
				}

				raw := make(map[string]any, len(columns))
				for i, col := range columns {
					raw[col] = normalizeValue(values[i])
				}

				violations = append(violations, Violation{
					RuleID:    rule.ID,
					Category:  rule.Category,
					Severity:  rule.Severity,
					FilePath:  asString(raw["file_path"]),
					Symbol:    asString(raw["symbol"]),
					Detail:    asString(raw["detail"]),
					Line:      asInt(raw["line"]),
					RawValues: raw,
				})
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterate rows for rule %s: %w", rule.ID, err)
			}
			return nil
		}(); err != nil {
			return nil, err
		}
	}

	return violations, nil
}

// AdhocQuery executes an arbitrary SELECT and returns row maps.
func (r *Runner) AdhocQuery(ctx context.Context, query string, args ...any) ([]Row, error) {
	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("run adhoc query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read adhoc columns: %w", err)
	}

	out := make([]Row, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan adhoc row: %w", err)
		}

		row := make(Row, len(columns))
		for i, col := range columns {
			row[col] = normalizeValue(values[i])
		}
		out = append(out, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate adhoc rows: %w", err)
	}

	return out, nil
}

func filterRules(rules []Rule, ids []string) []Rule {
	if len(ids) == 0 {
		return rules
	}

	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}

	filtered := make([]Rule, 0, len(ids))
	for _, rule := range rules {
		if _, ok := want[rule.ID]; ok {
			filtered = append(filtered, rule)
		}
	}

	return filtered
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case []byte:
		return string(x)
	default:
		return x
	}
}

func asString(v any) string {
	if v == nil {
		return ""
	}

	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

func asInt(v any) int {
	if v == nil {
		return 0
	}

	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(x))
		return i
	default:
		return 0
	}
}
