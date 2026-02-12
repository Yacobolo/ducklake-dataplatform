package engine

import (
	"context"
	"database/sql"

	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.DuckDBExecutor = (*DuckDBExecAdapter)(nil)

// DuckDBExecAdapter wraps a *sql.DB to implement domain.DuckDBExecutor.
// It is used for raw CALL statements that bypass the SQL parser.
type DuckDBExecAdapter struct {
	db *sql.DB
}

// NewDuckDBExecAdapter creates a new DuckDBExecAdapter.
func NewDuckDBExecAdapter(db *sql.DB) *DuckDBExecAdapter {
	return &DuckDBExecAdapter{db: db}
}

// ExecContext executes a raw SQL statement against DuckDB.
func (a *DuckDBExecAdapter) ExecContext(ctx context.Context, query string) error {
	_, err := a.db.ExecContext(ctx, query)
	return err
}
