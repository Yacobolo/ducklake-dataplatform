package compute

import (
	"context"
	"database/sql"

	"duck-demo/internal/domain"
)

var _ domain.ComputeExecutor = (*LocalExecutor)(nil)

// LocalExecutor wraps a *sql.DB and implements ComputeExecutor for local DuckDB queries.
type LocalExecutor struct {
	db *sql.DB
}

// NewLocalExecutor creates a LocalExecutor backed by the given database connection.
func NewLocalExecutor(db *sql.DB) *LocalExecutor {
	return &LocalExecutor{db: db}
}

// QueryContext executes the query against the local database.
func (e *LocalExecutor) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	return e.db.QueryContext(ctx, query)
}
