package engine

import (
	"context"
	"database/sql"
	"fmt"

	"duck-demo/config"
	"duck-demo/policy"
	"duck-demo/sqlrewrite"
)

// SecureEngine wraps a DuckDB connection and enforces RBAC + RLS
// by intercepting queries through SQL-level rewriting.
type SecureEngine struct {
	db    *sql.DB
	store *policy.PolicyStore
}

// NewSecureEngine creates a SecureEngine with the given database and policy store.
func NewSecureEngine(db *sql.DB, store *policy.PolicyStore) *SecureEngine {
	return &SecureEngine{db: db, store: store}
}

// Query executes a SQL query as the given role, enforcing RBAC and RLS.
// It accepts a context for cancellation and timeout support.
//
// The flow:
//  1. Look up the role in the policy store
//  2. Parse the SQL and extract table names
//  3. Check RBAC: does the role have access to all tables?
//  4. Build RLS rules map for tables in the query
//  5. Rewrite the SQL with injected WHERE clauses for RLS
//  6. Execute the rewritten SQL against DuckDB
func (e *SecureEngine) Query(ctx context.Context, roleName, sqlQuery string) (*sql.Rows, error) {
	// 1. Look up role
	role, err := e.store.GetRole(roleName)
	if err != nil {
		return nil, fmt.Errorf("policy error: %w", err)
	}

	// 2. Parse SQL and extract table names
	tables, err := sqlrewrite.ExtractTableNames(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("parse SQL: %w", err)
	}

	// 3. Check RBAC
	if err := role.CheckAccess(tables); err != nil {
		return nil, fmt.Errorf("RBAC check failed: %w", err)
	}

	// 4. Build RLS rules map for tables in this query
	rulesByTable := make(map[string][]policy.RLSRule)
	for _, table := range tables {
		rules := role.RLSRulesForTable(table)
		if len(rules) > 0 {
			rulesByTable[table] = rules
		}
	}

	// 5. Rewrite the SQL with RLS filters
	rewrittenSQL, err := sqlrewrite.RewriteQuery(sqlQuery, rulesByTable)
	if err != nil {
		return nil, fmt.Errorf("rewrite SQL: %w", err)
	}

	// 6. Execute the rewritten SQL
	rows, err := e.db.QueryContext(ctx, rewrittenSQL)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}

	return rows, nil
}

// SetupDuckLake initializes DuckDB with DuckLake extension, S3 credentials,
// and attaches the lake. On first run, it loads titanic.parquet into the lake.
func SetupDuckLake(ctx context.Context, db *sql.DB, cfg *config.Config) error {
	// 1. Install and load required extensions
	extensions := []string{
		"INSTALL ducklake; LOAD ducklake;",
		"INSTALL sqlite; LOAD sqlite;",
		"INSTALL httpfs; LOAD httpfs;",
	}
	for _, ext := range extensions {
		if _, err := db.ExecContext(ctx, ext); err != nil {
			return fmt.Errorf("extension setup (%s): %w", ext, err)
		}
	}

	// 2. Create S3 secret for Hetzner
	secretSQL := fmt.Sprintf(`CREATE SECRET hetzner_s3 (
		TYPE S3,
		KEY_ID '%s',
		SECRET '%s',
		ENDPOINT '%s',
		REGION '%s',
		URL_STYLE 'path'
	)`, cfg.S3KeyID, cfg.S3Secret, cfg.S3Endpoint, cfg.S3Region)

	if _, err := db.ExecContext(ctx, secretSQL); err != nil {
		return fmt.Errorf("create S3 secret: %w", err)
	}

	// 3. Attach DuckLake with SQLite metastore + Hetzner data path
	attachSQL := fmt.Sprintf(`ATTACH 'ducklake:sqlite:%s' AS lake (
		DATA_PATH 's3://%s/lake_data/'
	)`, cfg.MetaDBPath, cfg.S3Bucket)

	if _, err := db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("attach ducklake: %w", err)
	}

	// 4. Set the lake as the default catalog
	if _, err := db.ExecContext(ctx, "USE lake"); err != nil {
		return fmt.Errorf("use lake: %w", err)
	}

	// 5. On first run, load parquet data into the lake
	// CREATE TABLE IF NOT EXISTS ensures idempotency
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS lake.main.titanic AS SELECT * FROM '%s'", cfg.ParquetPath)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create titanic table: %w", err)
	}

	return nil
}
