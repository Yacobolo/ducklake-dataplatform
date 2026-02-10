package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"duck-demo/internal/config"
	"duck-demo/internal/domain"
	"duck-demo/internal/sqlrewrite"
)

// SecureEngine wraps a DuckDB connection and enforces RBAC + RLS + column masking
// by intercepting queries through the catalog permission model and SQL-level rewriting.
type SecureEngine struct {
	db      *sql.DB
	catalog domain.AuthorizationService
}

// NewSecureEngine creates a SecureEngine with the given DuckDB connection
// and authorization service (backed by the SQLite metastore).
func NewSecureEngine(db *sql.DB, cat domain.AuthorizationService) *SecureEngine {
	return &SecureEngine{db: db, catalog: cat}
}

// Query executes a SQL query as the given principal, enforcing:
//   - Statement type classification (DDL/DML protection)
//   - RBAC privilege checks via the catalog
//   - Row-level security via filter injection
//   - Column masking via SELECT rewriting
//
// The flow:
//  1. Classify statement type
//  2. Extract table names from the query
//  3. For each table: check privilege, get row filter, get column masks
//  4. Inject row filters and column masks into the SQL
//  5. Execute the rewritten SQL against DuckDB
func (e *SecureEngine) Query(ctx context.Context, principalName, sqlQuery string) (*sql.Rows, error) {
	// 1. Classify statement type
	stmtType, err := sqlrewrite.ClassifyStatement(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("classify statement: %w", err)
	}

	// Map statement type to required privilege
	requiredPriv, err := privilegeForStatement(stmtType)
	if err != nil {
		return nil, err
	}

	// 2. Extract table names
	tables, err := sqlrewrite.ExtractTableNames(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("parse SQL: %w", err)
	}

	if len(tables) == 0 {
		// No tables referenced — utility statement, just execute
		rows, err := e.db.QueryContext(ctx, sqlQuery)
		if err != nil {
			return nil, fmt.Errorf("execute query: %w", err)
		}
		return rows, nil
	}

	// 3. Check privileges + collect filters/masks for each table
	rewritten := sqlQuery
	for _, tableName := range tables {
		tableID, _, err := e.catalog.LookupTableID(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("catalog lookup: %w", err)
		}

		// Check privilege
		allowed, err := e.catalog.CheckPrivilege(ctx, principalName, domain.SecurableTable, tableID, requiredPriv)
		if err != nil {
			return nil, fmt.Errorf("privilege check: %w", err)
		}
		if !allowed {
			return nil, fmt.Errorf("access denied: %q lacks %s on table %q", principalName, requiredPriv, tableName)
		}

		// Get row filters (only for SELECT)
		if stmtType == sqlrewrite.StmtSelect {
			filters, err := e.catalog.GetEffectiveRowFilters(ctx, principalName, tableID)
			if err != nil {
				return nil, fmt.Errorf("row filter: %w", err)
			}
			if len(filters) > 0 {
				rewritten, err = sqlrewrite.InjectMultipleRowFilters(rewritten, tableName, filters)
				if err != nil {
					return nil, fmt.Errorf("inject row filter: %w", err)
				}
			}

			// Get column masks
			masks, err := e.catalog.GetEffectiveColumnMasks(ctx, principalName, tableID)
			if err != nil {
				return nil, fmt.Errorf("column masks: %w", err)
			}
			if masks != nil {
				rewritten, err = sqlrewrite.ApplyColumnMasks(rewritten, tableName, masks)
				if err != nil {
					return nil, fmt.Errorf("apply column masks: %w", err)
				}
			}
		}
	}

	log.Printf("[audit] principal=%q stmt=%s tables=%v sql=%q", principalName, stmtType, tables, rewritten)

	// 5. Execute
	rows, err := e.db.QueryContext(ctx, rewritten)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	return rows, nil
}

// privilegeForStatement maps a statement type to the required privilege.
func privilegeForStatement(t sqlrewrite.StatementType) (string, error) {
	switch t {
	case sqlrewrite.StmtSelect:
		return domain.PrivSelect, nil
	case sqlrewrite.StmtInsert:
		return domain.PrivInsert, nil
	case sqlrewrite.StmtUpdate:
		return domain.PrivUpdate, nil
	case sqlrewrite.StmtDelete:
		return domain.PrivDelete, nil
	case sqlrewrite.StmtDDL:
		return "", fmt.Errorf("DDL statements are not allowed through the query engine")
	default:
		return "", fmt.Errorf("unsupported statement type: %s", t)
	}
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
