package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"duck-demo/internal/ddl"
	"duck-demo/internal/domain"
	"duck-demo/internal/sqlrewrite"
)

// SecureEngine wraps a DuckDB connection and enforces RBAC + RLS + column masking
// by intercepting queries through the catalog permission model and SQL-level rewriting.
type SecureEngine struct {
	db         *sql.DB
	catalog    domain.AuthorizationService
	infoSchema *InformationSchemaProvider
}

// NewSecureEngine creates a SecureEngine with the given DuckDB connection
// and authorization service (backed by the SQLite metastore).
func NewSecureEngine(db *sql.DB, cat domain.AuthorizationService) *SecureEngine {
	return &SecureEngine{db: db, catalog: cat}
}

// SetInformationSchemaProvider attaches an information_schema provider.
func (e *SecureEngine) SetInformationSchemaProvider(p *InformationSchemaProvider) {
	e.infoSchema = p
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
	// Intercept information_schema queries
	if e.infoSchema != nil && IsInformationSchemaQuery(sqlQuery) {
		return e.infoSchema.HandleQuery(ctx, e.db, sqlQuery)
	}

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
		tableID, _, isExternal, err := e.catalog.LookupTableID(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("catalog lookup: %w", err)
		}

		// Block DML on external (read-only) tables
		if isExternal && stmtType != sqlrewrite.StmtSelect {
			return nil, fmt.Errorf("access denied: table %q is read-only (EXTERNAL)", tableName)
		}

		// Check privilege
		allowed, err := e.catalog.CheckPrivilege(ctx, principalName, domain.SecurableTable, tableID, requiredPriv)
		if err != nil {
			return nil, fmt.Errorf("privilege check: %w", err)
		}
		if !allowed {
			return nil, domain.ErrAccessDenied("principal %q lacks %s on table %q", principalName, requiredPriv, tableName)
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
				// Fetch column names so SELECT * can be expanded before masking.
				colNames, err := e.catalog.GetTableColumnNames(ctx, tableID)
				if err != nil {
					return nil, fmt.Errorf("get column names for masking: %w", err)
				}
				rewritten, err = sqlrewrite.ApplyColumnMasks(rewritten, tableName, masks, colNames)
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

// === DuckLake Setup Functions ===

// InstallExtensions installs and loads DuckDB extensions needed for DuckLake.
// Safe to call without S3 credentials — just makes the extensions available.
func InstallExtensions(ctx context.Context, db *sql.DB) error {
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
	return nil
}

// CreateS3Secret creates a named DuckDB secret for S3-compatible storage.
func CreateS3Secret(ctx context.Context, db *sql.DB, name, keyID, secret, endpoint, region, urlStyle string) error {
	secretSQL, err := ddl.CreateS3Secret(name, keyID, secret, endpoint, region, urlStyle)
	if err != nil {
		return fmt.Errorf("build DDL: %w", err)
	}
	if _, err := db.ExecContext(ctx, secretSQL); err != nil {
		return fmt.Errorf("create S3 secret %q: %w", name, err)
	}
	return nil
}

// DropS3Secret removes a named DuckDB secret.
func DropS3Secret(ctx context.Context, db *sql.DB, name string) error {
	dropSQL, err := ddl.DropS3Secret(name)
	if err != nil {
		return fmt.Errorf("build DDL: %w", err)
	}
	if _, err := db.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("drop S3 secret %q: %w", name, err)
	}
	return nil
}

// AttachDuckLake attaches the DuckLake catalog with the given metastore and data path.
func AttachDuckLake(ctx context.Context, db *sql.DB, metaDBPath, dataPath string) error {
	attachSQL, err := ddl.AttachDuckLake(metaDBPath, dataPath)
	if err != nil {
		return fmt.Errorf("build DDL: %w", err)
	}

	if _, err := db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("attach ducklake: %w", err)
	}
	if _, err := db.ExecContext(ctx, "USE lake"); err != nil {
		return fmt.Errorf("use lake: %w", err)
	}
	return nil
}

// IsCatalogAttached checks if the "lake" catalog is already attached to DuckDB.
func IsCatalogAttached(ctx context.Context, db *sql.DB) bool {
	rows, err := db.QueryContext(ctx, "SELECT catalog_name FROM information_schema.schemata WHERE catalog_name = 'lake'")
	if err != nil {
		return false
	}
	defer rows.Close()
	return rows.Next()
}
