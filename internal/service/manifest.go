package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

// ManifestColumn describes a column in the manifest response.
type ManifestColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ManifestResult holds the response for a table manifest request.
// It contains presigned URLs, RLS filters, and column masks
// that the client-side DuckDB extension uses to construct secure queries.
type ManifestResult struct {
	Table       string            `json:"table"`
	Schema      string            `json:"schema"`
	Columns     []ManifestColumn  `json:"columns"`
	Files       []string          `json:"files"`
	RowFilters  []string          `json:"row_filters"`
	ColumnMasks map[string]string `json:"column_masks"`
	ExpiresAt   time.Time         `json:"expires_at"`
}

// ManifestService resolves table names to presigned Parquet URLs with
// security policies (RLS filters, column masks) applied. It serves as
// the bridge between the client-side DuckDB extension and the server-side
// security model.
type ManifestService struct {
	metaDB    *sql.DB // DuckLake SQLite metastore (same DB as permissions)
	authSvc   domain.AuthorizationService
	presigner *S3Presigner
	introRepo domain.IntrospectionRepository
	auditRepo domain.AuditRepository
}

// NewManifestService creates a ManifestService backed by the given dependencies.
func NewManifestService(
	metaDB *sql.DB,
	authSvc domain.AuthorizationService,
	presigner *S3Presigner,
	introRepo domain.IntrospectionRepository,
	auditRepo domain.AuditRepository,
) *ManifestService {
	return &ManifestService{
		metaDB:    metaDB,
		authSvc:   authSvc,
		presigner: presigner,
		introRepo: introRepo,
		auditRepo: auditRepo,
	}
}

// GetManifest resolves a table name for a principal, returning presigned URLs,
// RLS filters, column masks, and column metadata. This is the primary endpoint
// consumed by the duck_access DuckDB extension.
func (s *ManifestService) GetManifest(
	ctx context.Context,
	principalName string,
	schemaName string,
	tableName string,
) (*ManifestResult, error) {
	start := time.Now()

	// 1. Resolve table name to DuckLake table ID
	tableID, _, err := s.authSvc.LookupTableID(ctx, tableName)
	if err != nil {
		s.logManifestAudit(ctx, principalName, tableName, "DENIED", err.Error(), time.Since(start))
		return nil, domain.ErrNotFound("table %q not found", tableName)
	}

	// 2. Check RBAC: principal needs SELECT on this table
	allowed, err := s.authSvc.CheckPrivilege(ctx, principalName, domain.SecurableTable, tableID, domain.PrivSelect)
	if err != nil {
		s.logManifestAudit(ctx, principalName, tableName, "ERROR", err.Error(), time.Since(start))
		return nil, fmt.Errorf("privilege check: %w", err)
	}
	if !allowed {
		s.logManifestAudit(ctx, principalName, tableName, "DENIED",
			fmt.Sprintf("%q lacks SELECT on table %q", principalName, tableName), time.Since(start))
		return nil, domain.ErrAccessDenied("%q lacks SELECT on table %q", principalName, tableName)
	}

	// 3. Get RLS row filters for this principal on this table
	rowFilters, err := s.authSvc.GetEffectiveRowFilters(ctx, principalName, tableID)
	if err != nil {
		return nil, fmt.Errorf("row filters: %w", err)
	}

	// 4. Get column masks for this principal on this table
	columnMasks, err := s.authSvc.GetEffectiveColumnMasks(ctx, principalName, tableID)
	if err != nil {
		return nil, fmt.Errorf("column masks: %w", err)
	}

	// 5. Get column metadata
	columns, err := s.introRepo.ListColumns(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("list columns: %w", err)
	}
	manifestCols := make([]ManifestColumn, len(columns))
	for i, c := range columns {
		manifestCols[i] = ManifestColumn{Name: c.Name, Type: c.Type}
	}

	// 6. Resolve Parquet file paths from DuckLake metastore
	s3Paths, err := s.resolveDataFiles(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("resolve files: %w", err)
	}

	// 7. Generate presigned URLs
	expiry := 1 * time.Hour // 1 hour to handle long-running queries
	presignedURLs := make([]string, len(s3Paths))
	for i, path := range s3Paths {
		presignedURL, err := s.presigner.PresignGetObject(ctx, path, expiry)
		if err != nil {
			return nil, fmt.Errorf("presign %q: %w", path, err)
		}
		presignedURLs[i] = presignedURL
	}

	// Normalize nil slices/maps to empty for JSON
	if rowFilters == nil {
		rowFilters = []string{}
	}
	if columnMasks == nil {
		columnMasks = map[string]string{}
	}

	s.logManifestAudit(ctx, principalName, tableName, "ALLOWED", "", time.Since(start))

	return &ManifestResult{
		Table:       tableName,
		Schema:      schemaName,
		Columns:     manifestCols,
		Files:       presignedURLs,
		RowFilters:  rowFilters,
		ColumnMasks: columnMasks,
		ExpiresAt:   time.Now().Add(expiry),
	}, nil
}

// resolveDataFiles queries the DuckLake SQLite metastore for Parquet file
// paths backing the given table. Returns fully-qualified S3 paths.
func (s *ManifestService) resolveDataFiles(ctx context.Context, tableID int64) ([]string, error) {
	// Get the global data_path from ducklake_metadata
	var dataPath string
	err := s.metaDB.QueryRowContext(ctx,
		`SELECT value FROM ducklake_metadata WHERE key = 'data_path'`).Scan(&dataPath)
	if err != nil {
		return nil, fmt.Errorf("read data_path from ducklake_metadata: %w", err)
	}

	// Query active data files for this table
	rows, err := s.metaDB.QueryContext(ctx,
		`SELECT path, path_is_relative FROM ducklake_data_file
		 WHERE table_id = ? AND end_snapshot IS NULL`, tableID)
	if err != nil {
		return nil, fmt.Errorf("query ducklake_data_file: %w", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		var isRelative bool
		if err := rows.Scan(&path, &isRelative); err != nil {
			return nil, err
		}
		if isRelative {
			path = dataPath + path
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(paths) == 0 {
		return nil, fmt.Errorf("no data files found for table_id=%d", tableID)
	}

	return paths, nil
}

// logManifestAudit records a manifest request in the audit log.
func (s *ManifestService) logManifestAudit(ctx context.Context, principal, table, status, errMsg string, duration time.Duration) {
	durationMs := duration.Milliseconds()
	action := "MANIFEST"
	origSQL := fmt.Sprintf("GET MANIFEST %s", table)
	entry := &domain.AuditEntry{
		PrincipalName:  principal,
		Action:         action,
		StatementType:  &action,
		OriginalSQL:    &origSQL,
		TablesAccessed: []string{table},
		Status:         status,
		DurationMs:     &durationMs,
	}
	if errMsg != "" {
		entry.ErrorMessage = &errMsg
	}
	// Best-effort audit logging — don't fail the manifest request if audit fails
	_ = s.auditRepo.Insert(ctx, entry)
}
