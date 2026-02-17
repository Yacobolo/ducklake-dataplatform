// Package query implements query execution and manifest services.
package query

import (
	"context"
	"fmt"
	"strings"
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

// FilePresigner generates accessible URLs or paths for data files.
// Implementations include S3Presigner (production) and test doubles (local paths).
type FilePresigner interface {
	PresignGetObject(ctx context.Context, path string, expiry time.Duration) (string, error)
}

// ManifestService resolves table names to presigned Parquet URLs with
// security policies (RLS filters, column masks) applied. It serves as
// the bridge between the client-side DuckDB extension and the server-side
// security model.
// All methods accept a catalogName parameter to resolve the correct metastore.
type ManifestService struct {
	metastoreFactory domain.MetastoreQuerierFactory // per-catalog metastore access
	authSvc          domain.AuthorizationService
	presigner        FilePresigner
	introRepo        domain.IntrospectionRepository
	auditRepo        domain.AuditRepository
	credRepo         domain.StorageCredentialRepository // for credential-aware presigning
	locRepo          domain.ExternalLocationRepository  // for resolving schema locations
}

// NewManifestService creates a ManifestService backed by the given dependencies.
func NewManifestService(
	metastoreFactory domain.MetastoreQuerierFactory,
	authSvc domain.AuthorizationService,
	presigner FilePresigner,
	introRepo domain.IntrospectionRepository,
	auditRepo domain.AuditRepository,
	credRepo domain.StorageCredentialRepository,
	locRepo domain.ExternalLocationRepository,
) *ManifestService {
	return &ManifestService{
		metastoreFactory: metastoreFactory,
		authSvc:          authSvc,
		presigner:        presigner,
		introRepo:        introRepo,
		auditRepo:        auditRepo,
		credRepo:         credRepo,
		locRepo:          locRepo,
	}
}

// GetManifest resolves a table name for a principal, returning presigned URLs,
// RLS filters, column masks, and column metadata. This is the primary endpoint
// consumed by the duck_access DuckDB extension.
func (s *ManifestService) GetManifest(
	ctx context.Context,
	principalName string,
	catalogName string,
	schemaName string,
	tableName string,
) (*ManifestResult, error) {
	start := time.Now()

	// 1. Resolve table name to DuckLake table ID
	lookupName := qualifiedTableName(catalogName, schemaName, tableName)
	tableID, _, _, err := s.authSvc.LookupTableID(ctx, lookupName)
	if err != nil {
		s.logManifestAudit(ctx, principalName, lookupName, "DENIED", err.Error(), time.Since(start))
		return nil, domain.ErrNotFound("table %q not found", lookupName)
	}

	// 2. Check RBAC: principal needs SELECT on this table
	allowed, err := s.authSvc.CheckPrivilege(ctx, principalName, domain.SecurableTable, tableID, domain.PrivSelect)
	if err != nil {
		s.logManifestAudit(ctx, principalName, lookupName, "ERROR", err.Error(), time.Since(start))
		return nil, fmt.Errorf("privilege check: %w", err)
	}
	if !allowed {
		s.logManifestAudit(ctx, principalName, lookupName, "DENIED",
			fmt.Sprintf("%q lacks SELECT on table %q", principalName, lookupName), time.Since(start))
		return nil, domain.ErrAccessDenied("%q lacks SELECT on table %q", principalName, lookupName)
	}

	// 3. Get RLS row filters for this principal on this table.
	// Multiple filters are combined with OR (each represents a visibility window),
	// then collapsed into a single expression. This ensures consistent semantics
	// between the server-side SQL rewrite engine (InjectMultipleRowFilters) and
	// the client-side DuckDB extension, which AND-combines manifest entries.
	rowFilters, err := s.authSvc.GetEffectiveRowFilters(ctx, principalName, tableID)
	if err != nil {
		return nil, fmt.Errorf("row filters: %w", err)
	}
	if len(rowFilters) > 1 {
		parts := make([]string, len(rowFilters))
		for i, f := range rowFilters {
			parts[i] = "(" + f + ")"
		}
		rowFilters = []string{strings.Join(parts, " OR ")}
	}

	// 4. Get column masks for this principal on this table
	columnMasks, err := s.authSvc.GetEffectiveColumnMasks(ctx, principalName, tableID)
	if err != nil {
		return nil, fmt.Errorf("column masks: %w", err)
	}

	// 5. Get column metadata (fetch all columns with large page size)
	columns, _, err := s.introRepo.ListColumns(ctx, tableID, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return nil, fmt.Errorf("list columns: %w", err)
	}
	manifestCols := make([]ManifestColumn, len(columns))
	for i, c := range columns {
		manifestCols[i] = ManifestColumn{Name: c.Name, Type: c.Type}
	}

	// 6. Resolve Parquet file paths from DuckLake metastore
	s3Paths, schemaPath, err := s.resolveDataFiles(ctx, catalogName, tableID, schemaName)
	if err != nil {
		return nil, fmt.Errorf("resolve files: %w", err)
	}

	// 7. Resolve the presigner from schema-bound credentials
	presigner, err := s.resolvePresigner(ctx, schemaPath)
	if err != nil {
		return nil, fmt.Errorf("resolve presigner: %w", err)
	}

	// 8. Generate presigned URLs
	expiry := 1 * time.Hour // 1 hour to handle long-running queries
	presignedURLs := make([]string, len(s3Paths))
	for i, path := range s3Paths {
		presignedURL, err := presigner.PresignGetObject(ctx, path, expiry)
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

	s.logManifestAudit(ctx, principalName, lookupName, "ALLOWED", "", time.Since(start))

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

func qualifiedTableName(catalogName, schemaName, tableName string) string {
	if catalogName != "" {
		return catalogName + "." + schemaName + "." + tableName
	}
	if schemaName != "" {
		return schemaName + "." + tableName
	}
	return tableName
}

// resolveDataFiles queries the DuckLake metastore for Parquet file
// paths backing the given table. Returns fully-qualified S3 paths and the
// schema-level storage path (if set), which is used to resolve the presigner.
func (s *ManifestService) resolveDataFiles(ctx context.Context, catalogName string, tableID string, schemaName string) ([]string, string, error) {
	metastore, err := s.metastoreFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, "", fmt.Errorf("resolve metastore for catalog %q: %w", catalogName, err)
	}

	dataPath, err := metastore.ReadDataPath(ctx)
	if err != nil {
		return nil, "", err
	}

	schemaPath, _ := metastore.ReadSchemaPath(ctx, schemaName)

	filePaths, isRelative, err := metastore.ListDataFiles(ctx, tableID)
	if err != nil {
		return nil, "", err
	}

	var paths []string
	for i, path := range filePaths {
		if isRelative[i] {
			path = dataPath + path
		}
		paths = append(paths, path)
	}

	if len(paths) == 0 {
		return nil, "", fmt.Errorf("no data files found for table_id=%s", tableID)
	}

	return paths, schemaPath, nil
}

// resolvePresigner returns the appropriate presigner for the given schema path.
// The schema path must resolve to an external location with stored credentials.
func (s *ManifestService) resolvePresigner(ctx context.Context, schemaPath string) (FilePresigner, error) {
	if schemaPath == "" {
		return nil, fmt.Errorf("schema has no storage path")
	}
	if schemaPath != "" && s.credRepo != nil && s.locRepo != nil {
		// Find the external location matching this schema path
		locations, _, err := s.locRepo.List(ctx, domain.PageRequest{MaxResults: 1000})
		if err == nil {
			for _, loc := range locations {
				if strings.HasPrefix(schemaPath, loc.URL) || schemaPath == loc.URL {
					cred, err := s.credRepo.GetByName(ctx, loc.CredentialName)
					if err == nil {
						presigner, err := NewPresignerFromCredential(cred, schemaPath)
						if err == nil {
							return presigner, nil
						}
					}
				}
			}
		}
	}

	return nil, fmt.Errorf("no storage credential found for schema path %q", schemaPath)
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
