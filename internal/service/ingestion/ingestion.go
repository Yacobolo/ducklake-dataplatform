// Package ingestion implements data ingestion services.
package ingestion

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
)

// IngestionService handles Parquet file ingestion into DuckLake tables
// via the ducklake_add_data_files() function.
//
//nolint:revive // Name chosen for clarity across package boundaries
type IngestionService struct {
	duckDB      *sql.DB
	metastore   domain.MetastoreQuerier
	authSvc     domain.AuthorizationService
	presigner   *query.S3Presigner // legacy presigner (from env config), may be nil
	auditRepo   domain.AuditRepository
	credRepo    domain.StorageCredentialRepository // for credential-aware presigning
	locRepo     domain.ExternalLocationRepository  // for resolving schema locations
	catalogName string                             // attached catalog name (e.g., "lake")
	bucket      string                             // S3 bucket name (legacy default)
}

// NewIngestionService creates a new IngestionService.
func NewIngestionService(
	duckDB *sql.DB,
	metastore domain.MetastoreQuerier,
	authSvc domain.AuthorizationService,
	presigner *query.S3Presigner,
	auditRepo domain.AuditRepository,
	catalogName string,
	bucket string,
	credRepo domain.StorageCredentialRepository,
	locRepo domain.ExternalLocationRepository,
) *IngestionService {
	return &IngestionService{
		duckDB:      duckDB,
		metastore:   metastore,
		authSvc:     authSvc,
		presigner:   presigner,
		auditRepo:   auditRepo,
		catalogName: catalogName,
		bucket:      bucket,
		credRepo:    credRepo,
		locRepo:     locRepo,
	}
}

// RequestUploadURL generates a presigned PUT URL for uploading a Parquet file.
// The caller must have INSERT privilege on the target table.
func (s *IngestionService) RequestUploadURL(
	ctx context.Context,
	principal string,
	schemaName, tableName string,
	filename *string,
) (*domain.UploadURLResult, error) {

	// Authorize: check INSERT on table
	if err := s.checkInsertPrivilege(ctx, principal, tableName); err != nil {
		return nil, err
	}

	// Resolve presigner and bucket: prefer per-schema location, fall back to legacy
	presigner, bucket, err := s.resolvePresigner(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	if presigner == nil {
		return nil, domain.ErrValidation("upload not available: S3 presigner not configured")
	}

	// Generate S3 key
	id := uuid.New().String()
	suffix := id + ".parquet"
	if filename != nil && *filename != "" {
		suffix = id + "_" + sanitizeFilename(*filename)
	}
	key := fmt.Sprintf("lake_data/%s/%s/uploads/%s", schemaName, tableName, suffix)

	// Generate presigned PUT URL
	expiry := 1 * time.Hour
	url, err := presigner.PresignPutObject(ctx, bucket, key, expiry)
	if err != nil {
		return nil, fmt.Errorf("generate upload URL: %w", err)
	}

	s.logAudit(ctx, principal, "INGESTION_UPLOAD_URL",
		fmt.Sprintf("Generated upload URL for %s.%s: %s", schemaName, tableName, key))

	return &domain.UploadURLResult{
		UploadURL: url,
		S3Key:     key,
		ExpiresAt: time.Now().Add(expiry),
	}, nil
}

// CommitIngestion registers previously uploaded files in DuckLake.
// s3Keys are relative keys (from upload-url response), converted to full s3:// URIs.
func (s *IngestionService) CommitIngestion(
	ctx context.Context,
	principal string,
	schemaName, tableName string,
	s3Keys []string,
	opts domain.IngestionOptions,
) (*domain.IngestionResult, error) {

	if len(s3Keys) == 0 {
		return nil, domain.ErrValidation("s3_keys must not be empty")
	}

	// Authorize: check INSERT on table
	if err := s.checkInsertPrivilege(ctx, principal, tableName); err != nil {
		return nil, err
	}

	// Resolve the bucket for this schema
	_, bucket, err := s.resolvePresigner(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	// Convert keys to full S3 URIs
	paths := make([]string, len(s3Keys))
	for i, key := range s3Keys {
		paths[i] = fmt.Sprintf("s3://%s/%s", bucket, key)
	}

	// Execute ducklake_add_data_files
	result, err := s.execAddDataFiles(ctx, schemaName, tableName, paths, opts)
	if err != nil {
		s.logAudit(ctx, principal, "INGESTION_COMMIT",
			fmt.Sprintf("Failed to commit %d file(s) to %s.%s: %v", len(s3Keys), schemaName, tableName, err))
		return nil, err
	}

	s.logAudit(ctx, principal, "INGESTION_COMMIT",
		fmt.Sprintf("Committed %d file(s) to %s.%s", result.FilesRegistered, schemaName, tableName))

	return result, nil
}

// LoadExternalFiles registers existing S3 files or globs in DuckLake.
// Paths can be full s3:// URIs or relative to the lake data path.
func (s *IngestionService) LoadExternalFiles(
	ctx context.Context,
	principal string,
	schemaName, tableName string,
	paths []string,
	opts domain.IngestionOptions,
) (*domain.IngestionResult, error) {

	if len(paths) == 0 {
		return nil, domain.ErrValidation("paths must not be empty")
	}

	// Authorize: check INSERT on table
	if err := s.checkInsertPrivilege(ctx, principal, tableName); err != nil {
		return nil, err
	}

	// Resolve paths: if no s3:// prefix, prepend the lake data path
	resolved := make([]string, len(paths))
	for i, p := range paths {
		if strings.HasPrefix(p, "s3://") {
			resolved[i] = p
		} else {
			dataPath, err := s.readDataPath(ctx)
			if err != nil {
				return nil, fmt.Errorf("resolve data path: %w", err)
			}
			resolved[i] = dataPath + p
		}
	}

	result, err := s.execAddDataFiles(ctx, schemaName, tableName, resolved, opts)
	if err != nil {
		s.logAudit(ctx, principal, "INGESTION_LOAD",
			fmt.Sprintf("Failed to load %d path(s) into %s.%s: %v", len(paths), schemaName, tableName, err))
		return nil, err
	}

	s.logAudit(ctx, principal, "INGESTION_LOAD",
		fmt.Sprintf("Loaded %d file(s) into %s.%s", result.FilesRegistered, schemaName, tableName))

	return result, nil
}

// execAddDataFiles builds and executes the CALL ducklake_add_data_files() statement.
func (s *IngestionService) execAddDataFiles(
	ctx context.Context,
	schemaName, tableName string,
	paths []string,
	opts domain.IngestionOptions,
) (*domain.IngestionResult, error) {
	if s.duckDB == nil {
		return nil, domain.ErrValidation("ingestion not available: DuckDB not configured")
	}

	// Build the file list as a DuckDB list literal
	quotedPaths := make([]string, len(paths))
	for i, p := range paths {
		quotedPaths[i] = "'" + strings.ReplaceAll(p, "'", "''") + "'"
	}
	fileList := "[" + strings.Join(quotedPaths, ", ") + "]"

	// Build the CALL statement
	q := fmt.Sprintf(
		"CALL ducklake_add_data_files('%s', '%s', %s, schema => '%s', allow_missing => %t, ignore_extra_columns => %t)",
		strings.ReplaceAll(s.catalogName, "'", "''"),
		strings.ReplaceAll(tableName, "'", "''"),
		fileList,
		strings.ReplaceAll(schemaName, "'", "''"),
		opts.AllowMissingColumns,
		opts.IgnoreExtraColumns,
	)

	// Execute directly on duckDB (bypasses SecureEngine — CALL not supported by pg_query_go)
	_, err := s.duckDB.ExecContext(ctx, q)
	if err != nil {
		return nil, classifyDuckDBError(err)
	}

	return &domain.IngestionResult{
		FilesRegistered: len(paths),
		FilesSkipped:    0,
		Table:           tableName,
		Schema:          schemaName,
	}, nil
}

// checkInsertPrivilege verifies the authenticated principal has INSERT on the table.
func (s *IngestionService) checkInsertPrivilege(ctx context.Context, principal, tableName string) error {
	tableID, _, _, err := s.authSvc.LookupTableID(ctx, tableName)
	if err != nil {
		return domain.ErrNotFound("table %q not found", tableName)
	}

	allowed, err := s.authSvc.CheckPrivilege(ctx, principal, domain.SecurableTable, tableID, domain.PrivInsert)
	if err != nil {
		return fmt.Errorf("check INSERT privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks INSERT on table %q", principal, tableName)
	}
	return nil
}

// readDataPath reads the data_path from the DuckLake metadata table.
func (s *IngestionService) readDataPath(ctx context.Context) (string, error) {
	return s.metastore.ReadDataPath(ctx)
}

// classifyDuckDBError maps DuckDB errors from ducklake_add_data_files into domain errors.
func classifyDuckDBError(err error) error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "does not exist"):
		return domain.ErrNotFound("%s", msg)
	case strings.Contains(msg, "type mismatch"),
		strings.Contains(msg, "not found in table"),
		strings.Contains(msg, "No files found"),
		strings.Contains(msg, "Could not read file"):
		return domain.ErrValidation("%s", msg)
	default:
		return domain.ErrValidation("ingestion failed: %s", msg)
	}
}

// sanitizeFilename strips path separators and keeps only safe characters.
func sanitizeFilename(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, "..", "_")
	if !strings.HasSuffix(name, ".parquet") {
		name += ".parquet"
	}
	return name
}

// resolvePresigner returns the appropriate presigner and bucket for a schema.
// If the schema has a per-schema external location with a stored credential,
// a dynamic presigner is created from that credential. Otherwise, the legacy
// presigner and bucket are returned.
func (s *IngestionService) resolvePresigner(ctx context.Context, schemaName string) (*query.S3Presigner, string, error) {
	// Try per-schema resolution via schema path in ducklake_schema
	if s.metastore != nil && s.credRepo != nil && s.locRepo != nil {
		schemaPath, err := s.metastore.ReadSchemaPath(ctx, schemaName)
		if err == nil && schemaPath != "" {
			// Schema has a custom path — find the location that matches this URL
			locations, _, err := s.locRepo.List(ctx, domain.PageRequest{MaxResults: 1000})
			if err == nil {
				for _, loc := range locations {
					if strings.HasPrefix(schemaPath, loc.URL) || schemaPath == loc.URL {
						// Found the matching location, look up its credential
						cred, err := s.credRepo.GetByName(ctx, loc.CredentialName)
						if err == nil {
							bucket, _, _ := query.ParseS3Path(schemaPath)
							if bucket == "" {
								bucket = s.bucket // fallback
							}
							presigner, err := query.NewS3PresignerFromCredential(cred, bucket)
							if err == nil {
								return presigner, bucket, nil
							}
						}
					}
				}
			}
		}
	}

	// Fall back to legacy presigner
	return s.presigner, s.bucket, nil
}

func (s *IngestionService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.auditRepo.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}
