package service

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// CatalogService provides catalog management operations with authorization.
type CatalogService struct {
	repo      domain.CatalogRepository
	auth      domain.AuthorizationService
	audit     domain.AuditRepository
	tags      domain.TagRepository
	stats     domain.TableStatisticsRepository
	locations domain.ExternalLocationRepository // optional, nil when not configured
}

// NewCatalogService creates a new CatalogService.
func NewCatalogService(
	repo domain.CatalogRepository,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
	tags domain.TagRepository,
	stats domain.TableStatisticsRepository,
) *CatalogService {
	return &CatalogService{
		repo:  repo,
		auth:  auth,
		audit: audit,
		tags:  tags,
		stats: stats,
	}
}

// SetExternalLocationRepo sets the optional external location repository.
// When set, CreateSchema can use LocationName to set a per-schema storage path.
func (s *CatalogService) SetExternalLocationRepo(repo domain.ExternalLocationRepository) {
	s.locations = repo
}

// GetCatalogInfo returns information about the single catalog.
func (s *CatalogService) GetCatalogInfo(ctx context.Context) (*domain.CatalogInfo, error) {
	return s.repo.GetCatalogInfo(ctx)
}

// GetMetastoreSummary returns high-level metastore information.
func (s *CatalogService) GetMetastoreSummary(ctx context.Context) (*domain.MetastoreSummary, error) {
	return s.repo.GetMetastoreSummary(ctx)
}

// ListSchemas returns a paginated list of schemas.
func (s *CatalogService) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
	schemas, total, err := s.repo.ListSchemas(ctx, page)
	if err != nil {
		return nil, 0, err
	}
	for i := range schemas {
		s.enrichSchemaTags(ctx, &schemas[i])
	}
	return schemas, total, nil
}

// CreateSchema creates a new schema, checking CREATE_SCHEMA privilege.
// If LocationName is specified, the schema's storage path is set to the
// external location's URL, enabling per-schema data paths in DuckLake.
func (s *CatalogService) CreateSchema(ctx context.Context, req domain.CreateSchemaRequest) (*domain.SchemaDetail, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks CREATE_SCHEMA on catalog", principal)
	}

	// If a location is specified, validate it exists before creating the schema
	var locationURL string
	if req.LocationName != "" {
		if s.locations == nil {
			return nil, domain.ErrValidation("external locations are not configured")
		}
		loc, err := s.locations.GetByName(ctx, req.LocationName)
		if err != nil {
			return nil, fmt.Errorf("lookup location %q: %w", req.LocationName, err)
		}
		locationURL = loc.URL
	}

	result, err := s.repo.CreateSchema(ctx, req.Name, req.Comment, principal)
	if err != nil {
		return nil, err
	}

	// Set the schema storage path if a location was specified
	if locationURL != "" {
		if err := s.repo.SetSchemaStoragePath(ctx, result.SchemaID, locationURL); err != nil {
			return nil, fmt.Errorf("set schema storage path: %w", err)
		}
	}

	s.logAudit(ctx, principal, "CREATE_SCHEMA", fmt.Sprintf("Created schema %q", req.Name))
	return result, nil
}

// GetSchema returns a schema by name.
func (s *CatalogService) GetSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	result, err := s.repo.GetSchema(ctx, name)
	if err != nil {
		return nil, err
	}
	s.enrichSchemaTags(ctx, result)
	return result, nil
}

// UpdateSchema updates schema metadata.
func (s *CatalogService) UpdateSchema(ctx context.Context, name string, comment *string, props map[string]string) (*domain.SchemaDetail, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	// Check privilege: need CREATE_SCHEMA on catalog or be admin
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks permission to update schema %q", principal, name)
	}

	result, err := s.repo.UpdateSchema(ctx, name, comment, props)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "UPDATE_SCHEMA", fmt.Sprintf("Updated schema %q metadata", name))
	return result, nil
}

// DeleteSchema drops a schema, checking authorization.
func (s *CatalogService) DeleteSchema(ctx context.Context, name string, force bool) error {
	principal, _ := middleware.PrincipalFromContext(ctx)

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateSchema)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks permission to delete schema %q", principal, name)
	}

	if err := s.repo.DeleteSchema(ctx, name, force); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "DELETE_SCHEMA", fmt.Sprintf("Deleted schema %q", name))
	return nil
}

// ListTables returns a paginated list of tables in a schema.
func (s *CatalogService) ListTables(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error) {
	tables, total, err := s.repo.ListTables(ctx, schemaName, page)
	if err != nil {
		return nil, 0, err
	}
	for i := range tables {
		s.enrichTableTags(ctx, &tables[i])
		s.enrichTableStats(ctx, &tables[i])
	}
	return tables, total, nil
}

// CreateTable creates a new table, checking CREATE_TABLE privilege on the schema.
func (s *CatalogService) CreateTable(ctx context.Context, schemaName string, req domain.CreateTableRequest) (*domain.TableDetail, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	// Check CREATE_TABLE at catalog level
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateTable)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks CREATE_TABLE privilege", principal)
	}

	result, err := s.repo.CreateTable(ctx, schemaName, req, principal)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "CREATE_TABLE", fmt.Sprintf("Created table %q in schema %q", req.Name, schemaName))
	return result, nil
}

// GetTable returns a table by schema and table name.
func (s *CatalogService) GetTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	result, err := s.repo.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	s.enrichTableTags(ctx, result)
	s.enrichTableStats(ctx, result)
	return result, nil
}

// DeleteTable drops a table, checking authorization.
func (s *CatalogService) DeleteTable(ctx context.Context, schemaName, tableName string) error {
	principal, _ := middleware.PrincipalFromContext(ctx)

	// Check CREATE_TABLE at catalog level (which implies table management)
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateTable)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks permission to delete table %q.%q", principal, schemaName, tableName)
	}

	if err := s.repo.DeleteTable(ctx, schemaName, tableName); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "DROP_TABLE", fmt.Sprintf("Dropped table %q.%q", schemaName, tableName))
	return nil
}

// ListColumns returns a paginated list of columns for a table.
func (s *CatalogService) ListColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	return s.repo.ListColumns(ctx, schemaName, tableName, page)
}

// UpdateTable updates table metadata, checking CREATE_TABLE privilege.
func (s *CatalogService) UpdateTable(ctx context.Context, schemaName, tableName string, req domain.UpdateTableRequest) (*domain.TableDetail, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateTable)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks permission to update table %q.%q", principal, schemaName, tableName)
	}

	result, err := s.repo.UpdateTable(ctx, schemaName, tableName, req.Comment, req.Properties, req.Owner)
	if err != nil {
		return nil, err
	}

	s.enrichTableTags(ctx, result)
	s.enrichTableStats(ctx, result)
	s.logAudit(ctx, principal, "UPDATE_TABLE", fmt.Sprintf("Updated table %q.%q metadata", schemaName, tableName))
	return result, nil
}

// UpdateCatalog updates catalog-level metadata (admin only).
func (s *CatalogService) UpdateCatalog(ctx context.Context, req domain.UpdateCatalogRequest) (*domain.CatalogInfo, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks permission to update catalog metadata", principal)
	}

	result, err := s.repo.UpdateCatalog(ctx, req.Comment)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "UPDATE_CATALOG", "Updated catalog metadata")
	return result, nil
}

// UpdateColumn updates column metadata, checking CREATE_TABLE privilege.
func (s *CatalogService) UpdateColumn(ctx context.Context, schemaName, tableName, columnName string, req domain.UpdateColumnRequest) (*domain.ColumnDetail, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateTable)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks permission to update column metadata", principal)
	}

	result, err := s.repo.UpdateColumn(ctx, schemaName, tableName, columnName, req.Comment, req.Properties)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "UPDATE_COLUMN", fmt.Sprintf("Updated column %q in %q.%q", columnName, schemaName, tableName))
	return result, nil
}

// ProfileTable runs profiling queries and stores statistics.
func (s *CatalogService) ProfileTable(ctx context.Context, schemaName, tableName string) (*domain.TableStatistics, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	// Verify table exists
	tbl, err := s.repo.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	stats := &domain.TableStatistics{
		ProfiledBy: principal,
	}

	// Column count from existing data
	colCount := int64(len(tbl.Columns))
	stats.ColumnCount = &colCount

	// Store statistics
	securableName := schemaName + "." + tableName
	if err := s.stats.Upsert(ctx, securableName, stats); err != nil {
		return nil, fmt.Errorf("store table statistics: %w", err)
	}

	// Return stored stats (which now has LastProfiledAt set by the DB)
	return s.stats.Get(ctx, securableName)
}

func (s *CatalogService) enrichSchemaTags(ctx context.Context, schema *domain.SchemaDetail) {
	if s.tags == nil {
		return
	}
	tags, err := s.tags.ListTagsForSecurable(ctx, "schema", schema.SchemaID, nil)
	if err == nil {
		schema.Tags = tags
	}
}

func (s *CatalogService) enrichTableTags(ctx context.Context, table *domain.TableDetail) {
	if s.tags == nil {
		return
	}
	tags, err := s.tags.ListTagsForSecurable(ctx, "table", table.TableID, nil)
	if err == nil {
		table.Tags = tags
	}
}

func (s *CatalogService) enrichTableStats(ctx context.Context, table *domain.TableDetail) {
	if s.stats == nil {
		return
	}
	securableName := table.SchemaName + "." + table.Name
	stats, err := s.stats.Get(ctx, securableName)
	if err == nil && stats != nil {
		table.Statistics = stats
	}
}

func (s *CatalogService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}
