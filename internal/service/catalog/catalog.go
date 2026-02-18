// Package catalog implements catalog and metadata services.
package catalog

import (
	"context"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// CatalogRepoFactory creates CatalogRepository instances scoped to a catalog.
//
//nolint:revive // Name chosen for clarity across package boundaries
type CatalogRepoFactory interface {
	ForCatalog(ctx context.Context, catalogName string) (domain.CatalogRepository, error)
}

// CatalogService provides catalog management operations with authorization.
// All methods accept a catalogName parameter to resolve the correct repo.
//
//nolint:revive // Name chosen for clarity across package boundaries
type CatalogService struct {
	repoFactory CatalogRepoFactory
	auth        domain.AuthorizationService
	audit       domain.AuditRepository
	tags        domain.TagRepository
	stats       domain.TableStatisticsRepository
	locations   domain.ExternalLocationRepository // optional, nil when not configured
}

// NewCatalogService creates a new CatalogService.
func NewCatalogService(
	repoFactory CatalogRepoFactory,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
	tags domain.TagRepository,
	stats domain.TableStatisticsRepository,
	locations domain.ExternalLocationRepository,
) *CatalogService {
	return &CatalogService{
		repoFactory: repoFactory,
		auth:        auth,
		audit:       audit,
		tags:        tags,
		stats:       stats,
		locations:   locations,
	}
}

// GetCatalogInfo returns information about a catalog.
func (s *CatalogService) GetCatalogInfo(ctx context.Context, catalogName string) (*domain.CatalogInfo, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	return repo.GetCatalogInfo(ctx)
}

// GetMetastoreSummary returns high-level metastore information.
func (s *CatalogService) GetMetastoreSummary(ctx context.Context, catalogName string) (*domain.MetastoreSummary, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	return repo.GetMetastoreSummary(ctx)
}

// ListSchemas returns a paginated list of schemas.
func (s *CatalogService) ListSchemas(ctx context.Context, catalogName string, page domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, 0, err
	}
	schemas, total, err := repo.ListSchemas(ctx, page)
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
func (s *CatalogService) CreateSchema(ctx context.Context, catalogName string, principal string, req domain.CreateSchemaRequest) (*domain.SchemaDetail, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, catalogName, domain.PrivCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "CREATE_SCHEMA", fmt.Sprintf("Denied create schema %q in catalog %q", req.Name, catalogName))
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

	result, err := repo.CreateSchema(ctx, req.Name, req.Comment, principal)
	if err != nil {
		return nil, err
	}

	// Set the schema storage path if a location was specified
	if locationURL != "" {
		if err := repo.SetSchemaStoragePath(ctx, result.SchemaID, locationURL); err != nil {
			return nil, fmt.Errorf("set schema storage path: %w", err)
		}
	}

	s.logAudit(ctx, principal, "CREATE_SCHEMA", fmt.Sprintf("Created schema %q in catalog %q", req.Name, catalogName))
	return result, nil
}

// GetSchema returns a schema by name.
func (s *CatalogService) GetSchema(ctx context.Context, catalogName string, name string) (*domain.SchemaDetail, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	result, err := repo.GetSchema(ctx, name)
	if err != nil {
		return nil, err
	}
	s.enrichSchemaTags(ctx, result)
	return result, nil
}

// UpdateSchema updates schema metadata.
func (s *CatalogService) UpdateSchema(ctx context.Context, catalogName string, principal string, name string, req domain.UpdateSchemaRequest) (*domain.SchemaDetail, error) {

	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}

	schema, err := repo.GetSchema(ctx, name)
	if err != nil {
		return nil, err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableSchema, schema.SchemaID, domain.PrivCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "UPDATE_SCHEMA", fmt.Sprintf("Denied update schema %q", name))
		return nil, domain.ErrAccessDenied("%q lacks permission to update schema %q", principal, name)
	}

	result, err := repo.UpdateSchema(ctx, name, req.Comment, req.Properties)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "UPDATE_SCHEMA", fmt.Sprintf("Updated schema %q metadata", name))
	return result, nil
}

// DeleteSchema drops a schema, checking authorization.
func (s *CatalogService) DeleteSchema(ctx context.Context, catalogName string, principal string, name string, force bool) error {

	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return err
	}

	schema, err := repo.GetSchema(ctx, name)
	if err != nil {
		return err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableSchema, schema.SchemaID, domain.PrivCreateSchema)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "DELETE_SCHEMA", fmt.Sprintf("Denied delete schema %q", name))
		return domain.ErrAccessDenied("%q lacks permission to delete schema %q", principal, name)
	}

	if err := repo.DeleteSchema(ctx, name, force); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "DELETE_SCHEMA", fmt.Sprintf("Deleted schema %q", name))
	return nil
}

// ListTables returns a paginated list of tables in a schema.
func (s *CatalogService) ListTables(ctx context.Context, catalogName string, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, 0, err
	}
	tables, total, err := repo.ListTables(ctx, schemaName, page)
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
// If req.TableType is "EXTERNAL", delegates to createExternalTable.
func (s *CatalogService) CreateTable(ctx context.Context, catalogName string, principal string, schemaName string, req domain.CreateTableRequest) (*domain.TableDetail, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}

	schema, err := repo.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableSchema, schema.SchemaID, domain.PrivCreateTable)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "CREATE_TABLE", fmt.Sprintf("Denied create table %q in schema %q", req.Name, schemaName))
		return nil, domain.ErrAccessDenied("%q lacks CREATE_TABLE privilege", principal)
	}

	switch req.TableType {
	case "", domain.TableTypeManaged:
		result, err := repo.CreateTable(ctx, schemaName, req, principal)
		if err != nil {
			return nil, err
		}
		s.logAudit(ctx, principal, "CREATE_TABLE", fmt.Sprintf("Created table %q in schema %q", req.Name, schemaName))
		return result, nil

	case domain.TableTypeExternal:
		return s.createExternalTable(ctx, catalogName, schemaName, req, principal)

	default:
		return nil, domain.ErrValidation("unsupported table_type: %q", req.TableType)
	}
}

// createExternalTable creates an external table backed by a DuckDB VIEW.
func (s *CatalogService) createExternalTable(ctx context.Context, catalogName string, schemaName string, req domain.CreateTableRequest, principal string) (*domain.TableDetail, error) {
	if req.SourcePath == "" {
		return nil, domain.ErrValidation("source_path is required for EXTERNAL tables")
	}
	if req.LocationName == "" {
		return nil, domain.ErrValidation("location_name is required for EXTERNAL tables")
	}

	// Validate location exists and source path falls under it
	if s.locations == nil {
		return nil, domain.ErrValidation("external locations are not configured")
	}
	loc, err := s.locations.GetByName(ctx, req.LocationName)
	if err != nil {
		return nil, fmt.Errorf("lookup location %q: %w", req.LocationName, err)
	}
	if !strings.HasPrefix(req.SourcePath, loc.URL) {
		return nil, domain.ErrValidation("source_path %q is not under location %q (URL: %s)", req.SourcePath, req.LocationName, loc.URL)
	}

	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	result, err := repo.CreateExternalTable(ctx, schemaName, req, principal)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "CREATE_EXTERNAL_TABLE", fmt.Sprintf("Created external table %q in schema %q", req.Name, schemaName))
	return result, nil
}

// GetTable returns a table by schema and table name.
func (s *CatalogService) GetTable(ctx context.Context, catalogName string, schemaName, tableName string) (*domain.TableDetail, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	result, err := repo.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	s.enrichTableTags(ctx, result)
	s.enrichTableStats(ctx, result)
	return result, nil
}

// DeleteTable drops a table, checking authorization.
func (s *CatalogService) DeleteTable(ctx context.Context, catalogName string, principal string, schemaName, tableName string) error {

	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return err
	}
	tbl, err := repo.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableTable, tbl.TableID, domain.PrivCreateTable)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "DROP_TABLE", fmt.Sprintf("Denied delete table %q.%q", schemaName, tableName))
		return domain.ErrAccessDenied("%q lacks permission to delete table %q.%q", principal, schemaName, tableName)
	}

	if err := repo.DeleteTable(ctx, schemaName, tableName); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "DROP_TABLE", fmt.Sprintf("Dropped table %q.%q", schemaName, tableName))
	return nil
}

// ListColumns returns a paginated list of columns for a table.
func (s *CatalogService) ListColumns(ctx context.Context, catalogName string, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, 0, err
	}
	return repo.ListColumns(ctx, schemaName, tableName, page)
}

// UpdateTable updates table metadata, checking CREATE_TABLE privilege.
func (s *CatalogService) UpdateTable(ctx context.Context, catalogName string, principal string, schemaName, tableName string, req domain.UpdateTableRequest) (*domain.TableDetail, error) {

	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	tbl, err := repo.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableTable, tbl.TableID, domain.PrivCreateTable)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "UPDATE_TABLE", fmt.Sprintf("Denied update table %q.%q", schemaName, tableName))
		return nil, domain.ErrAccessDenied("%q lacks permission to update table %q.%q", principal, schemaName, tableName)
	}

	result, err := repo.UpdateTable(ctx, schemaName, tableName, req.Comment, req.Properties, req.Owner)
	if err != nil {
		return nil, err
	}

	s.enrichTableTags(ctx, result)
	s.enrichTableStats(ctx, result)
	s.logAudit(ctx, principal, "UPDATE_TABLE", fmt.Sprintf("Updated table %q.%q metadata", schemaName, tableName))
	return result, nil
}

// UpdateCatalog updates catalog-level metadata (admin only).
func (s *CatalogService) UpdateCatalog(ctx context.Context, catalogName string, principal string, req domain.UpdateCatalogRequest) (*domain.CatalogInfo, error) {

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, catalogName, domain.PrivCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "UPDATE_CATALOG", "Denied update catalog metadata")
		return nil, domain.ErrAccessDenied("%q lacks permission to update catalog metadata", principal)
	}

	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	result, err := repo.UpdateCatalog(ctx, req.Comment)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "UPDATE_CATALOG", "Updated catalog metadata")
	return result, nil
}

// UpdateColumn updates column metadata, checking CREATE_TABLE privilege.
func (s *CatalogService) UpdateColumn(ctx context.Context, catalogName string, principal string, schemaName, tableName, columnName string, req domain.UpdateColumnRequest) (*domain.ColumnDetail, error) {

	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	tbl, err := repo.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableTable, tbl.TableID, domain.PrivCreateTable)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "UPDATE_COLUMN", fmt.Sprintf("Denied update column %q in %q.%q", columnName, schemaName, tableName))
		return nil, domain.ErrAccessDenied("%q lacks permission to update column metadata", principal)
	}

	result, err := repo.UpdateColumn(ctx, schemaName, tableName, columnName, req.Comment, req.Properties)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "UPDATE_COLUMN", fmt.Sprintf("Updated column %q in %q.%q", columnName, schemaName, tableName))
	return result, nil
}

// ProfileTable runs profiling queries and stores statistics.
func (s *CatalogService) ProfileTable(ctx context.Context, catalogName string, principal string, schemaName, tableName string) (*domain.TableStatistics, error) {

	// Verify table exists
	repo, err := s.repoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	tbl, err := repo.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	// Authorization: require SELECT on the table
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableTable, tbl.TableID, domain.PrivSelect)
	if err != nil {
		return nil, fmt.Errorf("check profile privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("principal %q lacks SELECT on %s.%s", principal, schemaName, tableName)
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
	result, err := s.stats.Get(ctx, securableName)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "PROFILE_TABLE", fmt.Sprintf("Profiled table %q.%q", schemaName, tableName))
	return result, nil
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

func (s *CatalogService) logAuditDenied(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "DENIED",
		OriginalSQL:   &detail,
	})
}
