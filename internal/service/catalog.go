package service

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// CatalogService provides catalog management operations with authorization.
type CatalogService struct {
	repo  domain.CatalogRepository
	auth  domain.AuthorizationService
	audit domain.AuditRepository
}

// NewCatalogService creates a new CatalogService.
func NewCatalogService(
	repo domain.CatalogRepository,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
) *CatalogService {
	return &CatalogService{
		repo:  repo,
		auth:  auth,
		audit: audit,
	}
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
	return s.repo.ListSchemas(ctx, page)
}

// CreateSchema creates a new schema, checking CREATE_SCHEMA privilege.
func (s *CatalogService) CreateSchema(ctx context.Context, req domain.CreateSchemaRequest) (*domain.SchemaDetail, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks CREATE_SCHEMA on catalog", principal)
	}

	result, err := s.repo.CreateSchema(ctx, req.Name, req.Comment, principal)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "CREATE_SCHEMA", fmt.Sprintf("Created schema %q", req.Name))
	return result, nil
}

// GetSchema returns a schema by name.
func (s *CatalogService) GetSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	return s.repo.GetSchema(ctx, name)
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
	return s.repo.ListTables(ctx, schemaName, page)
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
	return s.repo.GetTable(ctx, schemaName, tableName)
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

func (s *CatalogService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}
