package catalog

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

// ViewService provides view management operations.
type ViewService struct {
	repo    domain.ViewRepository
	catalog domain.CatalogRepository
	auth    domain.AuthorizationService
	audit   domain.AuditRepository
}

// NewViewService creates a new ViewService.
func NewViewService(
	repo domain.ViewRepository,
	catalog domain.CatalogRepository,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
) *ViewService {
	return &ViewService{
		repo:    repo,
		catalog: catalog,
		auth:    auth,
		audit:   audit,
	}
}

// CreateView creates a new view in the given schema.
func (s *ViewService) CreateView(ctx context.Context, principal string, schemaName string, req domain.CreateViewRequest) (*domain.ViewDetail, error) {

	// Check CREATE_TABLE privilege (views require same privilege)
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateTable)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks CREATE_TABLE privilege for creating views", principal)
	}

	// Resolve schema ID
	schema, err := s.catalog.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	view := &domain.ViewDetail{
		SchemaID:       schema.SchemaID,
		SchemaName:     schemaName,
		CatalogName:    schema.CatalogName,
		Name:           req.Name,
		ViewDefinition: req.ViewDefinition,
		Comment:        &req.Comment,
		Owner:          principal,
	}

	result, err := s.repo.Create(ctx, view)
	if err != nil {
		return nil, err
	}

	// Enrich with schema/catalog names (not stored in DB)
	result.SchemaName = schemaName
	result.CatalogName = schema.CatalogName

	s.logAudit(ctx, principal, "CREATE_VIEW", fmt.Sprintf("Created view %q in schema %q", req.Name, schemaName))
	return result, nil
}

// GetView returns a view by schema and name.
func (s *ViewService) GetView(ctx context.Context, schemaName, viewName string) (*domain.ViewDetail, error) {
	schema, err := s.catalog.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	result, err := s.repo.GetByName(ctx, schema.SchemaID, viewName)
	if err != nil {
		return nil, err
	}
	result.SchemaName = schemaName
	result.CatalogName = schema.CatalogName
	return result, nil
}

// ListViews returns a paginated list of views in a schema.
func (s *ViewService) ListViews(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.ViewDetail, int64, error) {
	schema, err := s.catalog.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, 0, err
	}
	views, total, err := s.repo.List(ctx, schema.SchemaID, page)
	if err != nil {
		return nil, 0, err
	}
	for i := range views {
		views[i].SchemaName = schemaName
		views[i].CatalogName = schema.CatalogName
	}
	return views, total, nil
}

// DeleteView drops a view from the given schema.
func (s *ViewService) DeleteView(ctx context.Context, principal string, schemaName, viewName string) error {

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateTable)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks permission to delete view %q.%q", principal, schemaName, viewName)
	}

	schema, err := s.catalog.GetSchema(ctx, schemaName)
	if err != nil {
		return err
	}

	if err := s.repo.Delete(ctx, schema.SchemaID, viewName); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "DROP_VIEW", fmt.Sprintf("Dropped view %q.%q", schemaName, viewName))
	return nil
}

// UpdateView updates a view's metadata.
func (s *ViewService) UpdateView(ctx context.Context, principal string, schemaName, viewName string, req domain.UpdateViewRequest) (*domain.ViewDetail, error) {

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, domain.PrivCreateTable)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return nil, domain.ErrAccessDenied("%q lacks permission to update view %q.%q", principal, schemaName, viewName)
	}

	schema, err := s.catalog.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	result, err := s.repo.Update(ctx, schema.SchemaID, viewName, req.Comment, req.Properties, req.ViewDefinition)
	if err != nil {
		return nil, err
	}

	result.SchemaName = schemaName
	result.CatalogName = schema.CatalogName

	s.logAudit(ctx, principal, "UPDATE_VIEW", fmt.Sprintf("Updated view %q.%q", schemaName, viewName))
	return result, nil
}

func (s *ViewService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}
