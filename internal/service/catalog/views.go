package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	servicepolicy "github.com/Yacobolo/quackstack/internal/service/policy"
)

type runtimeViewCatalog interface {
	CreateView(ctx context.Context, schemaName, viewName, viewDefinition string) error
	UpdateViewDefinition(ctx context.Context, schemaName, viewName, viewDefinition string) error
	DeleteView(ctx context.Context, schemaName, viewName string) error
}

// ViewService provides view management operations.
// All methods accept a catalogName parameter to resolve the correct catalog repo.
type ViewService struct {
	repo           domain.ViewRepository
	catalogFactory CatalogRepoFactory
	auth           domain.AuthorizationService
	audit          domain.AuditRepository
}

// NewViewService creates a new ViewService.
func NewViewService(
	repo domain.ViewRepository,
	catalogFactory CatalogRepoFactory,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
) *ViewService {
	return &ViewService{
		repo:           repo,
		catalogFactory: catalogFactory,
		auth:           auth,
		audit:          audit,
	}
}

// CreateView creates a new view in the given schema.
func (s *ViewService) CreateView(ctx context.Context, catalogName string, principal string, schemaName string, req domain.CreateViewRequest) (*domain.ViewDetail, error) {
	if err := ensureMutableCatalog(catalogName); err != nil {
		return nil, err
	}
	allowed, err := servicepolicy.CheckSecurablePrivilege(ctx, s.auth, principal, domain.SecurableCatalog, catalogName, domain.PrivCreateTable)
	if err != nil {
		return nil, err
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "CREATE_VIEW", fmt.Sprintf("Denied create view %q in schema %q", req.Name, schemaName))
		return nil, domain.ErrAccessDenied("%q lacks CREATE_TABLE privilege for creating views", principal)
	}

	catalogRepo, err := s.catalogFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	runtimeRepo, ok := catalogRepo.(runtimeViewCatalog)
	if !ok {
		return nil, domain.ErrNotImplemented("catalog runtime does not support managed views")
	}
	schema, err := catalogRepo.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	allowed, err = servicepolicy.HasAnySecurablePrivilege(ctx, s.auth, principal, domain.SecurableSchema, canonicalSchemaID(catalogName, schema.SchemaID), domain.PrivCreateView, domain.PrivCreateTable)
	if err != nil {
		return nil, err
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "CREATE_VIEW", fmt.Sprintf("Denied create view %q in schema %q", req.Name, schemaName))
		return nil, domain.ErrAccessDenied("%q lacks CREATE_TABLE privilege for creating views", principal)
	}
	if err := runtimeRepo.CreateView(ctx, schemaName, req.Name, req.ViewDefinition); err != nil {
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
		_ = runtimeRepo.DeleteView(ctx, schemaName, req.Name)
		return nil, err
	}

	// Enrich with schema/catalog names (not stored in DB)
	result.SchemaName = schemaName
	result.CatalogName = schema.CatalogName
	result.SchemaID = canonicalSchemaID(catalogName, result.SchemaID)

	s.logAudit(ctx, principal, "CREATE_VIEW", fmt.Sprintf("Created view %q in schema %q", req.Name, schemaName))
	return result, nil
}

// GetView returns a view by schema and name.
func (s *ViewService) GetView(ctx context.Context, catalogName string, schemaName, viewName string) (*domain.ViewDetail, error) {
	repo, err := s.catalogFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	schema, err := repo.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	result, err := s.repo.GetByName(ctx, schema.SchemaID, viewName)
	if err != nil {
		return nil, err
	}
	result.SchemaName = schemaName
	result.CatalogName = schema.CatalogName
	result.SchemaID = canonicalSchemaID(catalogName, result.SchemaID)
	columns, err := repo.DescribeViewColumns(ctx, schemaName, viewName)
	if err != nil {
		slog.Default().Warn("view column introspection failed", "catalog", catalogName, "schema", schemaName, "view", viewName, "error", err)
		return result, nil
	}
	result.Columns = columns
	return result, nil
}

// ListViews returns a paginated list of views in a schema.
func (s *ViewService) ListViews(ctx context.Context, catalogName string, schemaName string, page domain.PageRequest) ([]domain.ViewDetail, int64, error) {
	repo, err := s.catalogFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, 0, err
	}
	schema, err := repo.GetSchema(ctx, schemaName)
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
		views[i].SchemaID = canonicalSchemaID(catalogName, views[i].SchemaID)
	}
	return views, total, nil
}

// DeleteView drops a view from the given schema.
func (s *ViewService) DeleteView(ctx context.Context, catalogName string, principal string, schemaName, viewName string) error {
	if err := ensureMutableCatalog(catalogName); err != nil {
		return err
	}
	repo, err := s.catalogFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return err
	}
	runtimeRepo, ok := repo.(runtimeViewCatalog)
	if !ok {
		return domain.ErrNotImplemented("catalog runtime does not support managed views")
	}
	schema, err := repo.GetSchema(ctx, schemaName)
	if err != nil {
		return err
	}
	allowed, err := servicepolicy.CheckSecurablePrivilege(ctx, s.auth, principal, domain.SecurableSchema, canonicalSchemaID(catalogName, schema.SchemaID), domain.PrivManage)
	if err != nil {
		return err
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "DROP_VIEW", fmt.Sprintf("Denied drop view %q.%q", schemaName, viewName))
		return domain.ErrAccessDenied("%q lacks permission to delete view %q.%q", principal, schemaName, viewName)
	}

	allowed, err = servicepolicy.CheckSecurablePrivilege(ctx, s.auth, principal, domain.SecurableSchema, canonicalSchemaID(catalogName, schema.SchemaID), domain.PrivCreateTable)
	if err != nil {
		return err
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "DROP_VIEW", fmt.Sprintf("Denied delete view %q.%q", schemaName, viewName))
		return domain.ErrAccessDenied("%q lacks permission to delete view %q.%q", principal, schemaName, viewName)
	}

	if err := runtimeRepo.DeleteView(ctx, schemaName, viewName); err != nil {
		return err
	}
	if err := s.repo.Delete(ctx, schema.SchemaID, viewName); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "DROP_VIEW", fmt.Sprintf("Dropped view %q.%q", schemaName, viewName))
	return nil
}

// UpdateView updates a view's metadata.
func (s *ViewService) UpdateView(ctx context.Context, catalogName string, principal string, schemaName, viewName string, req domain.UpdateViewRequest) (*domain.ViewDetail, error) {
	if err := ensureMutableCatalog(catalogName); err != nil {
		return nil, err
	}
	repo, err := s.catalogFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	runtimeRepo, ok := repo.(runtimeViewCatalog)
	if !ok {
		return nil, domain.ErrNotImplemented("catalog runtime does not support managed views")
	}
	schema, err := repo.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	allowed, err := servicepolicy.CheckSecurablePrivilege(ctx, s.auth, principal, domain.SecurableSchema, canonicalSchemaID(catalogName, schema.SchemaID), domain.PrivModify)
	if err != nil {
		return nil, err
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "UPDATE_VIEW", fmt.Sprintf("Denied update view %q.%q", schemaName, viewName))
		return nil, domain.ErrAccessDenied("%q lacks permission to update view %q.%q", principal, schemaName, viewName)
	}

	allowed, err = servicepolicy.CheckSecurablePrivilege(ctx, s.auth, principal, domain.SecurableSchema, canonicalSchemaID(catalogName, schema.SchemaID), domain.PrivCreateTable)
	if err != nil {
		return nil, err
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "UPDATE_VIEW", fmt.Sprintf("Denied update view %q.%q", schemaName, viewName))
		return nil, domain.ErrAccessDenied("%q lacks permission to update view %q.%q", principal, schemaName, viewName)
	}

	existing, err := s.repo.GetByName(ctx, schema.SchemaID, viewName)
	if err != nil {
		return nil, err
	}
	if req.ViewDefinition != nil && strings.TrimSpace(*req.ViewDefinition) != "" {
		if err := runtimeRepo.UpdateViewDefinition(ctx, schemaName, viewName, *req.ViewDefinition); err != nil {
			return nil, err
		}
	}
	result, err := s.repo.Update(ctx, schema.SchemaID, viewName, req.Comment, req.Properties, req.ViewDefinition)
	if err != nil {
		if req.ViewDefinition != nil && strings.TrimSpace(existing.ViewDefinition) != "" {
			_ = runtimeRepo.UpdateViewDefinition(ctx, schemaName, viewName, existing.ViewDefinition)
		}
		return nil, err
	}

	result.SchemaName = schemaName
	result.CatalogName = schema.CatalogName
	result.SchemaID = canonicalSchemaID(catalogName, result.SchemaID)

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

func (s *ViewService) logAuditDenied(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "DENIED",
		OriginalSQL:   &detail,
	})
}
