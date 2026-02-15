package security

import (
	"context"

	"duck-demo/internal/domain"
	"duck-demo/internal/duckdbsql"
)

// RowFilterService provides row-level security filter operations.
type RowFilterService struct {
	repo  domain.RowFilterRepository
	audit domain.AuditRepository
}

// NewRowFilterService creates a new RowFilterService.
func NewRowFilterService(repo domain.RowFilterRepository, audit domain.AuditRepository) *RowFilterService {
	return &RowFilterService{repo: repo, audit: audit}
}

// Create validates and persists a new row filter. Requires admin privileges.
// The filter_sql expression is validated as syntactically correct SQL before persisting.
func (s *RowFilterService) Create(ctx context.Context, req domain.CreateRowFilterRequest) (*domain.RowFilter, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	// Validate filter_sql is syntactically valid SQL by parsing it through the DuckDB parser.
	// This catches malformed expressions early rather than failing at query time.
	if _, err := duckdbsql.ParseExpr(req.FilterSQL); err != nil {
		return nil, domain.ErrValidation("filter_sql is not valid SQL: %v", err)
	}
	f := &domain.RowFilter{
		TableID:     req.TableID,
		FilterSQL:   req.FilterSQL,
		Description: req.Description,
	}
	result, err := s.repo.Create(ctx, f)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "CREATE_ROW_FILTER",
		Status:        "ALLOWED",
	})
	return result, nil
}

// GetForTable returns a paginated list of row filters for a table. Requires admin privileges.
func (s *RowFilterService) GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.RowFilter, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	return s.repo.GetForTable(ctx, tableID, page)
}

// Delete removes a row filter by ID. Requires admin privileges.
func (s *RowFilterService) Delete(ctx context.Context, id string) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "DELETE_ROW_FILTER",
		Status:        "ALLOWED",
	})
	return nil
}

// Bind associates a row filter with a principal or group. Requires admin privileges.
func (s *RowFilterService) Bind(ctx context.Context, req domain.BindRowFilterRequest) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	b := &domain.RowFilterBinding{
		RowFilterID:   req.RowFilterID,
		PrincipalID:   req.PrincipalID,
		PrincipalType: req.PrincipalType,
	}
	if err := s.repo.Bind(ctx, b); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "BIND_ROW_FILTER",
		Status:        "ALLOWED",
	})
	return nil
}

// Unbind removes a row filter binding from a principal or group. Requires admin privileges.
func (s *RowFilterService) Unbind(ctx context.Context, req domain.BindRowFilterRequest) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	b := &domain.RowFilterBinding{
		RowFilterID:   req.RowFilterID,
		PrincipalID:   req.PrincipalID,
		PrincipalType: req.PrincipalType,
	}
	if err := s.repo.Unbind(ctx, b); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "UNBIND_ROW_FILTER",
		Status:        "ALLOWED",
	})
	return nil
}

// ListBindings returns all bindings for a row filter. Requires admin privileges.
func (s *RowFilterService) ListBindings(ctx context.Context, filterID string) ([]domain.RowFilterBinding, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	return s.repo.ListBindings(ctx, filterID)
}
