package security

import (
	"context"

	"duck-demo/internal/domain"
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
func (s *RowFilterService) Create(ctx context.Context, req domain.CreateRowFilterRequest) (*domain.RowFilter, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
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

// GetForTable returns a paginated list of row filters for a table.
func (s *RowFilterService) GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.RowFilter, int64, error) {
	return s.repo.GetForTable(ctx, tableID, page)
}

// Delete removes a row filter by ID. Requires admin privileges.
func (s *RowFilterService) Delete(ctx context.Context, id string) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.Delete(ctx, id)
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
	return s.repo.Bind(ctx, b)
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
	return s.repo.Unbind(ctx, b)
}

// ListBindings returns all bindings for a row filter.
func (s *RowFilterService) ListBindings(ctx context.Context, filterID string) ([]domain.RowFilterBinding, error) {
	return s.repo.ListBindings(ctx, filterID)
}
