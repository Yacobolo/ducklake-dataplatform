package service

import (
	"context"

	"duck-demo/internal/domain"
)

// ColumnMaskService provides column masking operations.
type ColumnMaskService struct {
	repo  domain.ColumnMaskRepository
	audit domain.AuditRepository
}

// NewColumnMaskService creates a new ColumnMaskService.
func NewColumnMaskService(repo domain.ColumnMaskRepository, audit domain.AuditRepository) *ColumnMaskService {
	return &ColumnMaskService{repo: repo, audit: audit}
}

// Create validates and persists a new column mask. Requires admin privileges.
func (s *ColumnMaskService) Create(ctx context.Context, _ string, m *domain.ColumnMask) (*domain.ColumnMask, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if m.ColumnName == "" {
		return nil, domain.ErrValidation("column_name is required")
	}
	if m.MaskExpression == "" {
		return nil, domain.ErrValidation("mask_expression is required")
	}
	result, err := s.repo.Create(ctx, m)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "CREATE_COLUMN_MASK",
		Status:        "ALLOWED",
	})
	return result, nil
}

// GetForTable returns a paginated list of column masks for a table.
func (s *ColumnMaskService) GetForTable(ctx context.Context, tableID int64, page domain.PageRequest) ([]domain.ColumnMask, int64, error) {
	return s.repo.GetForTable(ctx, tableID, page)
}

// Delete removes a column mask by ID. Requires admin privileges.
func (s *ColumnMaskService) Delete(ctx context.Context, id int64) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.Delete(ctx, id)
}

// Bind associates a column mask with a principal or group. Requires admin privileges.
func (s *ColumnMaskService) Bind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.Bind(ctx, b)
}

// Unbind removes a column mask binding from a principal or group. Requires admin privileges.
func (s *ColumnMaskService) Unbind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.Unbind(ctx, b)
}

// ListBindings returns all bindings for a column mask.
func (s *ColumnMaskService) ListBindings(ctx context.Context, maskID int64) ([]domain.ColumnMaskBinding, error) {
	return s.repo.ListBindings(ctx, maskID)
}
