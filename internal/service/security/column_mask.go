package security

import (
	"context"

	"duck-demo/internal/domain"
	"duck-demo/internal/duckdbsql"
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
func (s *ColumnMaskService) Create(ctx context.Context, req domain.CreateColumnMaskRequest) (*domain.ColumnMask, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if _, err := duckdbsql.ParseExpr(req.MaskExpression); err != nil {
		return nil, domain.ErrValidation("mask_expression must be a valid SQL expression: %v", err)
	}
	m := &domain.ColumnMask{
		TableID:        req.TableID,
		ColumnName:     req.ColumnName,
		MaskExpression: req.MaskExpression,
		Description:    req.Description,
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

// GetForTable returns a paginated list of column masks for a table. Requires admin privileges.
func (s *ColumnMaskService) GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	return s.repo.GetForTable(ctx, tableID, page)
}

// Delete removes a column mask by ID. Requires admin privileges.
func (s *ColumnMaskService) Delete(ctx context.Context, id string) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.Delete(ctx, id)
}

// Bind associates a column mask with a principal or group. Requires admin privileges.
func (s *ColumnMaskService) Bind(ctx context.Context, req domain.BindColumnMaskRequest) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	b := &domain.ColumnMaskBinding{
		ColumnMaskID:  req.ColumnMaskID,
		PrincipalID:   req.PrincipalID,
		PrincipalType: req.PrincipalType,
		SeeOriginal:   req.SeeOriginal,
	}
	return s.repo.Bind(ctx, b)
}

// Unbind removes a column mask binding from a principal or group. Requires admin privileges.
func (s *ColumnMaskService) Unbind(ctx context.Context, req domain.BindColumnMaskRequest) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	b := &domain.ColumnMaskBinding{
		ColumnMaskID:  req.ColumnMaskID,
		PrincipalID:   req.PrincipalID,
		PrincipalType: req.PrincipalType,
	}
	return s.repo.Unbind(ctx, b)
}

// ListBindings returns all bindings for a column mask. Requires admin privileges.
func (s *ColumnMaskService) ListBindings(ctx context.Context, maskID string) ([]domain.ColumnMaskBinding, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	return s.repo.ListBindings(ctx, maskID)
}
