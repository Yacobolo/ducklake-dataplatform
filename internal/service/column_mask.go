package service

import (
	"context"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

type ColumnMaskService struct {
	repo  domain.ColumnMaskRepository
	audit domain.AuditRepository
}

func NewColumnMaskService(repo domain.ColumnMaskRepository, audit domain.AuditRepository) *ColumnMaskService {
	return &ColumnMaskService{repo: repo, audit: audit}
}

func (s *ColumnMaskService) Create(ctx context.Context, m *domain.ColumnMask) (*domain.ColumnMask, error) {
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
	principal, _ := middleware.PrincipalFromContext(ctx)
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "CREATE_COLUMN_MASK",
		Status:        "ALLOWED",
	})
	return result, nil
}

func (s *ColumnMaskService) GetForTable(ctx context.Context, tableID int64, page domain.PageRequest) ([]domain.ColumnMask, int64, error) {
	return s.repo.GetForTable(ctx, tableID, page)
}

func (s *ColumnMaskService) Delete(ctx context.Context, id int64) error {
	return s.repo.Delete(ctx, id)
}

func (s *ColumnMaskService) Bind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	return s.repo.Bind(ctx, b)
}

func (s *ColumnMaskService) Unbind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	return s.repo.Unbind(ctx, b)
}

func (s *ColumnMaskService) ListBindings(ctx context.Context, maskID int64) ([]domain.ColumnMaskBinding, error) {
	return s.repo.ListBindings(ctx, maskID)
}
