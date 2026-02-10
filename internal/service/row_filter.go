package service

import (
	"context"

	"duck-demo/internal/domain"
)

type RowFilterService struct {
	repo  domain.RowFilterRepository
	audit domain.AuditRepository
}

func NewRowFilterService(repo domain.RowFilterRepository, audit domain.AuditRepository) *RowFilterService {
	return &RowFilterService{repo: repo, audit: audit}
}

func (s *RowFilterService) Create(ctx context.Context, f *domain.RowFilter) (*domain.RowFilter, error) {
	if f.FilterSQL == "" {
		return nil, domain.ErrValidation("filter_sql is required")
	}
	result, err := s.repo.Create(ctx, f)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: "system",
		Action:        "CREATE_ROW_FILTER",
		Status:        "ALLOWED",
	})
	return result, nil
}

func (s *RowFilterService) GetForTable(ctx context.Context, tableID int64) ([]domain.RowFilter, error) {
	return s.repo.GetForTable(ctx, tableID)
}

func (s *RowFilterService) Delete(ctx context.Context, id int64) error {
	return s.repo.Delete(ctx, id)
}

func (s *RowFilterService) Bind(ctx context.Context, b *domain.RowFilterBinding) error {
	return s.repo.Bind(ctx, b)
}

func (s *RowFilterService) Unbind(ctx context.Context, b *domain.RowFilterBinding) error {
	return s.repo.Unbind(ctx, b)
}

func (s *RowFilterService) ListBindings(ctx context.Context, filterID int64) ([]domain.RowFilterBinding, error) {
	return s.repo.ListBindings(ctx, filterID)
}
