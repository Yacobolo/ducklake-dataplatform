package service

import (
	"context"

	"duck-demo/internal/domain"
)

type AuditService struct {
	repo domain.AuditRepository
}

func NewAuditService(repo domain.AuditRepository) *AuditService {
	return &AuditService{repo: repo}
}

func (s *AuditService) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return s.repo.List(ctx, filter)
}
