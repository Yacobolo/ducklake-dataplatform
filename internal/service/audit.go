// Package service implements business logic for the data platform.
package service

import (
	"context"

	"duck-demo/internal/domain"
)

// AuditService provides audit log operations.
type AuditService struct {
	repo domain.AuditRepository
}

// NewAuditService creates a new AuditService.
func NewAuditService(repo domain.AuditRepository) *AuditService {
	return &AuditService{repo: repo}
}

// List returns a filtered, paginated list of audit log entries.
func (s *AuditService) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return s.repo.List(ctx, filter)
}
