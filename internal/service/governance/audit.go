// Package governance implements governance and audit services.
package governance

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

// List returns a filtered, paginated list of audit log entries. Requires admin privileges.
func (s *AuditService) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	return s.repo.List(ctx, filter)
}
