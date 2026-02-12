package security

import (
	"context"

	"duck-demo/internal/domain"
)

// GrantService provides privilege grant and revoke operations.
type GrantService struct {
	repo  domain.GrantRepository
	audit domain.AuditRepository
}

// NewGrantService creates a new GrantService.
func NewGrantService(repo domain.GrantRepository, audit domain.AuditRepository) *GrantService {
	return &GrantService{repo: repo, audit: audit}
}

// Grant creates a new privilege grant.
func (s *GrantService) Grant(ctx context.Context, principal string, g *domain.PrivilegeGrant) (*domain.PrivilegeGrant, error) {
	if g.Privilege == "" {
		return nil, domain.ErrValidation("privilege is required")
	}
	result, err := s.repo.Grant(ctx, g)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "GRANT",
		Status:        "ALLOWED",
	})
	return result, nil
}

// Revoke removes a privilege grant.
func (s *GrantService) Revoke(ctx context.Context, principal string, g *domain.PrivilegeGrant) error {
	if err := s.repo.Revoke(ctx, g); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "REVOKE",
		Status:        "ALLOWED",
	})
	return nil
}

// ListForPrincipal returns grants assigned to a specific principal.
func (s *GrantService) ListForPrincipal(ctx context.Context, principalID int64, principalType string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	return s.repo.ListForPrincipal(ctx, principalID, principalType, page)
}

// ListForSecurable returns grants on a specific securable object.
func (s *GrantService) ListForSecurable(ctx context.Context, securableType string, securableID int64, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	return s.repo.ListForSecurable(ctx, securableType, securableID, page)
}
