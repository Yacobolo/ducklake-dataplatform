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

// Grant creates a new privilege grant. Requires admin privileges.
func (s *GrantService) Grant(ctx context.Context, req domain.CreateGrantRequest) (*domain.PrivilegeGrant, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	caller := callerName(ctx)
	g := &domain.PrivilegeGrant{
		PrincipalID:   req.PrincipalID,
		PrincipalType: req.PrincipalType,
		SecurableType: req.SecurableType,
		SecurableID:   req.SecurableID,
		Privilege:     req.Privilege,
		GrantedBy:     &caller,
	}
	result, err := s.repo.Grant(ctx, g)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "GRANT",
		Status:        "ALLOWED",
	})
	return result, nil
}

// Revoke removes a privilege grant by ID. Requires admin privileges.
func (s *GrantService) Revoke(ctx context.Context, _ string, grantID string) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := s.repo.RevokeByID(ctx, grantID); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "REVOKE",
		Status:        "ALLOWED",
	})
	return nil
}

// ListAll returns all grants with pagination. Requires admin privileges.
func (s *GrantService) ListAll(ctx context.Context, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	return s.repo.ListAll(ctx, page)
}

// ListForPrincipal returns grants assigned to a specific principal. Requires admin privileges.
func (s *GrantService) ListForPrincipal(ctx context.Context, principalID string, principalType string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	return s.repo.ListForPrincipal(ctx, principalID, principalType, page)
}

// ListForSecurable returns grants on a specific securable object. Requires admin privileges.
func (s *GrantService) ListForSecurable(ctx context.Context, securableType string, securableID string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	return s.repo.ListForSecurable(ctx, securableType, securableID, page)
}
