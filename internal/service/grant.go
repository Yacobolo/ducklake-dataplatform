package service

import (
	"context"

	"duck-demo/internal/domain"
)

type GrantService struct {
	repo  domain.GrantRepository
	audit domain.AuditRepository
}

func NewGrantService(repo domain.GrantRepository, audit domain.AuditRepository) *GrantService {
	return &GrantService{repo: repo, audit: audit}
}

func (s *GrantService) Grant(ctx context.Context, g *domain.PrivilegeGrant) (*domain.PrivilegeGrant, error) {
	if g.Privilege == "" {
		return nil, domain.ErrValidation("privilege is required")
	}
	result, err := s.repo.Grant(ctx, g)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: "system",
		Action:        "GRANT",
		Status:        "ALLOWED",
	})
	return result, nil
}

func (s *GrantService) Revoke(ctx context.Context, g *domain.PrivilegeGrant) error {
	if err := s.repo.Revoke(ctx, g); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: "system",
		Action:        "REVOKE",
		Status:        "ALLOWED",
	})
	return nil
}

func (s *GrantService) ListForPrincipal(ctx context.Context, principalID int64, principalType string) ([]domain.PrivilegeGrant, error) {
	return s.repo.ListForPrincipal(ctx, principalID, principalType)
}

func (s *GrantService) ListForSecurable(ctx context.Context, securableType string, securableID int64) ([]domain.PrivilegeGrant, error) {
	return s.repo.ListForSecurable(ctx, securableType, securableID)
}
