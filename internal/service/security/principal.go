package security

import (
	"context"

	"duck-demo/internal/domain"
)

// PrincipalService provides principal management operations.
type PrincipalService struct {
	repo  domain.PrincipalRepository
	audit domain.AuditRepository
}

// NewPrincipalService creates a new PrincipalService.
func NewPrincipalService(repo domain.PrincipalRepository, audit domain.AuditRepository) *PrincipalService {
	return &PrincipalService{repo: repo, audit: audit}
}

// Create validates and persists a new principal.
func (s *PrincipalService) Create(ctx context.Context, p *domain.Principal) (*domain.Principal, error) {
	if p.Name == "" {
		return nil, domain.ErrValidation("principal name is required")
	}
	if p.Type == "" {
		p.Type = "user"
	}
	if p.Type != "user" && p.Type != "service_principal" {
		return nil, domain.ErrValidation("type must be 'user' or 'service_principal'")
	}
	result, err := s.repo.Create(ctx, p)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, p.Name, "CREATE_PRINCIPAL")
	return result, nil
}

// GetByID returns a principal by ID.
func (s *PrincipalService) GetByID(ctx context.Context, id int64) (*domain.Principal, error) {
	return s.repo.GetByID(ctx, id)
}

// List returns a paginated list of principals.
func (s *PrincipalService) List(ctx context.Context, page domain.PageRequest) ([]domain.Principal, int64, error) {
	return s.repo.List(ctx, page)
}

// Delete removes a principal by ID.
func (s *PrincipalService) Delete(ctx context.Context, id int64) error {
	p, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	s.logAudit(ctx, p.Name, "DELETE_PRINCIPAL")
	return nil
}

// SetAdmin updates the admin status of a principal.
func (s *PrincipalService) SetAdmin(ctx context.Context, id int64, isAdmin bool) error {
	if err := s.repo.SetAdmin(ctx, id, isAdmin); err != nil {
		return err
	}
	p, _ := s.repo.GetByID(ctx, id)
	action := "SET_ADMIN"
	if !isAdmin {
		action = "UNSET_ADMIN"
	}
	if p != nil {
		s.logAudit(ctx, p.Name, action)
	}
	return nil
}

func (s *PrincipalService) logAudit(ctx context.Context, principalName, action string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principalName,
		Action:        action,
		Status:        "ALLOWED",
	})
}
