package security

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

// Create validates and persists a new principal. Requires admin privileges.
func (s *PrincipalService) Create(ctx context.Context, req domain.CreatePrincipalRequest) (*domain.Principal, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	p := &domain.Principal{
		Name:    req.Name,
		Type:    req.Type,
		IsAdmin: req.IsAdmin,
	}
	result, err := s.repo.Create(ctx, p)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, callerName(ctx), "CREATE_PRINCIPAL")
	return result, nil
}

// GetByID returns a principal by ID.
func (s *PrincipalService) GetByID(ctx context.Context, id string) (*domain.Principal, error) {
	return s.repo.GetByID(ctx, id)
}

// GetByName returns a principal by name.
func (s *PrincipalService) GetByName(ctx context.Context, name string) (*domain.Principal, error) {
	return s.repo.GetByName(ctx, name)
}

// List returns a paginated list of principals. Requires admin privileges.
func (s *PrincipalService) List(ctx context.Context, page domain.PageRequest) ([]domain.Principal, int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, 0, err
	}
	return s.repo.List(ctx, page)
}

// Delete removes a principal by ID. Requires admin privileges.
func (s *PrincipalService) Delete(ctx context.Context, id string) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	p, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	s.logAudit(ctx, callerName(ctx), fmt.Sprintf("DELETE_PRINCIPAL(%s)", p.Name))
	return nil
}

// SetAdmin updates the admin status of a principal. Requires admin privileges.
func (s *PrincipalService) SetAdmin(ctx context.Context, id string, isAdmin bool) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	if err := s.repo.SetAdmin(ctx, id, isAdmin); err != nil {
		return err
	}
	p, _ := s.repo.GetByID(ctx, id)
	action := "SET_ADMIN"
	if !isAdmin {
		action = "UNSET_ADMIN"
	}
	if p != nil {
		s.logAudit(ctx, callerName(ctx), fmt.Sprintf("%s(%s)", action, p.Name))
	}
	return nil
}

// ResolveOrProvision resolves an existing principal by external identity,
// or creates a new one via JIT provisioning.
func (s *PrincipalService) ResolveOrProvision(ctx context.Context, req domain.ResolveOrProvisionRequest) (*domain.Principal, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	// Try to find existing principal by external ID.
	p, err := s.repo.GetByExternalID(ctx, req.Issuer, req.ExternalID)
	if err == nil {
		return p, nil
	}
	var notFound *domain.NotFoundError
	if !errors.As(err, &notFound) {
		return nil, fmt.Errorf("resolve external identity: %w", err)
	}

	// JIT provisioning: create a new principal.
	name := sanitizeName(req.DisplayName)
	if name == "" {
		name = sanitizeName(req.ExternalID)
	}

	newPrincipal := &domain.Principal{
		Name:           name,
		Type:           "user",
		IsAdmin:        req.IsBootstrap,
		ExternalID:     &req.ExternalID,
		ExternalIssuer: &req.Issuer,
	}

	result, err := s.repo.Create(ctx, newPrincipal)
	if err != nil {
		// Handle race condition: another request provisioned between our check and create.
		var conflict *domain.ConflictError
		if errors.As(err, &conflict) {
			return s.repo.GetByExternalID(ctx, req.Issuer, req.ExternalID)
		}
		return nil, fmt.Errorf("provision principal: %w", err)
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: result.Name,
		Action:        "JIT_PROVISION",
		Status:        "ALLOWED",
	})

	return result, nil
}

func sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	if len(name) > 255 {
		name = name[:255]
	}
	return name
}

func (s *PrincipalService) logAudit(ctx context.Context, principalName, action string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principalName,
		Action:        action,
		Status:        "ALLOWED",
	})
}
