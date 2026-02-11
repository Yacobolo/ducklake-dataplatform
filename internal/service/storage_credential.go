package service

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

// StorageCredentialService provides CRUD operations for storage credentials
// with RBAC enforcement and audit logging.
type StorageCredentialService struct {
	repo  domain.StorageCredentialRepository
	auth  domain.AuthorizationService
	audit domain.AuditRepository
}

// NewStorageCredentialService creates a new StorageCredentialService.
func NewStorageCredentialService(
	repo domain.StorageCredentialRepository,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
) *StorageCredentialService {
	return &StorageCredentialService{
		repo:  repo,
		auth:  auth,
		audit: audit,
	}
}

// Create validates and persists a new storage credential.
// Requires CREATE_STORAGE_CREDENTIAL on catalog.
func (s *StorageCredentialService) Create(ctx context.Context, principal string, req domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateStorageCredential); err != nil {
		return nil, err
	}

	if err := domain.ValidateStorageCredentialRequest(req); err != nil {
		return nil, err
	}

	cred := &domain.StorageCredential{
		Name:           req.Name,
		CredentialType: req.CredentialType,
		KeyID:          req.KeyID,
		Secret:         req.Secret,
		Endpoint:       req.Endpoint,
		Region:         req.Region,
		URLStyle:       req.URLStyle,
		Comment:        req.Comment,
		Owner:          principal,
	}

	result, err := s.repo.Create(ctx, cred)
	if err != nil {
		return nil, fmt.Errorf("create storage credential: %w", err)
	}

	s.logAudit(ctx, principal, "CREATE_STORAGE_CREDENTIAL", fmt.Sprintf("Created credential %q", req.Name))
	return result, nil
}

// GetByName returns a storage credential by name.
func (s *StorageCredentialService) GetByName(ctx context.Context, name string) (*domain.StorageCredential, error) {
	return s.repo.GetByName(ctx, name)
}

// List returns a paginated list of storage credentials.
func (s *StorageCredentialService) List(ctx context.Context, page domain.PageRequest) ([]domain.StorageCredential, int64, error) {
	return s.repo.List(ctx, page)
}

// Update updates a storage credential by name.
// Requires CREATE_STORAGE_CREDENTIAL on catalog.
func (s *StorageCredentialService) Update(ctx context.Context, principal string, name string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateStorageCredential); err != nil {
		return nil, err
	}

	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}

	result, err := s.repo.Update(ctx, existing.ID, req)
	if err != nil {
		return nil, fmt.Errorf("update storage credential: %w", err)
	}

	s.logAudit(ctx, principal, "UPDATE_STORAGE_CREDENTIAL", fmt.Sprintf("Updated credential %q", name))
	return result, nil
}

// Delete removes a storage credential by name.
// Requires CREATE_STORAGE_CREDENTIAL on catalog.
func (s *StorageCredentialService) Delete(ctx context.Context, principal string, name string) error {
	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateStorageCredential); err != nil {
		return err
	}

	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return err
	}

	if err := s.repo.Delete(ctx, existing.ID); err != nil {
		return fmt.Errorf("delete storage credential: %w", err)
	}

	s.logAudit(ctx, principal, "DELETE_STORAGE_CREDENTIAL", fmt.Sprintf("Deleted credential %q", name))
	return nil
}

// requirePrivilege checks that the principal has the given privilege on the catalog.
func (s *StorageCredentialService) requirePrivilege(ctx context.Context, principal string, privilege string) error {
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on catalog", principal, privilege)
	}
	return nil
}

func (s *StorageCredentialService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}
