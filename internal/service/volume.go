package service

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// VolumeService provides CRUD operations for volumes
// with RBAC enforcement and audit logging.
type VolumeService struct {
	repo  domain.VolumeRepository
	auth  domain.AuthorizationService
	audit domain.AuditRepository
}

// NewVolumeService creates a new VolumeService.
func NewVolumeService(
	repo domain.VolumeRepository,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
) *VolumeService {
	return &VolumeService{
		repo:  repo,
		auth:  auth,
		audit: audit,
	}
}

// Create validates and persists a new volume.
// Requires CREATE_VOLUME on catalog.
func (s *VolumeService) Create(ctx context.Context, schemaName string, req domain.CreateVolumeRequest) (*domain.Volume, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateVolume); err != nil {
		return nil, err
	}

	if err := domain.ValidateCreateVolumeRequest(req); err != nil {
		return nil, err
	}

	vol := &domain.Volume{
		Name:            req.Name,
		SchemaName:      schemaName,
		CatalogName:     "lake",
		VolumeType:      req.VolumeType,
		StorageLocation: req.StorageLocation,
		Comment:         req.Comment,
		Owner:           principal,
	}

	result, err := s.repo.Create(ctx, vol)
	if err != nil {
		return nil, fmt.Errorf("create volume: %w", err)
	}

	s.logAudit(ctx, principal, "CREATE_VOLUME", fmt.Sprintf("Created volume %q in schema %q", req.Name, schemaName))
	return result, nil
}

// GetByName returns a volume by schema and name.
func (s *VolumeService) GetByName(ctx context.Context, schemaName, name string) (*domain.Volume, error) {
	return s.repo.GetByName(ctx, schemaName, name)
}

// List returns a paginated list of volumes in a schema.
func (s *VolumeService) List(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error) {
	return s.repo.List(ctx, schemaName, page)
}

// Update updates a volume by schema and name.
// Requires CREATE_VOLUME on catalog.
func (s *VolumeService) Update(ctx context.Context, schemaName, name string, req domain.UpdateVolumeRequest) (*domain.Volume, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateVolume); err != nil {
		return nil, err
	}

	existing, err := s.repo.GetByName(ctx, schemaName, name)
	if err != nil {
		return nil, err
	}

	result, err := s.repo.Update(ctx, existing.ID, req)
	if err != nil {
		return nil, fmt.Errorf("update volume: %w", err)
	}

	s.logAudit(ctx, principal, "UPDATE_VOLUME", fmt.Sprintf("Updated volume %q in schema %q", name, schemaName))
	return result, nil
}

// Delete removes a volume by schema and name.
// Requires CREATE_VOLUME on catalog.
func (s *VolumeService) Delete(ctx context.Context, schemaName, name string) error {
	principal, _ := middleware.PrincipalFromContext(ctx)

	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateVolume); err != nil {
		return err
	}

	existing, err := s.repo.GetByName(ctx, schemaName, name)
	if err != nil {
		return err
	}

	if err := s.repo.Delete(ctx, existing.ID); err != nil {
		return fmt.Errorf("delete volume: %w", err)
	}

	s.logAudit(ctx, principal, "DELETE_VOLUME", fmt.Sprintf("Deleted volume %q from schema %q", name, schemaName))
	return nil
}

// requirePrivilege checks that the principal has the given privilege on the catalog.
func (s *VolumeService) requirePrivilege(ctx context.Context, principal string, privilege string) error {
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on catalog", principal, privilege)
	}
	return nil
}

func (s *VolumeService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}
