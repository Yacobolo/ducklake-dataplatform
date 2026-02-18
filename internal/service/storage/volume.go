package storage

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/auditutil"
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
func (s *VolumeService) Create(ctx context.Context, principal, catalogName, schemaName string, req domain.CreateVolumeRequest) (*domain.Volume, error) {
	if err := s.requirePrivilege(ctx, principal, domain.SecurableCatalog, catalogName, domain.PrivCreateVolume, "CREATE_VOLUME", fmt.Sprintf("Denied create volume %q in schema %q", req.Name, schemaName)); err != nil {
		return nil, err
	}

	if err := domain.ValidateCreateVolumeRequest(req); err != nil {
		return nil, err
	}

	vol := &domain.Volume{
		Name:            req.Name,
		SchemaName:      schemaName,
		CatalogName:     catalogName,
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
func (s *VolumeService) GetByName(ctx context.Context, catalogName, schemaName, name string) (*domain.Volume, error) {
	_ = catalogName // volumes are stored globally; catalogName reserved for future use
	return s.repo.GetByName(ctx, schemaName, name)
}

// List returns a paginated list of volumes in a schema.
func (s *VolumeService) List(ctx context.Context, catalogName, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error) {
	_ = catalogName // volumes are stored globally; catalogName reserved for future use
	return s.repo.List(ctx, schemaName, page)
}

// Update updates a volume by schema and name.
// Requires MANAGE on volume.
func (s *VolumeService) Update(ctx context.Context, principal, catalogName, schemaName, name string, req domain.UpdateVolumeRequest) (*domain.Volume, error) {
	_ = catalogName // volumes are stored globally; catalogName reserved for future use
	existing, err := s.repo.GetByName(ctx, schemaName, name)
	if err != nil {
		return nil, err
	}

	if err := s.requirePrivilege(ctx, principal, domain.SecurableVolume, existing.ID, domain.PrivManage, "UPDATE_VOLUME", fmt.Sprintf("Denied update volume %q in schema %q", name, schemaName)); err != nil {
		if errLegacy := s.requirePrivilege(ctx, principal, domain.SecurableVolume, existing.ID, domain.PrivCreateVolume, "UPDATE_VOLUME", fmt.Sprintf("Denied update volume %q in schema %q", name, schemaName)); errLegacy != nil {
			return nil, errLegacy
		}
	}

	result, err := s.repo.Update(ctx, existing.ID, req)
	if err != nil {
		return nil, fmt.Errorf("update volume: %w", err)
	}

	s.logAudit(ctx, principal, "UPDATE_VOLUME", fmt.Sprintf("Updated volume %q in schema %q", name, schemaName))
	return result, nil
}

// Delete removes a volume by schema and name.
// Requires MANAGE on volume.
func (s *VolumeService) Delete(ctx context.Context, principal, catalogName, schemaName, name string) error {
	_ = catalogName // volumes are stored globally; catalogName reserved for future use
	existing, err := s.repo.GetByName(ctx, schemaName, name)
	if err != nil {
		return err
	}

	if err := s.requirePrivilege(ctx, principal, domain.SecurableVolume, existing.ID, domain.PrivManage, "DELETE_VOLUME", fmt.Sprintf("Denied delete volume %q from schema %q", name, schemaName)); err != nil {
		if errLegacy := s.requirePrivilege(ctx, principal, domain.SecurableVolume, existing.ID, domain.PrivCreateVolume, "DELETE_VOLUME", fmt.Sprintf("Denied delete volume %q from schema %q", name, schemaName)); errLegacy != nil {
			return errLegacy
		}
	}

	if err := s.repo.Delete(ctx, existing.ID); err != nil {
		return fmt.Errorf("delete volume: %w", err)
	}

	s.logAudit(ctx, principal, "DELETE_VOLUME", fmt.Sprintf("Deleted volume %q from schema %q", name, schemaName))
	return nil
}

// requirePrivilege checks that the principal has the given privilege on a securable.
func (s *VolumeService) requirePrivilege(ctx context.Context, principal, securableType, securableID, privilege, action, detail string) error {
	allowed, err := s.auth.CheckPrivilege(ctx, principal, securableType, securableID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, action, detail)
		return domain.ErrAccessDenied("%q lacks %s on %s %q", principal, privilege, securableType, securableID)
	}
	return nil
}

func (s *VolumeService) logAudit(ctx context.Context, principal, action, detail string) {
	auditutil.LogAllowed(ctx, s.audit, principal, action, detail)
}

func (s *VolumeService) logAuditDenied(ctx context.Context, principal, action, detail string) {
	auditutil.LogDenied(ctx, s.audit, principal, action, detail)
}
