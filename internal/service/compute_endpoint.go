package service

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

// ComputeEndpointService provides CRUD operations for compute endpoints
// and assignments with RBAC enforcement and audit logging.
type ComputeEndpointService struct {
	repo  domain.ComputeEndpointRepository
	auth  domain.AuthorizationService
	audit domain.AuditRepository
}

// NewComputeEndpointService creates a new ComputeEndpointService.
func NewComputeEndpointService(
	repo domain.ComputeEndpointRepository,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
) *ComputeEndpointService {
	return &ComputeEndpointService{
		repo:  repo,
		auth:  auth,
		audit: audit,
	}
}

// Create validates and persists a new compute endpoint.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) Create(ctx context.Context, principal string, req domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute); err != nil {
		return nil, err
	}

	if err := domain.ValidateCreateComputeEndpointRequest(req); err != nil {
		return nil, err
	}

	ep := &domain.ComputeEndpoint{
		Name:        req.Name,
		URL:         req.URL,
		Type:        req.Type,
		Size:        req.Size,
		MaxMemoryGB: req.MaxMemoryGB,
		AuthToken:   req.AuthToken,
		Owner:       principal,
	}

	result, err := s.repo.Create(ctx, ep)
	if err != nil {
		return nil, fmt.Errorf("create compute endpoint: %w", err)
	}

	s.logAudit(ctx, principal, "CREATE_COMPUTE_ENDPOINT", fmt.Sprintf("Created compute endpoint %q", req.Name))
	return result, nil
}

// GetByName returns a compute endpoint by name.
func (s *ComputeEndpointService) GetByName(ctx context.Context, name string) (*domain.ComputeEndpoint, error) {
	return s.repo.GetByName(ctx, name)
}

// List returns a paginated list of compute endpoints.
func (s *ComputeEndpointService) List(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
	return s.repo.List(ctx, page)
}

// Update updates a compute endpoint by name.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) Update(ctx context.Context, principal string, name string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute); err != nil {
		return nil, err
	}

	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}

	result, err := s.repo.Update(ctx, existing.ID, req)
	if err != nil {
		return nil, fmt.Errorf("update compute endpoint: %w", err)
	}

	s.logAudit(ctx, principal, "UPDATE_COMPUTE_ENDPOINT", fmt.Sprintf("Updated compute endpoint %q", name))
	return result, nil
}

// Delete removes a compute endpoint by name.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) Delete(ctx context.Context, principal string, name string) error {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute); err != nil {
		return err
	}

	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return err
	}

	if err := s.repo.Delete(ctx, existing.ID); err != nil {
		return fmt.Errorf("delete compute endpoint: %w", err)
	}

	s.logAudit(ctx, principal, "DELETE_COMPUTE_ENDPOINT", fmt.Sprintf("Deleted compute endpoint %q", name))
	return nil
}

// UpdateStatus changes the status of a compute endpoint.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) UpdateStatus(ctx context.Context, principal string, name string, status string) error {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute); err != nil {
		return err
	}

	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return err
	}

	// Validate status
	switch status {
	case "ACTIVE", "INACTIVE", "STARTING", "STOPPING", "ERROR":
		// valid
	default:
		return domain.ErrValidation("invalid status %q", status)
	}

	if err := s.repo.UpdateStatus(ctx, existing.ID, status); err != nil {
		return fmt.Errorf("update compute endpoint status: %w", err)
	}

	s.logAudit(ctx, principal, "UPDATE_COMPUTE_ENDPOINT_STATUS",
		fmt.Sprintf("Updated compute endpoint %q status to %s", name, status))
	return nil
}

// Assign creates a compute assignment.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) Assign(ctx context.Context, principal string, endpointName string, req domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute); err != nil {
		return nil, err
	}

	if err := domain.ValidateCreateComputeAssignmentRequest(req); err != nil {
		return nil, err
	}

	ep, err := s.repo.GetByName(ctx, endpointName)
	if err != nil {
		return nil, err
	}

	a := &domain.ComputeAssignment{
		PrincipalID:   req.PrincipalID,
		PrincipalType: req.PrincipalType,
		EndpointID:    ep.ID,
		IsDefault:     req.IsDefault,
		FallbackLocal: req.FallbackLocal,
	}

	result, err := s.repo.Assign(ctx, a)
	if err != nil {
		return nil, fmt.Errorf("assign compute endpoint: %w", err)
	}

	s.logAudit(ctx, principal, "ASSIGN_COMPUTE_ENDPOINT",
		fmt.Sprintf("Assigned principal %d (%s) to compute endpoint %q", req.PrincipalID, req.PrincipalType, endpointName))
	return result, nil
}

// Unassign removes a compute assignment.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) Unassign(ctx context.Context, principal string, assignmentID int64) error {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute); err != nil {
		return err
	}

	if err := s.repo.Unassign(ctx, assignmentID); err != nil {
		return fmt.Errorf("unassign compute endpoint: %w", err)
	}

	s.logAudit(ctx, principal, "UNASSIGN_COMPUTE_ENDPOINT",
		fmt.Sprintf("Removed compute assignment %d", assignmentID))
	return nil
}

// ListAssignments returns assignments for a compute endpoint.
func (s *ComputeEndpointService) ListAssignments(ctx context.Context, endpointName string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
	ep, err := s.repo.GetByName(ctx, endpointName)
	if err != nil {
		return nil, 0, err
	}
	return s.repo.ListAssignments(ctx, ep.ID, page)
}

// requirePrivilege checks that the principal has the given privilege on the catalog.
func (s *ComputeEndpointService) requirePrivilege(ctx context.Context, principal string, privilege string) error {
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on catalog", principal, privilege)
	}
	return nil
}

func (s *ComputeEndpointService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}
