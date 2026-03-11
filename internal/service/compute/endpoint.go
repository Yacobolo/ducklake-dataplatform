// Package compute implements compute endpoint management services.
package compute

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"

	workercompute "duck-demo/internal/compute"
	computeproto "duck-demo/internal/compute/proto"
	"duck-demo/internal/domain"
	"duck-demo/internal/service/auditutil"
	servicepolicy "duck-demo/internal/service/policy"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ComputeEndpointService provides CRUD operations for compute endpoints
// and assignments with RBAC enforcement and audit logging.
//
//nolint:revive // Name chosen for clarity across package boundaries
type ComputeEndpointService struct {
	repo        domain.ComputeEndpointRepository
	routingRepo domain.ComputeRoutingRepository
	principals  domain.PrincipalRepository
	groups      domain.GroupRepository
	auth        domain.AuthorizationService
	audit       domain.AuditRepository
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

// SetRoutingRepository configures compute routing defaults storage.
func (s *ComputeEndpointService) SetRoutingRepository(repo domain.ComputeRoutingRepository) {
	s.routingRepo = repo
}

// SetPrincipalRepository configures principal lookup for principal-facing compute APIs.
func (s *ComputeEndpointService) SetPrincipalRepository(repo domain.PrincipalRepository) {
	s.principals = repo
}

// SetGroupRepository configures group lookup for principal-facing compute APIs.
func (s *ComputeEndpointService) SetGroupRepository(repo domain.GroupRepository) {
	s.groups = repo
}

// Create validates and persists a new compute endpoint.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) Create(ctx context.Context, principal string, req domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "CREATE_COMPUTE_ENDPOINT", fmt.Sprintf("Denied create compute endpoint %q", req.Name)); err != nil {
		return nil, err
	}

	if err := domain.ValidateCreateComputeEndpointRequest(req); err != nil {
		return nil, err
	}

	ep := &domain.ComputeEndpoint{
		Name:                       req.Name,
		URL:                        req.URL,
		Type:                       req.Type,
		SelectionPolicy:            normalizeSelectionPolicy(req.SelectionPolicy),
		WorkloadClass:              normalizeEndpointWorkloadClass(req.WorkloadClass),
		ReadinessStatus:            normalizeReadinessStatus(req.ReadinessStatus),
		Size:                       req.Size,
		MaxMemoryGB:                req.MaxMemoryGB,
		MaxConcurrency:             req.MaxConcurrency,
		MaxResultSizeMB:            req.MaxResultSizeMB,
		RecommendedForLargeQueries: req.RecommendedForLargeQueries,
		IsDraining:                 req.IsDraining,
		AuthToken:                  req.AuthToken,
		Owner:                      principal,
	}

	result, err := s.repo.Create(ctx, ep)
	if err != nil {
		return nil, fmt.Errorf("create compute endpoint: %w", err)
	}

	s.logAudit(ctx, principal, "CREATE_COMPUTE_ENDPOINT", fmt.Sprintf("Created compute endpoint %q", req.Name))
	return result, nil
}

// GetByName returns a compute endpoint by name.
func (s *ComputeEndpointService) GetByName(ctx context.Context, principal, name string) (*domain.ComputeEndpoint, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "GET_COMPUTE_ENDPOINT", fmt.Sprintf("Denied get compute endpoint %q", name)); err != nil {
		return nil, err
	}

	ep, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := s.requireEndpointPrivilege(ctx, principal, ep.ID, domain.PrivManageCompute); err != nil {
		s.logAuditDenied(ctx, principal, "GET_COMPUTE_ENDPOINT", fmt.Sprintf("Denied get compute endpoint %q", name))
		return nil, err
	}
	return ep, nil
}

// List returns a paginated list of compute endpoints.
func (s *ComputeEndpointService) List(ctx context.Context, principal string, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "LIST_COMPUTE_ENDPOINTS", "Denied list compute endpoints"); err != nil {
		return nil, 0, err
	}
	return s.repo.List(ctx, page)
}

// Update updates a compute endpoint by name.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) Update(ctx context.Context, principal string, name string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "UPDATE_COMPUTE_ENDPOINT", fmt.Sprintf("Denied update compute endpoint %q", name)); err != nil {
		return nil, err
	}

	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}

	if err := s.requireEndpointPrivilege(ctx, principal, existing.ID, domain.PrivManageCompute); err != nil {
		s.logAuditDenied(ctx, principal, "UPDATE_COMPUTE_ENDPOINT", fmt.Sprintf("Denied update compute endpoint %q", name))
		return nil, err
	}
	if req.URL != nil {
		if err := domain.ValidateComputeEndpointURL(*req.URL, existing.Type); err != nil {
			return nil, err
		}
	}
	if req.SelectionPolicy != nil {
		if err := validateSelectionPolicy(*req.SelectionPolicy); err != nil {
			return nil, err
		}
	}
	if req.WorkloadClass != nil {
		if err := validateEndpointWorkloadClass(*req.WorkloadClass); err != nil {
			return nil, err
		}
	}
	if req.ReadinessStatus != nil {
		if err := validateReadinessStatus(*req.ReadinessStatus); err != nil {
			return nil, err
		}
	}

	// Handle status update if provided
	if req.Status != nil {
		switch *req.Status {
		case "ACTIVE", "INACTIVE", "STARTING", "STOPPING", "ERROR":
			// valid
		default:
			return nil, domain.ErrValidation("invalid status %q", *req.Status)
		}
		if err := s.repo.UpdateStatus(ctx, existing.ID, *req.Status); err != nil {
			return nil, fmt.Errorf("update compute endpoint status: %w", err)
		}
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
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "DELETE_COMPUTE_ENDPOINT", fmt.Sprintf("Denied delete compute endpoint %q", name)); err != nil {
		return err
	}

	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return err
	}

	if err := s.requireEndpointPrivilege(ctx, principal, existing.ID, domain.PrivManageCompute); err != nil {
		s.logAuditDenied(ctx, principal, "DELETE_COMPUTE_ENDPOINT", fmt.Sprintf("Denied delete compute endpoint %q", name))
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
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "UPDATE_COMPUTE_ENDPOINT_STATUS", fmt.Sprintf("Denied update compute endpoint %q status", name)); err != nil {
		return err
	}

	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return err
	}

	if err := s.requireEndpointPrivilege(ctx, principal, existing.ID, domain.PrivManageCompute); err != nil {
		s.logAuditDenied(ctx, principal, "UPDATE_COMPUTE_ENDPOINT_STATUS", fmt.Sprintf("Denied update compute endpoint %q status", name))
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
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "ASSIGN_COMPUTE_ENDPOINT", fmt.Sprintf("Denied assign compute endpoint %q", endpointName)); err != nil {
		return nil, err
	}

	if err := domain.ValidateCreateComputeAssignmentRequest(req); err != nil {
		return nil, err
	}

	ep, err := s.repo.GetByName(ctx, endpointName)
	if err != nil {
		return nil, err
	}

	if err := s.requireEndpointPrivilege(ctx, principal, ep.ID, domain.PrivManageCompute); err != nil {
		s.logAuditDenied(ctx, principal, "ASSIGN_COMPUTE_ENDPOINT", fmt.Sprintf("Denied assign for endpoint %q", endpointName))
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
		fmt.Sprintf("Assigned principal %s (%s) to compute endpoint %q", req.PrincipalID, req.PrincipalType, endpointName))
	return result, nil
}

// Unassign removes a compute assignment.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) Unassign(ctx context.Context, principal string, assignmentID string) error {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "UNASSIGN_COMPUTE_ENDPOINT", fmt.Sprintf("Denied unassign compute assignment %q", assignmentID)); err != nil {
		return err
	}

	if err := s.repo.Unassign(ctx, assignmentID); err != nil {
		return fmt.Errorf("unassign compute endpoint: %w", err)
	}

	s.logAudit(ctx, principal, "UNASSIGN_COMPUTE_ENDPOINT",
		fmt.Sprintf("Removed compute assignment %s", assignmentID))
	return nil
}

// ListAssignments returns assignments for a compute endpoint.
func (s *ComputeEndpointService) ListAssignments(ctx context.Context, principal, endpointName string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "LIST_COMPUTE_ASSIGNMENTS", fmt.Sprintf("Denied list assignments for endpoint %q", endpointName)); err != nil {
		return nil, 0, err
	}

	ep, err := s.repo.GetByName(ctx, endpointName)
	if err != nil {
		return nil, 0, err
	}
	if err := s.requireEndpointPrivilege(ctx, principal, ep.ID, domain.PrivManageCompute); err != nil {
		s.logAuditDenied(ctx, principal, "LIST_COMPUTE_ASSIGNMENTS", fmt.Sprintf("Denied list assignments for endpoint %q", endpointName))
		return nil, 0, err
	}
	return s.repo.ListAssignments(ctx, ep.ID, page)
}

// HealthCheck proxies a health check to the remote compute agent.
// Requires MANAGE_COMPUTE on catalog.
func (s *ComputeEndpointService) HealthCheck(ctx context.Context, principal string, endpointName string) (*domain.ComputeEndpointHealthResult, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "HEALTH_CHECK_COMPUTE_ENDPOINT", fmt.Sprintf("Denied health check compute endpoint %q", endpointName)); err != nil {
		return nil, err
	}

	ep, err := s.repo.GetByName(ctx, endpointName)
	if err != nil {
		return nil, err
	}

	if err := s.requireEndpointPrivilege(ctx, principal, ep.ID, domain.PrivManageCompute); err != nil {
		s.logAuditDenied(ctx, principal, "GET_COMPUTE_ENDPOINT_HEALTH", fmt.Sprintf("Denied health check for endpoint %q", endpointName))
		return nil, err
	}

	if ep.Type == "LOCAL" {
		return nil, domain.ErrValidation("health checks are only supported for REMOTE compute endpoints")
	}

	result, err := s.grpcHealthCheck(ctx, ep.URL, ep.AuthToken)
	if err != nil {
		return nil, err
	}
	_ = s.repo.UpdateHealth(ctx, ep.ID, *result)
	return result, nil
}

func (s *ComputeEndpointService) grpcHealthCheck(ctx context.Context, endpointURL, authToken string) (*domain.ComputeEndpointHealthResult, error) {
	u, err := url.Parse(endpointURL)
	if err != nil {
		return nil, fmt.Errorf("parse grpc endpoint url: %w", err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("grpc endpoint host is required")
	}

	workercompute.EnsureGRPCJSONCodec()

	creds := insecure.NewCredentials()
	if strings.EqualFold(u.Scheme, "grpcs") {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	conn, err := grpc.NewClient(
		u.Host,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		return nil, fmt.Errorf("dial grpc health endpoint: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	ctxWithMD := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-agent-token", authToken))
	client := computeproto.NewComputeWorkerClient(conn)
	resp, err := client.Health(ctxWithMD, &computeproto.HealthRequest{})
	if err != nil {
		return nil, fmt.Errorf("grpc health check failed: %w", err)
	}

	status := resp.Status
	uptime := int(resp.UptimeSeconds)
	duckDBVersion := resp.DuckdbVersion
	memoryUsedMB := int(resp.MemoryUsedMb)
	maxMemoryGB := int(resp.MaxMemoryGb)

	return &domain.ComputeEndpointHealthResult{
		Status:                &status,
		UptimeSeconds:         &uptime,
		DuckdbVersion:         &duckDBVersion,
		MemoryUsedMb:          &memoryUsedMB,
		MaxMemoryGb:           &maxMemoryGB,
		ActiveQueries:         &resp.ActiveQueries,
		QueuedJobs:            &resp.QueuedJobs,
		RunningJobs:           &resp.RunningJobs,
		CompletedJobs:         &resp.CompletedJobs,
		StoredJobs:            &resp.StoredJobs,
		CleanedJobs:           &resp.CleanedJobs,
		QueryResultTTLSeconds: int32PtrToIntPtr(resp.ResultTtlSecs),
	}, nil
}

func normalizeSelectionPolicy(policy string) string {
	policy = strings.ToUpper(strings.TrimSpace(policy))
	if policy == "" {
		return domain.ComputeSelectionPolicyAllowedOnly
	}
	return policy
}

func normalizeEndpointWorkloadClass(workload string) string {
	workload = strings.ToUpper(strings.TrimSpace(workload))
	if workload == "" {
		return domain.ComputeEndpointWorkloadMixed
	}
	return workload
}

func normalizeReadinessStatus(status string) string {
	status = strings.ToUpper(strings.TrimSpace(status))
	if status == "" {
		return domain.ComputeReadinessReady
	}
	return status
}

func validateSelectionPolicy(policy string) error {
	switch normalizeSelectionPolicy(policy) {
	case domain.ComputeSelectionPolicyAdminOnly, domain.ComputeSelectionPolicyAllowedOnly, domain.ComputeSelectionPolicySelfService:
		return nil
	default:
		return domain.ErrValidation("invalid selection_policy %q", policy)
	}
}

func validateEndpointWorkloadClass(workload string) error {
	switch normalizeEndpointWorkloadClass(workload) {
	case domain.ComputeEndpointWorkloadInteractive, domain.ComputeEndpointWorkloadScheduled, domain.ComputeEndpointWorkloadHeavy, domain.ComputeEndpointWorkloadMixed:
		return nil
	default:
		return domain.ErrValidation("invalid workload_class %q", workload)
	}
}

func validateReadinessStatus(status string) error {
	switch normalizeReadinessStatus(status) {
	case domain.ComputeReadinessReady, domain.ComputeReadinessDegraded, domain.ComputeReadinessUnavailable:
		return nil
	default:
		return domain.ErrValidation("invalid readiness_status %q", status)
	}
}

func int32PtrToIntPtr(value int32) *int {
	v := int(value)
	return &v
}

// requirePrivilege checks that the principal has the given privilege on the catalog.
func (s *ComputeEndpointService) requirePrivilege(ctx context.Context, principal string, privilege, action, detail string) error {
	if err := servicepolicy.RequireCatalogPrivilege(ctx, s.auth, principal, privilege, "%s on catalog is required"); err != nil {
		s.logAuditDenied(ctx, principal, action, detail)
		return err
	}
	return nil
}

func (s *ComputeEndpointService) requireEndpointPrivilege(ctx context.Context, principal, endpointID, privilege string) error {
	return servicepolicy.RequireSecurablePrivilege(ctx, s.auth, principal, domain.SecurableComputeEndpoint, endpointID, privilege)
}

func (s *ComputeEndpointService) logAudit(ctx context.Context, principal, action, detail string) {
	auditutil.LogAllowed(ctx, s.audit, principal, action, detail)
}

func (s *ComputeEndpointService) logAuditDenied(ctx context.Context, principal, action, detail string) {
	auditutil.LogDenied(ctx, s.audit, principal, action, detail)
}
