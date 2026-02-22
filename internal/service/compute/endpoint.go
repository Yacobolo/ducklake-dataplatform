// Package compute implements compute endpoint management services.
package compute

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	workercompute "duck-demo/internal/compute"
	"duck-demo/internal/domain"
	"duck-demo/internal/service/auditutil"

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
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "CREATE_COMPUTE_ENDPOINT", fmt.Sprintf("Denied create compute endpoint %q", req.Name)); err != nil {
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
		status := "ok"
		return &domain.ComputeEndpointHealthResult{Status: &status}, nil
	}

	// Proxy health check to remote agent
	if strings.HasPrefix(strings.ToLower(ep.URL), "grpc://") || strings.HasPrefix(strings.ToLower(ep.URL), "grpcs://") {
		return s.grpcHealthCheck(ctx, ep.URL, ep.AuthToken)
	}

	return s.httpHealthCheck(ctx, ep.URL, ep.AuthToken)
}

func (s *ComputeEndpointService) httpHealthCheck(ctx context.Context, endpointURL, authToken string) (*domain.ComputeEndpointHealthResult, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
		},
	}

	url := strings.TrimRight(endpointURL, "/") + "/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create health request: %w", err)
	}
	workercompute.AttachSignedAgentHeaders(req, authToken, nil, time.Now())

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent returned status %d", resp.StatusCode)
	}

	var body struct {
		Status        string `json:"status"`
		UptimeSeconds int    `json:"uptime_seconds"`
		DuckdbVersion string `json:"duckdb_version"`
		MemoryUsedMb  int    `json:"memory_used_mb"`
		MaxMemoryGb   int    `json:"max_memory_gb"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode health response: %w", err)
	}

	return &domain.ComputeEndpointHealthResult{
		Status:        &body.Status,
		UptimeSeconds: &body.UptimeSeconds,
		DuckdbVersion: &body.DuckdbVersion,
		MemoryUsedMb:  &body.MemoryUsedMb,
		MaxMemoryGb:   &body.MaxMemoryGb,
	}, nil
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
	var resp workercompute.HealthResponse
	if err := conn.Invoke(
		ctxWithMD,
		"/duckdemo.compute.v1.ComputeWorker/Health",
		&workercompute.HealthRequest{},
		&resp,
	); err != nil {
		return nil, fmt.Errorf("grpc health check failed: %w", err)
	}

	status := resp.Status
	uptime := resp.UptimeSeconds
	duckDBVersion := resp.DuckDBVersion
	memoryUsedMB := int(resp.MemoryUsedMB)
	maxMemoryGB := resp.MaxMemoryGB

	return &domain.ComputeEndpointHealthResult{
		Status:        &status,
		UptimeSeconds: &uptime,
		DuckdbVersion: &duckDBVersion,
		MemoryUsedMb:  &memoryUsedMB,
		MaxMemoryGb:   &maxMemoryGB,
	}, nil
}

// requirePrivilege checks that the principal has the given privilege on the catalog.
func (s *ComputeEndpointService) requirePrivilege(ctx context.Context, principal string, privilege, action, detail string) error {
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, action, detail)
		return domain.ErrAccessDenied("%q lacks %s on catalog", principal, privilege)
	}
	return nil
}

func (s *ComputeEndpointService) requireEndpointPrivilege(ctx context.Context, principal, endpointID, privilege string) error {
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableComputeEndpoint, endpointID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on compute endpoint", principal, privilege)
	}
	return nil
}

func (s *ComputeEndpointService) logAudit(ctx context.Context, principal, action, detail string) {
	auditutil.LogAllowed(ctx, s.audit, principal, action, detail)
}

func (s *ComputeEndpointService) logAuditDenied(ctx context.Context, principal, action, detail string) {
	auditutil.LogDenied(ctx, s.audit, principal, action, detail)
}
