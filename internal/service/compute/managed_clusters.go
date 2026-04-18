package compute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/platform"
)

// CreateClusterTemplate persists a provider-neutral managed compute template.
func (s *ComputeEndpointService) CreateClusterTemplate(ctx context.Context, principal string, req domain.CreateComputeClusterTemplateRequest) (*domain.ComputeClusterTemplate, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "CREATE_COMPUTE_CLUSTER_TEMPLATE", fmt.Sprintf("Denied create compute cluster template %q", req.Name)); err != nil {
		return nil, err
	}
	if s.templateRepo == nil {
		return nil, fmt.Errorf("compute cluster template repository is not configured")
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}

	tpl := &domain.ComputeClusterTemplate{
		Name:                   req.Name,
		Provider:               strings.ToLower(strings.TrimSpace(req.Provider)),
		WorkloadClass:          strings.ToUpper(strings.TrimSpace(req.WorkloadClass)),
		Size:                   strings.ToUpper(strings.TrimSpace(req.Size)),
		MinReplicas:            req.MinReplicas,
		MaxReplicas:            req.MaxReplicas,
		IdleAutoStopSeconds:    req.IdleAutoStopSeconds,
		ScalingPolicy:          strings.TrimSpace(req.ScalingPolicy),
		StorageProfile:         strings.TrimSpace(req.StorageProfile),
		ResultRetentionSeconds: req.ResultRetentionSeconds,
	}

	created, err := s.templateRepo.Create(ctx, tpl)
	if err != nil {
		return nil, fmt.Errorf("create compute cluster template: %w", err)
	}
	s.logAudit(ctx, principal, "CREATE_COMPUTE_CLUSTER_TEMPLATE", fmt.Sprintf("Created compute cluster template %q", req.Name))
	return created, nil
}

// GetClusterTemplateByName returns a managed compute template by name.
func (s *ComputeEndpointService) GetClusterTemplateByName(ctx context.Context, principal, name string) (*domain.ComputeClusterTemplate, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "GET_COMPUTE_CLUSTER_TEMPLATE", fmt.Sprintf("Denied get compute cluster template %q", name)); err != nil {
		return nil, err
	}
	if s.templateRepo == nil {
		return nil, fmt.Errorf("compute cluster template repository is not configured")
	}
	return s.templateRepo.GetByName(ctx, name)
}

// ListClusterTemplates returns all configured managed compute templates.
func (s *ComputeEndpointService) ListClusterTemplates(ctx context.Context, principal string, page domain.PageRequest) ([]domain.ComputeClusterTemplate, int64, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "LIST_COMPUTE_CLUSTER_TEMPLATES", "Denied list compute cluster templates"); err != nil {
		return nil, 0, err
	}
	if s.templateRepo == nil {
		return nil, 0, fmt.Errorf("compute cluster template repository is not configured")
	}
	return s.templateRepo.List(ctx, page)
}

// DeleteClusterTemplate removes a managed compute template by name.
func (s *ComputeEndpointService) DeleteClusterTemplate(ctx context.Context, principal, name string) error {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "DELETE_COMPUTE_CLUSTER_TEMPLATE", fmt.Sprintf("Denied delete compute cluster template %q", name)); err != nil {
		return err
	}
	if s.templateRepo == nil {
		return fmt.Errorf("compute cluster template repository is not configured")
	}

	tpl, err := s.templateRepo.GetByName(ctx, name)
	if err != nil {
		return err
	}
	if err := s.templateRepo.Delete(ctx, tpl.ID); err != nil {
		return fmt.Errorf("delete compute cluster template: %w", err)
	}
	s.logAudit(ctx, principal, "DELETE_COMPUTE_CLUSTER_TEMPLATE", fmt.Sprintf("Deleted compute cluster template %q", name))
	return nil
}

// CreateManagedCluster creates a provider-backed cluster bound to a logical endpoint.
func (s *ComputeEndpointService) CreateManagedCluster(ctx context.Context, principal string, req domain.CreateManagedComputeClusterRequest) (*domain.ManagedComputeCluster, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "CREATE_MANAGED_COMPUTE_CLUSTER", fmt.Sprintf("Denied create managed compute cluster %q", req.Name)); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}

	lifecycle, err := s.computeLifecycle()
	if err != nil {
		return nil, err
	}
	if s.templateRepo == nil || s.clusterRepo == nil {
		return nil, fmt.Errorf("managed compute repositories are not configured")
	}

	template, err := s.templateRepo.GetByID(ctx, req.TemplateID)
	if err != nil {
		return nil, fmt.Errorf("load compute cluster template: %w", err)
	}

	endpoint, err := s.repo.GetByID(ctx, req.EndpointID)
	if err != nil {
		return nil, fmt.Errorf("load compute endpoint: %w", err)
	}
	if endpoint.Type != "REMOTE" {
		return nil, domain.ErrValidation("managed compute clusters require a REMOTE compute endpoint")
	}

	providerName := normalizeManagedClusterProvider(req.Provider, template.Provider)
	if providerName != s.provider.Name() {
		return nil, domain.ErrValidation("managed compute cluster provider %q does not match configured platform provider %q", providerName, s.provider.Name())
	}

	spec := platform.ClusterSpec{
		Name:          req.Name,
		WorkloadClass: template.WorkloadClass,
		MinReplicas:   chooseManagedClusterReplicaCount(req.MinReplicas, template.MinReplicas),
		MaxReplicas:   chooseManagedClusterReplicaCount(req.MaxReplicas, template.MaxReplicas),
		IdleTimeout:   time.Duration(template.IdleAutoStopSeconds) * time.Second,
	}

	state, err := lifecycle.CreateCluster(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("create managed compute cluster: %w", err)
	}

	desiredState := normalizeManagedClusterDesiredState(req.DesiredState)
	cluster := &domain.ManagedComputeCluster{
		Name:           req.Name,
		TemplateID:     template.ID,
		EndpointID:     endpoint.ID,
		Provider:       providerName,
		ExternalID:     managedClusterExternalID(req.Name, state),
		DesiredState:   desiredState,
		ObservedState:  normalizeManagedClusterObservedState(state.ObservedState),
		MinReplicas:    spec.MinReplicas,
		MaxReplicas:    spec.MaxReplicas,
		IsDraining:     desiredState == domain.ManagedClusterDesiredDraining,
		LastActivityAt: state.LastActivityAt,
		EndpointURL:    managedClusterEndpointURL(state),
	}

	created, err := s.clusterRepo.Create(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("persist managed compute cluster: %w", err)
	}

	if err := s.syncManagedEndpoint(ctx, endpoint, created, state.Connection); err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "CREATE_MANAGED_COMPUTE_CLUSTER", fmt.Sprintf("Created managed compute cluster %q", req.Name))
	return created, nil
}

// GetManagedClusterByName returns a managed compute cluster by name.
func (s *ComputeEndpointService) GetManagedClusterByName(ctx context.Context, principal, name string) (*domain.ManagedComputeCluster, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "GET_MANAGED_COMPUTE_CLUSTER", fmt.Sprintf("Denied get managed compute cluster %q", name)); err != nil {
		return nil, err
	}
	if s.clusterRepo == nil {
		return nil, fmt.Errorf("managed compute cluster repository is not configured")
	}
	return s.clusterRepo.GetByName(ctx, name)
}

// ListManagedClusters returns managed compute clusters.
func (s *ComputeEndpointService) ListManagedClusters(ctx context.Context, principal string, page domain.PageRequest) ([]domain.ManagedComputeCluster, int64, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "LIST_MANAGED_COMPUTE_CLUSTERS", "Denied list managed compute clusters"); err != nil {
		return nil, 0, err
	}
	if s.clusterRepo == nil {
		return nil, 0, fmt.Errorf("managed compute cluster repository is not configured")
	}
	return s.clusterRepo.List(ctx, page)
}

// StartManagedCluster requests that a managed cluster transitions to RUNNING.
func (s *ComputeEndpointService) StartManagedCluster(ctx context.Context, principal, name string) (*domain.ManagedComputeCluster, error) {
	return s.transitionManagedCluster(ctx, principal, name, domain.ManagedClusterDesiredRunning, "START_MANAGED_COMPUTE_CLUSTER", func(lifecycle platform.ComputeLifecycle, providerClusterID string) (*platform.ClusterState, error) {
		return lifecycle.StartCluster(ctx, providerClusterID)
	})
}

// StopManagedCluster requests that a managed cluster transitions to STOPPED.
func (s *ComputeEndpointService) StopManagedCluster(ctx context.Context, principal, name string) (*domain.ManagedComputeCluster, error) {
	return s.transitionManagedCluster(ctx, principal, name, domain.ManagedClusterDesiredStopped, "STOP_MANAGED_COMPUTE_CLUSTER", func(lifecycle platform.ComputeLifecycle, providerClusterID string) (*platform.ClusterState, error) {
		return lifecycle.StopCluster(ctx, providerClusterID)
	})
}

// DrainManagedCluster requests that a managed cluster enters a draining state.
func (s *ComputeEndpointService) DrainManagedCluster(ctx context.Context, principal, name string) (*domain.ManagedComputeCluster, error) {
	return s.transitionManagedCluster(ctx, principal, name, domain.ManagedClusterDesiredDraining, "DRAIN_MANAGED_COMPUTE_CLUSTER", func(lifecycle platform.ComputeLifecycle, providerClusterID string) (*platform.ClusterState, error) {
		return lifecycle.DrainCluster(ctx, providerClusterID)
	})
}

// DeleteManagedCluster removes a managed cluster after a provider-side delete.
func (s *ComputeEndpointService) DeleteManagedCluster(ctx context.Context, principal, name string) error {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "DELETE_MANAGED_COMPUTE_CLUSTER", fmt.Sprintf("Denied delete managed compute cluster %q", name)); err != nil {
		return err
	}

	lifecycle, err := s.computeLifecycle()
	if err != nil {
		return err
	}
	if s.clusterRepo == nil {
		return fmt.Errorf("managed compute cluster repository is not configured")
	}

	cluster, err := s.clusterRepo.GetByName(ctx, name)
	if err != nil {
		return err
	}

	if err := lifecycle.DeleteCluster(ctx, providerClusterID(cluster)); err != nil {
		return fmt.Errorf("delete managed compute cluster: %w", err)
	}
	if err := s.clusterRepo.Delete(ctx, cluster.ID); err != nil {
		return fmt.Errorf("delete managed compute cluster record: %w", err)
	}
	s.logAudit(ctx, principal, "DELETE_MANAGED_COMPUTE_CLUSTER", fmt.Sprintf("Deleted managed compute cluster %q", name))
	return nil
}

func (s *ComputeEndpointService) transitionManagedCluster(
	ctx context.Context,
	principal string,
	name string,
	desiredState string,
	action string,
	transition func(platform.ComputeLifecycle, string) (*platform.ClusterState, error),
) (*domain.ManagedComputeCluster, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, action, fmt.Sprintf("Denied %s for managed compute cluster %q", strings.ToLower(action), name)); err != nil {
		return nil, err
	}

	lifecycle, err := s.computeLifecycle()
	if err != nil {
		return nil, err
	}
	if s.clusterRepo == nil {
		return nil, fmt.Errorf("managed compute cluster repository is not configured")
	}

	cluster, err := s.clusterRepo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}
	endpoint, err := s.repo.GetByID(ctx, cluster.EndpointID)
	if err != nil {
		return nil, fmt.Errorf("load compute endpoint for managed cluster: %w", err)
	}

	state, err := transition(lifecycle, providerClusterID(cluster))
	if err != nil {
		return nil, fmt.Errorf("transition managed compute cluster: %w", err)
	}

	if err := s.clusterRepo.UpdateDesiredState(ctx, cluster.ID, desiredState); err != nil {
		return nil, fmt.Errorf("update managed compute desired state: %w", err)
	}
	if err := s.clusterRepo.UpdateObservedState(ctx, cluster.ID, normalizeManagedClusterObservedState(state.ObservedState), managedClusterEndpointURL(state), state.LastActivityAt); err != nil {
		return nil, fmt.Errorf("update managed compute observed state: %w", err)
	}

	updated, err := s.clusterRepo.GetByID(ctx, cluster.ID)
	if err != nil {
		return nil, fmt.Errorf("reload managed compute cluster: %w", err)
	}
	if err := s.syncManagedEndpoint(ctx, endpoint, updated, state.Connection); err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, action, fmt.Sprintf("Updated managed compute cluster %q to %s", name, desiredState))
	return updated, nil
}

func (s *ComputeEndpointService) hydrateEndpointManagedBacking(ctx context.Context, endpoint *domain.ComputeEndpoint) error {
	if endpoint == nil || s.clusterRepo == nil {
		return nil
	}

	cluster, err := s.clusterRepo.GetByEndpointID(ctx, endpoint.ID)
	if err != nil {
		var notFound *domain.NotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("load managed compute backing: %w", err)
	}

	endpoint.ManagedBacking = &domain.ComputeEndpointBacking{
		Provider:         cluster.Provider,
		ManagedClusterID: cluster.ID,
		TemplateID:       cluster.TemplateID,
	}
	endpoint.LastActivityAt = cluster.LastActivityAt
	if cluster.EndpointURL != nil && strings.TrimSpace(endpoint.URL) == "" {
		endpoint.URL = *cluster.EndpointURL
	}
	return nil
}

func (s *ComputeEndpointService) hydrateEndpointsManagedBacking(ctx context.Context, endpoints []domain.ComputeEndpoint) error {
	for i := range endpoints {
		if err := s.hydrateEndpointManagedBacking(ctx, &endpoints[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *ComputeEndpointService) syncManagedEndpoint(ctx context.Context, endpoint *domain.ComputeEndpoint, cluster *domain.ManagedComputeCluster, connection *platform.ConnectionInfo) error {
	if endpoint == nil || cluster == nil {
		return nil
	}

	updateReq := domain.UpdateComputeEndpointRequest{
		IsDraining:      boolPtr(cluster.IsDraining),
		ReadinessStatus: stringPtr(managedClusterReadiness(cluster.ObservedState)),
	}
	if cluster.EndpointURL != nil {
		updateReq.URL = cluster.EndpointURL
	}
	if connection != nil && strings.TrimSpace(connection.URL) != "" {
		updateReq.URL = &connection.URL
	}
	if connection != nil && strings.TrimSpace(connection.AuthToken) != "" {
		updateReq.AuthToken = &connection.AuthToken
	}

	if _, err := s.repo.Update(ctx, endpoint.ID, updateReq); err != nil {
		return fmt.Errorf("sync managed compute endpoint: %w", err)
	}
	if err := s.repo.UpdateStatus(ctx, endpoint.ID, managedClusterEndpointStatus(cluster.ObservedState)); err != nil {
		return fmt.Errorf("sync managed compute endpoint status: %w", err)
	}
	return nil
}

func (s *ComputeEndpointService) computeLifecycle() (platform.ComputeLifecycle, error) {
	if s.provider == nil {
		return nil, fmt.Errorf("platform provider is not configured")
	}
	return s.provider.ComputeLifecycle(), nil
}

func normalizeManagedClusterProvider(requestProvider, fallback string) string {
	if strings.TrimSpace(requestProvider) != "" {
		return strings.ToLower(strings.TrimSpace(requestProvider))
	}
	return strings.ToLower(strings.TrimSpace(fallback))
}

func normalizeManagedClusterDesiredState(state string) string {
	state = strings.ToUpper(strings.TrimSpace(state))
	if state == "" {
		return domain.ManagedClusterDesiredRunning
	}
	return state
}

func normalizeManagedClusterObservedState(state string) string {
	state = strings.ToUpper(strings.TrimSpace(state))
	if state == "" {
		return domain.ManagedClusterObservedStarting
	}
	return state
}

func chooseManagedClusterReplicaCount(requested, fallback int32) int32 {
	if requested > 0 {
		return requested
	}
	return fallback
}

func providerClusterID(cluster *domain.ManagedComputeCluster) string {
	if cluster == nil {
		return ""
	}
	if strings.TrimSpace(cluster.ExternalID) != "" {
		return cluster.ExternalID
	}
	return cluster.ID
}

func managedClusterExternalID(name string, state *platform.ClusterState) string {
	if state != nil && strings.TrimSpace(state.ID) != "" {
		return strings.TrimSpace(state.ID)
	}
	return name
}

func managedClusterEndpointURL(state *platform.ClusterState) *string {
	if state == nil || state.Connection == nil || strings.TrimSpace(state.Connection.URL) == "" {
		return nil
	}
	return &state.Connection.URL
}

func managedClusterEndpointStatus(observedState string) string {
	switch strings.ToUpper(strings.TrimSpace(observedState)) {
	case domain.ManagedClusterObservedReady, domain.ManagedClusterObservedDegraded:
		return "ACTIVE"
	case domain.ManagedClusterObservedStopped:
		return "INACTIVE"
	case domain.ManagedClusterObservedError:
		return "ERROR"
	default:
		return "STARTING"
	}
}

func managedClusterReadiness(observedState string) string {
	switch strings.ToUpper(strings.TrimSpace(observedState)) {
	case domain.ManagedClusterObservedReady:
		return domain.ComputeReadinessReady
	case domain.ManagedClusterObservedDegraded:
		return domain.ComputeReadinessDegraded
	default:
		return domain.ComputeReadinessUnavailable
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func stringPtr(value string) *string {
	return &value
}
