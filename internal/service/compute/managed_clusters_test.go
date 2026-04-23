package compute

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/platform"
)

func TestComputeEndpointService_CreateManagedCluster(t *testing.T) {
	endpointRepo := &mockComputeEndpointRepo{
		GetByIDFn: func(_ context.Context, id string) (*domain.ComputeEndpoint, error) {
			return &domain.ComputeEndpoint{
				ID:   id,
				Name: "analytics-remote",
				Type: "REMOTE",
			}, nil
		},
		UpdateFn: func(_ context.Context, id string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
			require.Equal(t, "endpoint-1", id)
			require.NotNil(t, req.URL)
			require.NotNil(t, req.AuthToken)
			require.NotNil(t, req.ReadinessStatus)
			require.NotNil(t, req.IsDraining)
			assert.Equal(t, "grpcs://cluster-a.compute.quackstack.local:9444", *req.URL)
			assert.Equal(t, "cluster-a-token", *req.AuthToken)
			assert.Equal(t, domain.ComputeReadinessReady, *req.ReadinessStatus)
			assert.False(t, *req.IsDraining)
			return &domain.ComputeEndpoint{ID: id, Name: "analytics-remote", URL: *req.URL}, nil
		},
		UpdateStatusFn: func(_ context.Context, id string, status string) error {
			assert.Equal(t, "endpoint-1", id)
			assert.Equal(t, "ACTIVE", status)
			return nil
		},
	}

	templateRepo := &mockClusterTemplateRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.ComputeClusterTemplate, error) {
			return &domain.ComputeClusterTemplate{
				ID:                  id,
				Name:                "standard",
				Provider:            domain.ComputeProviderAzure,
				WorkloadClass:       domain.ComputeEndpointWorkloadMixed,
				MinReplicas:         1,
				MaxReplicas:         3,
				IdleAutoStopSeconds: 600,
			}, nil
		},
	}

	clusterRepo := &mockManagedClusterRepo{
		createFn: func(_ context.Context, cluster *domain.ManagedComputeCluster) (*domain.ManagedComputeCluster, error) {
			cluster.ID = "cluster-record-1"
			assert.Equal(t, domain.ComputeProviderAzure, cluster.Provider)
			assert.Equal(t, domain.ManagedClusterDesiredRunning, cluster.DesiredState)
			assert.Equal(t, domain.ManagedClusterObservedReady, cluster.ObservedState)
			return cluster, nil
		},
	}

	now := time.Now().UTC()
	provider := fakePlatformProvider{
		name: platform.ProviderAzure,
		compute: fakeComputeLifecycle{
			createFn: func(_ context.Context, spec platform.ClusterSpec) (*platform.ClusterState, error) {
				assert.Equal(t, "analytics-cluster", spec.Name)
				assert.Equal(t, int32(1), spec.MinReplicas)
				assert.Equal(t, int32(3), spec.MaxReplicas)
				return &platform.ClusterState{
					ID:             "cluster-a",
					Name:           spec.Name,
					DesiredState:   domain.ManagedClusterDesiredRunning,
					ObservedState:  domain.ManagedClusterObservedReady,
					LastActivityAt: &now,
					Connection: &platform.ConnectionInfo{
						URL:       "grpcs://cluster-a.compute.quackstack.local:9444",
						AuthToken: "cluster-a-token",
					},
				}, nil
			},
		},
	}

	audit := &mockAuditRepo{}
	svc := newTestComputeEndpointService(endpointRepo, allowManageCompute(), audit)
	svc.SetClusterTemplateRepository(templateRepo)
	svc.SetManagedClusterRepository(clusterRepo)
	svc.SetPlatformProvider(provider)

	cluster, err := svc.CreateManagedCluster(context.Background(), "admin", domain.CreateManagedComputeClusterRequest{
		Name:         "analytics-cluster",
		TemplateID:   "template-1",
		EndpointID:   "endpoint-1",
		Provider:     domain.ComputeProviderAzure,
		DesiredState: domain.ManagedClusterDesiredRunning,
	})
	require.NoError(t, err)
	assert.Equal(t, "cluster-record-1", cluster.ID)
	assert.Equal(t, "cluster-a", cluster.ExternalID)
	assert.True(t, audit.HasAction("CREATE_MANAGED_COMPUTE_CLUSTER"))
}

func TestComputeEndpointService_GetByNameHydratesManagedBacking(t *testing.T) {
	endpointRepo := &mockComputeEndpointRepo{
		GetByNameFn: func(_ context.Context, name string) (*domain.ComputeEndpoint, error) {
			return &domain.ComputeEndpoint{ID: "endpoint-1", Name: name, Type: "REMOTE"}, nil
		},
	}
	clusterRepo := &mockManagedClusterRepo{
		getByEndpointIDFn: func(_ context.Context, endpointID string) (*domain.ManagedComputeCluster, error) {
			require.Equal(t, "endpoint-1", endpointID)
			now := time.Now().UTC()
			url := "grpcs://cluster-a.compute.quackstack.local:9444"
			return &domain.ManagedComputeCluster{
				ID:             "cluster-1",
				TemplateID:     "template-1",
				EndpointID:     endpointID,
				Provider:       domain.ComputeProviderAzure,
				LastActivityAt: &now,
				EndpointURL:    &url,
			}, nil
		},
	}

	svc := newTestComputeEndpointService(endpointRepo, allowManageCompute(), &mockAuditRepo{})
	svc.SetManagedClusterRepository(clusterRepo)

	endpoint, err := svc.GetByName(context.Background(), "admin", "analytics")
	require.NoError(t, err)
	require.NotNil(t, endpoint.ManagedBacking)
	assert.Equal(t, domain.ComputeProviderAzure, endpoint.ManagedBacking.Provider)
	assert.Equal(t, "cluster-1", endpoint.ManagedBacking.ManagedClusterID)
	assert.Equal(t, "template-1", endpoint.ManagedBacking.TemplateID)
}

type mockClusterTemplateRepo struct {
	createFn    func(context.Context, *domain.ComputeClusterTemplate) (*domain.ComputeClusterTemplate, error)
	getByIDFn   func(context.Context, string) (*domain.ComputeClusterTemplate, error)
	getByNameFn func(context.Context, string) (*domain.ComputeClusterTemplate, error)
	listFn      func(context.Context, domain.PageRequest) ([]domain.ComputeClusterTemplate, int64, error)
	deleteFn    func(context.Context, string) error
}

func (m *mockClusterTemplateRepo) Create(ctx context.Context, tpl *domain.ComputeClusterTemplate) (*domain.ComputeClusterTemplate, error) {
	if m.createFn == nil {
		panic("mockClusterTemplateRepo.Create called but not configured")
	}
	return m.createFn(ctx, tpl)
}

func (m *mockClusterTemplateRepo) GetByID(ctx context.Context, id string) (*domain.ComputeClusterTemplate, error) {
	if m.getByIDFn == nil {
		panic("mockClusterTemplateRepo.GetByID called but not configured")
	}
	return m.getByIDFn(ctx, id)
}

func (m *mockClusterTemplateRepo) GetByName(ctx context.Context, name string) (*domain.ComputeClusterTemplate, error) {
	if m.getByNameFn == nil {
		panic("mockClusterTemplateRepo.GetByName called but not configured")
	}
	return m.getByNameFn(ctx, name)
}

func (m *mockClusterTemplateRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ComputeClusterTemplate, int64, error) {
	if m.listFn == nil {
		panic("mockClusterTemplateRepo.List called but not configured")
	}
	return m.listFn(ctx, page)
}

func (m *mockClusterTemplateRepo) Delete(ctx context.Context, id string) error {
	if m.deleteFn == nil {
		panic("mockClusterTemplateRepo.Delete called but not configured")
	}
	return m.deleteFn(ctx, id)
}

type mockManagedClusterRepo struct {
	createFn          func(context.Context, *domain.ManagedComputeCluster) (*domain.ManagedComputeCluster, error)
	getByIDFn         func(context.Context, string) (*domain.ManagedComputeCluster, error)
	getByNameFn       func(context.Context, string) (*domain.ManagedComputeCluster, error)
	getByEndpointIDFn func(context.Context, string) (*domain.ManagedComputeCluster, error)
	listFn            func(context.Context, domain.PageRequest) ([]domain.ManagedComputeCluster, int64, error)
	updateDesiredFn   func(context.Context, string, string) error
	updateObservedFn  func(context.Context, string, string, *string, *time.Time) error
	deleteFn          func(context.Context, string) error
}

func (m *mockManagedClusterRepo) Create(ctx context.Context, cluster *domain.ManagedComputeCluster) (*domain.ManagedComputeCluster, error) {
	if m.createFn == nil {
		panic("mockManagedClusterRepo.Create called but not configured")
	}
	return m.createFn(ctx, cluster)
}

func (m *mockManagedClusterRepo) GetByID(ctx context.Context, id string) (*domain.ManagedComputeCluster, error) {
	if m.getByIDFn == nil {
		panic("mockManagedClusterRepo.GetByID called but not configured")
	}
	return m.getByIDFn(ctx, id)
}

func (m *mockManagedClusterRepo) GetByName(ctx context.Context, name string) (*domain.ManagedComputeCluster, error) {
	if m.getByNameFn == nil {
		panic("mockManagedClusterRepo.GetByName called but not configured")
	}
	return m.getByNameFn(ctx, name)
}

func (m *mockManagedClusterRepo) GetByEndpointID(ctx context.Context, endpointID string) (*domain.ManagedComputeCluster, error) {
	if m.getByEndpointIDFn == nil {
		panic("mockManagedClusterRepo.GetByEndpointID called but not configured")
	}
	return m.getByEndpointIDFn(ctx, endpointID)
}

func (m *mockManagedClusterRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ManagedComputeCluster, int64, error) {
	if m.listFn == nil {
		panic("mockManagedClusterRepo.List called but not configured")
	}
	return m.listFn(ctx, page)
}

func (m *mockManagedClusterRepo) UpdateDesiredState(ctx context.Context, id string, desiredState string) error {
	if m.updateDesiredFn == nil {
		panic("mockManagedClusterRepo.UpdateDesiredState called but not configured")
	}
	return m.updateDesiredFn(ctx, id, desiredState)
}

func (m *mockManagedClusterRepo) UpdateObservedState(ctx context.Context, id string, observedState string, endpointURL *string, lastActivityAt *time.Time) error {
	if m.updateObservedFn == nil {
		panic("mockManagedClusterRepo.UpdateObservedState called but not configured")
	}
	return m.updateObservedFn(ctx, id, observedState, endpointURL, lastActivityAt)
}

func (m *mockManagedClusterRepo) Delete(ctx context.Context, id string) error {
	if m.deleteFn == nil {
		panic("mockManagedClusterRepo.Delete called but not configured")
	}
	return m.deleteFn(ctx, id)
}

type fakePlatformProvider struct {
	name    string
	compute fakeComputeLifecycle
}

func (p fakePlatformProvider) Name() string                                        { return p.name }
func (p fakePlatformProvider) ControlPlaneDatabase() platform.ControlPlaneDatabase { return nil }
func (p fakePlatformProvider) ObjectStorage() platform.ObjectStorage               { return nil }
func (p fakePlatformProvider) Ingress() platform.Ingress                           { return nil }
func (p fakePlatformProvider) Identity() platform.Identity                         { return nil }
func (p fakePlatformProvider) ComputeLifecycle() platform.ComputeLifecycle         { return p.compute }
func (p fakePlatformProvider) SecretsRuntime() platform.SecretsRuntime             { return nil }

type fakeComputeLifecycle struct {
	createFn func(context.Context, platform.ClusterSpec) (*platform.ClusterState, error)
	startFn  func(context.Context, string) (*platform.ClusterState, error)
	stopFn   func(context.Context, string) (*platform.ClusterState, error)
	drainFn  func(context.Context, string) (*platform.ClusterState, error)
}

func (f fakeComputeLifecycle) Name() string { return "fake" }
func (f fakeComputeLifecycle) CreateCluster(ctx context.Context, spec platform.ClusterSpec) (*platform.ClusterState, error) {
	if f.createFn == nil {
		panic("fakeComputeLifecycle.CreateCluster called but not configured")
	}
	return f.createFn(ctx, spec)
}
func (f fakeComputeLifecycle) StartCluster(ctx context.Context, clusterID string) (*platform.ClusterState, error) {
	if f.startFn == nil {
		panic("fakeComputeLifecycle.StartCluster called but not configured")
	}
	return f.startFn(ctx, clusterID)
}
func (f fakeComputeLifecycle) StopCluster(ctx context.Context, clusterID string) (*platform.ClusterState, error) {
	if f.stopFn == nil {
		panic("fakeComputeLifecycle.StopCluster called but not configured")
	}
	return f.stopFn(ctx, clusterID)
}
func (f fakeComputeLifecycle) DrainCluster(ctx context.Context, clusterID string) (*platform.ClusterState, error) {
	if f.drainFn == nil {
		panic("fakeComputeLifecycle.DrainCluster called but not configured")
	}
	return f.drainFn(ctx, clusterID)
}
func (f fakeComputeLifecycle) DeleteCluster(context.Context, string) error { return nil }
func (f fakeComputeLifecycle) ResizeCluster(context.Context, string, platform.ClusterSize) (*platform.ClusterState, error) {
	return nil, nil
}
func (f fakeComputeLifecycle) ResolveConnection(context.Context, string) (*platform.ConnectionInfo, error) {
	return nil, nil
}
func (f fakeComputeLifecycle) ReportStatus(context.Context, string) (*platform.ClusterState, error) {
	return nil, nil
}
