package platform

import (
	"context"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/config"
)

type azureProvider struct {
	controlDB ControlPlaneDatabase
	storage   ObjectStorage
	ingress   Ingress
	identity  Identity
	compute   ComputeLifecycle
	secrets   SecretsRuntime
}

func newAzureProvider(cfg *config.Config) Provider {
	return &azureProvider{
		controlDB: azureControlPlaneDatabase{cfg: cfg},
		storage:   azureObjectStorage{},
		ingress:   azureIngress{},
		identity:  azureIdentity{},
		compute:   azureComputeLifecycle{},
		secrets:   azureSecretsRuntime{},
	}
}

func (p *azureProvider) Name() string                               { return ProviderAzure }
func (p *azureProvider) ControlPlaneDatabase() ControlPlaneDatabase { return p.controlDB }
func (p *azureProvider) ObjectStorage() ObjectStorage               { return p.storage }
func (p *azureProvider) Ingress() Ingress                           { return p.ingress }
func (p *azureProvider) Identity() Identity                         { return p.identity }
func (p *azureProvider) ComputeLifecycle() ComputeLifecycle         { return p.compute }
func (p *azureProvider) SecretsRuntime() SecretsRuntime             { return p.secrets }

type azureControlPlaneDatabase struct{ cfg *config.Config }

func (d azureControlPlaneDatabase) Driver() string { return d.cfg.ControlDBDriver }
func (d azureControlPlaneDatabase) Connect(context.Context) error {
	if strings.TrimSpace(d.cfg.ControlDBDSN) == "" {
		return &CapabilityNotConfiguredError{Provider: ProviderAzure, Capability: "control-plane postgres DSN"}
	}
	return nil
}
func (d azureControlPlaneDatabase) Migrate(context.Context) error {
	return &CapabilityNotConfiguredError{Provider: ProviderAzure, Capability: "postgres migrations"}
}
func (d azureControlPlaneDatabase) HealthCheck(context.Context) error { return nil }
func (d azureControlPlaneDatabase) Backup(context.Context) error      { return nil }
func (d azureControlPlaneDatabase) Restore(context.Context) error {
	return &CapabilityNotConfiguredError{Provider: ProviderAzure, Capability: "control-plane restore"}
}

type azureObjectStorage struct{}

func (azureObjectStorage) Name() string                      { return "azure-blob" }
func (azureObjectStorage) HealthCheck(context.Context) error { return nil }
func (azureObjectStorage) WriteArtifact(context.Context, ArtifactRef) error {
	return &CapabilityNotConfiguredError{Provider: ProviderAzure, Capability: "artifact writes"}
}
func (azureObjectStorage) ApplyRetention(context.Context, ArtifactRef) error {
	return &CapabilityNotConfiguredError{Provider: ProviderAzure, Capability: "artifact retention"}
}

type azureIngress struct{}

func (azureIngress) Name() string                                   { return "azure-ingress" }
func (azureIngress) HealthCheck(context.Context) error              { return nil }
func (azureIngress) ExposeHTTP(context.Context, ExposureSpec) error { return nil }
func (azureIngress) ExposeGRPC(context.Context, ExposureSpec) error { return nil }

type azureIdentity struct{}

func (azureIdentity) Name() string                                            { return "azure-workload-identity" }
func (azureIdentity) HealthCheck(context.Context) error                       { return nil }
func (azureIdentity) ConfigureOIDC(context.Context, string) error             { return nil }
func (azureIdentity) ConfigureWorkloadIdentity(context.Context, string) error { return nil }

type azureComputeLifecycle struct{}

func (azureComputeLifecycle) Name() string { return "aks-managed-compute" }
func (azureComputeLifecycle) CreateCluster(_ context.Context, spec ClusterSpec) (*ClusterState, error) {
	now := time.Now().UTC()
	return &ClusterState{
		ID:                spec.Name,
		Name:              spec.Name,
		DesiredState:      "RUNNING",
		ObservedState:     "STARTING",
		LastActivityAt:    &now,
		Connection:        azureConnectionForCluster(spec.Name),
		AvailableReplicas: 0,
	}, nil
}
func (azureComputeLifecycle) StartCluster(_ context.Context, clusterID string) (*ClusterState, error) {
	now := time.Now().UTC()
	return &ClusterState{
		ID:                clusterID,
		Name:              clusterID,
		DesiredState:      "RUNNING",
		ObservedState:     "READY",
		LastActivityAt:    &now,
		Connection:        azureConnectionForCluster(clusterID),
		AvailableReplicas: 1,
	}, nil
}
func (azureComputeLifecycle) StopCluster(_ context.Context, clusterID string) (*ClusterState, error) {
	return &ClusterState{
		ID:                clusterID,
		Name:              clusterID,
		DesiredState:      "STOPPED",
		ObservedState:     "STOPPED",
		AvailableReplicas: 0,
	}, nil
}
func (azureComputeLifecycle) DrainCluster(_ context.Context, clusterID string) (*ClusterState, error) {
	now := time.Now().UTC()
	return &ClusterState{
		ID:                clusterID,
		Name:              clusterID,
		DesiredState:      "DRAINING",
		ObservedState:     "DEGRADED",
		LastActivityAt:    &now,
		Connection:        azureConnectionForCluster(clusterID),
		AvailableReplicas: 1,
	}, nil
}
func (azureComputeLifecycle) DeleteCluster(context.Context, string) error {
	return nil
}
func (azureComputeLifecycle) ResizeCluster(_ context.Context, clusterID string, _ ClusterSize) (*ClusterState, error) {
	now := time.Now().UTC()
	return &ClusterState{
		ID:                clusterID,
		Name:              clusterID,
		DesiredState:      "RUNNING",
		ObservedState:     "READY",
		LastActivityAt:    &now,
		Connection:        azureConnectionForCluster(clusterID),
		AvailableReplicas: 1,
	}, nil
}
func (azureComputeLifecycle) ResolveConnection(_ context.Context, clusterID string) (*ConnectionInfo, error) {
	return azureConnectionForCluster(clusterID), nil
}
func (azureComputeLifecycle) ReportStatus(_ context.Context, clusterID string) (*ClusterState, error) {
	now := time.Now().UTC()
	return &ClusterState{
		ID:                clusterID,
		Name:              clusterID,
		DesiredState:      "RUNNING",
		ObservedState:     "READY",
		LastActivityAt:    &now,
		Connection:        azureConnectionForCluster(clusterID),
		AvailableReplicas: 1,
	}, nil
}

type azureSecretsRuntime struct{}

func (azureSecretsRuntime) Name() string                               { return "managed-secrets" }
func (azureSecretsRuntime) HealthCheck(context.Context) error          { return nil }
func (azureSecretsRuntime) InjectSecret(context.Context, string) error { return nil }
func (azureSecretsRuntime) RotateSecret(context.Context, string) error { return nil }

func azureConnectionForCluster(clusterID string) *ConnectionInfo {
	return &ConnectionInfo{
		URL:       "grpcs://" + clusterID + ".compute.quackstack.local:9444",
		AuthToken: clusterID + "-token",
	}
}
