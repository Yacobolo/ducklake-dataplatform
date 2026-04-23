package platform

import (
	"context"

	"github.com/Yacobolo/quackstack/internal/config"
)

type manualProvider struct {
	controlDB ControlPlaneDatabase
	storage   ObjectStorage
	ingress   Ingress
	identity  Identity
	compute   ComputeLifecycle
	secrets   SecretsRuntime
}

func newManualProvider(cfg *config.Config) Provider {
	return &manualProvider{
		controlDB: manualControlPlaneDatabase{cfg: cfg},
		storage:   manualObjectStorage{},
		ingress:   manualIngress{},
		identity:  manualIdentity{},
		compute:   manualComputeLifecycle{},
		secrets:   manualSecretsRuntime{},
	}
}

func (p *manualProvider) Name() string                               { return ProviderManual }
func (p *manualProvider) ControlPlaneDatabase() ControlPlaneDatabase { return p.controlDB }
func (p *manualProvider) ObjectStorage() ObjectStorage               { return p.storage }
func (p *manualProvider) Ingress() Ingress                           { return p.ingress }
func (p *manualProvider) Identity() Identity                         { return p.identity }
func (p *manualProvider) ComputeLifecycle() ComputeLifecycle         { return p.compute }
func (p *manualProvider) SecretsRuntime() SecretsRuntime             { return p.secrets }

type manualControlPlaneDatabase struct{ cfg *config.Config }

func (d manualControlPlaneDatabase) Driver() string                    { return d.cfg.ControlDBDriver }
func (d manualControlPlaneDatabase) Connect(context.Context) error     { return nil }
func (d manualControlPlaneDatabase) Migrate(context.Context) error     { return nil }
func (d manualControlPlaneDatabase) HealthCheck(context.Context) error { return nil }
func (d manualControlPlaneDatabase) Backup(context.Context) error {
	return &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "control-plane backup"}
}
func (d manualControlPlaneDatabase) Restore(context.Context) error {
	return &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "control-plane restore"}
}

type manualObjectStorage struct{}

func (manualObjectStorage) Name() string                      { return "compose-artifacts" }
func (manualObjectStorage) HealthCheck(context.Context) error { return nil }
func (manualObjectStorage) WriteArtifact(context.Context, ArtifactRef) error {
	return &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "artifact writes"}
}
func (manualObjectStorage) ApplyRetention(context.Context, ArtifactRef) error {
	return &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "artifact retention"}
}

type manualIngress struct{}

func (manualIngress) Name() string                                   { return "local-ingress" }
func (manualIngress) HealthCheck(context.Context) error              { return nil }
func (manualIngress) ExposeHTTP(context.Context, ExposureSpec) error { return nil }
func (manualIngress) ExposeGRPC(context.Context, ExposureSpec) error { return nil }

type manualIdentity struct{}

func (manualIdentity) Name() string                      { return "local-auth" }
func (manualIdentity) HealthCheck(context.Context) error { return nil }
func (manualIdentity) ConfigureOIDC(context.Context, string) error {
	return &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "OIDC configuration"}
}
func (manualIdentity) ConfigureWorkloadIdentity(context.Context, string) error {
	return &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "workload identity"}
}

type manualComputeLifecycle struct{}

func (manualComputeLifecycle) Name() string { return "static-compute" }
func (manualComputeLifecycle) CreateCluster(context.Context, ClusterSpec) (*ClusterState, error) {
	return nil, &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "managed compute create"}
}
func (manualComputeLifecycle) StartCluster(context.Context, string) (*ClusterState, error) {
	return nil, &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "managed compute start"}
}
func (manualComputeLifecycle) StopCluster(context.Context, string) (*ClusterState, error) {
	return nil, &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "managed compute stop"}
}
func (manualComputeLifecycle) DrainCluster(context.Context, string) (*ClusterState, error) {
	return nil, &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "managed compute drain"}
}
func (manualComputeLifecycle) DeleteCluster(context.Context, string) error {
	return &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "managed compute delete"}
}
func (manualComputeLifecycle) ResizeCluster(context.Context, string, ClusterSize) (*ClusterState, error) {
	return nil, &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "managed compute resize"}
}
func (manualComputeLifecycle) ResolveConnection(context.Context, string) (*ConnectionInfo, error) {
	return nil, &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "managed compute connection resolution"}
}
func (manualComputeLifecycle) ReportStatus(context.Context, string) (*ClusterState, error) {
	return nil, &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "managed compute status reporting"}
}

type manualSecretsRuntime struct{}

func (manualSecretsRuntime) Name() string                               { return "env-file" }
func (manualSecretsRuntime) HealthCheck(context.Context) error          { return nil }
func (manualSecretsRuntime) InjectSecret(context.Context, string) error { return nil }
func (manualSecretsRuntime) RotateSecret(context.Context, string) error {
	return &CapabilityNotConfiguredError{Provider: ProviderManual, Capability: "secret rotation"}
}
