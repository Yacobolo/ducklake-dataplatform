package platform

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/config"
)

const (
	ProviderManual = "manual"
	ProviderAzure  = "azure"
	ProviderAWS    = "aws"
	ProviderGCP    = "gcp"
)

// Provider bundles the platform capabilities QuackStack expects from a deployment target.
type Provider interface {
	Name() string
	ControlPlaneDatabase() ControlPlaneDatabase
	ObjectStorage() ObjectStorage
	Ingress() Ingress
	Identity() Identity
	ComputeLifecycle() ComputeLifecycle
	SecretsRuntime() SecretsRuntime
}

// ControlPlaneDatabase manages the authoritative control-plane metadata store.
type ControlPlaneDatabase interface {
	Driver() string
	Connect(ctx context.Context) error
	Migrate(ctx context.Context) error
	HealthCheck(ctx context.Context) error
	Backup(ctx context.Context) error
	Restore(ctx context.Context) error
}

// ObjectStorage manages artifact and result storage for enterprise workflows.
type ObjectStorage interface {
	Name() string
	HealthCheck(ctx context.Context) error
	WriteArtifact(ctx context.Context, ref ArtifactRef) error
	ApplyRetention(ctx context.Context, ref ArtifactRef) error
}

// Ingress manages HTTP and gRPC exposure concerns.
type Ingress interface {
	Name() string
	HealthCheck(ctx context.Context) error
	ExposeHTTP(ctx context.Context, spec ExposureSpec) error
	ExposeGRPC(ctx context.Context, spec ExposureSpec) error
}

// Identity manages both user-facing auth integration and workload identity.
type Identity interface {
	Name() string
	HealthCheck(ctx context.Context) error
	ConfigureOIDC(ctx context.Context, issuerURL string) error
	ConfigureWorkloadIdentity(ctx context.Context, serviceAccount string) error
}

// ComputeLifecycle manages provider-backed compute clusters.
type ComputeLifecycle interface {
	Name() string
	CreateCluster(ctx context.Context, spec ClusterSpec) (*ClusterState, error)
	StartCluster(ctx context.Context, clusterID string) (*ClusterState, error)
	StopCluster(ctx context.Context, clusterID string) (*ClusterState, error)
	DrainCluster(ctx context.Context, clusterID string) (*ClusterState, error)
	DeleteCluster(ctx context.Context, clusterID string) error
	ResizeCluster(ctx context.Context, clusterID string, size ClusterSize) (*ClusterState, error)
	ResolveConnection(ctx context.Context, clusterID string) (*ConnectionInfo, error)
	ReportStatus(ctx context.Context, clusterID string) (*ClusterState, error)
}

// SecretsRuntime manages configuration and secrets injection for workloads.
type SecretsRuntime interface {
	Name() string
	HealthCheck(ctx context.Context) error
	InjectSecret(ctx context.Context, name string) error
	RotateSecret(ctx context.Context, name string) error
}

type ArtifactRef struct {
	Bucket string
	Key    string
}

type ExposureSpec struct {
	Name string
	Host string
	Port int
}

type ClusterSpec struct {
	Name          string
	WorkloadClass string
	MinReplicas   int32
	MaxReplicas   int32
	IdleTimeout   time.Duration
}

type ClusterSize struct {
	Tier     string
	MemoryGB *int64
}

type ConnectionInfo struct {
	URL       string
	AuthToken string
}

type ClusterState struct {
	ID                string
	Name              string
	DesiredState      string
	ObservedState     string
	LastActivityAt    *time.Time
	Connection        *ConnectionInfo
	AvailableReplicas int32
}

// CapabilityNotConfiguredError reports an expected provider capability that is not yet wired.
type CapabilityNotConfiguredError struct {
	Provider   string
	Capability string
}

func (e *CapabilityNotConfiguredError) Error() string {
	return fmt.Sprintf("%s provider does not have %s configured", e.Provider, e.Capability)
}

// NewProvider selects the configured platform provider.
func NewProvider(cfg *config.Config) (Provider, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}

	switch strings.ToLower(strings.TrimSpace(cfg.PlatformProvider)) {
	case ProviderManual:
		return newManualProvider(cfg), nil
	case ProviderAzure:
		return newAzureProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported PLATFORM_PROVIDER %q", cfg.PlatformProvider)
	}
}
