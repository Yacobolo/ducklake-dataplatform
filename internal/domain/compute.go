package domain

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"time"
)

// Compute routing, selection-policy, workload, and readiness constants.
const (
	ComputeModeAuto           = "AUTO"
	ComputeModeByocLocal      = "BYOC_LOCAL"
	ComputeModeSharedEndpoint = "SHARED_ENDPOINT"

	ComputeWorkloadInteractive = "INTERACTIVE"
	ComputeWorkloadScheduled   = "SCHEDULED"
	ComputeWorkloadNotebook    = "NOTEBOOK"
	ComputeWorkloadHeavy       = "HEAVY"

	ComputeSelectionPolicyAdminOnly   = "ADMIN_ONLY"
	ComputeSelectionPolicyAllowedOnly = "ALLOWED_USERS"
	ComputeSelectionPolicySelfService = "SELF_SERVICE"

	ComputeEndpointWorkloadInteractive = "INTERACTIVE"
	ComputeEndpointWorkloadScheduled   = "SCHEDULED"
	ComputeEndpointWorkloadHeavy       = "HEAVY"
	ComputeEndpointWorkloadMixed       = "MIXED"

	ComputeReadinessReady       = "READY"
	ComputeReadinessDegraded    = "DEGRADED"
	ComputeReadinessUnavailable = "UNAVAILABLE"

	ComputeProviderManual = "manual"
	ComputeProviderAzure  = "azure"
	ComputeProviderAWS    = "aws"
	ComputeProviderGCP    = "gcp"

	ManagedClusterDesiredRunning  = "RUNNING"
	ManagedClusterDesiredStopped  = "STOPPED"
	ManagedClusterDesiredDraining = "DRAINING"
	ManagedClusterDesiredDeleting = "DELETING"

	ManagedClusterObservedStarting = "STARTING"
	ManagedClusterObservedReady    = "READY"
	ManagedClusterObservedDegraded = "DEGRADED"
	ManagedClusterObservedStopped  = "STOPPED"
	ManagedClusterObservedError    = "ERROR"
)

// ComputeEndpoint represents a SQL compute resource (local or remote DuckDB instance).
type ComputeEndpoint struct {
	ID                         string
	ExternalID                 string // UUID for logs/external integrations
	Name                       string // unique, e.g. "analytics-xl"
	URL                        string // e.g. "grpc://compute-1.example.com:9444" or "https://compute-1.example.com:9443"
	Type                       string // "LOCAL" or "REMOTE"
	Status                     string // ACTIVE, INACTIVE, STARTING, STOPPING, ERROR
	SelectionPolicy            string // ADMIN_ONLY, ALLOWED_USERS, SELF_SERVICE
	WorkloadClass              string // INTERACTIVE, SCHEDULED, HEAVY, MIXED
	ReadinessStatus            string // READY, DEGRADED, UNAVAILABLE
	Size                       string // SMALL, MEDIUM, LARGE (informational)
	MaxMemoryGB                *int64
	MaxConcurrency             *int64
	MaxResultSizeMB            *int64
	RecommendedForLargeQueries bool
	IsDraining                 bool
	LastHealthStatus           *string
	LastHealthCheckedAt        *time.Time
	ActiveQueries              *int64
	QueuedJobs                 *int64
	RunningJobs                *int64
	CompletedJobs              *int64
	StoredJobs                 *int64
	CleanedJobs                *int64
	QueryResultTTLSeconds      *int64
	ManagedBacking             *ComputeEndpointBacking
	LastActivityAt             *time.Time
	AuthToken                  string // pre-shared secret (AES-256-GCM encrypted at rest)
	Owner                      string // principal who created it
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
}

// ComputeEndpointBacking points a logical endpoint at a provider-managed cluster.
type ComputeEndpointBacking struct {
	Provider         string
	ManagedClusterID string
	TemplateID       string
}

// ComputeAssignment binds a principal to a compute endpoint.
type ComputeAssignment struct {
	ID            string
	PrincipalID   string
	PrincipalType string // "user" or "group"
	EndpointID    string
	EndpointName  string // populated on reads (from join)
	IsDefault     bool
	FallbackLocal bool // if true, fall back to local compute when remote is unavailable
	CreatedAt     time.Time
}

// CreateComputeEndpointRequest holds parameters for creating a compute endpoint.
type CreateComputeEndpointRequest struct {
	Name                       string
	URL                        string
	Type                       string
	SelectionPolicy            string
	WorkloadClass              string
	ReadinessStatus            string
	Size                       string
	MaxMemoryGB                *int64
	MaxConcurrency             *int64
	MaxResultSizeMB            *int64
	RecommendedForLargeQueries bool
	IsDraining                 bool
	AuthToken                  string
}

// UpdateComputeEndpointRequest holds partial-update parameters for a compute endpoint.
type UpdateComputeEndpointRequest struct {
	URL                        *string
	Size                       *string
	MaxMemoryGB                *int64
	MaxConcurrency             *int64
	MaxResultSizeMB            *int64
	SelectionPolicy            *string
	WorkloadClass              *string
	ReadinessStatus            *string
	RecommendedForLargeQueries *bool
	IsDraining                 *bool
	AuthToken                  *string
	Status                     *string
}

// Validate checks that the request is well-formed.
func (r *CreateComputeEndpointRequest) Validate() error {
	return ValidateCreateComputeEndpointRequest(*r)
}

// ValidateCreateComputeEndpointRequest validates the create request.
func ValidateCreateComputeEndpointRequest(r CreateComputeEndpointRequest) error {
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if r.URL == "" {
		return ErrValidation("url is required")
	}
	switch r.Type {
	case "LOCAL", "REMOTE":
		// valid
	case "":
		return ErrValidation("type is required (LOCAL or REMOTE)")
	default:
		return ErrValidation("type must be LOCAL or REMOTE, got %q", r.Type)
	}
	if r.Size != "" {
		switch r.Size {
		case "SMALL", "MEDIUM", "LARGE":
			// valid
		default:
			return ErrValidation("size must be SMALL, MEDIUM, or LARGE, got %q", r.Size)
		}
	}
	if r.MaxMemoryGB != nil && *r.MaxMemoryGB <= 0 {
		return ErrValidation("max_memory_gb must be greater than zero")
	}
	if r.MaxConcurrency != nil && *r.MaxConcurrency <= 0 {
		return ErrValidation("max_concurrency must be greater than zero")
	}
	if r.MaxResultSizeMB != nil && *r.MaxResultSizeMB <= 0 {
		return ErrValidation("max_result_size_mb must be greater than zero")
	}
	if err := validateComputeSelectionPolicy(r.SelectionPolicy); err != nil {
		return err
	}
	if err := validateComputeEndpointWorkloadClass(r.WorkloadClass); err != nil {
		return err
	}
	if err := validateComputeReadinessStatus(r.ReadinessStatus); err != nil {
		return err
	}
	if err := ValidateComputeEndpointURL(r.URL, r.Type); err != nil {
		return err
	}
	return nil
}

// ValidateComputeEndpointURL validates endpoint URL requirements by endpoint type.
func ValidateComputeEndpointURL(rawURL, endpointType string) error {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return ErrValidation("url is required")
	}

	if endpointType != "REMOTE" {
		return nil
	}

	u, err := url.Parse(trimmed)
	if err != nil {
		return ErrValidation("url must be a valid URI")
	}
	if u.Host == "" {
		return ErrValidation("remote url must include host")
	}
	scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
	if scheme != "grpc" && scheme != "grpcs" {
		return ErrValidation("remote url must use grpc:// or grpcs://")
	}

	return nil
}

// CreateComputeAssignmentRequest holds parameters for assigning a principal to an endpoint.
type CreateComputeAssignmentRequest struct {
	PrincipalID   string
	PrincipalType string
	IsDefault     bool
	FallbackLocal bool
}

// Validate checks that the request is well-formed.
func (r *CreateComputeAssignmentRequest) Validate() error {
	return ValidateCreateComputeAssignmentRequest(*r)
}

// ValidateCreateComputeAssignmentRequest validates the assignment create request.
func ValidateCreateComputeAssignmentRequest(r CreateComputeAssignmentRequest) error {
	if r.PrincipalID == "" {
		return ErrValidation("principal_id is required")
	}
	switch r.PrincipalType {
	case "user", "group":
		// valid
	case "":
		return ErrValidation("principal_type is required (user or group)")
	default:
		return ErrValidation("principal_type must be user or group, got %q", r.PrincipalType)
	}
	return nil
}

// ComputeEndpointHealthResult holds the health status returned from a remote agent.
type ComputeEndpointHealthResult struct {
	Status                *string
	UptimeSeconds         *int
	DuckdbVersion         *string
	MemoryUsedMb          *int
	MaxMemoryGb           *int
	ActiveQueries         *int64
	QueuedJobs            *int64
	RunningJobs           *int64
	CompletedJobs         *int64
	StoredJobs            *int64
	CleanedJobs           *int64
	QueryResultTTLSeconds *int
}

// ComputeExecutionRequest captures a caller's compute routing preference.
type ComputeExecutionRequest struct {
	Mode                  string
	EndpointName          string
	WorkloadType          string
	AuthoritativeEndpoint bool
	FallbackLocal         bool
}

// ComputeRoutingDefaults stores the platform defaults for workload routing.
type ComputeRoutingDefaults struct {
	InteractiveMode string
	ScheduledMode   string
	NotebookMode    string
}

// ComputeTarget is a principal-visible compute option for API/CLI selection.
type ComputeTarget struct {
	Mode                     string
	EndpointName             *string
	EndpointType             *string
	DisplayName              string
	Status                   string
	SuitabilityLabels        []string
	AvailabilityReason       *string
	IsDefault                bool
	SelectableForInteractive bool
	SelectableForScheduled   bool
}

// ComputeClusterTemplate describes a provider-neutral cluster template managed by the platform.
type ComputeClusterTemplate struct {
	ID                     string
	Name                   string
	Provider               string
	WorkloadClass          string
	Size                   string
	MinReplicas            int32
	MaxReplicas            int32
	IdleAutoStopSeconds    int64
	ScalingPolicy          string
	StorageProfile         string
	ResultRetentionSeconds int64
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// ManagedComputeCluster represents a provider-backed compute cluster bound to a logical endpoint.
type ManagedComputeCluster struct {
	ID             string
	Name           string
	TemplateID     string
	EndpointID     string
	Provider       string
	ExternalID     string
	DesiredState   string
	ObservedState  string
	MinReplicas    int32
	MaxReplicas    int32
	IsDraining     bool
	LastActivityAt *time.Time
	EndpointURL    *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateComputeClusterTemplateRequest holds parameters for creating a managed cluster template.
type CreateComputeClusterTemplateRequest struct {
	Name                   string
	Provider               string
	WorkloadClass          string
	Size                   string
	MinReplicas            int32
	MaxReplicas            int32
	IdleAutoStopSeconds    int64
	ScalingPolicy          string
	StorageProfile         string
	ResultRetentionSeconds int64
}

// CreateManagedComputeClusterRequest holds parameters for creating a managed cluster.
type CreateManagedComputeClusterRequest struct {
	Name         string
	TemplateID   string
	EndpointID   string
	Provider     string
	DesiredState string
	MinReplicas  int32
	MaxReplicas  int32
}

// Validate normalizes and validates a compute execution request.
func (r ComputeExecutionRequest) Validate() error {
	if strings.TrimSpace(r.Mode) == "" {
		return nil
	}

	switch strings.ToUpper(strings.TrimSpace(r.Mode)) {
	case ComputeModeAuto, ComputeModeByocLocal, ComputeModeSharedEndpoint:
	default:
		return ErrValidation("compute mode must be AUTO, BYOC_LOCAL, or SHARED_ENDPOINT, got %q", r.Mode)
	}

	if strings.TrimSpace(r.WorkloadType) != "" {
		switch strings.ToUpper(strings.TrimSpace(r.WorkloadType)) {
		case ComputeWorkloadInteractive, ComputeWorkloadScheduled, ComputeWorkloadNotebook, ComputeWorkloadHeavy:
		default:
			return ErrValidation("workload type must be INTERACTIVE, SCHEDULED, NOTEBOOK, or HEAVY, got %q", r.WorkloadType)
		}
	}

	if strings.EqualFold(strings.TrimSpace(r.Mode), ComputeModeByocLocal) && strings.TrimSpace(r.EndpointName) != "" {
		return ErrValidation("endpoint_name cannot be set when compute mode is BYOC_LOCAL")
	}

	return nil
}

// Normalize returns a copy with canonical enum casing and trimmed strings.
func (r ComputeExecutionRequest) Normalize() ComputeExecutionRequest {
	r.Mode = strings.ToUpper(strings.TrimSpace(r.Mode))
	r.EndpointName = strings.TrimSpace(r.EndpointName)
	r.WorkloadType = strings.ToUpper(strings.TrimSpace(r.WorkloadType))
	return r
}

// Normalize fills defaults and canonical casing for routing defaults.
func (d ComputeRoutingDefaults) Normalize() ComputeRoutingDefaults {
	d.InteractiveMode = normalizeComputeDefaultMode(d.InteractiveMode, ComputeModeByocLocal)
	d.ScheduledMode = normalizeComputeDefaultMode(d.ScheduledMode, ComputeModeSharedEndpoint)
	d.NotebookMode = normalizeComputeDefaultMode(d.NotebookMode, ComputeModeSharedEndpoint)
	return d
}

// Validate checks that the default routing configuration is well-formed.
func (d ComputeRoutingDefaults) Validate() error {
	norm := d.Normalize()
	modes := []string{norm.InteractiveMode, norm.ScheduledMode, norm.NotebookMode}
	for _, mode := range modes {
		switch mode {
		case ComputeModeAuto, ComputeModeByocLocal, ComputeModeSharedEndpoint:
		default:
			return ErrValidation("invalid compute routing default mode %q", mode)
		}
	}
	return nil
}

func normalizeComputeDefaultMode(mode, fallback string) string {
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" {
		return fallback
	}
	return mode
}

// Validate checks that the request is well-formed.
func (r *CreateComputeClusterTemplateRequest) Validate() error {
	return ValidateCreateComputeClusterTemplateRequest(*r)
}

// ValidateCreateComputeClusterTemplateRequest validates a managed cluster template create request.
func ValidateCreateComputeClusterTemplateRequest(r CreateComputeClusterTemplateRequest) error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("name is required")
	}
	if err := validateComputeProvider(r.Provider); err != nil {
		return err
	}
	if err := validateComputeEndpointWorkloadClass(r.WorkloadClass); err != nil {
		return err
	}
	if r.MinReplicas < 0 {
		return ErrValidation("min_replicas must be greater than or equal to zero")
	}
	if r.MaxReplicas <= 0 {
		return ErrValidation("max_replicas must be greater than zero")
	}
	if r.MaxReplicas < r.MinReplicas {
		return ErrValidation("max_replicas must be greater than or equal to min_replicas")
	}
	if r.IdleAutoStopSeconds < 0 {
		return ErrValidation("idle_auto_stop_seconds must be greater than or equal to zero")
	}
	if r.ResultRetentionSeconds < 0 {
		return ErrValidation("result_retention_seconds must be greater than or equal to zero")
	}
	return nil
}

// Validate checks that the request is well-formed.
func (r *CreateManagedComputeClusterRequest) Validate() error {
	return ValidateCreateManagedComputeClusterRequest(*r)
}

// ValidateCreateManagedComputeClusterRequest validates a managed cluster create request.
func ValidateCreateManagedComputeClusterRequest(r CreateManagedComputeClusterRequest) error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("name is required")
	}
	if strings.TrimSpace(r.TemplateID) == "" {
		return ErrValidation("template_id is required")
	}
	if strings.TrimSpace(r.EndpointID) == "" {
		return ErrValidation("endpoint_id is required")
	}
	if strings.TrimSpace(r.Provider) != "" {
		if err := validateComputeProvider(r.Provider); err != nil {
			return err
		}
	}
	if strings.TrimSpace(r.DesiredState) != "" {
		if err := validateManagedClusterDesiredState(r.DesiredState); err != nil {
			return err
		}
	}
	if r.MinReplicas < 0 {
		return ErrValidation("min_replicas must be greater than or equal to zero")
	}
	if r.MaxReplicas < 0 {
		return ErrValidation("max_replicas must be greater than or equal to zero")
	}
	if r.MaxReplicas > 0 && r.MaxReplicas < r.MinReplicas {
		return ErrValidation("max_replicas must be greater than or equal to min_replicas")
	}
	return nil
}

func validateComputeProvider(provider string) error {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case ComputeProviderManual, ComputeProviderAzure, ComputeProviderAWS, ComputeProviderGCP:
		return nil
	default:
		return ErrValidation("provider must be manual, azure, aws, or gcp, got %q", provider)
	}
}

func validateManagedClusterDesiredState(state string) error {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case ManagedClusterDesiredRunning, ManagedClusterDesiredStopped, ManagedClusterDesiredDraining, ManagedClusterDesiredDeleting:
		return nil
	default:
		return ErrValidation("desired_state must be RUNNING, STOPPED, DRAINING, or DELETING, got %q", state)
	}
}

func validateComputeSelectionPolicy(policy string) error {
	switch normalizeComputeSelectionPolicy(policy) {
	case ComputeSelectionPolicyAllowedOnly, ComputeSelectionPolicyAdminOnly, ComputeSelectionPolicySelfService:
		return nil
	default:
		return ErrValidation("selection_policy must be ADMIN_ONLY, ALLOWED_USERS, or SELF_SERVICE, got %q", policy)
	}
}

func normalizeComputeSelectionPolicy(policy string) string {
	policy = strings.ToUpper(strings.TrimSpace(policy))
	if policy == "" {
		return ComputeSelectionPolicyAllowedOnly
	}
	return policy
}

func validateComputeEndpointWorkloadClass(workload string) error {
	switch normalizeComputeEndpointWorkloadClass(workload) {
	case ComputeEndpointWorkloadInteractive, ComputeEndpointWorkloadScheduled, ComputeEndpointWorkloadHeavy, ComputeEndpointWorkloadMixed:
		return nil
	default:
		return ErrValidation("workload_class must be INTERACTIVE, SCHEDULED, HEAVY, or MIXED, got %q", workload)
	}
}

func normalizeComputeEndpointWorkloadClass(workload string) string {
	workload = strings.ToUpper(strings.TrimSpace(workload))
	if workload == "" {
		return ComputeEndpointWorkloadMixed
	}
	return workload
}

func validateComputeReadinessStatus(status string) error {
	switch normalizeComputeReadinessStatus(status) {
	case ComputeReadinessReady, ComputeReadinessDegraded, ComputeReadinessUnavailable:
		return nil
	default:
		return ErrValidation("readiness_status must be READY, DEGRADED, or UNAVAILABLE, got %q", status)
	}
}

func normalizeComputeReadinessStatus(status string) string {
	status = strings.ToUpper(strings.TrimSpace(status))
	if status == "" {
		return ComputeReadinessReady
	}
	return status
}

// ComputeExecutor executes pre-secured SQL on a compute resource.
type ComputeExecutor interface {
	QueryContext(ctx context.Context, query string) (*sql.Rows, error)
}

// ComputeResolver resolves a principal to a ComputeExecutor.
// Returns nil when no compute endpoint is assigned (engine uses local DB).
type ComputeResolver interface {
	Resolve(ctx context.Context, principalName string) (ComputeExecutor, error)
}
