package domain

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"time"
)

// ComputeEndpoint represents a SQL compute resource (local or remote DuckDB instance).
type ComputeEndpoint struct {
	ID          string
	ExternalID  string // UUID for logs/external integrations
	Name        string // unique, e.g. "analytics-xl"
	URL         string // e.g. "grpc://compute-1.example.com:9444" or "https://compute-1.example.com:9443"
	Type        string // "LOCAL" or "REMOTE"
	Status      string // ACTIVE, INACTIVE, STARTING, STOPPING, ERROR
	Size        string // SMALL, MEDIUM, LARGE (informational)
	MaxMemoryGB *int64
	AuthToken   string // pre-shared secret (AES-256-GCM encrypted at rest)
	Owner       string // principal who created it
	CreatedAt   time.Time
	UpdatedAt   time.Time
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
	Name        string
	URL         string
	Type        string
	Size        string
	MaxMemoryGB *int64
	AuthToken   string
}

// UpdateComputeEndpointRequest holds partial-update parameters for a compute endpoint.
type UpdateComputeEndpointRequest struct {
	URL         *string
	Size        *string
	MaxMemoryGB *int64
	AuthToken   *string
	Status      *string
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
	Status        *string
	UptimeSeconds *int
	DuckdbVersion *string
	MemoryUsedMb  *int
	MaxMemoryGb   *int
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
