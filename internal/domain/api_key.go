package domain

import (
	"strings"
	"time"
)

// APIKey represents an API key for programmatic access.
type APIKey struct {
	ID          string
	PrincipalID string
	Name        string
	KeyPrefix   string // first 8 chars for identification
	KeyHash     string // SHA-256 of raw key; raw key is never stored
	ExpiresAt   *time.Time
	CreatedAt   time.Time
}

// CreateAPIKeyRequest holds parameters for creating a new API key.
type CreateAPIKeyRequest struct {
	PrincipalID string
	Name        string
	ExpiresAt   *time.Time
}

// Validate checks that the request is well-formed.
func (r *CreateAPIKeyRequest) Validate() error {
	if r.PrincipalID == "" {
		return ErrValidation("principal_id is required")
	}
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("api key name is required")
	}
	if r.ExpiresAt != nil && r.ExpiresAt.Before(time.Now()) {
		return ErrValidation("expires_at must be in the future")
	}
	return nil
}
