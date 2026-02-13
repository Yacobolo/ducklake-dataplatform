package domain

import "time"

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
	PrincipalID int64
	Name        string
	ExpiresAt   *time.Time
}

// Validate checks that the request is well-formed.
func (r *CreateAPIKeyRequest) Validate() error {
	if r.PrincipalID <= 0 {
		return ErrValidation("principal_id is required")
	}
	if r.Name == "" {
		return ErrValidation("api key name is required")
	}
	return nil
}
