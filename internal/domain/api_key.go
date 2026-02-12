package domain

import "time"

// APIKey represents an API key for programmatic access.
type APIKey struct {
	ID          int64
	PrincipalID int64
	Name        string
	KeyPrefix   string // first 8 chars for identification
	KeyHash     string // SHA-256 of raw key; raw key is never stored
	ExpiresAt   *time.Time
	CreatedAt   time.Time
}
