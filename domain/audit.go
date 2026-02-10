package domain

import "time"

// AuditEntry represents a single audit log record.
type AuditEntry struct {
	ID             int64
	PrincipalName  string
	Action         string
	StatementType  *string
	OriginalSQL    *string
	RewrittenSQL   *string
	TablesAccessed []string
	Status         string // "ALLOWED", "DENIED", "ERROR"
	ErrorMessage   *string
	DurationMs     *int64
	CreatedAt      time.Time
}
