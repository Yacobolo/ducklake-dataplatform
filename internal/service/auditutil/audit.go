// Package auditutil provides shared audit logging helpers for service-layer
// authorization and mutation events.
package auditutil

import (
	"context"
	"time"

	"duck-demo/internal/domain"
)

const (
	auditInsertRetries = 3
	auditRetryDelay    = 25 * time.Millisecond
)

// Insert persists an audit entry with small bounded retries and warning logs.
func Insert(ctx context.Context, audit domain.AuditRepository, entry *domain.AuditEntry) error {
	if audit == nil {
		return nil
	}

	var lastErr error
	for attempt := 1; attempt <= auditInsertRetries; attempt++ {
		err := audit.Insert(ctx, entry)
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt < auditInsertRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(auditRetryDelay):
			}
		}
	}

	return lastErr
}

// LogAllowed records an ALLOWED audit decision for the given action.
func LogAllowed(ctx context.Context, audit domain.AuditRepository, principal, action, detail string) {
	logDecision(ctx, audit, principal, action, "ALLOWED", detail)
}

// LogDenied records a DENIED audit decision for the given action.
func LogDenied(ctx context.Context, audit domain.AuditRepository, principal, action, detail string) {
	logDecision(ctx, audit, principal, action, "DENIED", detail)
}

func logDecision(ctx context.Context, audit domain.AuditRepository, principal, action, status, detail string) {
	if audit == nil {
		return
	}
	_ = Insert(ctx, audit, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        status,
		OriginalSQL:   &detail,
	})
}
