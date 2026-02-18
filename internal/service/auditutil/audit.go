// Package auditutil provides shared audit logging helpers for service-layer
// authorization and mutation events.
package auditutil

import (
	"context"

	"duck-demo/internal/domain"
)

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
	_ = audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        status,
		OriginalSQL:   &detail,
	})
}
