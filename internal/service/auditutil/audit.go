package auditutil

import (
	"context"

	"duck-demo/internal/domain"
)

func LogAllowed(ctx context.Context, audit domain.AuditRepository, principal, action, detail string) {
	logDecision(ctx, audit, principal, action, "ALLOWED", detail)
}

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
