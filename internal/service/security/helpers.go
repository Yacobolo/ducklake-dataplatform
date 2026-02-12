package security

import (
	"context"

	"duck-demo/internal/domain"
)

// requireAdmin checks that the caller in context has admin privileges.
// Returns AccessDeniedError if not authenticated or not admin.
func requireAdmin(ctx context.Context) error {
	p, ok := domain.PrincipalFromContext(ctx)
	if !ok {
		return domain.ErrAccessDenied("authentication required")
	}
	if !p.IsAdmin {
		return domain.ErrAccessDenied("admin privileges required")
	}
	return nil
}

// callerName returns the name of the authenticated principal from context.
func callerName(ctx context.Context) string {
	p, _ := domain.PrincipalFromContext(ctx)
	return p.Name
}
