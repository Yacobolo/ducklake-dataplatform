package governance

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
