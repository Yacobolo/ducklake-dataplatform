package security

import (
	"context"
	servicepolicy "github.com/Yacobolo/quackstack/internal/service/policy"
)

// requireAdmin checks that the caller in context has admin privileges.
// Returns AccessDeniedError if not authenticated or not admin.
func requireAdmin(ctx context.Context) error {
	return servicepolicy.RequireAdmin(ctx)
}

// callerName returns the name of the authenticated principal from context.
func callerName(ctx context.Context) string {
	return servicepolicy.CallerName(ctx)
}
