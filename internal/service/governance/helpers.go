package governance

import (
	"context"
	servicepolicy "duck-demo/internal/service/policy"
)

// requireAdmin checks that the caller in context has admin privileges.
// Returns AccessDeniedError if not authenticated or not admin.
func requireAdmin(ctx context.Context) error {
	return servicepolicy.RequireAdmin(ctx)
}
