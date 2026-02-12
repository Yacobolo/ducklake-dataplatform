package security

import (
	"context"

	"duck-demo/internal/domain"
)

// adminCtx returns a context with an admin principal for testing.
func adminCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name: "admin-user", IsAdmin: true, Type: "user",
	})
}

// nonAdminCtx returns a context with a non-admin principal for testing.
func nonAdminCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name: "regular-user", IsAdmin: false, Type: "user",
	})
}
