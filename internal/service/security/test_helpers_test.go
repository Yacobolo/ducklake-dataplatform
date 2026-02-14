package security

import (
	"context"

	"duck-demo/internal/domain"
)

// adminCtx returns a context with an admin principal for testing.
func adminCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		ID: "admin-id", Name: "admin-user", IsAdmin: true, Type: "user",
	})
}

// nonAdminCtx returns a context with a non-admin principal for testing.
func nonAdminCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		ID: "non-admin-id", Name: "regular-user", IsAdmin: false, Type: "user",
	})
}

// principalCtx returns a context with a specific principal ID.
func principalCtx(id, name string, isAdmin bool) context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		ID: id, Name: name, IsAdmin: isAdmin, Type: "user",
	})
}
