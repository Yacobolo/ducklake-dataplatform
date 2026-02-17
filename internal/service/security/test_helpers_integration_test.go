//go:build integration

package security

import (
	"context"

	"duck-demo/internal/domain"
)

// principalCtx returns a context with a specific principal ID.
func principalCtx(id, name string, isAdmin bool) context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		ID: id, Name: name, IsAdmin: isAdmin, Type: "user",
	})
}
