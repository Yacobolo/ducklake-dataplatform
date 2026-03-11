package policy

import (
	"context"

	"duck-demo/internal/domain"
)

// RequireAdmin validates that the caller is authenticated and has admin rights.
func RequireAdmin(ctx context.Context) error {
	principal, ok := domain.PrincipalFromContext(ctx)
	if !ok {
		return domain.ErrAccessDenied("authentication required")
	}
	if !principal.IsAdmin {
		return domain.ErrAccessDenied("admin privileges required")
	}
	return nil
}

// IsAdmin reports whether the authenticated caller has admin rights.
func IsAdmin(ctx context.Context) bool {
	principal, ok := domain.PrincipalFromContext(ctx)
	return ok && principal.IsAdmin
}

// CallerName returns the authenticated principal name if present.
func CallerName(ctx context.Context) string {
	principal, _ := domain.PrincipalFromContext(ctx)
	return principal.Name
}

// CanReadOwnedResource grants read access to admins and exact owners.
func CanReadOwnedResource(ctx context.Context, principal, owner string) bool {
	if IsAdmin(ctx) {
		return true
	}
	return owner != "" && owner == principal
}
