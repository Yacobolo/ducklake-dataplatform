package policy

import (
	"context"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// RequireAuthenticatedPrincipal returns the caller identity or denies access.
func RequireAuthenticatedPrincipal(ctx context.Context) (domain.ContextPrincipal, error) {
	principal, ok := domain.PrincipalFromContext(ctx)
	if !ok {
		return domain.ContextPrincipal{}, domain.ErrAccessDenied("authentication required")
	}
	return principal, nil
}

// RequirePrincipalName returns the authenticated principal name when present.
func RequirePrincipalName(ctx context.Context) (string, error) {
	principal, ok := domain.PrincipalFromContext(ctx)
	if !ok || strings.TrimSpace(principal.Name) == "" {
		return "", domain.ErrAccessDenied("principal context is required")
	}
	return principal.Name, nil
}

// RequireAdmin validates that the caller is authenticated and has admin rights.
func RequireAdmin(ctx context.Context) error {
	principal, err := RequireAuthenticatedPrincipal(ctx)
	if err != nil {
		return err
	}
	if !principal.IsAdmin {
		return domain.ErrAccessDenied("admin privileges required")
	}
	return nil
}

// RequireAdminForAction validates that the caller is authenticated and has admin rights.
// The returned access denied message includes the action for clearer API responses.
func RequireAdminForAction(ctx context.Context, action string) error {
	principal, err := RequireAuthenticatedPrincipal(ctx)
	if err != nil {
		return err
	}
	if !principal.IsAdmin {
		return domain.ErrAccessDenied("%s requires admin privileges", action)
	}
	return nil
}

// RequireAdminIfPresentForAction validates admin rights when a principal is present.
// Background/system flows may call this without an authenticated principal.
func RequireAdminIfPresentForAction(ctx context.Context, action string) error {
	principal, ok := domain.PrincipalFromContext(ctx)
	if !ok {
		return nil
	}
	if !principal.IsAdmin {
		return domain.ErrAccessDenied("%s requires admin privileges", action)
	}
	return nil
}

// RequirePrincipalOrAdmin validates that the caller is authenticated and can act on the target principal.
func RequirePrincipalOrAdmin(ctx context.Context, principalID string, deniedMsg string) (domain.ContextPrincipal, error) {
	principal, err := RequireAuthenticatedPrincipal(ctx)
	if err != nil {
		return domain.ContextPrincipal{}, err
	}
	if !principal.IsAdmin && principal.ID != principalID {
		return domain.ContextPrincipal{}, domain.ErrAccessDenied("%s", deniedMsg)
	}
	return principal, nil
}

// RequireCatalogPrivilege validates that the principal can perform the given catalog-scoped action.
// If the authorization service is unavailable, only admins are allowed.
func RequireCatalogPrivilege(ctx context.Context, auth domain.AuthorizationService, principalName, privilege, authUnavailableMsgFmt string) error {
	if strings.TrimSpace(principalName) == "" {
		return domain.ErrAccessDenied("principal context is required")
	}
	principal, _ := domain.PrincipalFromContext(ctx)
	if auth == nil {
		if principal.IsAdmin {
			return nil
		}
		return domain.ErrAccessDenied(authUnavailableMsgFmt, privilege)
	}

	allowed, err := auth.CheckPrivilege(ctx, principalName, domain.SecurableCatalog, domain.CatalogID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on catalog", principalName, privilege)
	}
	return nil
}

// RequireSecurablePrivilege validates that the principal can perform the given action on a securable.
func RequireSecurablePrivilege(ctx context.Context, auth domain.AuthorizationService, principalName, securableType, securableID, privilege string) error {
	if strings.TrimSpace(principalName) == "" {
		return domain.ErrAccessDenied("principal context is required")
	}
	allowed, err := auth.CheckPrivilege(ctx, principalName, securableType, securableID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on %s %q", principalName, privilege, securableType, securableID)
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
