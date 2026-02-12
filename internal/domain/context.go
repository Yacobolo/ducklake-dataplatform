package domain

import "context"

type principalKey struct{}

// ContextPrincipal carries the authenticated identity through request context.
type ContextPrincipal struct {
	Name    string
	IsAdmin bool
	Type    string // "user" or "service_principal"
}

// WithPrincipal stores a ContextPrincipal in the context.
func WithPrincipal(ctx context.Context, p ContextPrincipal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFromContext extracts the ContextPrincipal from the context.
func PrincipalFromContext(ctx context.Context) (ContextPrincipal, bool) {
	p, ok := ctx.Value(principalKey{}).(ContextPrincipal)
	return p, ok
}
