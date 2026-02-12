package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"duck-demo/internal/config"
	"duck-demo/internal/domain"
)

// PrincipalProvisioner resolves or creates a principal from an external identity.
type PrincipalProvisioner interface {
	ResolveOrProvision(ctx context.Context, issuer, externalID, displayName string, isBootstrap bool) (*domain.Principal, error)
}

// APIKeyLookup abstracts the API key verification store.
type APIKeyLookup interface {
	LookupPrincipalByAPIKeyHash(ctx context.Context, keyHash string) (string, error)
}

// PrincipalLookup resolves a principal name to a full Principal object.
type PrincipalLookup interface {
	GetByName(ctx context.Context, name string) (*domain.Principal, error)
}

// Authenticator handles JWT and API key authentication.
type Authenticator struct {
	jwtValidator  JWTValidator
	apiKeyLookup  APIKeyLookup
	principalRepo PrincipalLookup
	provisioner   PrincipalProvisioner
	cfg           config.AuthConfig
	logger        *slog.Logger
}

// NewAuthenticator creates a new Authenticator with the given dependencies.
func NewAuthenticator(
	jwtValidator JWTValidator,
	apiKeyLookup APIKeyLookup,
	principalRepo PrincipalLookup,
	provisioner PrincipalProvisioner,
	cfg config.AuthConfig,
	logger *slog.Logger,
) *Authenticator {
	if logger == nil {
		logger = slog.Default()
	}
	return &Authenticator{
		jwtValidator:  jwtValidator,
		apiKeyLookup:  apiKeyLookup,
		principalRepo: principalRepo,
		provisioner:   provisioner,
		cfg:           cfg,
		logger:        logger,
	}
}

// Middleware returns an HTTP middleware that authenticates requests.
func (a *Authenticator) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			// Try JWT Bearer token first.
			if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				tokenStr := strings.TrimPrefix(auth, "Bearer ")
				if principal, err := a.authenticateJWT(ctx, tokenStr); err == nil {
					ctx = domain.WithPrincipal(ctx, *principal)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}

			// Try API Key.
			if a.cfg.APIKeyEnabled {
				if apiKey := r.Header.Get(a.cfg.APIKeyHeader); apiKey != "" && a.apiKeyLookup != nil {
					if principal, err := a.authenticateAPIKey(ctx, apiKey); err == nil {
						ctx = domain.WithPrincipal(ctx, *principal)
						next.ServeHTTP(w, r.WithContext(ctx))
						return
					}
				}
			}

			// Both methods failed.
			writeUnauthorized(w)
		})
	}
}

// authenticateJWT validates the JWT and resolves the principal.
func (a *Authenticator) authenticateJWT(ctx context.Context, tokenStr string) (*domain.ContextPrincipal, error) {
	if a.jwtValidator == nil {
		return nil, fmt.Errorf("no JWT validator configured")
	}

	claims, err := a.jwtValidator.Validate(ctx, tokenStr)
	if err != nil {
		return nil, err
	}

	if claims.Subject == "" {
		return nil, fmt.Errorf("JWT missing sub claim")
	}

	// Resolve principal name from configured claim chain.
	displayName := a.resolveDisplayName(claims)

	// If we have a provisioner, use JIT provisioning.
	if a.provisioner != nil {
		issuer := claims.Issuer
		isBootstrap := a.cfg.BootstrapAdmin != "" && claims.Subject == a.cfg.BootstrapAdmin

		p, err := a.provisioner.ResolveOrProvision(ctx, issuer, claims.Subject, displayName, isBootstrap)
		if err != nil {
			a.logger.Error("JIT provisioning failed", "error", err, "sub", claims.Subject)
			return nil, fmt.Errorf("principal resolution failed: %w", err)
		}
		return &domain.ContextPrincipal{
			Name:    p.Name,
			IsAdmin: p.IsAdmin,
			Type:    p.Type,
		}, nil
	}

	// Without a provisioner, look up by name (legacy HS256 path).
	if a.principalRepo != nil {
		p, err := a.principalRepo.GetByName(ctx, displayName)
		if err == nil {
			return &domain.ContextPrincipal{
				Name:    p.Name,
				IsAdmin: p.IsAdmin,
				Type:    p.Type,
			}, nil
		}
	}

	// Fallback: use the display name directly (backward compat with shared secret).
	return &domain.ContextPrincipal{
		Name: displayName,
		Type: "user",
	}, nil
}

// authenticateAPIKey validates an API key and resolves the principal.
func (a *Authenticator) authenticateAPIKey(ctx context.Context, rawKey string) (*domain.ContextPrincipal, error) {
	hash := sha256.Sum256([]byte(rawKey))
	hashStr := hex.EncodeToString(hash[:])

	principalName, err := a.apiKeyLookup.LookupPrincipalByAPIKeyHash(ctx, hashStr)
	if err != nil {
		return nil, err
	}

	// Look up the full principal to get IsAdmin.
	if a.principalRepo != nil {
		if p, err := a.principalRepo.GetByName(ctx, principalName); err == nil {
			return &domain.ContextPrincipal{
				Name:    p.Name,
				IsAdmin: p.IsAdmin,
				Type:    p.Type,
			}, nil
		}
	}

	return &domain.ContextPrincipal{
		Name: principalName,
		Type: "user",
	}, nil
}

// resolveDisplayName extracts the principal name from JWT claims
// using the configured claim chain (default: email -> preferred_username -> sub).
func (a *Authenticator) resolveDisplayName(claims *JWTClaims) string {
	// Try the configured primary claim.
	if a.cfg.NameClaim != "" && a.cfg.NameClaim != "sub" {
		if val, ok := claims.Raw[a.cfg.NameClaim].(string); ok && val != "" {
			return sanitizePrincipalName(val)
		}
	}

	// Fallback chain: email -> preferred_username -> sub.
	if claims.Email != nil && *claims.Email != "" {
		return sanitizePrincipalName(*claims.Email)
	}
	if pref, ok := claims.Raw["preferred_username"].(string); ok && pref != "" {
		return sanitizePrincipalName(pref)
	}
	return sanitizePrincipalName(claims.Subject)
}

// sanitizePrincipalName normalizes a principal name.
func sanitizePrincipalName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	if len(name) > 255 {
		name = name[:255]
	}
	return name
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    401,
		"message": "unauthorized: provide a valid JWT Bearer token or API key",
	})
}

// === Backward-compatible helpers ===

// WithPrincipal stores the principal name in the context.
//
// Deprecated: Use domain.WithPrincipal with a ContextPrincipal for enriched context.
func WithPrincipal(ctx context.Context, name string) context.Context {
	return domain.WithPrincipal(ctx, domain.ContextPrincipal{Name: name, Type: "user"})
}

// PrincipalFromContext extracts the principal name from the context.
//
// Deprecated: Use domain.PrincipalFromContext for enriched context.
func PrincipalFromContext(ctx context.Context) (string, bool) {
	p, ok := domain.PrincipalFromContext(ctx)
	return p.Name, ok
}

// AuthMiddleware is a backward-compatible wrapper that creates an Authenticator
// with a shared-secret JWT validator. Used when OIDC is not configured.
func AuthMiddleware(jwtSecret []byte, apiKeys APIKeyLookup) func(http.Handler) http.Handler {
	var validator JWTValidator
	if len(jwtSecret) > 0 {
		validator = NewSharedSecretValidator(string(jwtSecret))
	}
	auth := &Authenticator{
		jwtValidator: validator,
		apiKeyLookup: apiKeys,
		cfg: config.AuthConfig{
			APIKeyEnabled: true,
			APIKeyHeader:  "X-API-Key",
			NameClaim:     "sub",
		},
		logger: slog.Default(),
	}
	return auth.Middleware()
}
