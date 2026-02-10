package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	dbstore "duck-demo/db/catalog"

	"github.com/golang-jwt/jwt/v5"
)

type principalKey struct{}

// WithPrincipal stores the principal name in the context.
func WithPrincipal(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, principalKey{}, name)
}

// PrincipalFromContext extracts the principal name from the context.
func PrincipalFromContext(ctx context.Context) (string, bool) {
	name, ok := ctx.Value(principalKey{}).(string)
	return name, ok
}

// AuthMiddleware tries JWT first, then API key. Returns 401 if both fail.
func AuthMiddleware(jwtSecret []byte, apiKeyQueries *dbstore.Queries) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Try JWT Bearer token
			if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				tokenStr := strings.TrimPrefix(auth, "Bearer ")
				token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
					return jwtSecret, nil
				}, jwt.WithValidMethods([]string{"HS256"}))

				if err == nil && token.Valid {
					if claims, ok := token.Claims.(jwt.MapClaims); ok {
						if sub, ok := claims["sub"].(string); ok && sub != "" {
							ctx := WithPrincipal(r.Context(), sub)
							next.ServeHTTP(w, r.WithContext(ctx))
							return
						}
					}
				}
			}

			// Try API Key
			if apiKey := r.Header.Get("X-API-Key"); apiKey != "" && apiKeyQueries != nil {
				hash := sha256.Sum256([]byte(apiKey))
				hashStr := hex.EncodeToString(hash[:])

				row, err := apiKeyQueries.GetAPIKeyByHash(r.Context(), hashStr)
				if err == nil {
					ctx := WithPrincipal(r.Context(), row.PrincipalName)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}

			// Both methods failed
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"code":    401,
				"message": "unauthorized: provide a valid JWT Bearer token or API key",
			})
		})
	}
}
