package middleware

import (
	"context"
	"net/http"
	"regexp"

	"github.com/google/uuid"
)

type requestIDKey struct{}

// validRequestID matches alphanumeric characters, hyphens, and underscores, max 128 chars.
var validRequestID = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// RequestID returns an HTTP middleware that assigns a unique request ID to each
// request. If the incoming request contains a valid X-Request-ID header, it
// is reused; otherwise a new UUID is generated. The header is validated to
// contain only alphanumeric characters, hyphens, and underscores (max 128 chars)
// to prevent log-forging attacks.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if !isValidRequestID(id) {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// isValidRequestID checks that the ID is non-empty, at most 128 characters,
// and contains only alphanumeric characters, hyphens, and underscores.
func isValidRequestID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	return validRequestID.MatchString(id)
}

// RequestIDFromContext extracts the request ID from the context.
// Returns an empty string if no request ID is present.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
