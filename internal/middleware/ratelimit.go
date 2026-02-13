package middleware

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// RateLimitConfig holds configuration for the rate limiter middleware.
type RateLimitConfig struct {
	// RequestsPerSecond is the sustained rate limit (tokens added per second).
	RequestsPerSecond float64
	// Burst is the maximum number of requests allowed in a burst.
	Burst int
}

// RateLimiter returns an HTTP middleware that enforces a global token-bucket
// rate limit. When the limit is exceeded, it responds with 429 Too Many Requests
// and sets standard rate-limit headers.
func RateLimiter(cfg RateLimitConfig) func(http.Handler) http.Handler {
	limiter := rate.NewLimiter(rate.Limit(cfg.RequestsPerSecond), cfg.Burst)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reservation := limiter.Reserve()
			if !reservation.OK() {
				// Limiter cannot grant the request even with infinite wait.
				writeTooManyRequests(w, 0)
				return
			}

			delay := reservation.Delay()
			if delay > 0 {
				// Request would exceed the rate — cancel the reservation and reject.
				reservation.Cancel()
				retryAfter := int(delay.Seconds()) + 1
				writeTooManyRequests(w, retryAfter)
				return
			}

			// Set rate-limit headers on all responses.
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(cfg.Burst))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(int(limiter.Tokens())))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Second).Unix(), 10))

			next.ServeHTTP(w, r)
		})
	}
}

func writeTooManyRequests(w http.ResponseWriter, retryAfterSecs int) {
	if retryAfterSecs > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSecs))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    429,
		"message": "rate limit exceeded",
	})
}
