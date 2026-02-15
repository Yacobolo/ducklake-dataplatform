package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"sync"
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

// clientLimiter tracks a per-client rate limiter and when it was last seen.
type clientLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// RateLimiter returns an HTTP middleware that enforces a per-client token-bucket
// rate limit. When the limit is exceeded, it responds with 429 Too Many Requests
// and sets standard rate-limit headers.
func RateLimiter(cfg RateLimitConfig) func(http.Handler) http.Handler {
	var clients sync.Map // map[string]*clientLimiter

	// Background cleanup: remove stale entries every 5 minutes.
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			clients.Range(func(key, value any) bool {
				cl := value.(*clientLimiter)
				if time.Since(cl.lastSeen) > 10*time.Minute {
					clients.Delete(key)
				}
				return true
			})
		}
	}()

	getLimiter := func(ip string) *rate.Limiter {
		if v, ok := clients.Load(ip); ok {
			cl := v.(*clientLimiter)
			cl.lastSeen = time.Now()
			return cl.limiter
		}
		limiter := rate.NewLimiter(rate.Limit(cfg.RequestsPerSecond), cfg.Burst)
		clients.Store(ip, &clientLimiter{limiter: limiter, lastSeen: time.Now()})
		return limiter
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := clientIP(r)
			limiter := getLimiter(ip)

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

// clientIP extracts the client IP address from the request, stripping the port.
// Only uses RemoteAddr — X-Forwarded-For is untrusted and ignored to prevent
// rate-limit bypass via header spoofing.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
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
