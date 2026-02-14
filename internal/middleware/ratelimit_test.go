package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter_AllowsWithinLimit(t *testing.T) {
	handler := RateLimiter(RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             10,
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for range 5 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.NotEmpty(t, rec.Header().Get("X-RateLimit-Limit"))
	}
}

func TestRateLimiter_RejectsOverBurst(t *testing.T) {
	handler := RateLimiter(RateLimitConfig{
		RequestsPerSecond: 1,
		Burst:             2,
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Exhaust the burst
	for range 2 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Next request should be rate limited
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)

	var body map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&body)
	require.NoError(t, err)
	assert.InDelta(t, float64(429), body["code"], 0.001)
	assert.Equal(t, "rate limit exceeded", body["message"])
}

func TestRateLimiter_PerClientIsolation(t *testing.T) {
	handler := RateLimiter(RateLimitConfig{
		RequestsPerSecond: 1,
		Burst:             2,
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Client A exhausts its burst.
	for range 2 {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Client A is now rate limited.
	reqA := httptest.NewRequest(http.MethodGet, "/", nil)
	reqA.RemoteAddr = "10.0.0.1:5678"
	recA := httptest.NewRecorder()
	handler.ServeHTTP(recA, reqA)
	assert.Equal(t, http.StatusTooManyRequests, recA.Code)

	// Client B (different IP) should still be allowed.
	reqB := httptest.NewRequest(http.MethodGet, "/", nil)
	reqB.RemoteAddr = "10.0.0.2:1234"
	recB := httptest.NewRecorder()
	handler.ServeHTTP(recB, reqB)
	assert.Equal(t, http.StatusOK, recB.Code, "different client should not be affected by Client A's rate limit")
}

func TestClientIP_ExtractsHost(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{
			name:       "IPv4 with port",
			remoteAddr: "192.168.1.1:12345",
			want:       "192.168.1.1",
		},
		{
			name:       "IPv6 with port",
			remoteAddr: "[::1]:12345",
			want:       "::1",
		},
		{
			name:       "X-Forwarded-For single",
			remoteAddr: "10.0.0.1:1234",
			xff:        "203.0.113.50",
			want:       "203.0.113.50",
		},
		{
			name:       "X-Forwarded-For chain",
			remoteAddr: "10.0.0.1:1234",
			xff:        "203.0.113.50, 70.41.3.18, 150.172.238.178",
			want:       "203.0.113.50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			got := clientIP(req)
			assert.Equal(t, tt.want, got)
		})
	}
}
