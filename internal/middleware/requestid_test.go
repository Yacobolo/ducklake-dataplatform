package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestID_GeneratesNewID(t *testing.T) {
	var capturedID string
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotEmpty(t, capturedID)
	assert.Equal(t, capturedID, rec.Header().Get("X-Request-ID"))
}

func TestRequestID_PreservesValidID(t *testing.T) {
	var capturedID string
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "custom-id-123")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "custom-id-123", capturedID)
	assert.Equal(t, "custom-id-123", rec.Header().Get("X-Request-ID"))
}

func TestRequestID_RejectsInvalidCharacters(t *testing.T) {
	tests := []struct {
		name     string
		headerID string
		wantNew  bool
	}{
		{
			name:     "valid alphanumeric with hyphens",
			headerID: "abc-123_DEF",
			wantNew:  false,
		},
		{
			name:     "log forging with newline",
			headerID: "fake-id\nINJECTED: malicious",
			wantNew:  true,
		},
		{
			name:     "log forging with carriage return",
			headerID: "fake-id\rINJECTED: malicious",
			wantNew:  true,
		},
		{
			name:     "contains spaces",
			headerID: "id with spaces",
			wantNew:  true,
		},
		{
			name:     "contains special characters",
			headerID: "id<script>alert(1)</script>",
			wantNew:  true,
		},
		{
			name:     "too long (129 chars)",
			headerID: strings.Repeat("a", 129),
			wantNew:  true,
		},
		{
			name:     "max length (128 chars)",
			headerID: strings.Repeat("a", 128),
			wantNew:  false,
		},
		{
			name:     "empty string",
			headerID: "",
			wantNew:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedID string
			handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedID = RequestIDFromContext(r.Context())
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.headerID != "" {
				req.Header.Set("X-Request-ID", tt.headerID)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			require.NotEmpty(t, capturedID)

			if tt.wantNew {
				assert.NotEqual(t, tt.headerID, capturedID, "invalid ID should be replaced with a new UUID")
			} else {
				assert.Equal(t, tt.headerID, capturedID, "valid ID should be preserved")
			}
		})
	}
}

func TestRequestIDFromContext_EmptyWithoutMiddleware(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	id := RequestIDFromContext(req.Context())
	assert.Empty(t, id)
}
