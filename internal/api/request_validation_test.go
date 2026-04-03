package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestValidationMiddleware_RejectsInvalidPagination(t *testing.T) {
	t.Parallel()

	for _, target := range []string{
		"/v1/principals?max_results=0",
		"/v1/principals?max_results=2001",
		"/v1/principals?page_token=garbage",
	} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, target, nil)
			rec := httptest.NewRecorder()

			RequestValidationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		})
	}
}

func TestRequestValidationMiddleware_RejectsMalformedUUIDPath(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/v1/principals/not-a-uuid", nil)
	rec := httptest.NewRecorder()

	RequestValidationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "principalId")
}

func TestRequestValidationMiddleware_RejectsUnknownJSONFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "principal", path: "/v1/principals", body: `{"name":"alice","type":"user","unexpected":"x"}`},
		{name: "notebook", path: "/v1/notebooks", body: `{"name":"nb","owner_id":"x"}`},
		{name: "api key", path: "/v1/api-keys", body: `{"name":"key","principal_id":"00000000-0000-0000-0000-000000000000","unexpected":true}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			RequestValidationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusCreated)
			})).ServeHTTP(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "invalid JSON body")
		})
	}
}

func TestRequestValidationMiddleware_AllowsEmptySetDefaultBody(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPut, "/v1/catalogs/demo/default", http.NoBody)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	called := false
	RequestValidationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{}`, string(body))
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	require.True(t, called)
	require.Equal(t, http.StatusOK, rec.Code)
}
