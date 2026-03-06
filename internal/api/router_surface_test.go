package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestAPI_PipelineRoutesAreNotRegistered(t *testing.T) {
	t.Parallel()

	r := chi.NewRouter()
	HandlerFromMux(Unimplemented{}, r)

	err := chi.Walk(r, func(_ string, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		require.False(t, strings.HasPrefix(route, "/pipelines"), "legacy pipeline route should not be registered: %s", route)
		return nil
	})
	require.NoError(t, err)
}

func TestAPI_PipelineRoutesReturnNotFoundAcrossLegacySurface(t *testing.T) {
	t.Parallel()

	r := chi.NewRouter()
	HandlerFromMux(Unimplemented{}, r)

	tests := map[string][]string{
		"/pipelines":               {http.MethodGet, http.MethodPost},
		"/pipelines/daily":         {http.MethodGet, http.MethodPatch, http.MethodDelete},
		"/pipelines/daily/jobs":    {http.MethodGet, http.MethodPost},
		"/pipelines/daily/jobs/id": {http.MethodDelete},
	}

	for path, methods := range tests {
		for _, method := range methods {
			req := httptest.NewRequest(method, path, nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
			require.Equal(t, http.StatusNotFound, rr.Code, "%s %s should be absent", method, path)
		}
	}
}

func TestAPI_AssetRoutesRemainExposed(t *testing.T) {
	t.Parallel()

	r := chi.NewRouter()
	HandlerFromMux(Unimplemented{}, r)

	req := httptest.NewRequest(http.MethodGet, "/assets", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.NotEqual(t, http.StatusNotFound, rr.Code, "asset route should remain exposed")
}
