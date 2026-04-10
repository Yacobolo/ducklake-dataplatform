package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestAPI_PipelineRoutesRemainExposed(t *testing.T) {
	t.Parallel()

	r := chi.NewRouter()
	RegisterAPIGenStrictRoutes(r, &APIHandler{})

	found := false
	err := chi.Walk(r, func(_ string, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if strings.HasPrefix(route, "/pipelines") {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "pipeline routes should remain exposed")
}

func TestAPI_PipelineRoutesAreRegisteredAcrossLegacySurface(t *testing.T) {
	t.Parallel()

	r := chi.NewRouter()
	RegisterAPIGenStrictRoutes(r, &APIHandler{})

	tests := []string{
		"/pipelines",
		"/pipelines/{pipeline_name}",
		"/pipelines/{pipeline_name}/jobs",
		"/pipelines/{pipeline_name}/jobs/{job_id}",
	}
	found := map[string]bool{}
	err := chi.Walk(r, func(_ string, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		found[route] = true
		return nil
	})
	require.NoError(t, err)
	for _, route := range tests {
		require.True(t, found[route], "expected route to be registered: %s", route)
	}
}

func TestAPI_AssetRoutesRemainExposed(t *testing.T) {
	t.Parallel()

	r := chi.NewRouter()
	RegisterAPIGenStrictRoutes(r, &APIHandler{})

	found := false
	err := chi.Walk(r, func(_ string, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if route == "/assets" {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "asset route should remain exposed")
}
