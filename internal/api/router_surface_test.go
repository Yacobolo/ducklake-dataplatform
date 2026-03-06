package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestAPI_PipelineRoutesAreNotExposed(t *testing.T) {
	t.Parallel()

	r := chi.NewRouter()
	HandlerFromMux(Unimplemented{}, r)

	tests := []string{
		"/pipelines",
		"/pipelines/daily",
		"/pipelines/daily/jobs",
	}

	for _, path := range tests {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		require.Equal(t, http.StatusNotFound, rr.Code, "path %s should be absent", path)
	}
}
