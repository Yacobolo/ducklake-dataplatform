package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"

	"duck-demo/internal/api"
)

func TestCurlHostForListenAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		listenAddr string
		want       string
	}{
		{name: "port only", listenAddr: ":8080", want: "localhost:8080"},
		{name: "ipv4 host and port", listenAddr: "127.0.0.1:8080", want: "127.0.0.1:8080"},
		{name: "wildcard ipv4", listenAddr: "0.0.0.0:8080", want: "localhost:8080"},
		{name: "wildcard ipv6", listenAddr: "[::]:8080", want: "localhost:8080"},
		{name: "ipv6 loopback", listenAddr: "[::1]:8080", want: "[::1]:8080"},
		{name: "trim host and port", listenAddr: " localhost:9090 ", want: "localhost:9090"},
		{name: "trim port only", listenAddr: "  :7070  ", want: "localhost:7070"},
		{name: "empty falls back", listenAddr: "", want: "localhost:8080"},
		{name: "whitespace falls back", listenAddr: "   ", want: "localhost:8080"},
		{name: "malformed passes through", listenAddr: "localhost", want: "localhost"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := curlHostForListenAddr(tt.listenAddr)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAPIGenRoutes_MountUnderV1_NoDoublePrefix(t *testing.T) {
	t.Parallel()

	gen := &recordingGenServer{}
	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		api.RegisterAPIGenRoutes(r, gen)
	})

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantOpID   string
	}{
		{name: "execute query", method: http.MethodPost, path: "/v1/query", wantStatus: http.StatusNoContent, wantOpID: "executeQuery"},
		{name: "health", method: http.MethodGet, path: "/v1/healthz", wantStatus: http.StatusNoContent, wantOpID: "getHealth"},
		{name: "path parameter", method: http.MethodGet, path: "/v1/catalogs/test-catalog", wantStatus: http.StatusNoContent, wantOpID: "getCatalogRegistration"},
		{name: "nested path parameter", method: http.MethodPost, path: "/v1/catalogs/c1/schemas/s1/tables/t1/ingestion/commit", wantStatus: http.StatusNoContent, wantOpID: "commitTableIngestion"},
		{name: "method mismatch", method: http.MethodGet, path: "/v1/query", wantStatus: http.StatusMethodNotAllowed},
		{name: "double prefix", method: http.MethodPost, path: "/v1/v1/query", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			before := len(gen.operationIDs)
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			assert.Equal(t, tt.wantStatus, rr.Code)
			if tt.wantOpID == "" {
				assert.Len(t, gen.operationIDs, before)
				return
			}
			assert.Len(t, gen.operationIDs, before+1)
			assert.Equal(t, tt.wantOpID, gen.lastOperationID())
		})
	}
}

func TestAPIGenStrictAdapter_DispatchesExecuteQueryOnV1Route(t *testing.T) {
	t.Parallel()

	strict := &executeQueryStrictStub{}
	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		api.RegisterAPIGenRoutes(r, api.NewAPIGenStrictAdapter(strict))
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(`{"sql":"select 1"}`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, strict.called)
}

func TestAPIGenStrictAdapter_HandlesGetHealthOnV1Route(t *testing.T) {
	t.Parallel()

	strict := &executeQueryStrictStub{}
	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		api.RegisterAPIGenRoutes(r, api.NewAPIGenStrictAdapter(strict))
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/healthz", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"status":"ok"}`, rr.Body.String())
	assert.False(t, strict.called)
}

type recordingGenServer struct {
	operationIDs []string
}

func (s *recordingGenServer) HandleAPIGen(operationID string, w http.ResponseWriter, _ *http.Request) {
	s.operationIDs = append(s.operationIDs, operationID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *recordingGenServer) lastOperationID() string {
	if len(s.operationIDs) == 0 {
		return ""
	}
	return s.operationIDs[len(s.operationIDs)-1]
}

type executeQueryStrictStub struct {
	api.GenStrictServerInterface
	called bool
}

func (s *executeQueryStrictStub) ExecuteQuery(_ context.Context, _ api.GenExecuteQueryRequest) (api.GenExecuteQueryResponse, error) {
	s.called = true
	return api.ExecuteQuery200JSONResponse{}, nil
}
