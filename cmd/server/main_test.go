package main

import (
	"net/http"
	"net/http/httptest"
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

	req := httptest.NewRequest(http.MethodPost, "/v1/query", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusNoContent, rr.Code)
	assert.Equal(t, "executeQuery", gen.lastOperationID)

	wrongReq := httptest.NewRequest(http.MethodPost, "/v1/v1/query", nil)
	wrongRR := httptest.NewRecorder()
	r.ServeHTTP(wrongRR, wrongReq)
	assert.Equal(t, http.StatusNotFound, wrongRR.Code)
}

func TestAPIGenLegacyAdapter_DispatchesExecuteQueryOnV1Route(t *testing.T) {
	t.Parallel()

	strict := &executeQueryStrictStub{}
	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		api.RegisterAPIGenRoutes(r, api.NewAPIGenLegacyAdapter(strict))
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/query", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusCreated, rr.Code)
	assert.True(t, strict.called)
}

type recordingGenServer struct {
	lastOperationID string
}

func (s *recordingGenServer) HandleAPIGen(operationID string, w http.ResponseWriter, _ *http.Request) {
	s.lastOperationID = operationID
	w.WriteHeader(http.StatusNoContent)
}

type executeQueryStrictStub struct {
	api.Unimplemented
	called bool
}

func (s *executeQueryStrictStub) ExecuteQuery(w http.ResponseWriter, _ *http.Request) {
	s.called = true
	w.WriteHeader(http.StatusCreated)
}
