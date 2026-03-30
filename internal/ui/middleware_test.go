package ui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"duck-demo/internal/ui/core"

	"github.com/stretchr/testify/require"
)

func TestRequireCSRF_AcceptsDatastarSignalTokenAndPreservesBody(t *testing.T) {
	t.Helper()

	const body = `{"csrfToken":"token-123","urlParams":{"folder_id":"folder-1","kind":["folder"],"owner":[]}}`

	h := &Handler{Dependencies: &core.Dependencies{}}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(payload))
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodPost, "/ui/explore/updates/stream-1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: uiCSRFCookieName, Value: "token-123", Path: "/"})

	rec := httptest.NewRecorder()
	h.RequireCSRF(next).ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
}
