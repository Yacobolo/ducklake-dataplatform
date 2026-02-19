package ui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCSRFMiddleware_RejectsMissingToken(t *testing.T) {
	h := &Handler{}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	r := httptest.NewRequest(http.MethodPost, "/ui/models", strings.NewReader("name=x"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	h.RequireCSRF(next).ServeHTTP(rr, r)
	require.Equal(t, http.StatusForbidden, rr.Code)
}

func TestCSRFMiddleware_AllowsMatchingToken(t *testing.T) {
	h := &Handler{}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	form := url.Values{}
	form.Set("csrf_token", "abc123")
	r := httptest.NewRequest(http.MethodPost, "/ui/models", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "abc123"})
	rr := httptest.NewRecorder()

	h.RequireCSRF(next).ServeHTTP(rr, r)
	require.Equal(t, http.StatusNoContent, rr.Code)
}

func TestEnsureCSRFToken_SetsCookieWhenMissing(t *testing.T) {
	h := &Handler{}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	r := httptest.NewRequest(http.MethodGet, "/ui", nil)
	rr := httptest.NewRecorder()

	h.EnsureCSRFToken(next).ServeHTTP(rr, r)
	require.Equal(t, http.StatusNoContent, rr.Code)
	setCookie := rr.Header().Get("Set-Cookie")
	require.Contains(t, setCookie, csrfCookieName+"=")
}
