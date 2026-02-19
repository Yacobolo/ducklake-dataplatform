package ui

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"strings"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

const csrfCookieName = "ui_csrf"

type csrfContextKey struct{}

func (h *Handler) EnsureCSRFToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := readCSRFCookie(r)
		if token == "" {
			token = randomToken(32)
			http.SetCookie(w, &http.Cookie{
				Name:     csrfCookieName,
				Value:    token,
				Path:     "/",
				HttpOnly: true,
				Secure:   h.Production,
				SameSite: http.SameSiteLaxMode,
			})
		}
		ctx := context.WithValue(r.Context(), csrfContextKey{}, token)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *Handler) RequireCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			next.ServeHTTP(w, r)
			return
		}

		cookieToken := readCSRFCookie(r)
		if cookieToken == "" {
			renderHTML(w, http.StatusForbidden, errorPage("CSRF Validation Failed", "Missing CSRF token cookie."))
			return
		}

		formToken := strings.TrimSpace(r.Header.Get("X-CSRF-Token"))
		if formToken == "" {
			_ = r.ParseForm()
			formToken = strings.TrimSpace(r.Form.Get("csrf_token"))
		}

		if subtle.ConstantTimeCompare([]byte(cookieToken), []byte(formToken)) != 1 {
			renderHTML(w, http.StatusForbidden, errorPage("CSRF Validation Failed", "Invalid or missing CSRF token."))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func csrfField(r *http.Request) gomponents.Node {
	token, _ := r.Context().Value(csrfContextKey{}).(string)
	if token == "" {
		token = readCSRFCookie(r)
	}
	return html.Input(
		html.Type("hidden"),
		html.Name("csrf_token"),
		html.Value(token),
	)
}

func csrfFieldProvider(r *http.Request) func() gomponents.Node {
	return func() gomponents.Node {
		return csrfField(r)
	}
}

func readCSRFCookie(r *http.Request) string {
	cookie, err := r.Cookie(csrfCookieName)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(cookie.Value)
}

func randomToken(size int) string {
	if size < 16 {
		size = 16
	}
	b := make([]byte, size)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
