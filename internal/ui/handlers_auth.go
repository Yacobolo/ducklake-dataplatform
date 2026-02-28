package ui

import (
	"net/http"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

const (
	bearerCookieName = "ui_bearer"
	apiKeyCookieName = "ui_api_key"
)

func (h *Handler) LoginPage(w http.ResponseWriter, r *http.Request) {
	if _, ok := domain.PrincipalFromContext(r.Context()); ok {
		http.Redirect(w, r, "/ui", http.StatusSeeOther)
		return
	}
	renderHTML(w, http.StatusOK, loginPage(strings.TrimSpace(r.URL.Query().Get("error"))))
}

func (h *Handler) LoginSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/ui/login?error=invalid+form", http.StatusSeeOther)
		return
	}
	kind := strings.TrimSpace(r.Form.Get("kind"))
	token := strings.TrimSpace(r.Form.Get("token"))
	if token == "" {
		http.Redirect(w, r, "/ui/login?error=token+is+required", http.StatusSeeOther)
		return
	}

	expires := time.Now().Add(24 * time.Hour)
	bearerCookie := &http.Cookie{
		Name:     bearerCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		Expires:  expires,
	}
	apiKeyCookie := &http.Cookie{
		Name:     apiKeyCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		Expires:  expires,
	}

	switch kind {
	case "api_key":
		apiKeyCookie.Value = token
		bearerCookie.MaxAge = -1
	default:
		bearerCookie.Value = token
		apiKeyCookie.MaxAge = -1
	}
	http.SetCookie(w, bearerCookie)
	http.SetCookie(w, apiKeyCookie)
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     bearerCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     apiKeyCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

func (h *Handler) CookieHeaderBridge(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			if cookie, err := r.Cookie(bearerCookieName); err == nil && strings.TrimSpace(cookie.Value) != "" {
				r.Header.Set("Authorization", "Bearer "+strings.TrimSpace(cookie.Value))
			}
		}
		if h.Auth.APIKeyEnabled && r.Header.Get(h.Auth.APIKeyHeader) == "" {
			if cookie, err := r.Cookie(apiKeyCookieName); err == nil && strings.TrimSpace(cookie.Value) != "" {
				r.Header.Set(h.Auth.APIKeyHeader, strings.TrimSpace(cookie.Value))
			}
		}
		next.ServeHTTP(w, r)
	})
}

func RedirectToLogin(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/ui") {
		http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
		return
	}
	w.WriteHeader(http.StatusUnauthorized)
}
