package ui

import (
	"net"
	"net/http"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

const (
	uiSessionCookiePath = "/ui"
)

func (h *Handler) LoginPage(w http.ResponseWriter, r *http.Request) {
	if _, ok := domain.PrincipalFromContext(r.Context()); ok {
		http.Redirect(w, r, "/ui", http.StatusSeeOther)
		return
	}
	renderHTML(w, http.StatusOK, loginPage(strings.TrimSpace(r.URL.Query().Get("error")), h.hasOIDCLoginConfigured(r)))
}

func (h *Handler) LoginSubmit(w http.ResponseWriter, r *http.Request) {
	if h.AuthService == nil || h.WebSessionService == nil {
		http.Redirect(w, r, "/ui/login?error=auth+service+unavailable", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/ui/login?error=invalid+form", http.StatusSeeOther)
		return
	}

	username := strings.TrimSpace(r.Form.Get("username"))
	password := r.Form.Get("password")
	if username == "" || password == "" {
		http.Redirect(w, r, "/ui/login?error=username+and+password+are+required", http.StatusSeeOther)
		return
	}

	principal, err := h.AuthService.AuthenticateLocal(r.Context(), username, password, clientIP(r))
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=invalid+username+or+password", http.StatusSeeOther)
		return
	}

	sessionToken, session, err := h.WebSessionService.CreateForPrincipal(r.Context(), principal.ID, "local", r.UserAgent(), clientIP(r))
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=session+creation+failed", http.StatusSeeOther)
		return
	}

	http.SetCookie(w, h.newSessionCookie(sessionToken, session.ExpiresAt))
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	if h.WebSessionService != nil {
		if cookie, err := r.Cookie(h.sessionCookieName()); err == nil {
			_ = h.WebSessionService.Revoke(r.Context(), cookie.Value)
		}
	}

	http.SetCookie(w, h.clearCookie(h.sessionCookieName(), uiSessionCookiePath))
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

func (h *Handler) RequireWebSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.WebSessionService == nil {
			RedirectToLogin(w, r)
			return
		}

		cookie, err := r.Cookie(h.sessionCookieName())
		if err != nil || strings.TrimSpace(cookie.Value) == "" {
			RedirectToLogin(w, r)
			return
		}

		principal, _, resolveErr := h.WebSessionService.Resolve(r.Context(), cookie.Value)
		if resolveErr != nil {
			http.SetCookie(w, h.clearCookie(h.sessionCookieName(), uiSessionCookiePath))
			RedirectToLogin(w, r)
			return
		}

		ctx := domain.WithPrincipal(r.Context(), domain.ContextPrincipal{
			ID:      principal.ID,
			Name:    principal.Name,
			Type:    principal.Type,
			IsAdmin: principal.IsAdmin,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func RedirectToLogin(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/ui") {
		http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
		return
	}
	w.WriteHeader(http.StatusUnauthorized)
}

func (h *Handler) sessionCookieName() string {
	if strings.TrimSpace(h.Auth.WebSessionCookieName) == "" {
		return "ui_session"
	}
	return strings.TrimSpace(h.Auth.WebSessionCookieName)
}

func (h *Handler) newSessionCookie(token string, expiresAt time.Time) *http.Cookie {
	return &http.Cookie{
		Name:     h.sessionCookieName(),
		Value:    strings.TrimSpace(token),
		Path:     uiSessionCookiePath,
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		Expires:  expiresAt,
	}
}

func (h *Handler) clearCookie(name, path string) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Path:     path,
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	}
}

func clientIP(r *http.Request) string {
	if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); fwd != "" {
		parts := strings.Split(fwd, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}
