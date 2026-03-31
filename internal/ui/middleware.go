package ui

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"

	"duck-demo/internal/config"
	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
)

const (
	uiSessionCookiePath = "/ui"
	uiCSRFCookieName    = "ui_csrf"
)

type uiCSRFContextKey struct{}

func (h *Handler) EnsureCSRFToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := readUICSRFCookie(r)
		if token == "" {
			token = randomToken(32)
			http.SetCookie(w, &http.Cookie{
				Name:     uiCSRFCookieName,
				Value:    token,
				Path:     "/",
				HttpOnly: true,
				Secure:   h.Production,
				SameSite: http.SameSiteLaxMode,
			})
		}
		ctx := context.WithValue(r.Context(), uiCSRFContextKey{}, token)
		ctx = core.WithCSRFToken(ctx, token)
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

		cookieToken := readUICSRFCookie(r)
		if cookieToken == "" {
			core.RenderHTML(w, http.StatusForbidden, core.ErrorPage("CSRF Validation Failed", "Missing CSRF token cookie."))
			return
		}

		formToken := strings.TrimSpace(r.Header.Get("X-CSRF-Token"))
		if formToken == "" && isDatastarSignalRequest(r) {
			formToken = strings.TrimSpace(readDatastarCSRFToken(r))
		}
		if formToken == "" {
			_ = r.ParseForm()
			formToken = strings.TrimSpace(r.Form.Get("csrf_token"))
		}

		if subtle.ConstantTimeCompare([]byte(cookieToken), []byte(formToken)) != 1 {
			core.RenderHTML(w, http.StatusForbidden, core.ErrorPage("CSRF Validation Failed", "Invalid or missing CSRF token."))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (h *Handler) RequireWebSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.Dependencies.Auth.UIDevBypass && !h.Production && isLoopbackRequest(r) {
			ctx := domain.WithPrincipal(r.Context(), h.uiDevBypassPrincipal(r.Context()))
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		if h.WebSessionService == nil {
			redirectToLogin(w, r)
			return
		}

		cookie, err := r.Cookie(sessionCookieName(h.Dependencies.Auth))
		if err != nil || strings.TrimSpace(cookie.Value) == "" {
			redirectToLogin(w, r)
			return
		}

		principal, _, resolveErr := h.WebSessionService.Resolve(r.Context(), cookie.Value)
		if resolveErr != nil {
			http.SetCookie(w, clearCookie(sessionCookieName(h.Dependencies.Auth), uiSessionCookiePath, h.Production))
			redirectToLogin(w, r)
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

func redirectToLogin(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/ui") {
		http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
		return
	}
	w.WriteHeader(http.StatusUnauthorized)
}

func sessionCookieName(authCfg config.AuthConfig) string {
	name := strings.TrimSpace(authCfg.WebSessionCookieName)
	if name == "" {
		return "ui_session"
	}
	return name
}

func clearCookie(name, path string, secure bool) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Path:     path,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	}
}

func isLoopbackRequest(r *http.Request) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		host = strings.TrimSpace(r.RemoteAddr)
	}
	ip := net.ParseIP(host)
	if ip != nil {
		return ip.IsLoopback()
	}
	return strings.EqualFold(host, "localhost")
}

func (h *Handler) uiDevBypassPrincipal(ctx context.Context) domain.ContextPrincipal {
	if h.PrincipalResolver != nil {
		principal, err := h.PrincipalResolver.ResolveOrProvision(ctx, domain.ResolveOrProvisionRequest{
			Issuer:      "dev-local",
			ExternalID:  "ui-dev-bypass",
			DisplayName: "dev-admin",
			IsBootstrap: true,
		})
		if err == nil {
			return domain.ContextPrincipal{
				ID:      principal.ID,
				Name:    principal.Name,
				Type:    principal.Type,
				IsAdmin: principal.IsAdmin,
			}
		}
	}

	return domain.ContextPrincipal{
		ID:      "ui-dev-bypass",
		Name:    "dev-admin",
		Type:    "user",
		IsAdmin: true,
	}
}

func readUICSRFCookie(r *http.Request) string {
	cookie, err := r.Cookie(uiCSRFCookieName)
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

func readDatastarCSRFToken(r *http.Request) string {
	if r.Body == nil {
		return ""
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		r.Body = http.NoBody
		return ""
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	if len(bytes.TrimSpace(body)) == 0 {
		return ""
	}

	var payload struct {
		CSRFToken string `json:"csrfToken"`
		Legacy    string `json:"csrf_token"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	if token := strings.TrimSpace(payload.CSRFToken); token != "" {
		return token
	}
	return strings.TrimSpace(payload.Legacy)
}

func isDatastarSignalRequest(r *http.Request) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type"))), "application/json")
}
