package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

	"github.com/Yacobolo/quackstack/internal/config"
	"github.com/Yacobolo/quackstack/internal/domain"
	authsvc "github.com/Yacobolo/quackstack/internal/service/auth"
	"github.com/Yacobolo/quackstack/internal/ui/core"
)

const (
	uiSessionCookiePath = "/ui"
	oidcStateCookieName = "ui_oidc_state"
)

type PrincipalResolver interface {
	ResolveOrProvision(ctx context.Context, req domain.ResolveOrProvisionRequest) (*domain.Principal, error)
}

type Handler struct {
	AuthService       *authsvc.Service
	WebSessionService *authsvc.SessionService
	PrincipalResolver PrincipalResolver
	Auth              config.AuthConfig
	Production        bool
}

func New(authService *authsvc.Service, webSessionService *authsvc.SessionService, principalResolver PrincipalResolver, authCfg config.AuthConfig, production bool) *Handler {
	return &Handler{
		AuthService:       authService,
		WebSessionService: webSessionService,
		PrincipalResolver: principalResolver,
		Auth:              authCfg,
		Production:        production,
	}
}

func (h *Handler) LoginPage(w http.ResponseWriter, r *http.Request) {
	if _, ok := domain.PrincipalFromContext(r.Context()); ok {
		http.Redirect(w, r, "/ui", http.StatusSeeOther)
		return
	}
	core.RenderHTML(w, http.StatusOK, loginPage(strings.TrimSpace(r.URL.Query().Get("error")), h.hasOIDCLoginConfigured(r)))
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

func (h *Handler) OIDCLoginStart(w http.ResponseWriter, r *http.Request) {
	oauthCfg, err := h.oauth2ConfigForRequest(r)
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=oidc+not+configured", http.StatusSeeOther)
		return
	}

	state, err := randomHex(24)
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=oidc+state+generation+failed", http.StatusSeeOther)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     oidcStateCookieName,
		Value:    state,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   300,
	})

	http.Redirect(w, r, oauthCfg.AuthCodeURL(state), http.StatusFound)
}

func (h *Handler) OIDCLoginCallback(w http.ResponseWriter, r *http.Request) {
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if state == "" || code == "" {
		http.Redirect(w, r, "/ui/login?error=invalid+oidc+callback", http.StatusSeeOther)
		return
	}

	cookie, err := r.Cookie(oidcStateCookieName)
	if err != nil || strings.TrimSpace(cookie.Value) == "" || cookie.Value != state {
		http.Redirect(w, r, "/ui/login?error=invalid+oidc+state", http.StatusSeeOther)
		return
	}

	oauthCfg, err := h.oauth2ConfigForRequest(r)
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=oidc+not+configured", http.StatusSeeOther)
		return
	}

	tok, err := oauthCfg.Exchange(r.Context(), code)
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=oidc+exchange+failed", http.StatusSeeOther)
		return
	}

	idToken, ok := tok.Extra("id_token").(string)
	if !ok || strings.TrimSpace(idToken) == "" {
		http.Redirect(w, r, "/ui/login?error=oidc+id_token+missing", http.StatusSeeOther)
		return
	}

	if h.PrincipalResolver == nil || h.WebSessionService == nil || h.AuthService == nil {
		http.Redirect(w, r, "/ui/login?error=auth+service+unavailable", http.StatusSeeOther)
		return
	}
	providerCfg, err := h.AuthService.GetOIDCProvider(r.Context())
	if err != nil || providerCfg == nil || providerCfg.OIDCIssuerURL == nil || providerCfg.OIDCClientID == nil {
		http.Redirect(w, r, "/ui/login?error=oidc+not+configured", http.StatusSeeOther)
		return
	}

	provider, err := oidc.NewProvider(r.Context(), strings.TrimSpace(*providerCfg.OIDCIssuerURL))
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=oidc+verification+setup+failed", http.StatusSeeOther)
		return
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: strings.TrimSpace(*providerCfg.OIDCClientID)})
	verified, err := verifier.Verify(r.Context(), strings.TrimSpace(idToken))
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=oidc+token+verification+failed", http.StatusSeeOther)
		return
	}

	var claims struct {
		Issuer            string `json:"iss"`
		Subject           string `json:"sub"`
		Email             string `json:"email"`
		PreferredUsername string `json:"preferred_username"`
	}
	if err := verified.Claims(&claims); err != nil {
		http.Redirect(w, r, "/ui/login?error=oidc+claims+invalid", http.StatusSeeOther)
		return
	}

	displayName := strings.TrimSpace(claims.Email)
	if displayName == "" {
		displayName = strings.TrimSpace(claims.PreferredUsername)
	}
	if displayName == "" {
		displayName = strings.TrimSpace(claims.Subject)
	}

	principal, err := h.PrincipalResolver.ResolveOrProvision(r.Context(), domain.ResolveOrProvisionRequest{
		Issuer:      strings.TrimSpace(claims.Issuer),
		ExternalID:  strings.TrimSpace(claims.Subject),
		DisplayName: displayName,
	})
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=oidc+principal+resolution+failed", http.StatusSeeOther)
		return
	}

	sessionToken, session, err := h.WebSessionService.CreateForPrincipal(r.Context(), principal.ID, "oidc", r.UserAgent(), clientIP(r))
	if err != nil {
		http.Redirect(w, r, "/ui/login?error=session+creation+failed", http.StatusSeeOther)
		return
	}

	http.SetCookie(w, h.newSessionCookie(sessionToken, session.ExpiresAt))
	http.SetCookie(w, &http.Cookie{
		Name:     oidcStateCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (h *Handler) hasOIDCLoginConfigured(r *http.Request) bool {
	_, err := h.oauth2ConfigForRequest(r)
	return err == nil
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

func (h *Handler) oauth2ConfigForRequest(r *http.Request) (*oauth2.Config, error) {
	if h.AuthService == nil {
		return nil, fmt.Errorf("auth service unavailable")
	}
	providerCfg, err := h.AuthService.GetOIDCProvider(r.Context())
	if err != nil {
		return nil, err
	}
	if providerCfg == nil || !providerCfg.OIDCEnabled || providerCfg.OIDCIssuerURL == nil || providerCfg.OIDCClientID == nil {
		return nil, fmt.Errorf("oidc provider not configured")
	}

	provider, err := oidc.NewProvider(r.Context(), strings.TrimSpace(*providerCfg.OIDCIssuerURL))
	if err != nil {
		return nil, fmt.Errorf("oidc discovery: %w", err)
	}

	clientSecret := ""
	if providerCfg.OIDCClientSecretEnc != nil {
		clientSecret = strings.TrimSpace(*providerCfg.OIDCClientSecretEnc)
	}

	scopes := []string{oidc.ScopeOpenID, "profile", "email"}
	if providerCfg.OIDCScopes != nil && strings.TrimSpace(*providerCfg.OIDCScopes) != "" {
		scopes = strings.Fields(strings.TrimSpace(*providerCfg.OIDCScopes))
	}

	return &oauth2.Config{
		ClientID:     strings.TrimSpace(*providerCfg.OIDCClientID),
		ClientSecret: clientSecret,
		Endpoint:     provider.Endpoint(),
		RedirectURL:  uiOIDCCallbackURL(r, h.Production),
		Scopes:       scopes,
	}, nil
}

func uiOIDCCallbackURL(r *http.Request, production bool) string {
	scheme := "http"
	if production {
		scheme = "https"
	}
	if r.TLS != nil {
		scheme = "https"
	}
	if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); fwd != "" {
		scheme = fwd
	}
	return fmt.Sprintf("%s://%s/ui/login/oidc/callback", scheme, r.Host)
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
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
