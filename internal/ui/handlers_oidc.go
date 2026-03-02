package ui

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const oidcStateCookieName = "ui_oidc_state"

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

	expires := tok.Expiry
	if expires.IsZero() {
		expires = time.Now().Add(24 * time.Hour)
	}

	http.SetCookie(w, &http.Cookie{
		Name:     bearerCookieName,
		Value:    strings.TrimSpace(idToken),
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		Expires:  expires,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     apiKeyCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
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
