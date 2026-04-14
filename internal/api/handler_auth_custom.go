//nolint:revive
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	authsvc "github.com/Yacobolo/quackstack/internal/service/auth"
)

type AuthHTTPHandler struct {
	auth        *authsvc.Service
	webSessions *authsvc.SessionService
}

func NewAuthHTTPHandler(auth *authsvc.Service, webSessions *authsvc.SessionService) *AuthHTTPHandler {
	return &AuthHTTPHandler{auth: auth, webSessions: webSessions}
}

type bootstrapCompleteRequest struct {
	Username       string `json:"username"`
	Password       string `json:"password"`
	PrincipalName  string `json:"principal_name"`
	BootstrapToken string `json:"bootstrap_token"`
}

type localLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type bootstrapTokenRequest struct {
	TTLSeconds int64 `json:"ttl_seconds"`
}

type oidcProviderRequest struct {
	Enabled      *bool   `json:"enabled"`
	IssuerURL    *string `json:"issuer_url"`
	JWKSURL      *string `json:"jwks_url"`
	Audience     *string `json:"audience"`
	ClientID     *string `json:"client_id"`
	ClientSecret *string `json:"client_secret"`
	Scopes       *string `json:"scopes"`
}

type revokeWebSessionsRequest struct {
	PrincipalID string `json:"principal_id"`
}

func (h *AuthHTTPHandler) BootstrapComplete(w http.ResponseWriter, r *http.Request) {
	var req bootstrapCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAuthError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	res, err := h.auth.Bootstrap(r.Context(), authsvc.BootstrapRequest{
		Username:       req.Username,
		Password:       req.Password,
		PrincipalName:  req.PrincipalName,
		BootstrapToken: req.BootstrapToken,
	})
	if err != nil {
		writeAuthError(w, httpStatusFromDomainError(err), err.Error())
		return
	}

	writeAuthJSON(w, http.StatusCreated, map[string]interface{}{
		"token": res.Token,
		"principal": map[string]interface{}{
			"id":       res.Principal.ID,
			"name":     res.Principal.Name,
			"is_admin": res.Principal.IsAdmin,
		},
	})
}

func (h *AuthHTTPHandler) LocalLogin(w http.ResponseWriter, r *http.Request) {
	var req localLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAuthError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	res, err := h.auth.Login(r.Context(), req.Username, req.Password, clientIP(r))
	if err != nil {
		writeAuthError(w, httpStatusFromDomainError(err), err.Error())
		return
	}

	writeAuthJSON(w, http.StatusCreated, map[string]interface{}{
		"token": res.Token,
		"principal": map[string]interface{}{
			"id":       res.Principal.ID,
			"name":     res.Principal.Name,
			"is_admin": res.Principal.IsAdmin,
		},
	})
}

func (h *AuthHTTPHandler) CreateBootstrapToken(w http.ResponseWriter, r *http.Request) {
	if !isAdminRequest(r) {
		writeAuthError(w, http.StatusForbidden, "admin access required")
		return
	}

	var req bootstrapTokenRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAuthError(w, http.StatusBadRequest, "invalid request body")
			return
		}
	}

	ttl := 30 * time.Minute
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}

	token, err := h.auth.CreateBootstrapToken(r.Context(), ttl)
	if err != nil {
		writeAuthError(w, httpStatusFromDomainError(err), err.Error())
		return
	}

	writeAuthJSON(w, http.StatusCreated, map[string]interface{}{"bootstrap_token": token, "ttl_seconds": int64(ttl.Seconds())})
}

func (h *AuthHTTPHandler) GetOIDCProvider(w http.ResponseWriter, r *http.Request) {
	if !isAdminRequest(r) {
		writeAuthError(w, http.StatusForbidden, "admin access required")
		return
	}

	cfg, err := h.auth.GetOIDCProvider(r.Context())
	if err != nil {
		writeAuthError(w, httpStatusFromDomainError(err), err.Error())
		return
	}

	writeAuthJSON(w, http.StatusOK, map[string]interface{}{
		"enabled":       cfg.OIDCEnabled,
		"issuer_url":    cfg.OIDCIssuerURL,
		"jwks_url":      cfg.OIDCJWKSURL,
		"audience":      cfg.OIDCAudience,
		"client_id":     cfg.OIDCClientID,
		"scopes":        cfg.OIDCScopes,
		"updated_at":    cfg.UpdatedAt,
		"secret_stored": cfg.OIDCClientSecretEnc != nil,
	})
}

func (h *AuthHTTPHandler) UpsertOIDCProvider(w http.ResponseWriter, r *http.Request) {
	if !isAdminRequest(r) {
		writeAuthError(w, http.StatusForbidden, "admin access required")
		return
	}

	var req oidcProviderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAuthError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Enabled == nil {
		writeAuthError(w, http.StatusBadRequest, "enabled is required")
		return
	}

	err := h.auth.UpsertOIDCProvider(r.Context(), &domain.AuthProviderConfig{
		OIDCEnabled:         *req.Enabled,
		OIDCIssuerURL:       normalizePtr(req.IssuerURL),
		OIDCJWKSURL:         normalizePtr(req.JWKSURL),
		OIDCAudience:        normalizePtr(req.Audience),
		OIDCClientID:        normalizePtr(req.ClientID),
		OIDCClientSecretEnc: normalizePtr(req.ClientSecret),
		OIDCScopes:          normalizePtr(req.Scopes),
	})
	if err != nil {
		writeAuthError(w, httpStatusFromDomainError(err), err.Error())
		return
	}

	writeAuthJSON(w, http.StatusNoContent, map[string]interface{}{})
}

func (h *AuthHTTPHandler) RevokeAllWebSessions(w http.ResponseWriter, r *http.Request) {
	if !isAdminRequest(r) {
		writeAuthError(w, http.StatusForbidden, "admin access required")
		return
	}
	if h.webSessions == nil {
		writeAuthError(w, http.StatusServiceUnavailable, "web session service unavailable")
		return
	}

	var req revokeWebSessionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAuthError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	principalID := strings.TrimSpace(req.PrincipalID)
	if principalID == "" {
		writeAuthError(w, http.StatusBadRequest, "principal_id is required")
		return
	}

	if err := h.webSessions.RevokeAll(r.Context(), principalID); err != nil {
		writeAuthError(w, httpStatusFromDomainError(err), err.Error())
		return
	}

	writeAuthJSON(w, http.StatusNoContent, map[string]interface{}{})
}

func (h *AuthHTTPHandler) GetWebSessionStats(w http.ResponseWriter, r *http.Request) {
	if !isAdminRequest(r) {
		writeAuthError(w, http.StatusForbidden, "admin access required")
		return
	}
	if h.webSessions == nil {
		writeAuthError(w, http.StatusServiceUnavailable, "web session service unavailable")
		return
	}

	stats, err := h.webSessions.Stats(r.Context())
	if err != nil {
		writeAuthError(w, httpStatusFromDomainError(err), err.Error())
		return
	}

	writeAuthJSON(w, http.StatusOK, map[string]interface{}{
		"created_total":        stats.CreatedTotal,
		"resolved_total":       stats.ResolvedTotal,
		"resolve_failed_total": stats.ResolveFailed,
		"revoked_total":        stats.RevokedTotal,
		"revoked_all_total":    stats.RevokedAllTotal,
		"reaped_total":         stats.ReapedTotal,
		"active_sessions":      stats.ActiveSessions,
		"idle_ttl_seconds":     stats.IdleTTLSeconds,
		"absolute_ttl_seconds": stats.AbsoluteTTLSeconds,
	})
}

func isAdminRequest(r *http.Request) bool {
	p, ok := domain.PrincipalFromContext(r.Context())
	return ok && p.IsAdmin
}

func normalizePtr(v *string) *string {
	if v == nil {
		return nil
	}
	s := strings.TrimSpace(*v)
	if s == "" {
		return nil
	}
	return &s
}

func writeAuthError(w http.ResponseWriter, code int, msg string) {
	writeAuthJSON(w, code, map[string]interface{}{"code": code, "message": msg})
}

func writeAuthJSON(w http.ResponseWriter, code int, payload map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%v", defaultRateLimitLimit))
	w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%v", defaultRateLimitRemaining))
	w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%v", defaultRateLimitReset))
	w.WriteHeader(code)
	if code == http.StatusNoContent {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func clientIP(r *http.Request) string {
	if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); fwd != "" {
		parts := strings.Split(fwd, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	if fwd := strings.TrimSpace(r.Header.Get("X-Real-IP")); fwd != "" {
		return fwd
	}
	return r.RemoteAddr
}
