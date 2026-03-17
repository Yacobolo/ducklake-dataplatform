package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	authsvc "duck-demo/internal/service/auth"
)

func TestAuthHTTPHandler_BootstrapComplete(t *testing.T) {
	h, _ := setupAuthHandler(t)

	body := map[string]string{"username": "admin", "password": "super-secure-password", "principal_name": "admin_user"}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/bootstrap/complete", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.BootstrapComplete(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code)
	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["token"])
	principal, ok := resp["principal"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "admin_user", principal["name"])
}

func TestAuthHTTPHandler_LocalLogin(t *testing.T) {
	h, svc := setupAuthHandler(t)

	_, err := svc.Bootstrap(context.Background(), authsvc.BootstrapRequest{Username: "localadmin", Password: "super-secure-password", PrincipalName: "localadmin"})
	require.NoError(t, err)

	body := map[string]string{"username": "localadmin", "password": "super-secure-password"}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/local/login", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.LocalLogin(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code)
	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["token"])
}

func TestAuthHTTPHandler_OIDCProviderEndpoints(t *testing.T) {
	h, _ := setupAuthHandler(t)

	t.Run("non_admin_forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/auth/provider/oidc", nil)
		req = req.WithContext(domain.WithPrincipal(req.Context(), domain.ContextPrincipal{Name: "analyst", IsAdmin: false, Type: "user"}))
		rr := httptest.NewRecorder()
		h.GetOIDCProvider(rr, req)
		require.Equal(t, http.StatusForbidden, rr.Code)
	})

	t.Run("admin_can_upsert_and_get", func(t *testing.T) {
		upsert := map[string]interface{}{"enabled": true, "issuer_url": " https://issuer.example.com ", "client_secret": "secret"}
		payload, err := json.Marshal(upsert)
		require.NoError(t, err)

		putReq := httptest.NewRequest(http.MethodPut, "/v1/auth/provider/oidc", bytes.NewReader(payload))
		putReq = putReq.WithContext(domain.WithPrincipal(putReq.Context(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"}))
		putRR := httptest.NewRecorder()
		h.UpsertOIDCProvider(putRR, putReq)
		require.Equal(t, http.StatusNoContent, putRR.Code)

		getReq := httptest.NewRequest(http.MethodGet, "/v1/auth/provider/oidc", nil)
		getReq = getReq.WithContext(domain.WithPrincipal(getReq.Context(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"}))
		getRR := httptest.NewRecorder()
		h.GetOIDCProvider(getRR, getReq)
		require.Equal(t, http.StatusOK, getRR.Code)

		var resp map[string]interface{}
		require.NoError(t, json.Unmarshal(getRR.Body.Bytes(), &resp))
		assert.Equal(t, true, resp["enabled"])
		assert.Equal(t, "https://issuer.example.com", resp["issuer_url"])
		assert.Equal(t, true, resp["secret_stored"])
	})

	t.Run("admin_upsert_requires_enabled", func(t *testing.T) {
		payload := []byte(`{"issuer_url":"https://issuer.example.com"}`)
		putReq := httptest.NewRequest(http.MethodPut, "/v1/auth/provider/oidc", bytes.NewReader(payload))
		putReq = putReq.WithContext(domain.WithPrincipal(putReq.Context(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"}))
		putRR := httptest.NewRecorder()
		h.UpsertOIDCProvider(putRR, putReq)
		require.Equal(t, http.StatusBadRequest, putRR.Code)
		assert.Contains(t, putRR.Body.String(), "enabled is required")
	})
}

func setupAuthHandler(t *testing.T) (*AuthHTTPHandler, *authsvc.Service) {
	t.Helper()
	principals := &apiStubPrincipalRepo{nextID: 1, byID: map[string]*domain.Principal{}, byName: map[string]*domain.Principal{}}
	credentials := &apiStubCredentialRepo{byUsername: map[string]*domain.LocalCredential{}}
	loginAttempts := &apiStubLoginAttemptRepo{}
	setupState := &apiStubSetupStateRepo{state: &domain.SetupState{}}
	providers := &apiStubProviderRepo{cfg: &domain.AuthProviderConfig{}}
	audit := &apiStubAuditRepo{}
	svc := authsvc.NewService(principals, credentials, loginAttempts, setupState, providers, audit, "handler-test-secret")
	webSessions := authsvc.NewSessionService(principals, newAPIStubWebSessionRepo(), audit, 30*time.Minute, 24*time.Hour)
	return NewAuthHTTPHandler(svc, webSessions), svc
}

func TestAuthHTTPHandler_WebSessionAdminEndpoints(t *testing.T) {
	h, svc := setupAuthHandler(t)

	bootstrap, err := svc.Bootstrap(context.Background(), authsvc.BootstrapRequest{Username: "admin", Password: "super-secure-password", PrincipalName: "admin"})
	require.NoError(t, err)

	nonAdminReq := httptest.NewRequest(http.MethodGet, "/v1/auth/sessions/stats", nil)
	nonAdminReq = nonAdminReq.WithContext(domain.WithPrincipal(nonAdminReq.Context(), domain.ContextPrincipal{Name: "user", IsAdmin: false, Type: "user"}))
	nonAdminRR := httptest.NewRecorder()
	h.GetWebSessionStats(nonAdminRR, nonAdminReq)
	require.Equal(t, http.StatusForbidden, nonAdminRR.Code)

	revokeBody, err := json.Marshal(map[string]string{"principal_id": bootstrap.Principal.ID})
	require.NoError(t, err)
	revokeReq := httptest.NewRequest(http.MethodPost, "/v1/auth/sessions/revoke-all", bytes.NewReader(revokeBody))
	revokeReq = revokeReq.WithContext(domain.WithPrincipal(revokeReq.Context(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"}))
	revokeRR := httptest.NewRecorder()
	h.RevokeAllWebSessions(revokeRR, revokeReq)
	require.Equal(t, http.StatusNoContent, revokeRR.Code)

	statsReq := httptest.NewRequest(http.MethodGet, "/v1/auth/sessions/stats", nil)
	statsReq = statsReq.WithContext(domain.WithPrincipal(statsReq.Context(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"}))
	statsRR := httptest.NewRecorder()
	h.GetWebSessionStats(statsRR, statsReq)
	require.Equal(t, http.StatusOK, statsRR.Code)
	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(statsRR.Body.Bytes(), &payload))
	assert.Contains(t, payload, "active_sessions")
	assert.Contains(t, payload, "revoked_all_total")
}

type apiStubPrincipalRepo struct {
	nextID int
	byID   map[string]*domain.Principal
	byName map[string]*domain.Principal
}

func (r *apiStubPrincipalRepo) Create(_ context.Context, p *domain.Principal) (*domain.Principal, error) {
	if _, exists := r.byName[p.Name]; exists {
		return nil, domain.ErrConflict("principal %q already exists", p.Name)
	}
	id := fmt.Sprintf("p-%d", r.nextID)
	r.nextID++
	cp := *p
	cp.ID = id
	r.byID[id] = &cp
	r.byName[cp.Name] = &cp
	return &cp, nil
}

func (r *apiStubPrincipalRepo) GetByID(_ context.Context, id string) (*domain.Principal, error) {
	p, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound("principal %q not found", id)
	}
	cp := *p
	return &cp, nil
}

func (r *apiStubPrincipalRepo) GetByName(_ context.Context, name string) (*domain.Principal, error) {
	p, ok := r.byName[name]
	if !ok {
		return nil, domain.ErrNotFound("principal %q not found", name)
	}
	cp := *p
	return &cp, nil
}

func (r *apiStubPrincipalRepo) GetByExternalID(_ context.Context, _, _ string) (*domain.Principal, error) {
	return nil, domain.ErrNotFound("not found")
}

func (r *apiStubPrincipalRepo) List(_ context.Context, _ domain.PageRequest) ([]domain.Principal, int64, error) {
	out := make([]domain.Principal, 0, len(r.byID))
	for _, p := range r.byID {
		out = append(out, *p)
	}
	return out, int64(len(out)), nil
}

func (r *apiStubPrincipalRepo) Delete(_ context.Context, _ string) error           { return nil }
func (r *apiStubPrincipalRepo) SetAdmin(_ context.Context, _ string, _ bool) error { return nil }
func (r *apiStubPrincipalRepo) BindExternalID(_ context.Context, _ string, _ string, _ string) error {
	return nil
}

type apiStubCredentialRepo struct {
	byUsername map[string]*domain.LocalCredential
}

func (r *apiStubCredentialRepo) Upsert(_ context.Context, c *domain.LocalCredential) error {
	cp := *c
	r.byUsername[c.Username] = &cp
	return nil
}

func (r *apiStubCredentialRepo) GetByUsername(_ context.Context, username string) (*domain.LocalCredential, error) {
	c, ok := r.byUsername[username]
	if !ok {
		return nil, domain.ErrNotFound("credential %q not found", username)
	}
	cp := *c
	return &cp, nil
}

func (r *apiStubCredentialRepo) GetByPrincipalID(_ context.Context, principalID string) (*domain.LocalCredential, error) {
	for _, c := range r.byUsername {
		if c.PrincipalID == principalID {
			cp := *c
			return &cp, nil
		}
	}
	return nil, domain.ErrNotFound("credential for principal %q not found", principalID)
}

func (r *apiStubCredentialRepo) Delete(_ context.Context, _ string) error { return nil }

type apiStubLoginAttemptRepo struct{}

func (s *apiStubLoginAttemptRepo) Insert(_ context.Context, _ *domain.AuthLoginAttempt) error {
	return nil
}

func (s *apiStubLoginAttemptRepo) CountRecentFailedByUsername(_ context.Context, _ string, _ time.Time) (int64, error) {
	return 0, nil
}

func (s *apiStubLoginAttemptRepo) CountRecentFailedByIP(_ context.Context, _ string, _ time.Time) (int64, error) {
	return 0, nil
}

type apiStubSetupStateRepo struct{ state *domain.SetupState }

func (s *apiStubSetupStateRepo) Get(_ context.Context) (*domain.SetupState, error) {
	cp := *s.state
	return &cp, nil
}

func (s *apiStubSetupStateRepo) Complete(_ context.Context, principalID string) error {
	now := time.Now()
	s.state.SetupCompleted = true
	s.state.SetupCompletedBy = &principalID
	s.state.SetupCompletedAt = &now
	return nil
}

func (s *apiStubSetupStateRepo) SetBootstrapToken(_ context.Context, tokenHash string, expiresAt time.Time) error {
	s.state.BootstrapTokenHash = &tokenHash
	s.state.BootstrapTokenExpiresAt = &expiresAt
	return nil
}

func (s *apiStubSetupStateRepo) ClearBootstrapToken(_ context.Context) error {
	s.state.BootstrapTokenHash = nil
	s.state.BootstrapTokenExpiresAt = nil
	return nil
}

type apiStubProviderRepo struct{ cfg *domain.AuthProviderConfig }

func (s *apiStubProviderRepo) Get(_ context.Context) (*domain.AuthProviderConfig, error) {
	cp := *s.cfg
	return &cp, nil
}

func (s *apiStubProviderRepo) Upsert(_ context.Context, cfg *domain.AuthProviderConfig) error {
	cp := *cfg
	s.cfg = &cp
	return nil
}

type apiStubAuditRepo struct{}

func (s *apiStubAuditRepo) Insert(_ context.Context, _ *domain.AuditEntry) error { return nil }

func (s *apiStubAuditRepo) List(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return nil, 0, nil
}

type apiStubWebSessionRepo struct {
	nextID   int
	sessions map[string]*domain.AuthSession
	byHash   map[string]*domain.AuthSession
}

func newAPIStubWebSessionRepo() *apiStubWebSessionRepo {
	return &apiStubWebSessionRepo{nextID: 1, sessions: map[string]*domain.AuthSession{}, byHash: map[string]*domain.AuthSession{}}
}

func (r *apiStubWebSessionRepo) Create(_ context.Context, session *domain.AuthSession) (*domain.AuthSession, error) {
	cp := *session
	cp.ID = fmt.Sprintf("ws-%d", r.nextID)
	r.nextID++
	now := time.Now()
	cp.CreatedAt = now
	cp.UpdatedAt = now
	cp.LastSeenAt = now
	r.sessions[cp.ID] = &cp
	r.byHash[cp.SessionHash] = &cp
	return &cp, nil
}

func (r *apiStubWebSessionRepo) GetActiveByHash(_ context.Context, sessionHash string) (*domain.AuthSession, error) {
	s, ok := r.byHash[sessionHash]
	if !ok || s.RevokedAt != nil || !s.ExpiresAt.After(time.Now()) || !s.IdleExpiresAt.After(time.Now()) {
		return nil, domain.ErrNotFound("session not found")
	}
	cp := *s
	return &cp, nil
}

func (r *apiStubWebSessionRepo) Touch(_ context.Context, sessionID string, idleExpiresAt time.Time) error {
	s, ok := r.sessions[sessionID]
	if !ok {
		return domain.ErrNotFound("session not found")
	}
	s.IdleExpiresAt = idleExpiresAt
	s.LastSeenAt = time.Now()
	return nil
}

func (r *apiStubWebSessionRepo) Revoke(_ context.Context, sessionID string) error {
	s, ok := r.sessions[sessionID]
	if !ok {
		return domain.ErrNotFound("session not found")
	}
	now := time.Now()
	s.RevokedAt = &now
	return nil
}

func (r *apiStubWebSessionRepo) RevokeByHash(_ context.Context, sessionHash string) error {
	s, ok := r.byHash[sessionHash]
	if !ok {
		return domain.ErrNotFound("session not found")
	}
	now := time.Now()
	s.RevokedAt = &now
	return nil
}

func (r *apiStubWebSessionRepo) RevokeAllForPrincipal(_ context.Context, principalID string) error {
	now := time.Now()
	for _, s := range r.sessions {
		if s.PrincipalID == principalID {
			s.RevokedAt = &now
		}
	}
	return nil
}

func (r *apiStubWebSessionRepo) CountActive(_ context.Context) (int64, error) {
	var count int64
	now := time.Now()
	for _, s := range r.sessions {
		if s.RevokedAt == nil && s.ExpiresAt.After(now) && s.IdleExpiresAt.After(now) {
			count++
		}
	}
	return count, nil
}

func (r *apiStubWebSessionRepo) DeleteExpiredOrRevoked(_ context.Context) (int64, error) {
	var deleted int64
	now := time.Now()
	for id, s := range r.sessions {
		if s.RevokedAt != nil || !s.ExpiresAt.After(now) || !s.IdleExpiresAt.After(now) {
			delete(r.sessions, id)
			delete(r.byHash, s.SessionHash)
			deleted++
		}
	}
	return deleted, nil
}
