package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"

	"duck-demo/internal/config"
	"duck-demo/internal/domain"
)

// === Test JWT Validator ===

type stubValidator struct {
	claims *JWTClaims
	err    error
}

func (v *stubValidator) Validate(_ context.Context, _ string) (*JWTClaims, error) {
	return v.claims, v.err
}

// === Test API Key Lookup ===

type stubAPIKeyLookup struct {
	keys map[string]string // hash -> principal name
}

func (s *stubAPIKeyLookup) LookupPrincipalByAPIKeyHash(_ context.Context, keyHash string) (string, error) {
	name, ok := s.keys[keyHash]
	if !ok {
		return "", fmt.Errorf("api key not found")
	}
	return name, nil
}

// === Test Principal Lookup ===

type stubPrincipalLookup struct {
	principals map[string]*domain.Principal
}

func (s *stubPrincipalLookup) GetByName(_ context.Context, name string) (*domain.Principal, error) {
	p, ok := s.principals[name]
	if !ok {
		return nil, domain.ErrNotFound("principal %s not found", name)
	}
	return p, nil
}

// === Test Provisioner ===

type stubProvisioner struct {
	result *domain.Principal
	err    error
	called bool
}

func (s *stubProvisioner) ResolveOrProvision(_ context.Context, _ domain.ResolveOrProvisionRequest) (*domain.Principal, error) {
	s.called = true
	return s.result, s.err
}

// nextHandler is a simple handler that records the context principal.
func nextHandler() (http.Handler, func() (domain.ContextPrincipal, bool)) {
	var cp domain.ContextPrincipal
	var found bool
	h := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		cp, found = domain.PrincipalFromContext(r.Context())
	})
	return h, func() (domain.ContextPrincipal, bool) { return cp, found }
}

// hashKey returns the SHA-256 hex hash of a key.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

func TestAuth_ValidJWT(t *testing.T) {
	handler, getPrincipal := nextHandler()

	auth := NewAuthenticator(
		&stubValidator{claims: &JWTClaims{
			Subject: "user1",
			Issuer:  "https://issuer.example.com",
			Raw:     map[string]interface{}{"sub": "user1", "email": "user1@example.com"},
			Email:   strPtr("user1@example.com"),
		}},
		nil, // no API key lookup
		&stubPrincipalLookup{principals: map[string]*domain.Principal{
			"user1@example.com": {Name: "user1@example.com", IsAdmin: true, Type: "user"},
		}},
		nil, // no provisioner
		config.AuthConfig{NameClaim: "email"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w := httptest.NewRecorder()

	auth.Middleware()(handler).ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	cp, found := getPrincipal()
	require.True(t, found)
	assert.Equal(t, "user1@example.com", cp.Name)
	assert.True(t, cp.IsAdmin)
}

func TestAuth_ExpiredJWT(t *testing.T) {
	auth := NewAuthenticator(
		&stubValidator{err: fmt.Errorf("token expired")},
		nil, nil, nil,
		config.AuthConfig{},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer expired-token")
	w := httptest.NewRecorder()

	auth.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	})).ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuth_MissingSubClaim(t *testing.T) {
	auth := NewAuthenticator(
		&stubValidator{claims: &JWTClaims{
			Subject: "",
			Raw:     map[string]interface{}{},
		}},
		nil, nil, nil,
		config.AuthConfig{NameClaim: "sub"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer no-sub-token")
	w := httptest.NewRecorder()

	auth.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	})).ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuth_ValidAPIKey(t *testing.T) {
	handler, getPrincipal := nextHandler()
	rawKey := "test-api-key-12345678"

	auth := NewAuthenticator(
		nil, // no JWT validator
		&stubAPIKeyLookup{keys: map[string]string{
			hashKey(rawKey): "api-user",
		}},
		&stubPrincipalLookup{principals: map[string]*domain.Principal{
			"api-user": {Name: "api-user", IsAdmin: false, Type: "service_principal"},
		}},
		nil,
		config.AuthConfig{APIKeyEnabled: true, APIKeyHeader: "X-API-Key"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", rawKey)
	w := httptest.NewRecorder()

	auth.Middleware()(handler).ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	cp, found := getPrincipal()
	require.True(t, found)
	assert.Equal(t, "api-user", cp.Name)
	assert.False(t, cp.IsAdmin)
	assert.Equal(t, "service_principal", cp.Type)
}

func TestAuth_UnknownAPIKey(t *testing.T) {
	auth := NewAuthenticator(
		nil,
		&stubAPIKeyLookup{keys: map[string]string{}},
		nil, nil,
		config.AuthConfig{APIKeyEnabled: true, APIKeyHeader: "X-API-Key"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "unknown-key")
	w := httptest.NewRecorder()

	auth.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	})).ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuth_NoCredentials(t *testing.T) {
	auth := NewAuthenticator(
		nil, nil, nil, nil,
		config.AuthConfig{APIKeyEnabled: true, APIKeyHeader: "X-API-Key"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	auth.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	})).ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuth_BearerPrecedence(t *testing.T) {
	handler, getPrincipal := nextHandler()
	rawKey := "test-api-key-12345678"

	auth := NewAuthenticator(
		&stubValidator{claims: &JWTClaims{
			Subject: "jwt-user",
			Raw:     map[string]interface{}{"sub": "jwt-user"},
		}},
		&stubAPIKeyLookup{keys: map[string]string{
			hashKey(rawKey): "api-user",
		}},
		&stubPrincipalLookup{principals: map[string]*domain.Principal{
			"jwt-user": {Name: "jwt-user", Type: "user"},
		}},
		nil,
		config.AuthConfig{APIKeyEnabled: true, APIKeyHeader: "X-API-Key", NameClaim: "sub"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("X-API-Key", rawKey)
	w := httptest.NewRecorder()

	auth.Middleware()(handler).ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	cp, found := getPrincipal()
	require.True(t, found)
	assert.Equal(t, "jwt-user", cp.Name, "Bearer token should take precedence over API key")
}

func TestAuth_JITProvisionNewUser(t *testing.T) {
	handler, getPrincipal := nextHandler()

	prov := &stubProvisioner{
		result: &domain.Principal{Name: "new-user@example.com", IsAdmin: false, Type: "user"},
	}
	auth := NewAuthenticator(
		&stubValidator{claims: &JWTClaims{
			Subject: "ext-id-123",
			Issuer:  "https://issuer.example.com",
			Email:   strPtr("new-user@example.com"),
			Raw:     map[string]interface{}{"sub": "ext-id-123", "email": "new-user@example.com"},
		}},
		nil,
		nil, // no principal lookup, relies on provisioner
		prov,
		config.AuthConfig{NameClaim: "email"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer new-user-token")
	w := httptest.NewRecorder()

	auth.Middleware()(handler).ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	require.True(t, prov.called)
	cp, found := getPrincipal()
	require.True(t, found)
	assert.Equal(t, "new-user@example.com", cp.Name)
}

func TestAuth_JITBootstrapAdmin(t *testing.T) {
	handler, getPrincipal := nextHandler()

	prov := &stubProvisioner{
		result: &domain.Principal{Name: "admin@example.com", IsAdmin: true, Type: "user"},
	}
	auth := NewAuthenticator(
		&stubValidator{claims: &JWTClaims{
			Subject: "bootstrap-sub-123",
			Issuer:  "https://issuer.example.com",
			Email:   strPtr("admin@example.com"),
			Raw:     map[string]interface{}{"sub": "bootstrap-sub-123", "email": "admin@example.com"},
		}},
		nil, nil,
		prov,
		config.AuthConfig{
			NameClaim:      "email",
			BootstrapAdmin: "bootstrap-sub-123",
		},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer bootstrap-token")
	w := httptest.NewRecorder()

	auth.Middleware()(handler).ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	cp, found := getPrincipal()
	require.True(t, found)
	assert.True(t, cp.IsAdmin)
}

func TestAuth_JWTFallbackDeniesEmptyPrincipal(t *testing.T) {
	// When no provisioner is configured and principal lookup fails,
	// the middleware should return 401 instead of creating a principal with empty ID.
	auth := NewAuthenticator(
		&stubValidator{claims: &JWTClaims{
			Subject: "unknown-user",
			Raw:     map[string]interface{}{"sub": "unknown-user"},
		}},
		nil,
		&stubPrincipalLookup{principals: map[string]*domain.Principal{}}, // empty — lookup will fail
		nil, // no provisioner
		config.AuthConfig{NameClaim: "sub"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer valid-but-unknown-user-token")
	w := httptest.NewRecorder()

	auth.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called for unresolvable principal")
	})).ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuth_APIKeyDisabled(t *testing.T) {
	rawKey := "test-api-key-12345678"

	auth := NewAuthenticator(
		nil,
		&stubAPIKeyLookup{keys: map[string]string{
			hashKey(rawKey): "api-user",
		}},
		nil, nil,
		config.AuthConfig{APIKeyEnabled: false, APIKeyHeader: "X-API-Key"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", rawKey)
	w := httptest.NewRecorder()

	auth.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	})).ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuth_ResolveDisplayName(t *testing.T) {
	tests := []struct {
		name     string
		cfg      config.AuthConfig
		claims   *JWTClaims
		wantName string
	}{
		{
			name: "email claim",
			cfg:  config.AuthConfig{NameClaim: "email"},
			claims: &JWTClaims{
				Subject: "sub-id",
				Email:   strPtr("user@example.com"),
				Raw:     map[string]interface{}{"sub": "sub-id", "email": "user@example.com"},
			},
			wantName: "user@example.com",
		},
		{
			name: "preferred_username fallback",
			cfg:  config.AuthConfig{NameClaim: "email"},
			claims: &JWTClaims{
				Subject: "sub-id",
				Raw:     map[string]interface{}{"sub": "sub-id", "preferred_username": "jdoe"},
			},
			wantName: "jdoe",
		},
		{
			name: "sub fallback",
			cfg:  config.AuthConfig{NameClaim: "email"},
			claims: &JWTClaims{
				Subject: "sub-guid-123",
				Raw:     map[string]interface{}{"sub": "sub-guid-123"},
			},
			wantName: "sub-guid-123",
		},
		{
			name: "custom claim",
			cfg:  config.AuthConfig{NameClaim: "upn"},
			claims: &JWTClaims{
				Subject: "sub-id",
				Raw:     map[string]interface{}{"sub": "sub-id", "upn": "custom@example.com"},
			},
			wantName: "custom@example.com",
		},
		{
			name: "name sanitization - uppercase",
			cfg:  config.AuthConfig{NameClaim: "sub"},
			claims: &JWTClaims{
				Subject: "UPPER-CASE-USER",
				Raw:     map[string]interface{}{"sub": "UPPER-CASE-USER"},
			},
			wantName: "upper-case-user",
		},
		{
			name: "name sanitization - whitespace",
			cfg:  config.AuthConfig{NameClaim: "sub"},
			claims: &JWTClaims{
				Subject: "  spaced  ",
				Raw:     map[string]interface{}{"sub": "  spaced  "},
			},
			wantName: "spaced",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth := &Authenticator{cfg: tt.cfg}
			got := auth.resolveDisplayName(tt.claims)
			assert.Equal(t, tt.wantName, got)
		})
	}
}

func TestAuth_JWTAdminClaim_OverridesPrincipalRole(t *testing.T) {
	handler, getPrincipal := nextHandler()

	auth := NewAuthenticator(
		&stubValidator{claims: &JWTClaims{
			Subject: "dev-admin",
			Raw:     map[string]interface{}{"sub": "dev-admin", "admin": true},
		}},
		nil,
		&stubPrincipalLookup{principals: map[string]*domain.Principal{
			"dev-admin": {Name: "dev-admin", IsAdmin: false, Type: "user"},
		}},
		nil,
		config.AuthConfig{NameClaim: "sub"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w := httptest.NewRecorder()

	auth.Middleware()(handler).ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	cp, found := getPrincipal()
	require.True(t, found)
	assert.Equal(t, "dev-admin", cp.Name)
	assert.True(t, cp.IsAdmin)
}

func TestAuth_JWTAdminClaim_DeniesWithoutPrincipalResolution(t *testing.T) {
	handler, _ := nextHandler()

	auth := NewAuthenticator(
		&stubValidator{claims: &JWTClaims{
			Subject: "ephemeral-admin",
			Raw:     map[string]interface{}{"sub": "ephemeral-admin", "admin": true},
		}},
		nil,
		nil,
		nil,
		config.AuthConfig{NameClaim: "sub"},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w := httptest.NewRecorder()

	auth.Middleware()(handler).ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func strPtr(s string) *string {
	return &s
}
