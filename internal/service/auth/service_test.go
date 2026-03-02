package auth

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestService_BootstrapFirstRun_NoTokenRequired(t *testing.T) {
	deps := newServiceTestDeps()
	svc := NewService(deps.principals, deps.credentials, deps.loginAttempts, deps.setupState, deps.providers, deps.audit, "unit-test-secret")

	res, err := svc.Bootstrap(context.Background(), BootstrapRequest{
		Username:      "Admin",
		Password:      "super-secure-password",
		PrincipalName: "admin_user",
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Principal)

	assert.Equal(t, "admin_user", res.Principal.Name)
	assert.True(t, res.Principal.IsAdmin)

	cred, err := deps.credentials.GetByUsername(context.Background(), "admin")
	require.NoError(t, err)
	assert.Equal(t, res.Principal.ID, cred.PrincipalID)

	claims := parseJWTClaims(t, res.Token)
	assert.Equal(t, "admin_user", claims["sub"])
	assert.Equal(t, true, claims["admin"])
}

func TestService_BootstrapRequiresToken_AfterSetup(t *testing.T) {
	deps := newServiceTestDeps()
	deps.setupState.state.SetupCompleted = true
	svc := NewService(deps.principals, deps.credentials, deps.loginAttempts, deps.setupState, deps.providers, deps.audit, "unit-test-secret")

	_, err := svc.Bootstrap(context.Background(), BootstrapRequest{
		Username:      "newadmin",
		Password:      "super-secure-password",
		PrincipalName: "newadmin",
	})
	require.Error(t, err)

	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestService_LocalLogin_Success(t *testing.T) {
	deps := newServiceTestDeps()
	svc := NewService(deps.principals, deps.credentials, deps.loginAttempts, deps.setupState, deps.providers, deps.audit, "unit-test-secret")

	_, err := svc.Bootstrap(context.Background(), BootstrapRequest{
		Username:      "localadmin",
		Password:      "super-secure-password",
		PrincipalName: "localadmin",
	})
	require.NoError(t, err)

	res, err := svc.Login(context.Background(), "localadmin", "super-secure-password", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.Equal(t, "localadmin", res.Principal.Name)
	claims := parseJWTClaims(t, res.Token)
	assert.Equal(t, "localadmin", claims["sub"])
}

func TestService_LocalLogin_RateLimitedByUsername(t *testing.T) {
	deps := newServiceTestDeps()
	deps.loginAttempts.failedByUsername = 5
	svc := NewService(deps.principals, deps.credentials, deps.loginAttempts, deps.setupState, deps.providers, deps.audit, "unit-test-secret")

	_, err := svc.Bootstrap(context.Background(), BootstrapRequest{
		Username:      "limited",
		Password:      "super-secure-password",
		PrincipalName: "limited",
	})
	require.NoError(t, err)

	_, err = svc.Login(context.Background(), "limited", "super-secure-password", "10.0.0.9")
	var denied *domain.AccessDeniedError
	require.ErrorAs(t, err, &denied)
	assert.Equal(t, 1, deps.loginAttempts.insertCalls)
}

func parseJWTClaims(t *testing.T, token string) jwt.MapClaims {
	t.Helper()
	parsed, err := jwt.Parse(token, func(_ *jwt.Token) (interface{}, error) {
		return []byte("unit-test-secret"), nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	claims, ok := parsed.Claims.(jwt.MapClaims)
	require.True(t, ok)
	return claims
}

type serviceTestDeps struct {
	principals    *stubPrincipalRepo
	credentials   *stubCredentialRepo
	loginAttempts *stubLoginAttemptRepo
	setupState    *stubSetupStateRepo
	providers     *stubProviderRepo
	audit         *stubAuditRepo
}

func newServiceTestDeps() *serviceTestDeps {
	return &serviceTestDeps{
		principals:    newStubPrincipalRepo(),
		credentials:   newStubCredentialRepo(),
		loginAttempts: &stubLoginAttemptRepo{},
		setupState:    &stubSetupStateRepo{state: &domain.SetupState{}},
		providers:     &stubProviderRepo{cfg: &domain.AuthProviderConfig{}},
		audit:         &stubAuditRepo{},
	}
}

type stubPrincipalRepo struct {
	nextID int
	byID   map[string]*domain.Principal
	byName map[string]*domain.Principal
}

func newStubPrincipalRepo() *stubPrincipalRepo {
	return &stubPrincipalRepo{nextID: 1, byID: map[string]*domain.Principal{}, byName: map[string]*domain.Principal{}}
}

func (r *stubPrincipalRepo) Create(_ context.Context, p *domain.Principal) (*domain.Principal, error) {
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

func (r *stubPrincipalRepo) GetByID(_ context.Context, id string) (*domain.Principal, error) {
	p, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound("principal %q not found", id)
	}
	cp := *p
	return &cp, nil
}

func (r *stubPrincipalRepo) GetByName(_ context.Context, name string) (*domain.Principal, error) {
	p, ok := r.byName[name]
	if !ok {
		return nil, domain.ErrNotFound("principal %q not found", name)
	}
	cp := *p
	return &cp, nil
}

func (r *stubPrincipalRepo) GetByExternalID(_ context.Context, _, _ string) (*domain.Principal, error) {
	return nil, domain.ErrNotFound("not found")
}

func (r *stubPrincipalRepo) List(_ context.Context, _ domain.PageRequest) ([]domain.Principal, int64, error) {
	out := make([]domain.Principal, 0, len(r.byID))
	for _, p := range r.byID {
		out = append(out, *p)
	}
	return out, int64(len(out)), nil
}

func (r *stubPrincipalRepo) Delete(_ context.Context, id string) error {
	p, ok := r.byID[id]
	if !ok {
		return domain.ErrNotFound("principal %q not found", id)
	}
	delete(r.byID, id)
	delete(r.byName, p.Name)
	return nil
}

func (r *stubPrincipalRepo) SetAdmin(_ context.Context, id string, isAdmin bool) error {
	p, ok := r.byID[id]
	if !ok {
		return domain.ErrNotFound("principal %q not found", id)
	}
	p.IsAdmin = isAdmin
	return nil
}

func (r *stubPrincipalRepo) BindExternalID(_ context.Context, id string, externalID string, externalIssuer string) error {
	p, ok := r.byID[id]
	if !ok {
		return domain.ErrNotFound("principal %q not found", id)
	}
	p.ExternalID = &externalID
	p.ExternalIssuer = &externalIssuer
	return nil
}

type stubCredentialRepo struct {
	byUsername map[string]*domain.LocalCredential
}

func newStubCredentialRepo() *stubCredentialRepo {
	return &stubCredentialRepo{byUsername: map[string]*domain.LocalCredential{}}
}

func (r *stubCredentialRepo) Upsert(_ context.Context, c *domain.LocalCredential) error {
	cp := *c
	r.byUsername[c.Username] = &cp
	return nil
}

func (r *stubCredentialRepo) GetByUsername(_ context.Context, username string) (*domain.LocalCredential, error) {
	c, ok := r.byUsername[username]
	if !ok {
		return nil, domain.ErrNotFound("credential %q not found", username)
	}
	cp := *c
	return &cp, nil
}

func (r *stubCredentialRepo) GetByPrincipalID(_ context.Context, principalID string) (*domain.LocalCredential, error) {
	for _, c := range r.byUsername {
		if c.PrincipalID == principalID {
			cp := *c
			return &cp, nil
		}
	}
	return nil, domain.ErrNotFound("credential for principal %q not found", principalID)
}

func (r *stubCredentialRepo) Delete(_ context.Context, principalID string) error {
	for username, c := range r.byUsername {
		if c.PrincipalID == principalID {
			delete(r.byUsername, username)
			return nil
		}
	}
	return domain.ErrNotFound("credential for principal %q not found", principalID)
}

type stubLoginAttemptRepo struct {
	failedByUsername int64
	failedByIP       int64
	insertCalls      int
}

func (s *stubLoginAttemptRepo) Insert(_ context.Context, _ *domain.AuthLoginAttempt) error {
	s.insertCalls++
	return nil
}

func (s *stubLoginAttemptRepo) CountRecentFailedByUsername(_ context.Context, _ string, _ time.Time) (int64, error) {
	return s.failedByUsername, nil
}

func (s *stubLoginAttemptRepo) CountRecentFailedByIP(_ context.Context, _ string, _ time.Time) (int64, error) {
	return s.failedByIP, nil
}

type stubSetupStateRepo struct {
	state *domain.SetupState
}

func (s *stubSetupStateRepo) Get(_ context.Context) (*domain.SetupState, error) {
	cp := *s.state
	return &cp, nil
}

func (s *stubSetupStateRepo) Complete(_ context.Context, principalID string) error {
	now := time.Now()
	s.state.SetupCompleted = true
	s.state.SetupCompletedBy = &principalID
	s.state.SetupCompletedAt = &now
	return nil
}

func (s *stubSetupStateRepo) SetBootstrapToken(_ context.Context, tokenHash string, expiresAt time.Time) error {
	s.state.BootstrapTokenHash = &tokenHash
	s.state.BootstrapTokenExpiresAt = &expiresAt
	return nil
}

func (s *stubSetupStateRepo) ClearBootstrapToken(_ context.Context) error {
	s.state.BootstrapTokenHash = nil
	s.state.BootstrapTokenExpiresAt = nil
	return nil
}

type stubProviderRepo struct {
	cfg *domain.AuthProviderConfig
}

func (s *stubProviderRepo) Get(_ context.Context) (*domain.AuthProviderConfig, error) {
	cp := *s.cfg
	return &cp, nil
}

func (s *stubProviderRepo) Upsert(_ context.Context, cfg *domain.AuthProviderConfig) error {
	cp := *cfg
	s.cfg = &cp
	return nil
}

type stubAuditRepo struct{}

func (s *stubAuditRepo) Insert(_ context.Context, _ *domain.AuditEntry) error { return nil }

func (s *stubAuditRepo) List(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return nil, 0, nil
}
