package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	authsvc "duck-demo/internal/service/auth"
	securitysvc "duck-demo/internal/service/security"
)

type uiSecurityTestEnv struct {
	router        *chi.Mux
	principalRepo *repository.PrincipalRepo
}

func TestUISecurity_PrincipalsFlow(t *testing.T) {
	env := newUISecurityTestEnv(t)
	sessionCookie := loginSessionCookie(t, env.router)
	csrfCookie := fetchCSRFCookie(t, env.router, sessionCookie, "/ui/security/principals/new")

	form := url.Values{}
	form.Set("csrf_token", csrfCookie.Value)
	form.Set("name", "analyst")
	form.Set("type", "user")

	resp := postFormWithCookies(t, env.router, "/ui/security/principals", form, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, resp.Code)
	location := resp.Header().Get("Location")
	require.Contains(t, location, "/ui/security/principals/")

	detail := getWithCookies(t, env.router, location, sessionCookie, csrfCookie)
	assert.Equal(t, http.StatusOK, detail.Code)
	assert.Contains(t, detail.Body.String(), "analyst")
	assert.Contains(t, detail.Body.String(), "Grant admin")

	toggleForm := url.Values{}
	toggleForm.Set("csrf_token", csrfCookie.Value)
	toggleForm.Set("is_admin", "true")
	toggleResp := postFormWithCookies(t, env.router, location+"/admin", toggleForm, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, toggleResp.Code)

	updated := getWithCookies(t, env.router, location, sessionCookie, csrfCookie)
	assert.Equal(t, http.StatusOK, updated.Code)
	assert.Contains(t, updated.Body.String(), "Revoke admin")
}

func TestUISecurity_GroupsAndGrantsFlow(t *testing.T) {
	env := newUISecurityTestEnv(t)
	member, err := env.principalRepo.Create(context.Background(), &domain.Principal{Name: "member-user", Type: "user"})
	require.NoError(t, err)

	sessionCookie := loginSessionCookie(t, env.router)
	csrfCookie := fetchCSRFCookie(t, env.router, sessionCookie, "/ui/security/groups/new")

	groupForm := url.Values{}
	groupForm.Set("csrf_token", csrfCookie.Value)
	groupForm.Set("name", "ops")
	groupForm.Set("description", "operations")
	groupResp := postFormWithCookies(t, env.router, "/ui/security/groups", groupForm, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, groupResp.Code)
	groupLocation := groupResp.Header().Get("Location")
	require.Contains(t, groupLocation, "/ui/security/groups/")

	memberForm := url.Values{}
	memberForm.Set("csrf_token", csrfCookie.Value)
	memberForm.Set("member_id", member.ID)
	memberForm.Set("member_type", "user")
	memberResp := postFormWithCookies(t, env.router, groupLocation+"/members", memberForm, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, memberResp.Code)

	groupDetail := getWithCookies(t, env.router, groupLocation, sessionCookie, csrfCookie)
	assert.Equal(t, http.StatusOK, groupDetail.Code)
	assert.Contains(t, groupDetail.Body.String(), member.ID)

	groupID := strings.TrimPrefix(groupLocation, "/ui/security/groups/")
	grantForm := url.Values{}
	grantForm.Set("csrf_token", csrfCookie.Value)
	grantForm.Set("principal_id", groupID)
	grantForm.Set("principal_type", "group")
	grantForm.Set("privilege", domain.PrivUseCatalog)
	grantForm.Set("securable_type", domain.SecurableCatalog)
	grantForm.Set("securable_id", domain.CatalogID)
	grantResp := postFormWithCookies(t, env.router, "/ui/security/grants", grantForm, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, grantResp.Code)

	grantsPage := getWithCookies(t, env.router, "/ui/security/grants?principal_id="+url.QueryEscape(groupID)+"&principal_type=group", sessionCookie, csrfCookie)
	assert.Equal(t, http.StatusOK, grantsPage.Code)
	assert.Contains(t, grantsPage.Body.String(), domain.PrivUseCatalog)
	assert.Contains(t, grantsPage.Body.String(), groupID)
}

func TestUISecurity_APIKeysCreateFlow(t *testing.T) {
	env := newUISecurityTestEnv(t)
	admin, err := env.principalRepo.GetByName(context.Background(), "uiadmin")
	require.NoError(t, err)

	sessionCookie := loginSessionCookie(t, env.router)
	csrfCookie := fetchCSRFCookie(t, env.router, sessionCookie, "/ui/security/api-keys")

	form := url.Values{}
	form.Set("csrf_token", csrfCookie.Value)
	form.Set("principal_id", admin.ID)
	form.Set("name", "automation")
	form.Set("expires_at", time.Now().Add(24*time.Hour).UTC().Format(time.RFC3339))

	resp := postFormWithCookies(t, env.router, "/ui/security/api-keys", form, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, resp.Body.String(), "API key created")
	assert.Contains(t, resp.Body.String(), "automation")

	list := getWithCookies(t, env.router, "/ui/security/api-keys?principal_id="+url.QueryEscape(admin.ID), sessionCookie, csrfCookie)
	assert.Equal(t, http.StatusOK, list.Code)
	assert.Contains(t, list.Body.String(), "automation")
}

func newUISecurityTestEnv(t *testing.T) uiSecurityTestEnv {
	t.Helper()

	writeDB, _ := internaldb.OpenTestSQLite(t)
	principalRepo := repository.NewPrincipalRepo(writeDB)
	groupRepo := repository.NewGroupRepo(writeDB)
	grantRepo := repository.NewGrantRepo(writeDB)
	apiKeyRepo := repository.NewAPIKeyRepo(writeDB)
	localCredRepo := repository.NewLocalCredentialRepo(writeDB)
	loginAttemptRepo := repository.NewAuthLoginAttemptRepo(writeDB)
	setupStateRepo := repository.NewSetupStateRepo(writeDB)
	providerRepo := repository.NewAuthProviderRepo(writeDB)
	auditRepo := repository.NewAuditRepo(writeDB)
	webSessionRepo := repository.NewWebSessionRepo(writeDB)

	authService := authsvc.NewService(principalRepo, localCredRepo, loginAttemptRepo, setupStateRepo, providerRepo, auditRepo, "ui-test-secret")
	_, err := authService.Bootstrap(context.Background(), authsvc.BootstrapRequest{
		Username:      "uiadmin",
		Password:      "super-secure-password",
		PrincipalName: "uiadmin",
	})
	require.NoError(t, err)

	webSessionService := authsvc.NewSessionService(principalRepo, webSessionRepo, auditRepo, 30*time.Minute, 24*time.Hour)
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, authService, webSessionService, nil, config.AuthConfig{WebSessionCookieName: "ui_session"}, false)
	h.Principal = securitysvc.NewPrincipalService(principalRepo, auditRepo)
	h.Group = securitysvc.NewGroupService(groupRepo, auditRepo)
	h.Grant = securitysvc.NewGrantService(grantRepo, auditRepo)
	h.APIKey = securitysvc.NewAPIKeyService(apiKeyRepo, auditRepo)

	router := chi.NewRouter()
	router.Route("/ui", func(r chi.Router) {
		MountRoutes(r, h)
	})
	return uiSecurityTestEnv{router: router, principalRepo: principalRepo}
}

func fetchCSRFCookie(t *testing.T, router http.Handler, sessionCookie *http.Cookie, path string) *http.Cookie {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(sessionCookie)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	for _, cookie := range resp.Result().Cookies() {
		if cookie.Name == csrfCookieName {
			return cookie
		}
	}
	t.Fatalf("csrf cookie not set for %s", path)
	return nil
}

func postFormWithCookies(t *testing.T, router http.Handler, path string, form url.Values, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for i := range cookies {
		req.AddCookie(cookies[i])
	}
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

func getWithCookies(t *testing.T, router http.Handler, path string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for i := range cookies {
		req.AddCookie(cookies[i])
	}
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}
