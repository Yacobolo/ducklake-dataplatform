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
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	authsvc "duck-demo/internal/service/auth"
)

func TestUIAuth_LoginSetsSessionCookieAndAllowsHome(t *testing.T) {
	router, cookieName := newUITestRouter(t)

	form := url.Values{}
	form.Set("username", "uiadmin")
	form.Set("password", "super-secure-password")

	r := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)

	require.Equal(t, http.StatusSeeOther, w.Code)
	var sessionCookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == cookieName {
			sessionCookie = c
			break
		}
	}
	require.NotNil(t, sessionCookie)
	require.NotEmpty(t, sessionCookie.Value)

	homeReq := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	homeReq.AddCookie(sessionCookie)
	homeResp := httptest.NewRecorder()
	router.ServeHTTP(homeResp, homeReq)

	assert.Equal(t, http.StatusOK, homeResp.Code)
}

func TestUIAuth_LogoutRevokesSession(t *testing.T) {
	router, cookieName := newUITestRouter(t)
	sessionCookie := loginSessionCookie(t, router)

	logoutReq := httptest.NewRequest(http.MethodPost, "/ui/logout", nil)
	logoutReq.AddCookie(sessionCookie)
	logoutResp := httptest.NewRecorder()
	router.ServeHTTP(logoutResp, logoutReq)

	require.Equal(t, http.StatusSeeOther, logoutResp.Code)
	assert.Equal(t, "/ui/login", logoutResp.Header().Get("Location"))
	var cleared bool
	for _, c := range logoutResp.Result().Cookies() {
		if c.Name == cookieName && c.MaxAge < 0 {
			cleared = true
		}
	}
	assert.True(t, cleared)

	homeReq := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	homeReq.AddCookie(sessionCookie)
	homeResp := httptest.NewRecorder()
	router.ServeHTTP(homeResp, homeReq)

	assert.Equal(t, http.StatusSeeOther, homeResp.Code)
	assert.Equal(t, "/ui/login", homeResp.Header().Get("Location"))
}

func TestUIAuth_TokenCookieIgnored(t *testing.T) {
	router, _ := newUITestRouter(t)

	r := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	r.AddCookie(&http.Cookie{Name: "ui_bearer", Value: "some-token"})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusSeeOther, w.Code)
	assert.Equal(t, "/ui/login", w.Header().Get("Location"))
}

func TestUIAuth_UIDevBypassLoopbackAllowsHome(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, config.AuthConfig{UIDevBypass: true}, false)
	r := chi.NewRouter()
	r.Route("/ui", func(r chi.Router) {
		MountRoutes(r, h)
	})

	req := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestUIAuth_UIDevBypassNonLoopbackStillRedirects(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, config.AuthConfig{UIDevBypass: true}, false)
	r := chi.NewRouter()
	r.Route("/ui", func(r chi.Router) {
		MountRoutes(r, h)
	})

	req := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusSeeOther, resp.Code)
	assert.Equal(t, "/ui/login", resp.Header().Get("Location"))
}

func TestUIAuth_LegacyPipelinesRouteNotMounted(t *testing.T) {
	router, _ := newUITestRouter(t)
	sessionCookie := loginSessionCookie(t, router)

	r := httptest.NewRequest(http.MethodGet, "/ui/pipelines", nil)
	r.AddCookie(sessionCookie)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestUIAuth_UIDevBypassUsesPrincipalResolverWhenAvailable(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &stubPrincipalResolver{principal: &domain.Principal{ID: "p-dev", Name: "resolver-admin", Type: "user", IsAdmin: true}}, config.AuthConfig{UIDevBypass: true}, false)
	r := chi.NewRouter()
	r.Route("/ui", func(r chi.Router) {
		MountRoutes(r, h)
	})

	req := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	req.RemoteAddr = "127.0.0.1:23456"
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
	body := resp.Body.String()
	assert.Contains(t, body, "resolver-admin")
	assert.Contains(t, body, "admin")
}

type stubPrincipalResolver struct {
	principal *domain.Principal
	err       error
}

func (s *stubPrincipalResolver) ResolveOrProvision(_ context.Context, _ domain.ResolveOrProvisionRequest) (*domain.Principal, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.principal, nil
}

func newUITestRouter(t *testing.T) (*chi.Mux, string) {
	t.Helper()

	writeDB, _ := internaldb.OpenTestSQLite(t)
	principalRepo := repository.NewPrincipalRepo(writeDB)
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
	authCfg := config.AuthConfig{WebSessionCookieName: "ui_session"}
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, authService, webSessionService, nil, authCfg, false)

	r := chi.NewRouter()
	r.Route("/ui", func(r chi.Router) {
		MountRoutes(r, h)
	})
	return r, "ui_session"
}

func loginSessionCookie(t *testing.T, router http.Handler) *http.Cookie {
	t.Helper()
	form := url.Values{}
	form.Set("username", "uiadmin")
	form.Set("password", "super-secure-password")
	r := httptest.NewRequest(http.MethodPost, "/ui/login", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	require.Equal(t, http.StatusSeeOther, w.Code)
	for _, c := range w.Result().Cookies() {
		if c.Name == "ui_session" {
			return c
		}
	}
	t.Fatalf("ui_session cookie not set")
	return nil
}
