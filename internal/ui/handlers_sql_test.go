package ui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	authsvc "duck-demo/internal/service/auth"
	"duck-demo/internal/service/query"
)

func TestUISQLRuntimeManifest_RequiresWebSession(t *testing.T) {
	router, _ := newUITestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/ui/sql/runtime/manifest?schema=main&table=items", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	require.Equal(t, http.StatusSeeOther, resp.Code)
	assert.Equal(t, "/ui/login", resp.Header().Get("Location"))
}

func TestUISQLRuntimeManifest_ReturnsJSONErrorWhenServiceMissing(t *testing.T) {
	router, _ := newUITestRouter(t)
	sessionCookie := loginSessionCookie(t, router)

	req := httptest.NewRequest(http.MethodGet, "/ui/sql/runtime/manifest?schema=main&table=items", nil)
	req.AddCookie(sessionCookie)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	require.Equal(t, http.StatusInternalServerError, resp.Code)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header().Get("Content-Type"))
	var payload map[string]string
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	assert.Equal(t, "manifest service is not configured", payload["error"])
}

func TestUISQLRuntimeManifest_RequiresTableParameter(t *testing.T) {
	router := newUISQLManifestRouter(t, &query.ManifestService{})
	sessionCookie := loginSessionCookie(t, router)

	req := httptest.NewRequest(http.MethodGet, "/ui/sql/runtime/manifest?schema=main", nil)
	req.AddCookie(sessionCookie)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header().Get("Content-Type"))
	var payload map[string]string
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	assert.Equal(t, "table is required", payload["error"])
}

func newUISQLManifestRouter(t *testing.T, manifestSvc *query.ManifestService) *chi.Mux {
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
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, authService, webSessionService, nil, config.AuthConfig{WebSessionCookieName: "ui_session"}, false)
	h.Manifest = manifestSvc

	r := chi.NewRouter()
	r.Route("/ui", func(r chi.Router) {
		MountRoutes(r, h)
	})
	return r
}
