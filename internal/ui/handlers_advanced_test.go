package ui

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	authsvc "duck-demo/internal/service/auth"
	notebooksvc "duck-demo/internal/service/notebook"
	semanticsvc "duck-demo/internal/service/semantic"
)

type uiAdvancedTestEnv struct {
	router *chi.Mux
}

func TestUIAdvanced_NotebookGitRepoCreateFlow(t *testing.T) {
	env := newUIAdvancedTestEnv(t)
	sessionCookie := loginSessionCookie(t, env.router)
	csrfCookie := fetchCSRFCookie(t, env.router, sessionCookie, "/ui/notebooks/git-repos/new")

	form := url.Values{}
	form.Set("csrf_token", csrfCookie.Value)
	form.Set("url", "https://github.com/acme/notebooks.git")
	form.Set("branch", "main")
	form.Set("path", "analytics")

	resp := postFormWithCookies(t, env.router, "/ui/notebooks/git-repos", form, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, resp.Code)
	location := resp.Header().Get("Location")
	require.Contains(t, location, "/ui/notebooks/git-repos/")

	detail := getWithCookies(t, env.router, location, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusOK, detail.Code)
	assert.Contains(t, detail.Body.String(), "https://github.com/acme/notebooks.git")
	assert.Contains(t, detail.Body.String(), "analytics")
}

func TestUIAdvanced_SemanticModelCreateFlow(t *testing.T) {
	env := newUIAdvancedTestEnv(t)
	sessionCookie := loginSessionCookie(t, env.router)
	csrfCookie := fetchCSRFCookie(t, env.router, sessionCookie, "/ui/semantic/models/new")

	form := url.Values{}
	form.Set("csrf_token", csrfCookie.Value)
	form.Set("project_name", "analytics")
	form.Set("name", "sales")
	form.Set("description", "Sales semantic layer")
	form.Set("base_model_ref", "analytics.fct_sales")
	form.Set("default_time_dimension", "order_date")

	resp := postFormWithCookies(t, env.router, "/ui/semantic/models", form, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, resp.Code)
	location := resp.Header().Get("Location")
	require.Equal(t, "/ui/semantic/models/analytics/sales", location)

	detail := getWithCookies(t, env.router, location, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusOK, detail.Code)
	assert.Contains(t, detail.Body.String(), "analytics.fct_sales")
	assert.Contains(t, detail.Body.String(), "order_date")
	assert.Contains(t, detail.Body.String(), "Create metric")
}

func newUIAdvancedTestEnv(t *testing.T) uiAdvancedTestEnv {
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
	h.GitService = notebooksvc.NewGitService(repository.NewGitRepoRepo(writeDB), auditRepo)
	h.Semantic = semanticsvc.NewService(
		repository.NewSemanticModelRepo(writeDB),
		repository.NewSemanticMetricRepo(writeDB),
		repository.NewSemanticRelationshipRepo(writeDB),
		repository.NewSemanticPreAggregationRepo(writeDB),
	)

	router := chi.NewRouter()
	router.Route("/ui", func(r chi.Router) {
		MountRoutes(r, h)
	})
	return uiAdvancedTestEnv{router: router}
}
