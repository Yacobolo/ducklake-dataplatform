package legacy

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func TestUIAdvanced_NotebookGitRepoSyncFlow(t *testing.T) {
	env := newUIAdvancedTestEnv(t)
	sessionCookie := loginSessionCookie(t, env.router)
	csrfCookie := fetchCSRFCookie(t, env.router, sessionCookie, "/ui/notebooks/git-repos/new")

	repoDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(repoDir, "notebooks"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "notebooks", "sales.yaml"), []byte(`
apiVersion: duck/v1
kind: Notebook
metadata:
  name: sales
spec:
  cells:
    - type: sql
      name: output
      role: output
      content: SELECT 1 AS value
`), 0o644))
	runGit(t, repoDir, "init", "--initial-branch=master")
	runGit(t, repoDir, "config", "user.name", "codex")
	runGit(t, repoDir, "config", "user.email", "codex@example.com")
	runGit(t, repoDir, "add", "notebooks/sales.yaml")
	runGit(t, repoDir, "commit", "-m", "add notebook")

	createForm := url.Values{}
	createForm.Set("csrf_token", csrfCookie.Value)
	createForm.Set("url", repoDir)
	createForm.Set("branch", "master")

	createResp := postFormWithCookies(t, env.router, "/ui/notebooks/git-repos", createForm, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, createResp.Code)
	location := createResp.Header().Get("Location")

	syncForm := url.Values{}
	syncForm.Set("csrf_token", csrfCookie.Value)
	syncResp := postFormWithCookies(t, env.router, location+"/sync", syncForm, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusOK, syncResp.Code)
	assert.Contains(t, syncResp.Body.String(), "Created notebooks: 1")
	assert.Contains(t, syncResp.Body.String(), "Updated notebooks: 0")
	assert.Contains(t, syncResp.Body.String(), "Deleted notebooks: 0")
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
	h.GitService = notebooksvc.NewGitService(repository.NewGitRepoRepo(writeDB), repository.NewNotebookRepo(writeDB), auditRepo)
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

func runGit(t *testing.T, repoDir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = repoDir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %s failed: %s", strings.Join(args, " "), strings.TrimSpace(string(output)))
	return string(output)
}
