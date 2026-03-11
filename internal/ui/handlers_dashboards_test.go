package ui

import (
	"context"
	"net/http"
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
	dashboardsvc "duck-demo/internal/service/dashboard"
	"duck-demo/internal/service/query"
)

type dashboardHandlerQueryStub struct{}

func (s *dashboardHandlerQueryStub) Execute(_ context.Context, _ string, _ string) (*query.QueryResult, error) {
	return nil, domain.ErrNotImplemented("query execution not needed")
}

type uiDashboardTestEnv struct {
	router     *chi.Mux
	widgetRepo *repository.DashboardWidgetRepo
}

func newUIDashboardTestEnv(t *testing.T) uiDashboardTestEnv {
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
	widgetRepo := repository.NewDashboardWidgetRepo(writeDB)
	h.Dashboard = dashboardsvc.NewService(
		repository.NewDashboardRepo(writeDB),
		widgetRepo,
		repository.NewNotebookRepo(writeDB),
		auditRepo,
		&dashboardHandlerQueryStub{},
		nil,
	)

	router := chi.NewRouter()
	router.Route("/ui", func(r chi.Router) {
		MountRoutes(r, h)
	})
	return uiDashboardTestEnv{router: router, widgetRepo: widgetRepo}
}

func TestUIDashboards_CreateAndEditWidgetFlow(t *testing.T) {
	env := newUIDashboardTestEnv(t)
	sessionCookie := loginSessionCookie(t, env.router)
	csrfCookie := fetchCSRFCookie(t, env.router, sessionCookie, "/ui/dashboards/new")

	createDashboard := url.Values{}
	createDashboard.Set("csrf_token", csrfCookie.Value)
	createDashboard.Set("name", "Executive Overview")
	createDashboard.Set("description", "KPIs")

	dashResp := postFormWithCookies(t, env.router, "/ui/dashboards", createDashboard, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, dashResp.Code)
	dashboardLocation := dashResp.Header().Get("Location")
	require.Contains(t, dashboardLocation, "/ui/dashboards/")
	dashboardID := dashboardLocation[strings.LastIndex(dashboardLocation, "/")+1:]

	createWidget := url.Values{}
	createWidget.Set("csrf_token", csrfCookie.Value)
	createWidget.Set("name", "Revenue")
	createWidget.Set("description", "Revenue table")
	createWidget.Set("source_kind", "sql_query")
	createWidget.Set("sql", "select region, revenue from summary")
	createWidget.Set("visual_kind", "table")
	createWidget.Set("layout_x", "0")
	createWidget.Set("layout_y", "0")
	createWidget.Set("layout_w", "4")
	createWidget.Set("layout_h", "3")

	widgetResp := postFormWithCookies(t, env.router, dashboardLocation+"/widgets", createWidget, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, widgetResp.Code)
	widgets, err := env.widgetRepo.ListByDashboard(context.Background(), dashboardID)
	require.NoError(t, err)
	require.Len(t, widgets, 1)
	widgetID := widgets[0].ID

	editPage := getWithCookies(t, env.router, dashboardLocation+"/widgets/"+widgetID+"/edit", sessionCookie, csrfCookie)
	require.Equal(t, http.StatusOK, editPage.Code)
	assert.Contains(t, editPage.Body.String(), "Edit Widget")
	assert.Contains(t, editPage.Body.String(), "Revenue")

	updateWidget := url.Values{}
	updateWidget.Set("csrf_token", csrfCookie.Value)
	updateWidget.Set("name", "Revenue Updated")
	updateWidget.Set("description", "Updated revenue table")
	updateWidget.Set("source_kind", "sql_query")
	updateWidget.Set("sql", "select region, revenue from summary")
	updateWidget.Set("visual_kind", "table")
	updateWidget.Set("layout_x", "1")
	updateWidget.Set("layout_y", "0")
	updateWidget.Set("layout_w", "5")
	updateWidget.Set("layout_h", "3")

	updateResp := postFormWithCookies(t, env.router, dashboardLocation+"/widgets/"+widgetID+"/update", updateWidget, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, updateResp.Code)
	assert.Equal(t, dashboardLocation, updateResp.Header().Get("Location"))
}

func TestUIDashboards_WidgetCreateInvalidInput(t *testing.T) {
	env := newUIDashboardTestEnv(t)
	sessionCookie := loginSessionCookie(t, env.router)
	csrfCookie := fetchCSRFCookie(t, env.router, sessionCookie, "/ui/dashboards/new")

	createDashboard := url.Values{}
	createDashboard.Set("csrf_token", csrfCookie.Value)
	createDashboard.Set("name", "Executive Overview")
	dashResp := postFormWithCookies(t, env.router, "/ui/dashboards", createDashboard, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusSeeOther, dashResp.Code)
	dashboardLocation := dashResp.Header().Get("Location")

	form := url.Values{}
	form.Set("csrf_token", csrfCookie.Value)
	form.Set("name", "Broken widget")
	form.Set("source_kind", "semantic_query")
	form.Set("project_name", "analytics")
	form.Set("semantic_model_name", "sales")
	form.Set("visual_kind", "table")

	resp := postFormWithCookies(t, env.router, dashboardLocation+"/widgets", form, sessionCookie, csrfCookie)
	require.Equal(t, http.StatusBadRequest, resp.Code)
	assert.Contains(t, resp.Body.String(), "at least one metric")
}
