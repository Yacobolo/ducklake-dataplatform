//go:build integration

package dashboards

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/db/repository"
	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/dashboard"
	"github.com/Yacobolo/quackstack/internal/service/query"
	"github.com/Yacobolo/quackstack/internal/service/semantic"
	"github.com/Yacobolo/quackstack/internal/ui/core"
)

type dashboardStateQueryRule struct {
	contains []string
	result   *query.QueryResult
}

type dashboardStateQueryExecutorStub struct {
	rules []dashboardStateQueryRule
	sqls  []string
}

func (s *dashboardStateQueryExecutorStub) Execute(_ context.Context, _ string, sqlQuery string) (*query.QueryResult, error) {
	s.sqls = append(s.sqls, sqlQuery)
	for _, rule := range s.rules {
		matched := true
		for _, fragment := range rule.contains {
			if !strings.Contains(sqlQuery, fragment) {
				matched = false
				break
			}
		}
		if matched {
			return rule.result, nil
		}
	}
	return &query.QueryResult{
		Columns:  []string{"gross_revenue"},
		Rows:     [][]interface{}{{7032274.4}},
		RowCount: 1,
	}, nil
}

type dashboardStateFixture struct {
	router           http.Handler
	dashboardID      string
	tableWidgetID    string
	tableOriginKey   string
	chartOriginKey   string
	overviewWidgetID string
	queryExec        *dashboardStateQueryExecutorStub
}

func TestDashboardStateEndpoint_UnfilteredPageScopesWidgets(t *testing.T) {
	t.Parallel()

	fixture := setupDashboardStateFixture(t)
	response := fetchDashboardStateResponse(t, fixture.router, "/ui/dashboards/"+fixture.dashboardID+"/state?page=overview")

	assert.Equal(t, fixture.dashboardID, response.DashboardID)
	assert.Equal(t, "overview", response.Page)
	assert.Empty(t, response.FilterKey)
	assert.Equal(t, domain.ComputeModeAuto, response.Compute.Mode)
	require.Len(t, response.Widgets, 1)
	assert.Equal(t, fixture.overviewWidgetID, response.Widgets[0].WidgetID)
	assert.Equal(t, domain.VisualOutputMetric, response.Widgets[0].VisualKind)
	assert.Equal(t, []interface{}{7032274.4}, response.Widgets[0].Payload.Rows[0])
}

func TestDashboardStateEndpoint_ChartOriginFilterAffectsSiblingsWithoutSelectingTable(t *testing.T) {
	t.Parallel()

	fixture := setupDashboardStateFixture(t)
	response := fetchDashboardStateResponse(t, fixture.router, "/ui/dashboards/"+fixture.dashboardID+"/state?page=geography&fo="+fixture.chartOriginKey+"%7Cpickup_zone%3AJFK+Airport")

	require.Len(t, response.Widgets, 3)
	byOrigin := widgetsByOriginKey(response.Widgets)

	pie := byOrigin[fixture.chartOriginKey]
	require.NotNil(t, pie)
	assert.Equal(t, []string{"JFK Airport"}, pie.Payload.Interaction.OriginFilters["pickup_zone"])
	assert.Len(t, pie.Payload.Rows, 2)

	table := byOrigin[fixture.tableOriginKey]
	require.NotNil(t, table)
	assert.Nil(t, table.Payload.Interaction.OriginFilters)
	assert.Equal(t, map[string][]string{"pickup_zone": []string{"JFK Airport"}}, table.Payload.Interaction.ActiveFilters)
	require.Len(t, table.Payload.Rows, 1)
	assert.Equal(t, "JFK Airport", table.Payload.Rows[0][0])

	boroughChart := byOrigin["chart-revenue-by-borough"]
	require.NotNil(t, boroughChart)
	require.Len(t, boroughChart.Payload.Rows, 1)
	assert.Equal(t, "Queens", boroughChart.Payload.Rows[0][0])
}

func TestDashboardStateEndpoint_TableOriginFilterMarksOriginFiltersAndCharts(t *testing.T) {
	t.Parallel()

	fixture := setupDashboardStateFixture(t)
	response := fetchDashboardStateResponse(t, fixture.router, "/ui/dashboards/"+fixture.dashboardID+"/state?page=geography&fo="+fixture.tableOriginKey+"%7Cborough%3AQueens&fo="+fixture.tableOriginKey+"%7Cpickup_zone%3AJFK+Airport")

	require.Len(t, response.Widgets, 3)
	byOrigin := widgetsByOriginKey(response.Widgets)

	table := byOrigin[fixture.tableOriginKey]
	require.NotNil(t, table)
	assert.Equal(t, []string{"Queens"}, table.Payload.Interaction.OriginFilters["borough"])
	assert.Equal(t, []string{"JFK Airport"}, table.Payload.Interaction.OriginFilters["pickup_zone"])

	pie := byOrigin[fixture.chartOriginKey]
	require.NotNil(t, pie)
	assert.Equal(t, map[string][]string{
		"borough":     []string{"Queens"},
		"pickup_zone": []string{"JFK Airport"},
	}, pie.Payload.Interaction.ActiveFilters)
	require.Len(t, pie.Payload.Rows, 1)
	assert.Equal(t, "JFK Airport", pie.Payload.Rows[0][0])

	boroughChart := byOrigin["chart-revenue-by-borough"]
	require.NotNil(t, boroughChart)
	require.Len(t, boroughChart.Payload.Rows, 1)
	assert.Equal(t, "Queens", boroughChart.Payload.Rows[0][0])
}

func TestDashboardStateEndpoint_TablePagingAndSort(t *testing.T) {
	t.Parallel()

	fixture := setupDashboardStateFixture(t)
	response := fetchDashboardStateResponse(t, fixture.router, "/ui/dashboards/"+fixture.dashboardID+"/state?page=geography&table_widget_id="+fixture.tableWidgetID+"&offset=1&limit=1&sort_column=pickup_zone&sort_direction=desc")

	byOrigin := widgetsByOriginKey(response.Widgets)
	table := byOrigin[fixture.tableOriginKey]
	require.NotNil(t, table)
	require.NotNil(t, table.Payload.Page)
	assert.Equal(t, 1, table.Payload.Page.Offset)
	assert.False(t, table.Payload.Page.Append)
	assert.True(t, table.Payload.Page.HasMore)
	require.NotNil(t, table.Payload.Sort)
	assert.Equal(t, "pickup_zone", table.Payload.Sort.Column)
	assert.Equal(t, "desc", table.Payload.Sort.Direction)
	require.Len(t, table.Payload.Rows, 1)
	assert.Equal(t, "JFK Airport", table.Payload.Rows[0][0])
}

func setupDashboardStateFixture(t *testing.T) dashboardStateFixture {
	t.Helper()

	writeDB, _ := internaldb.OpenTestSQLite(t)
	notebookRepo := repository.NewNotebookRepo(writeDB)
	dashboardRepo := repository.NewDashboardRepo(writeDB)
	widgetRepo := repository.NewDashboardWidgetRepo(writeDB)
	auditRepo := repository.NewAuditRepo(writeDB)
	modelRepo := repository.NewModelRepo(writeDB)
	semanticSvc := semantic.NewService(
		repository.NewSemanticModelRepo(writeDB),
		repository.NewSemanticMetricRepo(writeDB),
		repository.NewSemanticRelationshipRepo(writeDB),
		repository.NewSemanticPreAggregationRepo(writeDB),
		modelRepo,
	)

	queryExec := &dashboardStateQueryExecutorStub{
		rules: []dashboardStateQueryRule{
			{
				contains: []string{"dashboard_widget_count"},
				result: &query.QueryResult{
					Columns:  []string{"row_count"},
					Rows:     [][]interface{}{{2}},
					RowCount: 1,
				},
			},
			{
				contains: []string{"AS \"pickup_zone\"", "AS \"borough\"", "ORDER BY pickup_zone DESC", "OFFSET 1", "LIMIT 2"},
				result: &query.QueryResult{
					Columns:  []string{"pickup_zone", "borough", "gross_revenue"},
					Rows:     [][]interface{}{{"JFK Airport", "Queens", 1338981.25}, {"East Village", "Manhattan", 266675.71}},
					RowCount: 2,
				},
			},
			{
				contains: []string{"AS \"pickup_zone\"", "AS \"borough\"", "pickup_zone = 'JFK Airport'"},
				result: &query.QueryResult{
					Columns:  []string{"pickup_zone", "borough", "gross_revenue"},
					Rows:     [][]interface{}{{"JFK Airport", "Queens", 1338981.25}},
					RowCount: 1,
				},
			},
			{
				contains: []string{"AS \"pickup_zone\"", "AS \"borough\""},
				result: &query.QueryResult{
					Columns:  []string{"pickup_zone", "borough", "gross_revenue"},
					Rows:     [][]interface{}{{"JFK Airport", "Queens", 1338981.25}, {"East Village", "Manhattan", 266675.71}},
					RowCount: 2,
				},
			},
			{
				contains: []string{"pickup_zone = 'JFK Airport'", "AS \"borough\""},
				result: &query.QueryResult{
					Columns:  []string{"borough", "gross_revenue"},
					Rows:     [][]interface{}{{"Queens", 1338981.25}},
					RowCount: 1,
				},
			},
			{
				contains: []string{"AS \"borough\""},
				result: &query.QueryResult{
					Columns:  []string{"borough", "gross_revenue"},
					Rows:     [][]interface{}{{"Manhattan", 266675.71}, {"Queens", 1338981.25}},
					RowCount: 2,
				},
			},
			{
				contains: []string{"pickup_zone = 'JFK Airport'", "AS \"pickup_zone\""},
				result: &query.QueryResult{
					Columns:  []string{"pickup_zone", "gross_revenue"},
					Rows:     [][]interface{}{{"JFK Airport", 1338981.25}},
					RowCount: 1,
				},
			},
			{
				contains: []string{"AS \"pickup_zone\""},
				result: &query.QueryResult{
					Columns:  []string{"pickup_zone", "gross_revenue"},
					Rows:     [][]interface{}{{"JFK Airport", 1338981.25}, {"East Village", 266675.71}},
					RowCount: 2,
				},
			},
		},
	}
	semanticSvc.SetQueryExecutor(queryExec)

	dashboardSvc := dashboard.NewService(dashboardRepo, widgetRepo, notebookRepo, auditRepo, queryExec, semanticSvc)
	deps := &core.Dependencies{Dashboard: dashboardSvc}
	handler := New(deps)

	ctx := context.Background()
	_, err := semanticSvc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.sales",
	})
	require.NoError(t, err)
	_, err = semanticSvc.CreateMetric(ctx, "alice", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID: "ignored",
		Name:            "gross_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(amount)",
	})
	require.NoError(t, err)

	item, err := dashboardSvc.CreateDashboard(ctx, "alice", domain.CreateDashboardRequest{
		Name:                "Revenue Dashboard",
		Description:         "Executive metrics",
		SemanticProjectName: "analytics",
		SemanticModelName:   "sales",
	})
	require.NoError(t, err)

	doughnut := domain.VisualChartDoughnut
	bar := domain.VisualChartBar
	overviewWidget, err := dashboardSvc.CreateWidget(ctx, "alice", false, item.ID, domain.CreateDashboardWidgetRequest{
		PageName: "Overview",
		Name:     "Total Revenue",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceSemanticQuery,
			SemanticQuery: &domain.DashboardSemanticQuerySource{
				ProjectName:       "analytics",
				SemanticModelName: "sales",
				Metrics:           []string{"gross_revenue"},
			},
		},
		VisualSpec: &domain.VisualSpec{
			Kind: domain.VisualOutputMetric,
			Encodings: domain.VisualEncodings{
				Value: &domain.VisualFieldBinding{Field: "gross_revenue"},
			},
		},
		Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 3, H: 2},
	})
	require.NoError(t, err)
	_, err = dashboardSvc.CreateWidget(ctx, "alice", false, item.ID, domain.CreateDashboardWidgetRequest{
		PageName: "Geography",
		Name:     "Revenue by Borough",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceSemanticQuery,
			SemanticQuery: &domain.DashboardSemanticQuerySource{
				ProjectName:       "analytics",
				SemanticModelName: "sales",
				Metrics:           []string{"gross_revenue"},
				Dimensions:        []string{"borough"},
			},
		},
		VisualSpec: &domain.VisualSpec{
			Kind:      domain.VisualOutputChart,
			ChartType: &bar,
			Encodings: domain.VisualEncodings{
				X: &domain.VisualFieldBinding{Field: "borough"},
				Y: &domain.VisualFieldBinding{Field: "gross_revenue"},
			},
		},
		Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 6, H: 4},
	})
	require.NoError(t, err)
	pieWidget, err := dashboardSvc.CreateWidget(ctx, "alice", false, item.ID, domain.CreateDashboardWidgetRequest{
		PageName: "Geography",
		Name:     "Top Pickup Zones",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceSemanticQuery,
			SemanticQuery: &domain.DashboardSemanticQuerySource{
				ProjectName:       "analytics",
				SemanticModelName: "sales",
				Metrics:           []string{"gross_revenue"},
				Dimensions:        []string{"pickup_zone"},
			},
		},
		VisualSpec: &domain.VisualSpec{
			Kind:      domain.VisualOutputChart,
			ChartType: &doughnut,
			Encodings: domain.VisualEncodings{
				Label: &domain.VisualFieldBinding{Field: "pickup_zone"},
				Value: &domain.VisualFieldBinding{Field: "gross_revenue"},
			},
		},
		Layout: domain.DashboardWidgetLayout{X: 6, Y: 0, W: 6, H: 4},
	})
	require.NoError(t, err)
	tableWidget, err := dashboardSvc.CreateWidget(ctx, "alice", false, item.ID, domain.CreateDashboardWidgetRequest{
		PageName: "Geography",
		Name:     "Zone Revenue Detail",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceSemanticQuery,
			SemanticQuery: &domain.DashboardSemanticQuerySource{
				ProjectName:       "analytics",
				SemanticModelName: "sales",
				Metrics:           []string{"gross_revenue"},
				Dimensions:        []string{"pickup_zone", "borough"},
			},
		},
		VisualSpec: &domain.VisualSpec{Kind: domain.VisualOutputTable},
		Layout:     domain.DashboardWidgetLayout{X: 0, Y: 4, W: 12, H: 5},
	})
	require.NoError(t, err)

	router := chi.NewRouter()
	router.Route("/ui", func(r chi.Router) {
		MountRoutes(r, handler)
	})
	return dashboardStateFixture{
		router:           router,
		dashboardID:      item.ID,
		tableWidgetID:    tableWidget.ID,
		tableOriginKey:   tableWidget.FilterOriginKey,
		chartOriginKey:   pieWidget.FilterOriginKey,
		overviewWidgetID: overviewWidget.ID,
		queryExec:        queryExec,
	}
}

func fetchDashboardStateResponse(t *testing.T, handler http.Handler, rawURL string) dashboardStateResponse {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, rawURL, nil)
	req = req.WithContext(domain.WithPrincipal(req.Context(), domain.ContextPrincipal{Name: "alice", IsAdmin: true, Type: "user"}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var response dashboardStateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	return response
}

func widgetsByOriginKey(items []dashboardStateWidgetResponseItem) map[string]*dashboardStateWidgetResponseItem {
	out := make(map[string]*dashboardStateWidgetResponseItem, len(items))
	for index := range items {
		item := &items[index]
		out[item.FilterOriginKey] = item
	}
	return out
}
