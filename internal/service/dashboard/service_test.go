//go:build integration

package dashboard

import (
	"context"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/semantic"
)

type dashboardQueryExecutorStub struct {
	lastSQL          string
	sqls             []string
	result           *query.QueryResult
	resultsByContain map[string]*query.QueryResult
	err              error
}

func (s *dashboardQueryExecutorStub) Execute(_ context.Context, _ string, sqlQuery string) (*query.QueryResult, error) {
	s.lastSQL = sqlQuery
	s.sqls = append(s.sqls, sqlQuery)
	if s.err != nil {
		return nil, s.err
	}
	for needle, result := range s.resultsByContain {
		if needle != "" && strings.Contains(sqlQuery, needle) {
			return result, nil
		}
	}
	if s.result != nil {
		return s.result, nil
	}
	return &query.QueryResult{Columns: []string{"value"}, Rows: [][]interface{}{{1}}, RowCount: 1}, nil
}

func setupDashboardService(t *testing.T) (*Service, *repository.NotebookRepo, *repository.DashboardRepo, *repository.DashboardWidgetRepo, *semantic.Service, *dashboardQueryExecutorStub) {
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
	queryExec := &dashboardQueryExecutorStub{}
	semanticSvc.SetQueryExecutor(queryExec)

	svc := NewService(dashboardRepo, widgetRepo, notebookRepo, auditRepo, queryExec, semanticSvc)
	return svc, notebookRepo, dashboardRepo, widgetRepo, semanticSvc, queryExec
}

func createDashboardForTest(t *testing.T, svc *Service, owner string) *domain.Dashboard {
	t.Helper()
	item, err := svc.CreateDashboard(context.Background(), owner, domain.CreateDashboardRequest{
		Name:        "Revenue Dashboard",
		Description: "Executive metrics",
	})
	require.NoError(t, err)
	return item
}

func TestService_ResolveWidget_SQLQuery(t *testing.T) {
	t.Parallel()

	svc, _, _, _, _, queryExec := setupDashboardService(t)
	queryExec.result = &query.QueryResult{
		Columns:  []string{"region", "revenue"},
		Rows:     [][]interface{}{{"APAC", 42}},
		RowCount: 1,
	}
	chartType := domain.VisualChartBar
	widget := domain.DashboardWidget{
		ID:   "widget-1",
		Name: "Revenue by Region",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceSQLQuery,
			SQLQuery: &domain.DashboardSQLQuerySource{
				SQL: "select region, revenue from summary",
			},
		},
		VisualSpec: &domain.VisualSpec{
			Kind:      domain.VisualOutputChart,
			ChartType: &chartType,
			Encodings: domain.VisualEncodings{
				X: &domain.VisualFieldBinding{Field: "region"},
				Y: &domain.VisualFieldBinding{Field: "revenue"},
			},
		},
	}

	resolved, err := svc.ResolveWidget(context.Background(), "alice", widget)
	require.NoError(t, err)
	assert.Equal(t, "select region, revenue from summary", queryExec.lastSQL)
	assert.Equal(t, 1, resolved.RowCount)
	assert.Equal(t, []string{"region", "revenue"}, resolved.Columns)
	assert.Equal(t, "select region, revenue from summary", resolved.GeneratedSQL)
}

func TestService_ResolveWidget_NotebookCell(t *testing.T) {
	t.Parallel()

	svc, notebookRepo, _, _, _, _ := setupDashboardService(t)
	ctx := context.Background()
	notebook, err := notebookRepo.CreateNotebook(ctx, &domain.Notebook{Name: "Notebook", Owner: "alice"})
	require.NoError(t, err)
	cell, err := notebookRepo.CreateCell(ctx, &domain.Cell{
		NotebookID: notebook.ID,
		CellType:   domain.CellTypeSQL,
		Role:       domain.CellRoleOutput,
		Content:    "select region, revenue from summary",
		Position:   0,
	})
	require.NoError(t, err)
	resultJSON := `{"Columns":["region","revenue"],"Rows":[["APAC",42]],"RowCount":1}`
	require.NoError(t, notebookRepo.UpdateCellResult(ctx, cell.ID, &resultJSON))
	chartType := domain.VisualChartBar

	widget := domain.DashboardWidget{
		Name: "Notebook widget",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceNotebookCell,
			NotebookCell: &domain.DashboardNotebookCellSource{
				NotebookID: notebook.ID,
				CellID:     cell.ID,
			},
		},
		VisualSpec: &domain.VisualSpec{
			Kind:      domain.VisualOutputChart,
			ChartType: &chartType,
			Encodings: domain.VisualEncodings{
				X: &domain.VisualFieldBinding{Field: "region"},
				Y: &domain.VisualFieldBinding{Field: "revenue"},
			},
		},
	}

	resolved, err := svc.ResolveWidget(ctx, "alice", widget)
	require.NoError(t, err)
	assert.Equal(t, []string{"region", "revenue"}, resolved.Columns)
	assert.Equal(t, 1, resolved.RowCount)
	assert.Empty(t, resolved.GeneratedSQL)
}

func TestService_ResolveWidget_NotebookCellErrors(t *testing.T) {
	t.Parallel()

	svc, notebookRepo, _, _, _, _ := setupDashboardService(t)
	ctx := context.Background()
	notebook, err := notebookRepo.CreateNotebook(ctx, &domain.Notebook{Name: "Notebook", Owner: "alice"})
	require.NoError(t, err)
	cell, err := notebookRepo.CreateCell(ctx, &domain.Cell{
		NotebookID: notebook.ID,
		CellType:   domain.CellTypeSQL,
		Role:       domain.CellRoleOutput,
		Content:    "select 1",
		Position:   0,
	})
	require.NoError(t, err)
	metricSpec := &domain.VisualSpec{
		Kind: domain.VisualOutputMetric,
		Encodings: domain.VisualEncodings{
			Value: &domain.VisualFieldBinding{Field: "total_revenue"},
		},
	}

	t.Run("missing cached result", func(t *testing.T) {
		_, err := svc.ResolveWidget(ctx, "alice", domain.DashboardWidget{
			Name: "Widget",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceNotebookCell,
				NotebookCell: &domain.DashboardNotebookCellSource{
					NotebookID: notebook.ID,
					CellID:     cell.ID,
				},
			},
			VisualSpec: metricSpec,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cached result")
	})

	t.Run("invalid cached result", func(t *testing.T) {
		invalid := `{"Columns":`
		require.NoError(t, notebookRepo.UpdateCellResult(ctx, cell.ID, &invalid))
		_, err := svc.ResolveWidget(ctx, "alice", domain.DashboardWidget{
			Name: "Widget",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceNotebookCell,
				NotebookCell: &domain.DashboardNotebookCellSource{
					NotebookID: notebook.ID,
					CellID:     cell.ID,
				},
			},
			VisualSpec: metricSpec,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse cached notebook result")
	})
}

func TestService_ResolveWidget_SemanticQuery(t *testing.T) {
	t.Parallel()

	svc, _, _, _, semanticSvc, queryExec := setupDashboardService(t)
	ctx := context.Background()
	queryExec.result = &query.QueryResult{
		Columns:  []string{"region", "revenue"},
		Rows:     [][]interface{}{{"APAC", 42}},
		RowCount: 1,
	}

	_, err := semanticSvc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.sales",
	})
	require.NoError(t, err)
	_, err = semanticSvc.CreateMetric(ctx, "alice", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID: "ignored",
		Name:            "revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(sales.amount)",
	})
	require.NoError(t, err)

	chartType := domain.VisualChartBar
	widget := domain.DashboardWidget{
		Name: "Semantic widget",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceSemanticQuery,
			SemanticQuery: &domain.DashboardSemanticQuerySource{
				ProjectName:       "analytics",
				SemanticModelName: "sales",
				Metrics:           []string{"revenue"},
				Dimensions:        []string{"region"},
			},
		},
		VisualSpec: &domain.VisualSpec{
			Kind:      domain.VisualOutputChart,
			ChartType: &chartType,
			Encodings: domain.VisualEncodings{
				X: &domain.VisualFieldBinding{Field: "region"},
				Y: &domain.VisualFieldBinding{Field: "revenue"},
			},
		},
	}

	resolved, err := svc.ResolveWidget(ctx, "alice", widget)
	require.NoError(t, err)
	assert.Equal(t, []string{"region", "revenue"}, resolved.Columns)
	assert.Contains(t, resolved.GeneratedSQL, "SELECT")
	assert.Contains(t, queryExec.lastSQL, "SUM(sales.amount)")
}

func TestService_ResolveWidgetsForDashboard_AppliesDashboardFiltersAndTimeBuckets(t *testing.T) {
	t.Parallel()

	svc, _, _, _, semanticSvc, queryExec := setupDashboardService(t)
	ctx := context.Background()
	queryExec.result = &query.QueryResult{
		Columns:  []string{"region", "__time_grain", "revenue"},
		Rows:     [][]interface{}{{"APAC", "2024-01-01T00:00:00Z", 42}},
		RowCount: 1,
	}

	_, err := semanticSvc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:          "analytics",
		Name:                 "sales",
		BaseModelRef:         "analytics.sales",
		DefaultTimeDimension: "order_date",
	})
	require.NoError(t, err)
	_, err = semanticSvc.CreateMetric(ctx, "alice", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID:    "ignored",
		Name:               "revenue",
		MetricType:         domain.MetricTypeSum,
		ExpressionMode:     domain.MetricExpressionModeSQL,
		Expression:         "SUM(revenue)",
		DefaultTimeGrain:   "month",
		CertificationState: domain.CertificationCertified,
	})
	require.NoError(t, err)

	chartType := domain.VisualChartLine
	resolved, err := svc.ResolveWidgetsForDashboard(ctx, "alice", &domain.Dashboard{
		ID:                  "dash-1",
		Name:                "Revenue Dashboard",
		Owner:               "alice",
		SemanticProjectName: "analytics",
		SemanticModelName:   "sales",
	}, []domain.DashboardWidget{
		{
			ID:   "widget-1",
			Name: "Revenue by Month",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"revenue"},
					Dimensions:        []string{"region"},
					TimeGrain:         strPtr("month"),
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &chartType,
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "region"},
					Y: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
		},
	}, []InteractiveFilter{
		{Dimension: "order_date@month", Values: []string{"2024-01-01T00:00:00Z"}},
		{WidgetID: "widget-1", Dimension: "region", Values: []string{"APAC", "EMEA"}},
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.NotNil(t, resolved[0].Interaction)
	assert.True(t, resolved[0].Interaction.Participates)
	assert.True(t, resolved[0].Interaction.CanInitiate)
	assert.Equal(t, "region", resolved[0].Interaction.Bindings[0].Dimension)
	assert.Contains(t, queryExec.lastSQL, "date_trunc('month', order_date) = CAST('2024-01-01T00:00:00Z' AS TIMESTAMP)")
	assert.NotContains(t, queryExec.lastSQL, "(region = 'APAC' OR region = 'EMEA')")
	assert.Equal(t, []string{"APAC", "EMEA"}, resolved[0].Interaction.ActiveFilters["region"])
}

func TestService_ResolveWidgetsForDashboardPaged_TablePagesSemanticResults(t *testing.T) {
	t.Parallel()

	svc, _, _, _, semanticSvc, queryExec := setupDashboardService(t)
	ctx := context.Background()

	rows := make([][]interface{}, 0, 101)
	for index := 0; index < 101; index += 1 {
		rows = append(rows, []interface{}{fmt.Sprintf("zone-%03d", index), float64(index)})
	}
	queryExec.result = &query.QueryResult{
		Columns:  []string{"pickup_zone", "gross_revenue"},
		Rows:     rows,
		RowCount: len(rows),
	}
	queryExec.resultsByContain = map[string]*query.QueryResult{
		"dashboard_widget_count": {
			Columns:  []string{"row_count"},
			Rows:     [][]interface{}{{101}},
			RowCount: 1,
		},
	}

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

	widgets, err := svc.ResolveWidgetsForDashboardPaged(ctx, "alice", &domain.Dashboard{
		ID:                  "dash-1",
		Name:                "Revenue Dashboard",
		Owner:               "alice",
		SemanticProjectName: "analytics",
		SemanticModelName:   "sales",
	}, []domain.DashboardWidget{
		{
			ID:   "widget-table",
			Name: "Zone Revenue Detail",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"gross_revenue"},
					Dimensions:        []string{"pickup_zone"},
					OrderBy:           []string{"gross_revenue DESC"},
				},
			},
			VisualSpec: &domain.VisualSpec{Kind: domain.VisualOutputTable},
		},
	}, nil, map[string]TablePageRequest{
		"widget-table": {
			Offset:        100,
			Limit:         100,
			Append:        true,
			SortColumn:    "pickup_zone",
			SortDirection: "desc",
		},
	})
	require.NoError(t, err)
	require.Len(t, widgets, 1)
	require.NotNil(t, widgets[0].Page)
	assert.True(t, widgets[0].Page.Append)
	assert.Equal(t, 100, widgets[0].Page.Offset)
	assert.True(t, widgets[0].Page.HasMore)
	assert.Equal(t, 101, widgets[0].RowCount)
	require.NotNil(t, widgets[0].Sort)
	assert.Equal(t, "pickup_zone", widgets[0].Sort.Column)
	assert.Equal(t, "desc", widgets[0].Sort.Direction)
	assert.Len(t, widgets[0].Rows, 100)
	assert.True(t, containsSQLFragment(queryExec.sqls, "LIMIT 101"))
	assert.True(t, containsSQLFragment(queryExec.sqls, "OFFSET 100"))
	assert.True(t, containsSQLFragment(queryExec.sqls, "ORDER BY pickup_zone DESC"))
	assert.True(t, containsSQLFragment(queryExec.sqls, "dashboard_widget_count"))
}

func TestService_BuildDashboardPageState_ScopesPageAndPreservesOriginFilters(t *testing.T) {
	t.Parallel()

	svc, _, _, _, semanticSvc, queryExec := setupDashboardService(t)
	ctx := context.Background()
	queryExec.result = &query.QueryResult{
		Columns:  []string{"region", "sales_owner", "revenue"},
		Rows:     [][]interface{}{{"APAC", "Taylor", 42}},
		RowCount: 1,
	}

	_, err := semanticSvc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.sales",
	})
	require.NoError(t, err)
	_, err = semanticSvc.CreateMetric(ctx, "alice", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID:    "ignored",
		Name:               "revenue",
		MetricType:         domain.MetricTypeSum,
		ExpressionMode:     domain.MetricExpressionModeSQL,
		Expression:         "SUM(revenue)",
		CertificationState: domain.CertificationCertified,
	})
	require.NoError(t, err)

	pieChart := domain.VisualChartDoughnut
	barChart := domain.VisualChartBar
	state, err := svc.BuildDashboardPageState(ctx, "alice", &domain.Dashboard{
		ID:                  "dash-1",
		Name:                "Revenue Dashboard",
		Owner:               "alice",
		SemanticProjectName: "analytics",
		SemanticModelName:   "sales",
	}, []domain.DashboardWidget{
		{
			ID:       "widget-overview",
			PageName: "Overview",
			Name:     "Overview Revenue",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"revenue"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind: domain.VisualOutputMetric,
				Encodings: domain.VisualEncodings{
					Value: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
		},
		{
			ID:       "widget-pie",
			PageName: "Geography",
			Name:     "Revenue by Region",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"revenue"},
					Dimensions:        []string{"region"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &pieChart,
				Encodings: domain.VisualEncodings{
					Label: &domain.VisualFieldBinding{Field: "region"},
					Value: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
		},
		{
			ID:       "widget-bar",
			PageName: "Geography",
			Name:     "Revenue by Owner",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"revenue"},
					Dimensions:        []string{"sales_owner"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &barChart,
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "sales_owner"},
					Y: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
		},
	}, "Geography", []InteractiveFilter{
		{WidgetID: "widget-pie", Dimension: "region", Values: []string{"APAC"}},
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, "Geography", state.PageName)
	require.Len(t, state.Widgets, 2)
	assert.Equal(t, []string{"APAC"}, state.ActiveFilters[0].Values)
	require.NotNil(t, state.Widgets[0].Interaction)
	require.NotNil(t, state.Widgets[1].Interaction)
	assert.Equal(t, []string{"APAC"}, state.Widgets[0].Interaction.OriginFilters["region"])
	assert.Nil(t, state.Widgets[1].Interaction.OriginFilters)
	require.Len(t, queryExec.sqls, 2)
	assert.NotContains(t, queryExec.sqls[0], "region = 'APAC'")
	assert.Contains(t, queryExec.sqls[1], "region = 'APAC'")
}

func TestService_BuildDashboardPageState_AppliesTablePageRequests(t *testing.T) {
	t.Parallel()

	svc, _, _, _, semanticSvc, queryExec := setupDashboardService(t)
	ctx := context.Background()

	rows := make([][]interface{}, 0, 101)
	for index := 0; index < 101; index += 1 {
		rows = append(rows, []interface{}{fmt.Sprintf("zone-%03d", index), float64(index)})
	}
	queryExec.result = &query.QueryResult{
		Columns:  []string{"pickup_zone", "gross_revenue"},
		Rows:     rows,
		RowCount: len(rows),
	}
	queryExec.resultsByContain = map[string]*query.QueryResult{
		"dashboard_widget_count": {
			Columns:  []string{"row_count"},
			Rows:     [][]interface{}{{101}},
			RowCount: 1,
		},
	}

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

	state, err := svc.BuildDashboardPageState(ctx, "alice", &domain.Dashboard{
		ID:                  "dash-1",
		Name:                "Revenue Dashboard",
		Owner:               "alice",
		SemanticProjectName: "analytics",
		SemanticModelName:   "sales",
	}, []domain.DashboardWidget{
		{
			ID:       "widget-overview",
			PageName: "Overview",
			Name:     "Overview Revenue",
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
		},
		{
			ID:       "widget-table",
			PageName: "Geography",
			Name:     "Zone Revenue Detail",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"gross_revenue"},
					Dimensions:        []string{"pickup_zone"},
					OrderBy:           []string{"gross_revenue DESC"},
				},
			},
			VisualSpec: &domain.VisualSpec{Kind: domain.VisualOutputTable},
		},
	}, "Geography", nil, map[string]TablePageRequest{
		"widget-table": {
			Offset:        100,
			Limit:         100,
			Append:        true,
			SortColumn:    "pickup_zone",
			SortDirection: "desc",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Len(t, state.Widgets, 1)
	require.NotNil(t, state.Widgets[0].Page)
	assert.True(t, state.Widgets[0].Page.Append)
	assert.Equal(t, 100, state.Widgets[0].Page.Offset)
	assert.True(t, state.Widgets[0].Page.HasMore)
	require.NotNil(t, state.Widgets[0].Sort)
	assert.Equal(t, "pickup_zone", state.Widgets[0].Sort.Column)
	assert.Equal(t, "desc", state.Widgets[0].Sort.Direction)
	assert.Len(t, state.Widgets[0].Rows, 100)
}

func TestService_ResolveWidgetForDashboardPage_SortsSQLTablePageBeforeSlice(t *testing.T) {
	t.Parallel()

	svc, _, _, _, _, queryExec := setupDashboardService(t)
	ctx := context.Background()

	queryExec.result = &query.QueryResult{
		Columns:  []string{"pickup_zone", "gross_revenue"},
		Rows:     [][]interface{}{{"Queens Plaza", 10.0}, {"JFK Airport", 30.0}, {"East Village", 20.0}},
		RowCount: 3,
	}

	dashboard := &domain.Dashboard{ID: "dash-1", Name: "Revenue Dashboard", Owner: "alice"}
	widgets := []domain.DashboardWidget{
		{
			ID:   "widget-table",
			Name: "Zone Revenue Detail",
			Source: domain.DashboardWidgetSource{
				Kind:     domain.DashboardWidgetSourceSQLQuery,
				SQLQuery: &domain.DashboardSQLQuerySource{SQL: "select pickup_zone, gross_revenue from zone_metrics"},
			},
			VisualSpec: &domain.VisualSpec{Kind: domain.VisualOutputTable},
		},
	}

	resolved, err := svc.ResolveWidgetForDashboardPage(ctx, "alice", dashboard, widgets, "widget-table", nil, TablePageRequest{
		Offset:        0,
		Limit:         2,
		Append:        false,
		SortColumn:    "gross_revenue",
		SortDirection: "desc",
	})
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.NotNil(t, resolved.Page)
	require.NotNil(t, resolved.Sort)
	assert.Equal(t, "gross_revenue", resolved.Sort.Column)
	assert.Equal(t, "desc", resolved.Sort.Direction)
	assert.Equal(t, [][]interface{}{
		{"JFK Airport", 30.0},
		{"East Village", 20.0},
	}, resolved.Rows)
	assert.True(t, resolved.Page.HasMore)
	assert.False(t, resolved.Page.Append)
}

func TestService_ResolveWidgetsForDashboard_TableWidgetsCanInitiateFilters(t *testing.T) {
	t.Parallel()

	svc, _, _, _, semanticSvc, queryExec := setupDashboardService(t)
	ctx := context.Background()
	queryExec.result = &query.QueryResult{
		Columns:  []string{"pickup_zone", "borough", "gross_revenue"},
		Rows:     [][]interface{}{{"JFK Airport", "Queens", 10.0}},
		RowCount: 1,
	}

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

	widgets, err := svc.ResolveWidgetsForDashboard(ctx, "alice", &domain.Dashboard{
		ID:                  "dash-1",
		Name:                "Revenue Dashboard",
		Owner:               "alice",
		SemanticProjectName: "analytics",
		SemanticModelName:   "sales",
	}, []domain.DashboardWidget{
		{
			ID:   "widget-table",
			Name: "Zone Revenue Detail",
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
		},
	}, nil)
	require.NoError(t, err)
	require.Len(t, widgets, 1)
	require.NotNil(t, widgets[0].Interaction)
	assert.True(t, widgets[0].Interaction.Participates)
	assert.True(t, widgets[0].Interaction.CanInitiate)
	require.Len(t, widgets[0].Interaction.Bindings, 2)
	assert.Equal(t, "column", widgets[0].Interaction.Bindings[0].Encoding)
	assert.Equal(t, "pickup_zone", widgets[0].Interaction.Bindings[0].Field)
	assert.Equal(t, "pickup_zone", widgets[0].Interaction.Bindings[0].Dimension)
	assert.Equal(t, "borough", widgets[0].Interaction.Bindings[1].Field)
	assert.Equal(t, "borough", widgets[0].Interaction.Bindings[1].Dimension)
}

func containsSQLFragment(sqls []string, fragment string) bool {
	for _, sql := range sqls {
		if strings.Contains(sql, fragment) {
			return true
		}
	}
	return false
}

func TestService_ResolveWidgetsForDashboard_SourceChartKeepsOwnDimensionUnfiltered(t *testing.T) {
	t.Parallel()

	svc, _, _, _, semanticSvc, queryExec := setupDashboardService(t)
	ctx := context.Background()
	queryExec.result = &query.QueryResult{
		Columns:  []string{"region", "sales_owner", "revenue"},
		Rows:     [][]interface{}{{"APAC", "Taylor", 42}},
		RowCount: 1,
	}

	_, err := semanticSvc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.sales",
	})
	require.NoError(t, err)
	_, err = semanticSvc.CreateMetric(ctx, "alice", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID:    "ignored",
		Name:               "revenue",
		MetricType:         domain.MetricTypeSum,
		ExpressionMode:     domain.MetricExpressionModeSQL,
		Expression:         "SUM(revenue)",
		CertificationState: domain.CertificationCertified,
	})
	require.NoError(t, err)

	pieChart := domain.VisualChartDoughnut
	barChart := domain.VisualChartBar
	resolved, err := svc.ResolveWidgetsForDashboard(ctx, "alice", &domain.Dashboard{
		ID:                  "dash-1",
		Name:                "Revenue Dashboard",
		Owner:               "alice",
		SemanticProjectName: "analytics",
		SemanticModelName:   "sales",
	}, []domain.DashboardWidget{
		{
			ID:   "widget-pie",
			Name: "Revenue by Region",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"revenue"},
					Dimensions:        []string{"region"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &pieChart,
				Encodings: domain.VisualEncodings{
					Label: &domain.VisualFieldBinding{Field: "region"},
					Value: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
		},
		{
			ID:   "widget-bar",
			Name: "Revenue by Owner",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"revenue"},
					Dimensions:        []string{"sales_owner"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &barChart,
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "sales_owner"},
					Y: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
		},
	}, []InteractiveFilter{
		{WidgetID: "widget-pie", Dimension: "region", Values: []string{"APAC"}},
	})
	require.NoError(t, err)
	require.Len(t, resolved, 2)
	require.NotNil(t, resolved[0].Interaction)
	require.NotNil(t, resolved[1].Interaction)
	assert.Equal(t, []string{"APAC"}, resolved[0].Interaction.ActiveFilters["region"])
	assert.Equal(t, []string{"APAC"}, resolved[1].Interaction.ActiveFilters["region"])
	require.Len(t, queryExec.sqls, 2)
	assert.NotContains(t, queryExec.sqls[0], "region = 'APAC'")
	assert.Contains(t, queryExec.sqls[1], "region = 'APAC'")
}

func TestService_ResolveWidgetsForDashboard_ExcludesNonInteractiveWidgets(t *testing.T) {
	t.Parallel()

	svc, _, _, _, semanticSvc, queryExec := setupDashboardService(t)
	ctx := context.Background()
	queryExec.result = &query.QueryResult{
		Columns:  []string{"revenue"},
		Rows:     [][]interface{}{{42}},
		RowCount: 1,
	}

	_, err := semanticSvc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.sales",
	})
	require.NoError(t, err)
	_, err = semanticSvc.CreateSemanticModel(ctx, "alice", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "marketing",
		BaseModelRef: "analytics.marketing",
	})
	require.NoError(t, err)
	_, err = semanticSvc.CreateMetric(ctx, "alice", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID:    "ignored",
		Name:               "revenue",
		MetricType:         domain.MetricTypeSum,
		ExpressionMode:     domain.MetricExpressionModeSQL,
		Expression:         "SUM(revenue)",
		CertificationState: domain.CertificationCertified,
	})
	require.NoError(t, err)
	_, err = semanticSvc.CreateMetric(ctx, "alice", "analytics", "marketing", domain.CreateSemanticMetricRequest{
		SemanticModelID:    "ignored",
		Name:               "revenue",
		MetricType:         domain.MetricTypeSum,
		ExpressionMode:     domain.MetricExpressionModeSQL,
		Expression:         "SUM(revenue)",
		CertificationState: domain.CertificationCertified,
	})
	require.NoError(t, err)

	resolved, err := svc.ResolveWidgetsForDashboard(ctx, "alice", &domain.Dashboard{
		ID:                  "dash-1",
		Name:                "Revenue Dashboard",
		Owner:               "alice",
		SemanticProjectName: "analytics",
		SemanticModelName:   "sales",
	}, []domain.DashboardWidget{
		{
			ID:   "widget-metric",
			Name: "Revenue",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"revenue"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind: domain.VisualOutputMetric,
				Encodings: domain.VisualEncodings{
					Value: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
		},
		{
			ID:   "widget-sql",
			Name: "SQL Widget",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSQLQuery,
				SQLQuery: &domain.DashboardSQLQuerySource{
					SQL: "select revenue",
				},
			},
		},
		{
			ID:   "widget-other-model",
			Name: "Other Model",
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSemanticQuery,
				SemanticQuery: &domain.DashboardSemanticQuerySource{
					ProjectName:       "analytics",
					SemanticModelName: "marketing",
					Metrics:           []string{"revenue"},
				},
			},
			VisualSpec: &domain.VisualSpec{
				Kind: domain.VisualOutputMetric,
				Encodings: domain.VisualEncodings{
					Value: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	require.Len(t, resolved, 3)
	require.NotNil(t, resolved[0].Interaction)
	assert.True(t, resolved[0].Interaction.Participates)
	assert.False(t, resolved[0].Interaction.CanInitiate)
	require.NotNil(t, resolved[1].Interaction)
	assert.False(t, resolved[1].Interaction.Participates)
	assert.Equal(t, "Not interactive in this dashboard.", resolved[1].Interaction.DisabledReason)
	require.NotNil(t, resolved[2].Interaction)
	assert.False(t, resolved[2].Interaction.Participates)
	assert.Equal(t, "Not interactive in this dashboard.", resolved[2].Interaction.DisabledReason)
}

func TestService_UpdateWidget_AuthorizationAndValidation(t *testing.T) {
	t.Parallel()

	svc, _, _, _, _, _ := setupDashboardService(t)
	ctx := context.Background()
	dashboard := createDashboardForTest(t, svc, "alice")
	widget, err := svc.CreateWidget(ctx, "alice", false, dashboard.ID, domain.CreateDashboardWidgetRequest{
		Name: "Revenue",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceSQLQuery,
			SQLQuery: &domain.DashboardSQLQuerySource{
				SQL: "select region, revenue from summary",
			},
		},
		VisualSpec: &domain.VisualSpec{Kind: domain.VisualOutputTable},
		Layout:     domain.DashboardWidgetLayout{X: 0, Y: 0, W: 4, H: 3},
	})
	require.NoError(t, err)

	t.Run("non owner denied", func(t *testing.T) {
		_, err := svc.UpdateWidget(ctx, "bob", false, widget.ID, domain.UpdateDashboardWidgetRequest{
			Name: &[]string{"Updated"}[0],
		})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("invalid layout rejected", func(t *testing.T) {
		_, err := svc.UpdateWidget(ctx, "alice", false, widget.ID, domain.UpdateDashboardWidgetRequest{
			Layout: &domain.DashboardWidgetLayout{X: -1, Y: 0, W: 0, H: 0},
		})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		assert.ErrorAs(t, err, &validationErr)
	})

	t.Run("admin allowed", func(t *testing.T) {
		name := "Updated"
		updated, err := svc.UpdateWidget(ctx, "admin", true, widget.ID, domain.UpdateDashboardWidgetRequest{
			Name: &name,
		})
		require.NoError(t, err)
		assert.Equal(t, "Updated", updated.Name)
	})
}

func TestService_CreateWidget_AssignsUniqueFilterOriginKeys(t *testing.T) {
	t.Parallel()

	svc, _, _, _, _, _ := setupDashboardService(t)
	ctx := context.Background()
	dashboard := createDashboardForTest(t, svc, "alice")

	createWidget := func(name string) *domain.DashboardWidget {
		widget, err := svc.CreateWidget(ctx, "alice", false, dashboard.ID, domain.CreateDashboardWidgetRequest{
			Name: name,
			Source: domain.DashboardWidgetSource{
				Kind: domain.DashboardWidgetSourceSQLQuery,
				SQLQuery: &domain.DashboardSQLQuerySource{
					SQL: "select region, revenue from summary",
				},
			},
			VisualSpec: &domain.VisualSpec{Kind: domain.VisualOutputTable},
			Layout:     domain.DashboardWidgetLayout{X: 0, Y: 0, W: 4, H: 3},
		})
		require.NoError(t, err)
		return widget
	}

	first := createWidget("Revenue Detail")
	second := createWidget("Revenue Detail")

	assert.Equal(t, "table-revenue-detail", first.FilterOriginKey)
	assert.Equal(t, "table-revenue-detail-2", second.FilterOriginKey)
}

func strPtr(v string) *string {
	return &v
}
