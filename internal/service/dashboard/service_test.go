//go:build integration

package dashboard

import (
	"context"
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
	lastSQL string
	result  *query.QueryResult
	err     error
}

func (s *dashboardQueryExecutorStub) Execute(_ context.Context, _ string, sqlQuery string) (*query.QueryResult, error) {
	s.lastSQL = sqlQuery
	if s.err != nil {
		return nil, s.err
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
		{Dimension: "region", Values: []string{"APAC", "EMEA"}},
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.NotNil(t, resolved[0].Interaction)
	assert.True(t, resolved[0].Interaction.Participates)
	assert.True(t, resolved[0].Interaction.CanInitiate)
	assert.Equal(t, "region", resolved[0].Interaction.Bindings[0].Dimension)
	assert.Contains(t, queryExec.lastSQL, "date_trunc('month', order_date) = CAST('2024-01-01T00:00:00Z' AS TIMESTAMP)")
	assert.Contains(t, queryExec.lastSQL, "(region = 'APAC' OR region = 'EMEA')")
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

func strPtr(v string) *string {
	return &v
}
