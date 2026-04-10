package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
)

type mockDashboardService struct {
	createDashboardFn            func(ctx context.Context, owner string, req domain.CreateDashboardRequest) (*domain.Dashboard, error)
	listDashboardsFn             func(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Dashboard, int64, error)
	getDashboardFn               func(ctx context.Context, id string) (*domain.Dashboard, []domain.DashboardWidget, error)
	listWidgetsFn                func(ctx context.Context, dashboardID string) ([]domain.DashboardWidget, error)
	getWidgetFn                  func(ctx context.Context, dashboardID, widgetID string) (*domain.DashboardWidget, error)
	resolveWidgetsFn             func(ctx context.Context, principal string, widgets []domain.DashboardWidget) ([]dashboardsvc.ResolvedWidget, error)
	resolveWidgetsForDashboardFn func(ctx context.Context, principal string, dashboard *domain.Dashboard, widgets []domain.DashboardWidget, filters []dashboardsvc.InteractiveFilter) ([]dashboardsvc.ResolvedWidget, error)
	updateDashboardFn            func(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateDashboardRequest) (*domain.Dashboard, error)
	deleteDashboardFn            func(ctx context.Context, principal string, isAdmin bool, id string) error
	createWidgetFn               func(ctx context.Context, principal string, isAdmin bool, dashboardID string, req domain.CreateDashboardWidgetRequest) (*domain.DashboardWidget, error)
	updateWidgetFn               func(ctx context.Context, principal string, isAdmin bool, widgetID string, req domain.UpdateDashboardWidgetRequest) (*domain.DashboardWidget, error)
	deleteWidgetFn               func(ctx context.Context, principal string, isAdmin bool, widgetID string) error
}

func (m *mockDashboardService) CreateDashboard(ctx context.Context, owner string, req domain.CreateDashboardRequest) (*domain.Dashboard, error) {
	return m.createDashboardFn(ctx, owner, req)
}

func (m *mockDashboardService) ListDashboards(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Dashboard, int64, error) {
	return m.listDashboardsFn(ctx, owner, page)
}

func (m *mockDashboardService) GetDashboard(ctx context.Context, id string) (*domain.Dashboard, []domain.DashboardWidget, error) {
	return m.getDashboardFn(ctx, id)
}

func (m *mockDashboardService) ListWidgets(ctx context.Context, dashboardID string) ([]domain.DashboardWidget, error) {
	if m.listWidgetsFn == nil {
		panic("mockDashboardService.ListWidgets called but not configured")
	}
	return m.listWidgetsFn(ctx, dashboardID)
}

func (m *mockDashboardService) GetWidget(ctx context.Context, dashboardID, widgetID string) (*domain.DashboardWidget, error) {
	if m.getWidgetFn == nil {
		panic("mockDashboardService.GetWidget called but not configured")
	}
	return m.getWidgetFn(ctx, dashboardID, widgetID)
}

func (m *mockDashboardService) ResolveWidgets(ctx context.Context, principal string, widgets []domain.DashboardWidget) ([]dashboardsvc.ResolvedWidget, error) {
	return m.resolveWidgetsFn(ctx, principal, widgets)
}

func (m *mockDashboardService) ResolveWidgetsForDashboard(ctx context.Context, principal string, dashboard *domain.Dashboard, widgets []domain.DashboardWidget, filters []dashboardsvc.InteractiveFilter) ([]dashboardsvc.ResolvedWidget, error) {
	return m.resolveWidgetsForDashboardFn(ctx, principal, dashboard, widgets, filters)
}

func (m *mockDashboardService) UpdateDashboard(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateDashboardRequest) (*domain.Dashboard, error) {
	return m.updateDashboardFn(ctx, principal, isAdmin, id, req)
}

func (m *mockDashboardService) DeleteDashboard(ctx context.Context, principal string, isAdmin bool, id string) error {
	return m.deleteDashboardFn(ctx, principal, isAdmin, id)
}

func (m *mockDashboardService) CreateWidget(ctx context.Context, principal string, isAdmin bool, dashboardID string, req domain.CreateDashboardWidgetRequest) (*domain.DashboardWidget, error) {
	return m.createWidgetFn(ctx, principal, isAdmin, dashboardID, req)
}

func (m *mockDashboardService) UpdateWidget(ctx context.Context, principal string, isAdmin bool, widgetID string, req domain.UpdateDashboardWidgetRequest) (*domain.DashboardWidget, error) {
	return m.updateWidgetFn(ctx, principal, isAdmin, widgetID, req)
}

func (m *mockDashboardService) DeleteWidget(ctx context.Context, principal string, isAdmin bool, widgetID string) error {
	return m.deleteWidgetFn(ctx, principal, isAdmin, widgetID)
}

func TestHandler_CreateDashboard_UsesPrincipalAndMapsRequest(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true, Type: "user"})
	now := time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC)
	h := &APIHandler{
		dashboards: &mockDashboardService{
			createDashboardFn: func(_ context.Context, owner string, req domain.CreateDashboardRequest) (*domain.Dashboard, error) {
				assert.Equal(t, "alice", owner)
				assert.Equal(t, "Executive Overview", req.Name)
				assert.Equal(t, "KPI dashboard", req.Description)
				assert.Equal(t, "analytics", req.SemanticProjectName)
				assert.Equal(t, "sales", req.SemanticModelName)
				return &domain.Dashboard{
					ID:                  "dash-1",
					Name:                req.Name,
					Description:         req.Description,
					Owner:               owner,
					SemanticProjectName: req.SemanticProjectName,
					SemanticModelName:   req.SemanticModelName,
					CreatedAt:           now,
					UpdatedAt:           now,
				}, nil
			},
		},
	}

	desc := "KPI dashboard"
	semanticProjectName := "analytics"
	semanticModelName := "sales"
	resp, err := h.CreateDashboard(ctx, GenCreateDashboardRequest{Body: &GenCreateDashboardJSONBody{
		Name:                "Executive Overview",
		Description:         &desc,
		SemanticProjectName: &semanticProjectName,
		SemanticModelName:   &semanticModelName,
	}})
	require.NoError(t, err)

	created, ok := resp.(GenCreateDashboard201JSONResponse)
	require.True(t, ok)
	require.NotNil(t, created.Body.Id)
	assert.Equal(t, "dash-1", *created.Body.Id)
	require.NotNil(t, created.Body.Name)
	assert.Equal(t, "Executive Overview", *created.Body.Name)
	require.NotNil(t, created.Body.SemanticProjectName)
	assert.Equal(t, "analytics", *created.Body.SemanticProjectName)
	require.NotNil(t, created.Body.SemanticModelName)
	assert.Equal(t, "sales", *created.Body.SemanticModelName)
}

func TestHandler_CreateDashboardWidget_MapsSourceAndVisualSpec(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", Type: "user"})
	chartType := VisualChartTypeBar
	title := "Revenue by Region"
	h := &APIHandler{
		dashboards: &mockDashboardService{
			createWidgetFn: func(_ context.Context, principal string, isAdmin bool, dashboardID string, req domain.CreateDashboardWidgetRequest) (*domain.DashboardWidget, error) {
				assert.Equal(t, "alice", principal)
				assert.False(t, isAdmin)
				assert.Equal(t, "dash-1", dashboardID)
				assert.Equal(t, domain.DashboardWidgetSourceSemanticQuery, req.Source.Kind)
				require.NotNil(t, req.Source.SemanticQuery)
				assert.Equal(t, "sm-sales", req.Source.SemanticQuery.SemanticModelID)
				assert.Equal(t, []string{"revenue"}, req.Source.SemanticQuery.Metrics)
				require.NotNil(t, req.Source.SemanticQuery.TimeGrain)
				assert.Equal(t, "day", *req.Source.SemanticQuery.TimeGrain)
				require.NotNil(t, req.VisualSpec)
				assert.Equal(t, domain.VisualOutputChart, req.VisualSpec.Kind)
				require.NotNil(t, req.VisualSpec.ChartType)
				assert.Equal(t, domain.VisualChartBar, *req.VisualSpec.ChartType)
				require.NotNil(t, req.VisualSpec.Encodings.X)
				assert.Equal(t, "region", req.VisualSpec.Encodings.X.Field)
				require.NotNil(t, req.VisualSpec.Encodings.Y)
				assert.Equal(t, "revenue", req.VisualSpec.Encodings.Y.Field)
				assert.Equal(t, 6, req.Layout.W)
				return &domain.DashboardWidget{
					ID:          "widget-1",
					DashboardID: dashboardID,
					Name:        req.Name,
					Description: req.Description,
					Source:      req.Source,
					VisualSpec:  req.VisualSpec,
					Layout:      req.Layout,
				}, nil
			},
		},
	}

	timeGrain := "day"
	resp, err := h.CreateDashboardWidget(ctx, GenCreateDashboardWidgetRequest{
		DashboardId: "dash-1",
		Body: &GenCreateDashboardWidgetJSONBody{
			Name:        "Revenue by Region",
			Description: strPtr("Grouped revenue"),
			Source: DashboardWidgetSource{
				Kind: DashboardWidgetSourceKindSemanticQuery,
				SemanticQuery: &DashboardSemanticQuerySource{
					SemanticModelId: "sm-sales",
					Metrics:         []string{"revenue"},
					Dimensions:      &[]string{"region"},
					TimeGrain:       &timeGrain,
				},
			},
			VisualSpec: &VisualSpec{
				Kind:      VisualOutputKindChart,
				ChartType: &chartType,
				Title:     &title,
				Encodings: &VisualEncodings{
					X: &VisualFieldBinding{Field: "region"},
					Y: &VisualFieldBinding{Field: "revenue"},
				},
			},
			Layout: DashboardWidgetLayout{X: 0, Y: 0, W: 6, H: 4},
		},
	})
	require.NoError(t, err)

	created, ok := resp.(GenCreateDashboardWidget201JSONResponse)
	require.True(t, ok)
	require.NotNil(t, created.Body.Id)
	assert.Equal(t, "widget-1", *created.Body.Id)
	require.NotNil(t, created.Body.Source)
	require.NotNil(t, created.Body.VisualSpec)
	assert.Equal(t, DashboardWidgetSourceKindSemanticQuery, created.Body.Source.Kind)
}

func TestHandler_ListDashboards_MapsOwnerAndPagination(t *testing.T) {
	t.Parallel()

	owner := "alice"
	h := &APIHandler{
		dashboards: &mockDashboardService{
			listDashboardsFn: func(_ context.Context, gotOwner *string, page domain.PageRequest) ([]domain.Dashboard, int64, error) {
				require.NotNil(t, gotOwner)
				assert.Equal(t, owner, *gotOwner)
				assert.Equal(t, 10, page.MaxResults)
				assert.Equal(t, "20", page.PageToken)
				return []domain.Dashboard{{ID: "dash-1", Name: "Revenue", Owner: owner}}, 1, nil
			},
		},
	}

	resp, err := h.ListDashboards(context.Background(), GenListDashboardsRequest{
		Params: GenListDashboardsParams{
			Owner:      &owner,
			MaxResults: int32Ptr(10),
			PageToken:  strPtr("20"),
		},
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenListDashboards200JSONResponse)
	require.True(t, ok)
	require.Len(t, okResp.Body.Data, 1)
	require.NotNil(t, okResp.Body.Data[0].Id)
	assert.Equal(t, "dash-1", *okResp.Body.Data[0].Id)
}

func TestHandler_GetDashboard_MapsWidgets(t *testing.T) {
	t.Parallel()

	chartType := domain.VisualChartBar
	h := &APIHandler{
		dashboards: &mockDashboardService{
			getDashboardFn: func(_ context.Context, id string) (*domain.Dashboard, []domain.DashboardWidget, error) {
				assert.Equal(t, "dash-1", id)
				return &domain.Dashboard{
						ID:                  id,
						Name:                "Revenue",
						Owner:               "alice",
						SemanticProjectName: "analytics",
						SemanticModelName:   "sales",
						Compute: domain.DashboardComputePolicy{
							Mode:          domain.ComputeModeSharedEndpoint,
							EndpointName:  "analytics-xl",
							FallbackLocal: true,
						},
					}, []domain.DashboardWidget{
						{
							ID:          "widget-1",
							DashboardID: id,
							Name:        "Revenue by Region",
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
							Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 4, H: 3},
						},
					}, nil
			},
		},
	}

	resp, err := h.GetDashboard(context.Background(), GenGetDashboardRequest{DashboardId: "dash-1"})
	require.NoError(t, err)

	okResp, ok := resp.(GenGetDashboard200JSONResponse)
	require.True(t, ok)
	require.NotNil(t, okResp.Body.Dashboard)
	require.NotNil(t, okResp.Body.Dashboard.SemanticProjectName)
	assert.Equal(t, "analytics", *okResp.Body.Dashboard.SemanticProjectName)
	require.NotNil(t, okResp.Body.Dashboard.Compute)
	require.NotNil(t, okResp.Body.Dashboard.Compute.Mode)
	assert.Equal(t, "SHARED_ENDPOINT", *okResp.Body.Dashboard.Compute.Mode)
	assert.Equal(t, "analytics-xl", *okResp.Body.Dashboard.Compute.EndpointName)
	require.NotNil(t, okResp.Body.Dashboard.Compute.FallbackLocal)
	assert.True(t, *okResp.Body.Dashboard.Compute.FallbackLocal)
	require.NotNil(t, okResp.Body.Widgets)
	require.Len(t, *okResp.Body.Widgets, 1)
	require.NotNil(t, (*okResp.Body.Widgets)[0].Source)
	assert.Equal(t, DashboardWidgetSourceKindSqlQuery, (*okResp.Body.Widgets)[0].Source.Kind)
}

func TestHandler_ListDashboardWidgets_MapsWidgets(t *testing.T) {
	t.Parallel()

	chartType := domain.VisualChartBar
	h := &APIHandler{
		dashboards: &mockDashboardService{
			listWidgetsFn: func(_ context.Context, dashboardID string) ([]domain.DashboardWidget, error) {
				assert.Equal(t, "dash-1", dashboardID)
				return []domain.DashboardWidget{{
					ID:          "widget-1",
					DashboardID: dashboardID,
					Name:        "Revenue by Region",
					Source: domain.DashboardWidgetSource{
						Kind: domain.DashboardWidgetSourceSQLQuery,
						SQLQuery: &domain.DashboardSQLQuerySource{
							SQL: "select region, revenue from summary",
						},
					},
					VisualSpec: &domain.VisualSpec{
						Kind:      domain.VisualOutputChart,
						ChartType: &chartType,
					},
					Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 4, H: 3},
				}}, nil
			},
		},
	}

	resp, err := h.ListDashboardWidgets(context.Background(), GenListDashboardWidgetsRequest{DashboardId: "dash-1"})
	require.NoError(t, err)

	okResp, ok := resp.(GenListDashboardWidgets200JSONResponse)
	require.True(t, ok)
	require.Len(t, okResp.Body, 1)
	require.NotNil(t, okResp.Body[0].Id)
	assert.Equal(t, "widget-1", *okResp.Body[0].Id)
}

func TestHandler_GetDashboardWidget_MapsWidget(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		dashboards: &mockDashboardService{
			getWidgetFn: func(_ context.Context, dashboardID, widgetID string) (*domain.DashboardWidget, error) {
				assert.Equal(t, "dash-1", dashboardID)
				assert.Equal(t, "widget-1", widgetID)
				return &domain.DashboardWidget{
					ID:          widgetID,
					DashboardID: dashboardID,
					Name:        "Revenue by Region",
					Source: domain.DashboardWidgetSource{
						Kind: domain.DashboardWidgetSourceSQLQuery,
						SQLQuery: &domain.DashboardSQLQuerySource{
							SQL: "select region, revenue from summary",
						},
					},
				}, nil
			},
		},
	}

	resp, err := h.GetDashboardWidget(context.Background(), GenGetDashboardWidgetRequest{
		DashboardId: "dash-1",
		WidgetId:    "widget-1",
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenGetDashboardWidget200JSONResponse)
	require.True(t, ok)
	require.NotNil(t, okResp.Body.Id)
	assert.Equal(t, "widget-1", *okResp.Body.Id)
}
func TestHandler_GetRenderedDashboard_MapsResolvedWidgets(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", Type: "user"})
	chartType := domain.VisualChartBar
	widget := domain.DashboardWidget{
		ID:          "widget-1",
		DashboardID: "dash-1",
		Name:        "Revenue by Region",
		Source: domain.DashboardWidgetSource{
			Kind: domain.DashboardWidgetSourceSemanticQuery,
			SemanticQuery: &domain.DashboardSemanticQuerySource{
				SemanticModelID: "sm-sales",
				Metrics:         []string{"revenue"},
				Dimensions:      []string{"region"},
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
		Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 4, H: 3},
	}

	h := &APIHandler{
		dashboards: &mockDashboardService{
			getDashboardFn: func(_ context.Context, id string) (*domain.Dashboard, []domain.DashboardWidget, error) {
				return &domain.Dashboard{
					ID:                  id,
					Name:                "Revenue",
					Owner:               "alice",
					SemanticProjectName: "analytics",
					SemanticModelName:   "sales",
					Compute: domain.DashboardComputePolicy{
						Mode: domain.ComputeModeByocLocal,
					},
				}, []domain.DashboardWidget{widget}, nil
			},
			resolveWidgetsForDashboardFn: func(_ context.Context, principal string, dashboard *domain.Dashboard, widgets []domain.DashboardWidget, filters []dashboardsvc.InteractiveFilter) ([]dashboardsvc.ResolvedWidget, error) {
				assert.Equal(t, "alice", principal)
				require.NotNil(t, dashboard)
				assert.Equal(t, "analytics", dashboard.SemanticProjectName)
				require.Len(t, widgets, 1)
				assert.Equal(t, []dashboardsvc.InteractiveFilter{
					{Dimension: "region", Values: []string{"APAC", "EMEA"}},
					{Dimension: "borough", Values: []string{"Queens"}},
				}, filters)
				return []dashboardsvc.ResolvedWidget{{
					Widget:       widgets[0],
					Columns:      []string{"region", "revenue"},
					Rows:         [][]interface{}{{"APAC", 123}},
					RowCount:     1,
					GeneratedSQL: "SELECT region, SUM(revenue) AS revenue FROM sales GROUP BY 1",
				}}, nil
			},
		},
	}

	resp, err := h.GetRenderedDashboard(ctx, GenGetRenderedDashboardRequest{
		DashboardId: "dash-1",
		Params: GenGetRenderedDashboardParams{
			Filters: &[]string{"region:APAC", "region:EMEA", "borough:Queens"},
		},
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenGetRenderedDashboard200JSONResponse)
	require.True(t, ok)
	require.NotNil(t, okResp.Body.Dashboard)
	require.NotNil(t, okResp.Body.Dashboard.Compute)
	require.NotNil(t, okResp.Body.Dashboard.Compute.Mode)
	assert.Equal(t, "BYOC_LOCAL", *okResp.Body.Dashboard.Compute.Mode)
	require.NotNil(t, okResp.Body.Widgets)
	require.Len(t, *okResp.Body.Widgets, 1)
	require.NotNil(t, (*okResp.Body.Widgets)[0].Widget)
	assert.Equal(t, "widget-1", *(*okResp.Body.Widgets)[0].Widget.Id)
	assert.Equal(t, []string{"region", "revenue"}, (*okResp.Body.Widgets)[0].Columns)
	require.NotNil(t, (*okResp.Body.Widgets)[0].Rows)
	assert.Equal(t, [][]string{{"APAC", "123"}}, *(*okResp.Body.Widgets)[0].Rows)
	require.NotNil(t, (*okResp.Body.Widgets)[0].GeneratedSql)
	assert.Contains(t, *(*okResp.Body.Widgets)[0].GeneratedSql, "SELECT")
}

func TestHandler_UpdateDashboard_MapsRequestAndAccessDenied(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", Type: "user"})
	h := &APIHandler{
		dashboards: &mockDashboardService{
			updateDashboardFn: func(_ context.Context, principal string, isAdmin bool, id string, req domain.UpdateDashboardRequest) (*domain.Dashboard, error) {
				assert.Equal(t, "alice", principal)
				assert.False(t, isAdmin)
				assert.Equal(t, "dash-1", id)
				require.NotNil(t, req.Name)
				assert.Equal(t, "Updated dashboard", *req.Name)
				require.NotNil(t, req.SemanticProjectName)
				assert.Equal(t, "analytics", *req.SemanticProjectName)
				require.NotNil(t, req.SemanticModelName)
				assert.Equal(t, "sales", *req.SemanticModelName)
				require.NotNil(t, req.Compute)
				assert.Equal(t, domain.ComputeModeSharedEndpoint, req.Compute.Mode)
				assert.Equal(t, "analytics-xl", req.Compute.EndpointName)
				assert.True(t, req.Compute.FallbackLocal)
				return nil, domain.ErrAccessDenied("forbidden")
			},
		},
	}

	semanticProjectName := "analytics"
	semanticModelName := "sales"
	computeMode := domain.ComputeModeSharedEndpoint
	computeEndpointName := "analytics-xl"
	fallbackLocal := true
	resp, err := h.UpdateDashboard(ctx, GenUpdateDashboardRequest{
		DashboardId: "dash-1",
		Body: &GenUpdateDashboardJSONBody{
			Name:                strPtr("Updated dashboard"),
			SemanticProjectName: &semanticProjectName,
			SemanticModelName:   &semanticModelName,
			Compute: &DashboardComputePolicy{
				Mode:          &computeMode,
				EndpointName:  &computeEndpointName,
				FallbackLocal: &fallbackLocal,
			},
		},
	})
	require.NoError(t, err)
	_, ok := resp.(UpdateDashboard403JSONResponse)
	require.True(t, ok)
}

func TestHandler_CreateDashboard_MapsComputePolicy(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", Type: "user"})
	h := &APIHandler{
		dashboards: &mockDashboardService{
			createDashboardFn: func(_ context.Context, principal string, req domain.CreateDashboardRequest) (*domain.Dashboard, error) {
				assert.Equal(t, "alice", principal)
				require.NotNil(t, req.Compute)
				assert.Equal(t, domain.ComputeModeByocLocal, req.Compute.Mode)
				assert.Empty(t, req.Compute.EndpointName)
				assert.False(t, req.Compute.FallbackLocal)
				return &domain.Dashboard{
					ID:      "dash-1",
					Name:    req.Name,
					Owner:   principal,
					Compute: req.Compute.Normalize(),
				}, nil
			},
		},
	}

	computeMode := domain.ComputeModeByocLocal
	resp, err := h.CreateDashboard(ctx, GenCreateDashboardRequest{
		Body: &GenCreateDashboardJSONBody{
			Name: "Revenue Dashboard",
			Compute: &DashboardComputePolicy{
				Mode: &computeMode,
			},
		},
	})
	require.NoError(t, err)
	okResp, ok := resp.(GenCreateDashboard201JSONResponse)
	require.True(t, ok)
	require.NotNil(t, okResp.Body.Compute)
	require.NotNil(t, okResp.Body.Compute.Mode)
	assert.Equal(t, "BYOC_LOCAL", *okResp.Body.Compute.Mode)
}

func TestHandler_DeleteDashboard_MapsRequest(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", Type: "user", IsAdmin: true})
	h := &APIHandler{
		dashboards: &mockDashboardService{
			deleteDashboardFn: func(_ context.Context, principal string, isAdmin bool, id string) error {
				assert.Equal(t, "alice", principal)
				assert.True(t, isAdmin)
				assert.Equal(t, "dash-1", id)
				return nil
			},
		},
	}

	resp, err := h.DeleteDashboard(ctx, GenDeleteDashboardRequest{DashboardId: "dash-1"})
	require.NoError(t, err)
	_, ok := resp.(GenDeleteDashboard204Response)
	require.True(t, ok)
}

func TestHandler_UpdateDashboardWidget_MapsOptionalFields(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", Type: "user"})
	chartType := VisualChartTypeLine
	h := &APIHandler{
		dashboards: &mockDashboardService{
			updateWidgetFn: func(_ context.Context, principal string, isAdmin bool, widgetID string, req domain.UpdateDashboardWidgetRequest) (*domain.DashboardWidget, error) {
				assert.Equal(t, "alice", principal)
				assert.False(t, isAdmin)
				assert.Equal(t, "widget-1", widgetID)
				require.NotNil(t, req.Name)
				assert.Equal(t, "Updated widget", *req.Name)
				require.NotNil(t, req.Source)
				assert.Equal(t, domain.DashboardWidgetSourceSQLQuery, req.Source.Kind)
				require.NotNil(t, req.Layout)
				assert.Equal(t, 5, req.Layout.W)
				require.NotNil(t, req.VisualSpec)
				require.NotNil(t, req.VisualSpec.ChartType)
				assert.Equal(t, domain.VisualChartLine, *req.VisualSpec.ChartType)
				return &domain.DashboardWidget{
					ID:          widgetID,
					DashboardID: "dash-1",
					Name:        *req.Name,
					Source:      *req.Source,
					VisualSpec:  req.VisualSpec,
					Layout:      *req.Layout,
				}, nil
			},
		},
	}

	resp, err := h.UpdateDashboardWidget(ctx, GenUpdateDashboardWidgetRequest{
		DashboardId: "dash-1",
		WidgetId:    "widget-1",
		Body: &GenUpdateDashboardWidgetJSONBody{
			Name: strPtr("Updated widget"),
			Source: &DashboardWidgetSource{
				Kind: DashboardWidgetSourceKindSqlQuery,
				SqlQuery: &DashboardSQLQuerySource{
					Sql: "select region, revenue from summary",
				},
			},
			VisualSpec: &VisualSpec{
				Kind:      VisualOutputKindChart,
				ChartType: &chartType,
				Encodings: &VisualEncodings{
					X: &VisualFieldBinding{Field: "region"},
					Y: &VisualFieldBinding{Field: "revenue"},
				},
			},
			Layout: &DashboardWidgetLayout{X: 1, Y: 2, W: 5, H: 3},
		},
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenUpdateDashboardWidget200JSONResponse)
	require.True(t, ok)
	require.NotNil(t, okResp.Body.Id)
	assert.Equal(t, "widget-1", *okResp.Body.Id)
}

func TestHandler_DeleteDashboardWidget_MapsRequest(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", Type: "user"})
	h := &APIHandler{
		dashboards: &mockDashboardService{
			deleteWidgetFn: func(_ context.Context, principal string, isAdmin bool, widgetID string) error {
				assert.Equal(t, "alice", principal)
				assert.False(t, isAdmin)
				assert.Equal(t, "widget-1", widgetID)
				return nil
			},
		},
	}

	resp, err := h.DeleteDashboardWidget(ctx, GenDeleteDashboardWidgetRequest{
		DashboardId: "dash-1",
		WidgetId:    "widget-1",
	})
	require.NoError(t, err)
	_, ok := resp.(GenDeleteDashboardWidget204Response)
	require.True(t, ok)
}
