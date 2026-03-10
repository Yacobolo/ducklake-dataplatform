package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

type mockDashboardService struct {
	createDashboardFn func(ctx context.Context, owner string, req domain.CreateDashboardRequest) (*domain.Dashboard, error)
	listDashboardsFn  func(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Dashboard, int64, error)
	getDashboardFn    func(ctx context.Context, id string) (*domain.Dashboard, []domain.DashboardWidget, error)
	updateDashboardFn func(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateDashboardRequest) (*domain.Dashboard, error)
	deleteDashboardFn func(ctx context.Context, principal string, isAdmin bool, id string) error
	createWidgetFn    func(ctx context.Context, principal string, isAdmin bool, dashboardID string, req domain.CreateDashboardWidgetRequest) (*domain.DashboardWidget, error)
	updateWidgetFn    func(ctx context.Context, principal string, isAdmin bool, widgetID string, req domain.UpdateDashboardWidgetRequest) (*domain.DashboardWidget, error)
	deleteWidgetFn    func(ctx context.Context, principal string, isAdmin bool, widgetID string) error
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
				return &domain.Dashboard{
					ID:          "dash-1",
					Name:        req.Name,
					Description: req.Description,
					Owner:       owner,
					CreatedAt:   now,
					UpdatedAt:   now,
				}, nil
			},
		},
	}

	desc := "KPI dashboard"
	resp, err := h.CreateDashboard(ctx, GenCreateDashboardRequest{Body: &GenCreateDashboardJSONBody{
		Name:        "Executive Overview",
		Description: &desc,
	}})
	require.NoError(t, err)

	created, ok := resp.(GenCreateDashboard201JSONResponse)
	require.True(t, ok)
	require.NotNil(t, created.Body.Id)
	assert.Equal(t, "dash-1", *created.Body.Id)
	require.NotNil(t, created.Body.Name)
	assert.Equal(t, "Executive Overview", *created.Body.Name)
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
				assert.Equal(t, "analytics", req.Source.SemanticQuery.ProjectName)
				assert.Equal(t, "sales", req.Source.SemanticQuery.SemanticModelName)
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
					ProjectName:       "analytics",
					SemanticModelName: "sales",
					Metrics:           []string{"revenue"},
					Dimensions:        &[]string{"region"},
					TimeGrain:         &timeGrain,
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
