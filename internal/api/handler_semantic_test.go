package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	querysvc "duck-demo/internal/service/query"
	semantic "duck-demo/internal/service/semantic"
)

type mockSemanticService struct {
	createSemanticModelFn func(ctx context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error)
	listSemanticModelsFn  func(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.SemanticModel, int64, error)
	getSemanticModelFn    func(ctx context.Context, projectName, name string) (*domain.SemanticModel, error)
	listMetricsFn         func(ctx context.Context, projectName, semanticModelName string) ([]domain.SemanticMetric, error)
	explainMetricQueryFn  func(ctx context.Context, req semantic.MetricQueryRequest) (*semantic.MetricQueryPlan, error)
	runMetricQueryFn      func(ctx context.Context, principal string, req semantic.MetricQueryRequest) (*semantic.MetricQueryResult, error)
}

func (m *mockSemanticService) CreateSemanticModel(ctx context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error) {
	if m.createSemanticModelFn != nil {
		return m.createSemanticModelFn(ctx, principal, req)
	}
	panic("CreateSemanticModel not implemented")
}

func (m *mockSemanticService) GetSemanticModel(ctx context.Context, projectName, name string) (*domain.SemanticModel, error) {
	if m.getSemanticModelFn != nil {
		return m.getSemanticModelFn(ctx, projectName, name)
	}
	panic("GetSemanticModel not implemented")
}

func (m *mockSemanticService) ListSemanticModels(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.SemanticModel, int64, error) {
	if m.listSemanticModelsFn != nil {
		return m.listSemanticModelsFn(ctx, projectName, page)
	}
	panic("ListSemanticModels not implemented")
}

func (m *mockSemanticService) UpdateSemanticModel(context.Context, string, string, domain.UpdateSemanticModelRequest) (*domain.SemanticModel, error) {
	panic("UpdateSemanticModel not implemented")
}

func (m *mockSemanticService) DeleteSemanticModel(context.Context, string, string) error {
	panic("DeleteSemanticModel not implemented")
}

func (m *mockSemanticService) CreateMetric(context.Context, string, string, string, domain.CreateSemanticMetricRequest) (*domain.SemanticMetric, error) {
	panic("CreateMetric not implemented")
}

func (m *mockSemanticService) ListMetrics(ctx context.Context, projectName, semanticModelName string) ([]domain.SemanticMetric, error) {
	if m.listMetricsFn != nil {
		return m.listMetricsFn(ctx, projectName, semanticModelName)
	}
	panic("ListMetrics not implemented")
}

func (m *mockSemanticService) UpdateMetric(context.Context, string, string, string, domain.UpdateSemanticMetricRequest) (*domain.SemanticMetric, error) {
	panic("UpdateMetric not implemented")
}

func (m *mockSemanticService) DeleteMetric(context.Context, string, string, string) error {
	panic("DeleteMetric not implemented")
}

func (m *mockSemanticService) CreatePreAggregation(context.Context, string, string, string, domain.CreateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error) {
	panic("CreatePreAggregation not implemented")
}

func (m *mockSemanticService) ListPreAggregations(context.Context, string, string) ([]domain.SemanticPreAggregation, error) {
	panic("ListPreAggregations not implemented")
}

func (m *mockSemanticService) UpdatePreAggregation(context.Context, string, string, string, domain.UpdateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error) {
	panic("UpdatePreAggregation not implemented")
}

func (m *mockSemanticService) DeletePreAggregation(context.Context, string, string, string) error {
	panic("DeletePreAggregation not implemented")
}

func (m *mockSemanticService) CreateRelationship(context.Context, string, domain.CreateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	panic("CreateRelationship not implemented")
}

func (m *mockSemanticService) ListRelationships(context.Context, domain.PageRequest) ([]domain.SemanticRelationship, int64, error) {
	panic("ListRelationships not implemented")
}

func (m *mockSemanticService) UpdateRelationship(context.Context, string, domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	panic("UpdateRelationship not implemented")
}

func (m *mockSemanticService) DeleteRelationship(context.Context, string) error {
	panic("DeleteRelationship not implemented")
}

func (m *mockSemanticService) ExplainMetricQuery(ctx context.Context, req semantic.MetricQueryRequest) (*semantic.MetricQueryPlan, error) {
	if m.explainMetricQueryFn != nil {
		return m.explainMetricQueryFn(ctx, req)
	}
	panic("ExplainMetricQuery not implemented")
}

func (m *mockSemanticService) RunMetricQuery(ctx context.Context, principal string, req semantic.MetricQueryRequest) (*semantic.MetricQueryResult, error) {
	if m.runMetricQueryFn != nil {
		return m.runMetricQueryFn(ctx, principal, req)
	}
	panic("RunMetricQuery not implemented")
}

func TestHandler_CreateSemanticModel_UsesPrincipalAndMapsRequest(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true, Type: "user"})
	desc := "Sales semantic"
	defaultTimeDim := "order_date"
	tags := []string{"finance", "core"}

	fixed := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	h := &APIHandler{
		semantics: &mockSemanticService{
			createSemanticModelFn: func(_ context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error) {
				assert.Equal(t, "alice", principal)
				assert.Equal(t, "analytics", req.ProjectName)
				assert.Equal(t, "sales", req.Name)
				assert.Equal(t, "analytics.fct_sales", req.BaseModelRef)
				assert.Equal(t, desc, req.Description)
				assert.Equal(t, defaultTimeDim, req.DefaultTimeDimension)
				assert.Equal(t, tags, req.Tags)
				return &domain.SemanticModel{
					ID:                   "sm-1",
					ProjectName:          req.ProjectName,
					Name:                 req.Name,
					Description:          req.Description,
					BaseModelRef:         req.BaseModelRef,
					DefaultTimeDimension: req.DefaultTimeDimension,
					Tags:                 req.Tags,
					CreatedAt:            fixed,
					UpdatedAt:            fixed,
				}, nil
			},
		},
	}

	resp, err := h.CreateSemanticModel(ctx, GenCreateSemanticModelRequest{Body: &GenCreateSemanticModelJSONBody{
		ProjectName:          "analytics",
		Name:                 "sales",
		BaseModelRef:         "analytics.fct_sales",
		Description:          &desc,
		DefaultTimeDimension: &defaultTimeDim,
		Tags:                 &tags,
	}})
	require.NoError(t, err)

	created, ok := resp.(GenCreateSemanticModel201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)
	require.NotNil(t, created.Body.ProjectName)
	assert.Equal(t, "analytics", *created.Body.ProjectName)
	require.NotNil(t, created.Body.Name)
	assert.Equal(t, "sales", *created.Body.Name)
	require.NotNil(t, created.Body.Tags)
	assert.Equal(t, tags, *created.Body.Tags)
}

func TestHandler_CreateSemanticModel_ValidationErrorMaps400(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		semantics: &mockSemanticService{
			createSemanticModelFn: func(context.Context, string, domain.CreateSemanticModelRequest) (*domain.SemanticModel, error) {
				return nil, domain.ErrValidation("bad semantic model")
			},
		},
	}

	resp, err := h.CreateSemanticModel(context.Background(), GenCreateSemanticModelRequest{Body: &GenCreateSemanticModelJSONBody{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.fct_sales",
	}})
	require.NoError(t, err)

	badReq, ok := resp.(CreateSemanticModel400JSONResponse)
	require.True(t, ok, "expected 400 response, got %T", resp)
	assert.Contains(t, badReq.Body.Message, "bad semantic model")
}

func TestHandler_ListSemanticModels_PassesFiltersAndPagination(t *testing.T) {
	t.Parallel()

	projectName := "analytics"
	maxResults := int32(2)

	h := &APIHandler{
		semantics: &mockSemanticService{
			listSemanticModelsFn: func(_ context.Context, project *string, page domain.PageRequest) ([]domain.SemanticModel, int64, error) {
				require.NotNil(t, project)
				assert.Equal(t, projectName, *project)
				assert.Equal(t, int(maxResults), page.Limit())
				assert.Equal(t, 0, page.Offset())
				return []domain.SemanticModel{{ProjectName: "analytics", Name: "sales", BaseModelRef: "analytics.fct_sales"}}, 3, nil
			},
		},
	}

	resp, err := h.ListSemanticModels(context.Background(), GenListSemanticModelsRequest{Params: GenListSemanticModelsParams{ProjectName: &projectName, MaxResults: &maxResults}})
	require.NoError(t, err)

	okResp, ok := resp.(GenListSemanticModels200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.Data)
	require.Len(t, okResp.Body.Data, 1)
	require.NotNil(t, okResp.Body.NextPageToken)
	assert.NotEmpty(t, *okResp.Body.NextPageToken)
}

func TestHandler_ExplainMetricQuery_MapsRequestAndResponse(t *testing.T) {
	t.Parallel()

	limit := int32(50)
	dimensions := []string{"order_date"}
	filters := []string{"region = 'us'"}
	orderBy := []string{"order_date desc"}
	timeGrain := "day"

	preAgg := "analytics.agg_daily_sales"
	h := &APIHandler{
		semantics: &mockSemanticService{
			explainMetricQueryFn: func(_ context.Context, req semantic.MetricQueryRequest) (*semantic.MetricQueryPlan, error) {
				assert.Equal(t, "analytics", req.ProjectName)
				assert.Equal(t, "sales", req.SemanticModelName)
				assert.Equal(t, []string{"total_revenue"}, req.Metrics)
				assert.Equal(t, dimensions, req.Dimensions)
				assert.Equal(t, filters, req.Filters)
				assert.Equal(t, orderBy, req.OrderBy)
				require.NotNil(t, req.Limit)
				assert.Equal(t, int(limit), *req.Limit)
				require.NotNil(t, req.TimeGrain)
				assert.Equal(t, timeGrain, *req.TimeGrain)
				return &semantic.MetricQueryPlan{
					BaseModelName:          "sales",
					BaseRelation:           "analytics.fct_sales",
					Metrics:                req.Metrics,
					Dimensions:             req.Dimensions,
					TimeGrain:              &timeGrain,
					JoinPath:               []semantic.JoinStep{{RelationshipName: "sales_to_customers", FromModel: "sales", ToModel: "customers", RelationshipType: domain.RelationshipTypeManyToOne, JoinSQL: "sales.customer_id = customers.id"}},
					SelectedPreAggregation: &preAgg,
					GeneratedSQL:           "select ...",
					FreshnessStatus:        "fresh",
					FreshnessBasis:         []string{"analytics.fct_sales@2026-02-18T10:00:00Z"},
				}, nil
			},
		},
	}

	resp, err := h.ExplainMetricQuery(context.Background(), GenExplainMetricQueryRequest{Body: &GenExplainMetricQueryJSONBody{
		ProjectName:       "analytics",
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Dimensions:        &dimensions,
		Filters:           &filters,
		OrderBy:           &orderBy,
		Limit:             &limit,
		TimeGrain:         &timeGrain,
	}})
	require.NoError(t, err)

	okResp, ok := resp.(ExplainMetricQuery200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.Plan)
	require.NotNil(t, okResp.Body.Plan.GeneratedSql)
	assert.Equal(t, "select ...", *okResp.Body.Plan.GeneratedSql)
	require.NotNil(t, okResp.Body.Plan.SelectedPreAggregation)
	assert.Equal(t, preAgg, *okResp.Body.Plan.SelectedPreAggregation)
	require.NotNil(t, okResp.Body.Plan.TimeGrain)
	assert.Equal(t, timeGrain, *okResp.Body.Plan.TimeGrain)
	require.NotNil(t, okResp.Body.Plan.JoinPath)
	require.Len(t, *okResp.Body.Plan.JoinPath, 1)
	require.NotNil(t, (*okResp.Body.Plan.JoinPath)[0].RelationshipType)
	assert.Equal(t, domain.RelationshipTypeManyToOne, *(*okResp.Body.Plan.JoinPath)[0].RelationshipType)
}

func TestHandler_RunMetricQuery_UsesPrincipalAndMapsResult(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "analyst1", IsAdmin: false, Type: "user"})
	limit := int32(10)

	h := &APIHandler{
		semantics: &mockSemanticService{
			runMetricQueryFn: func(_ context.Context, principal string, req semantic.MetricQueryRequest) (*semantic.MetricQueryResult, error) {
				assert.Equal(t, "analyst1", principal)
				assert.Equal(t, "analytics", req.ProjectName)
				require.NotNil(t, req.Limit)
				assert.Equal(t, 10, *req.Limit)
				return &semantic.MetricQueryResult{
					Plan: semantic.MetricQueryPlan{
						BaseModelName:   "sales",
						BaseRelation:    "analytics.fct_sales",
						Metrics:         []string{"total_revenue"},
						Dimensions:      []string{"order_date"},
						GeneratedSQL:    "select ...",
						FreshnessStatus: "fresh",
					},
					Result: &querysvc.QueryResult{
						Columns:  []string{"order_date", "total_revenue"},
						Rows:     [][]interface{}{{"2026-02-18", 123.45}},
						RowCount: 1,
					},
				}, nil
			},
		},
	}

	resp, err := h.RunMetricQuery(ctx, GenRunMetricQueryRequest{Body: &GenRunMetricQueryJSONBody{
		ProjectName:       "analytics",
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Limit:             &limit,
	}})
	require.NoError(t, err)

	okResp, ok := resp.(RunMetricQuery200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.Result)
	require.NotNil(t, okResp.Body.Result.RowCount)
	assert.EqualValues(t, 1, *okResp.Body.Result.RowCount)
	require.NotNil(t, okResp.Body.Result.Columns)
	assert.Equal(t, []string{"order_date", "total_revenue"}, okResp.Body.Result.Columns)
}

func TestHandler_CheckMetricFreshness_ResolvesMetricAndReturnsFreshness(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		semantics: &mockSemanticService{
			listSemanticModelsFn: func(_ context.Context, _ *string, _ domain.PageRequest) ([]domain.SemanticModel, int64, error) {
				return []domain.SemanticModel{{ProjectName: "analytics", Name: "sales", BaseModelRef: "analytics.fct_sales"}}, 1, nil
			},
			listMetricsFn: func(_ context.Context, projectName, semanticModelName string) ([]domain.SemanticMetric, error) {
				assert.Equal(t, "analytics", projectName)
				assert.Equal(t, "sales", semanticModelName)
				return []domain.SemanticMetric{{Name: "total_revenue"}}, nil
			},
			explainMetricQueryFn: func(_ context.Context, req semantic.MetricQueryRequest) (*semantic.MetricQueryPlan, error) {
				assert.Equal(t, "analytics", req.ProjectName)
				assert.Equal(t, "sales", req.SemanticModelName)
				assert.Equal(t, []string{"total_revenue"}, req.Metrics)
				return &semantic.MetricQueryPlan{
					FreshnessStatus:        "fresh",
					FreshnessBasis:         []string{"analytics.fct_sales"},
					SelectedPreAggregation: nil,
				}, nil
			},
		},
	}

	resp, err := h.CheckMetricFreshness(context.Background(), GenCheckMetricFreshnessRequest{MetricName: "total_revenue"})
	require.NoError(t, err)

	okResp, ok := resp.(GenCheckMetricFreshness200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.MetricName)
	assert.Equal(t, "total_revenue", *okResp.Body.MetricName)
	require.NotNil(t, okResp.Body.ProjectName)
	assert.Equal(t, "analytics", *okResp.Body.ProjectName)
	require.NotNil(t, okResp.Body.SemanticModelName)
	assert.Equal(t, "sales", *okResp.Body.SemanticModelName)
	require.NotNil(t, okResp.Body.FreshnessStatus)
	assert.Equal(t, "fresh", *okResp.Body.FreshnessStatus)
	require.NotNil(t, okResp.Body.FreshnessBasis)
	assert.Equal(t, []string{"analytics.fct_sales"}, *okResp.Body.FreshnessBasis)
	require.NotNil(t, okResp.Body.CheckedAt)
}

func TestHandler_CheckMetricFreshness_AmbiguousMetricReturns400(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		semantics: &mockSemanticService{
			listSemanticModelsFn: func(_ context.Context, _ *string, _ domain.PageRequest) ([]domain.SemanticModel, int64, error) {
				return []domain.SemanticModel{{ProjectName: "analytics", Name: "sales"}, {ProjectName: "analytics", Name: "marketing"}}, 2, nil
			},
			listMetricsFn: func(_ context.Context, _, semanticModelName string) ([]domain.SemanticMetric, error) {
				if semanticModelName == "sales" || semanticModelName == "marketing" {
					return []domain.SemanticMetric{{Name: "total_revenue"}}, nil
				}
				return nil, nil
			},
		},
	}

	resp, err := h.CheckMetricFreshness(context.Background(), GenCheckMetricFreshnessRequest{MetricName: "total_revenue"})
	require.NoError(t, err)

	badReq, ok := resp.(CheckMetricFreshness400JSONResponse)
	require.True(t, ok, "expected 400 response, got %T", resp)
	assert.Contains(t, badReq.Body.Message, "ambiguous")
}
