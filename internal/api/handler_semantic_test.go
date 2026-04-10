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
	createSemanticModelFn        func(ctx context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error)
	listSemanticModelsFn         func(ctx context.Context, page domain.PageRequest) ([]domain.SemanticModel, int64, error)
	getSemanticModelFn           func(ctx context.Context, semanticModelID string) (*domain.SemanticModel, error)
	createMetricFn               func(ctx context.Context, principal, semanticModelID string, req domain.CreateSemanticMetricRequest) (*domain.SemanticMetric, error)
	listMetricsFn                func(ctx context.Context, semanticModelID string) ([]domain.SemanticMetric, error)
	updateMetricFn               func(ctx context.Context, semanticModelID, metricName string, req domain.UpdateSemanticMetricRequest) (*domain.SemanticMetric, error)
	deleteMetricFn               func(ctx context.Context, semanticModelID, metricName string) error
	createRelationshipForModelFn func(ctx context.Context, principal, semanticModelID string, req domain.CreateSemanticRelationshipRequest) (*domain.SemanticRelationship, error)
	listRelationshipsForModelFn  func(ctx context.Context, semanticModelID string) ([]domain.SemanticRelationship, error)
	updateRelationshipForModelFn func(ctx context.Context, semanticModelID, relationshipName string, req domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error)
	deleteRelationshipForModelFn func(ctx context.Context, semanticModelID, relationshipName string) error
	explainMetricQueryFn         func(ctx context.Context, req semantic.MetricQueryRequest) (*semantic.MetricQueryPlan, error)
	runMetricQueryFn             func(ctx context.Context, principal string, req semantic.MetricQueryRequest) (*semantic.MetricQueryResult, error)
}

func (m *mockSemanticService) CreateSemanticModel(ctx context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error) {
	if m.createSemanticModelFn != nil {
		return m.createSemanticModelFn(ctx, principal, req)
	}
	panic("CreateSemanticModel not implemented")
}

func (m *mockSemanticService) GetSemanticModel(ctx context.Context, identifiers ...string) (*domain.SemanticModel, error) {
	if m.getSemanticModelFn != nil {
		return m.getSemanticModelFn(ctx, identifiers[0])
	}
	panic("GetSemanticModel not implemented")
}

func (m *mockSemanticService) ListSemanticModels(ctx context.Context, args ...any) ([]domain.SemanticModel, int64, error) {
	if m.listSemanticModelsFn != nil {
		page, _ := args[len(args)-1].(domain.PageRequest)
		return m.listSemanticModelsFn(ctx, page)
	}
	panic("ListSemanticModels not implemented")
}

func (m *mockSemanticService) UpdateSemanticModel(context.Context, string, ...any) (*domain.SemanticModel, error) {
	panic("UpdateSemanticModel not implemented")
}

func (m *mockSemanticService) DeleteSemanticModel(context.Context, string) error {
	panic("DeleteSemanticModel not implemented")
}

func (m *mockSemanticService) CreateMetric(ctx context.Context, principal, semanticModelID string, args ...any) (*domain.SemanticMetric, error) {
	if m.createMetricFn != nil {
		req, _ := args[len(args)-1].(domain.CreateSemanticMetricRequest)
		return m.createMetricFn(ctx, principal, semanticModelID, req)
	}
	panic("CreateMetric not implemented")
}

func (m *mockSemanticService) ListMetrics(ctx context.Context, semanticModelID string, _ ...string) ([]domain.SemanticMetric, error) {
	if m.listMetricsFn != nil {
		return m.listMetricsFn(ctx, semanticModelID)
	}
	panic("ListMetrics not implemented")
}

func (m *mockSemanticService) UpdateMetric(ctx context.Context, semanticModelID string, args ...any) (*domain.SemanticMetric, error) {
	if m.updateMetricFn != nil {
		metricName, _ := args[len(args)-2].(string)
		req, _ := args[len(args)-1].(domain.UpdateSemanticMetricRequest)
		return m.updateMetricFn(ctx, semanticModelID, metricName, req)
	}
	panic("UpdateMetric not implemented")
}

func (m *mockSemanticService) DeleteMetric(ctx context.Context, semanticModelID string, args ...string) error {
	if m.deleteMetricFn != nil {
		metricName := args[len(args)-1]
		return m.deleteMetricFn(ctx, semanticModelID, metricName)
	}
	panic("DeleteMetric not implemented")
}

func (m *mockSemanticService) CreatePreAggregation(context.Context, string, string, ...any) (*domain.SemanticPreAggregation, error) {
	panic("CreatePreAggregation not implemented")
}

func (m *mockSemanticService) ListPreAggregations(context.Context, string, ...string) ([]domain.SemanticPreAggregation, error) {
	panic("ListPreAggregations not implemented")
}

func (m *mockSemanticService) UpdatePreAggregation(context.Context, string, ...any) (*domain.SemanticPreAggregation, error) {
	panic("UpdatePreAggregation not implemented")
}

func (m *mockSemanticService) DeletePreAggregation(context.Context, string, ...string) error {
	panic("DeletePreAggregation not implemented")
}

func (m *mockSemanticService) CreateRelationshipForModel(ctx context.Context, principal, semanticModelID string, req domain.CreateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	if m.createRelationshipForModelFn != nil {
		return m.createRelationshipForModelFn(ctx, principal, semanticModelID, req)
	}
	panic("CreateRelationshipForModel not implemented")
}

func (m *mockSemanticService) ListRelationshipsForModel(ctx context.Context, semanticModelID string) ([]domain.SemanticRelationship, error) {
	if m.listRelationshipsForModelFn != nil {
		return m.listRelationshipsForModelFn(ctx, semanticModelID)
	}
	panic("ListRelationshipsForModel not implemented")
}

func (m *mockSemanticService) UpdateRelationshipForModel(ctx context.Context, semanticModelID, relationshipName string, req domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	if m.updateRelationshipForModelFn != nil {
		return m.updateRelationshipForModelFn(ctx, semanticModelID, relationshipName, req)
	}
	panic("UpdateRelationshipForModel not implemented")
}

func (m *mockSemanticService) DeleteRelationshipForModel(ctx context.Context, semanticModelID, relationshipName string) error {
	if m.deleteRelationshipForModelFn != nil {
		return m.deleteRelationshipForModelFn(ctx, semanticModelID, relationshipName)
	}
	panic("DeleteRelationshipForModel not implemented")
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
				assert.Equal(t, "sales", req.Name)
				assert.Equal(t, "analytics.fct_sales", req.BaseModelRef)
				assert.Equal(t, desc, req.Description)
				assert.Equal(t, defaultTimeDim, req.DefaultTimeDimension)
				assert.Equal(t, tags, req.Tags)
				return &domain.SemanticModel{
					ID:                   "sm-1",
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
		Name:                 "sales",
		BaseModelRef:         "analytics.fct_sales",
		Description:          &desc,
		DefaultTimeDimension: &defaultTimeDim,
		Tags:                 &tags,
	}})
	require.NoError(t, err)

	created, ok := resp.(GenCreateSemanticModel201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)
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

	maxResults := int32(2)

	h := &APIHandler{
		semantics: &mockSemanticService{
			listSemanticModelsFn: func(_ context.Context, page domain.PageRequest) ([]domain.SemanticModel, int64, error) {
				assert.Equal(t, int(maxResults), page.Limit())
				assert.Equal(t, 0, page.Offset())
				return []domain.SemanticModel{{ID: "sm-sales", Name: "sales", BaseModelRef: "analytics.fct_sales"}}, 3, nil
			},
		},
	}

	resp, err := h.ListSemanticModels(context.Background(), GenListSemanticModelsRequest{Params: GenListSemanticModelsParams{MaxResults: &maxResults}})
	require.NoError(t, err)

	okResp, ok := resp.(GenListSemanticModels200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.Data)
	require.Len(t, okResp.Body.Data, 1)
	require.NotNil(t, okResp.Body.NextPageToken)
	assert.NotEmpty(t, *okResp.Body.NextPageToken)
}

func TestHandler_ListSemanticMetrics_MapsDashboardFields(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		semantics: &mockSemanticService{
			listMetricsFn: func(_ context.Context, semanticModelID string) ([]domain.SemanticMetric, error) {
				assert.Equal(t, "sm-sales", semanticModelID)
				return []domain.SemanticMetric{{
					ID:                 "metric-1",
					SemanticModelID:    "sm-1",
					Name:               "net_revenue",
					Description:        "Net revenue after discounts",
					Label:              "Net Revenue",
					MetricType:         domain.MetricTypeRatio,
					ExpressionMode:     domain.MetricExpressionModeSQL,
					Expression:         "SUM(net_revenue) / SUM(orders)",
					FilterSQL:          "region = 'apac'",
					DefaultTimeGrain:   "day",
					Format:             "currency",
					Owner:              "alice",
					CertificationState: domain.CertificationCertified,
				}}, nil
			},
		},
	}

	resp, err := h.ListSemanticMetrics(context.Background(), GenListSemanticMetricsRequest{
		SemanticModelId:   "sm-sales",
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenListSemanticMetrics200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.Len(t, okResp.Body.Data, 1)
	metric := okResp.Body.Data[0]
	require.NotNil(t, metric.Name)
	assert.Equal(t, "net_revenue", *metric.Name)
	require.NotNil(t, metric.FilterSql)
	assert.Equal(t, "region = 'apac'", *metric.FilterSql)
	require.NotNil(t, metric.DefaultTimeGrain)
	assert.Equal(t, "day", *metric.DefaultTimeGrain)
	require.NotNil(t, metric.Format)
	assert.Equal(t, "currency", *metric.Format)
	require.NotNil(t, metric.ExpressionMode)
	assert.Equal(t, SemanticMetricExpressionModeSQL, *metric.ExpressionMode)
	require.NotNil(t, metric.MetricType)
	assert.Equal(t, SemanticMetricMetricTypeRATIO, *metric.MetricType)
	require.NotNil(t, metric.CertificationState)
	assert.Equal(t, CreateSemanticMetricRequestCertificationStateCERTIFIED, *metric.CertificationState)
}

func TestHandler_CreateSemanticMetric_MapsDashboardFields(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true, Type: "user"})
	filterSQL := "region = 'apac'"
	defaultTimeGrain := "day"
	format := "currency"
	label := "Net Revenue"
	description := "Net revenue after discounts"
	expressionMode := SemanticMetricExpressionModeSQL
	certification := CreateSemanticMetricRequestCertificationStateCERTIFIED

	h := &APIHandler{
		semantics: &mockSemanticService{
			createMetricFn: func(_ context.Context, principal, semanticModelID string, req domain.CreateSemanticMetricRequest) (*domain.SemanticMetric, error) {
				assert.Equal(t, "alice", principal)
				assert.Equal(t, "sm-sales", semanticModelID)
				assert.Equal(t, "net_revenue", req.Name)
				assert.Equal(t, "Net Revenue", req.Label)
				assert.Equal(t, domain.MetricTypeRatio, req.MetricType)
				assert.Equal(t, domain.MetricExpressionModeSQL, req.ExpressionMode)
				assert.Equal(t, "SUM(net_revenue) / SUM(orders)", req.Expression)
				assert.Equal(t, filterSQL, req.FilterSQL)
				assert.Equal(t, defaultTimeGrain, req.DefaultTimeGrain)
				assert.Equal(t, format, req.Format)
				assert.Equal(t, domain.CertificationCertified, req.CertificationState)
				return &domain.SemanticMetric{
					ID:                 "metric-1",
					SemanticModelID:    "sm-1",
					Name:               req.Name,
					Description:        req.Description,
					Label:              req.Label,
					MetricType:         req.MetricType,
					ExpressionMode:     req.ExpressionMode,
					Expression:         req.Expression,
					FilterSQL:          req.FilterSQL,
					DefaultTimeGrain:   req.DefaultTimeGrain,
					Format:             req.Format,
					CertificationState: req.CertificationState,
				}, nil
			},
		},
	}

	resp, err := h.CreateSemanticMetric(ctx, GenCreateSemanticMetricRequest{
		SemanticModelId:   "sm-sales",
		Body: &GenCreateSemanticMetricJSONBody{
			Name:               "net_revenue",
			Description:        &description,
			Label:              &label,
			MetricType:         SemanticMetricMetricTypeRATIO,
			ExpressionMode:     &expressionMode,
			Expression:         "SUM(net_revenue) / SUM(orders)",
			FilterSql:          &filterSQL,
			DefaultTimeGrain:   &defaultTimeGrain,
			Format:             &format,
			CertificationState: &certification,
		},
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenCreateSemanticMetric201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)
	require.NotNil(t, okResp.Body.FilterSql)
	assert.Equal(t, filterSQL, *okResp.Body.FilterSql)
	require.NotNil(t, okResp.Body.DefaultTimeGrain)
	assert.Equal(t, defaultTimeGrain, *okResp.Body.DefaultTimeGrain)
	require.NotNil(t, okResp.Body.Format)
	assert.Equal(t, format, *okResp.Body.Format)
}

func TestHandler_UpdateSemanticMetric_MapsDashboardFields(t *testing.T) {
	t.Parallel()

	filterSQL := "region = 'emea'"
	defaultTimeGrain := "week"
	format := "percent"
	owner := "finance-team"
	label := "Margin Rate"
	description := "Margin rate"
	metricType := SemanticMetricMetricTypeRATIO
	expressionMode := SemanticMetricExpressionModeSQL
	certification := CreateSemanticMetricRequestCertificationStateDEPRECATED

	h := &APIHandler{
		semantics: &mockSemanticService{
			updateMetricFn: func(_ context.Context, semanticModelID, metricName string, req domain.UpdateSemanticMetricRequest) (*domain.SemanticMetric, error) {
				assert.Equal(t, "sm-sales", semanticModelID)
				assert.Equal(t, "margin_rate", metricName)
				require.NotNil(t, req.Description)
				assert.Equal(t, description, *req.Description)
				require.NotNil(t, req.Label)
				assert.Equal(t, label, *req.Label)
				require.NotNil(t, req.MetricType)
				assert.Equal(t, domain.MetricTypeRatio, *req.MetricType)
				require.NotNil(t, req.ExpressionMode)
				assert.Equal(t, domain.MetricExpressionModeSQL, *req.ExpressionMode)
				require.NotNil(t, req.FilterSQL)
				assert.Equal(t, filterSQL, *req.FilterSQL)
				require.NotNil(t, req.DefaultTimeGrain)
				assert.Equal(t, defaultTimeGrain, *req.DefaultTimeGrain)
				require.NotNil(t, req.Format)
				assert.Equal(t, format, *req.Format)
				require.NotNil(t, req.Owner)
				assert.Equal(t, owner, *req.Owner)
				require.NotNil(t, req.CertificationState)
				assert.Equal(t, domain.CertificationDeprecated, *req.CertificationState)

				return &domain.SemanticMetric{
					ID:                 "metric-1",
					SemanticModelID:    "sm-1",
					Name:               metricName,
					Description:        *req.Description,
					Label:              *req.Label,
					MetricType:         *req.MetricType,
					ExpressionMode:     *req.ExpressionMode,
					Expression:         "SUM(profit) / SUM(revenue)",
					FilterSQL:          *req.FilterSQL,
					DefaultTimeGrain:   *req.DefaultTimeGrain,
					Format:             *req.Format,
					Owner:              *req.Owner,
					CertificationState: *req.CertificationState,
				}, nil
			},
		},
	}

	expression := "SUM(profit) / SUM(revenue)"
	resp, err := h.UpdateSemanticMetric(context.Background(), GenUpdateSemanticMetricRequest{
		SemanticModelId:   "sm-sales",
		MetricName:        "margin_rate",
		Body: &GenUpdateSemanticMetricJSONBody{
			Description:        &description,
			Label:              &label,
			MetricType:         &metricType,
			ExpressionMode:     &expressionMode,
			Expression:         &expression,
			FilterSql:          &filterSQL,
			DefaultTimeGrain:   &defaultTimeGrain,
			Format:             &format,
			Owner:              &owner,
			CertificationState: &certification,
		},
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenUpdateSemanticMetric200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.Owner)
	assert.Equal(t, owner, *okResp.Body.Owner)
	require.NotNil(t, okResp.Body.CertificationState)
	assert.Equal(t, CreateSemanticMetricRequestCertificationStateDEPRECATED, *okResp.Body.CertificationState)
}

func TestHandler_DeleteSemanticMetric_MapsRequest(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		semantics: &mockSemanticService{
			deleteMetricFn: func(_ context.Context, semanticModelID, metricName string) error {
				assert.Equal(t, "sm-sales", semanticModelID)
				assert.Equal(t, "margin_rate", metricName)
				return nil
			},
		},
	}

	resp, err := h.DeleteSemanticMetric(context.Background(), GenDeleteSemanticMetricRequest{
		SemanticModelId:   "sm-sales",
		MetricName:        "margin_rate",
	})
	require.NoError(t, err)
	_, ok := resp.(GenDeleteSemanticMetric204Response)
	require.True(t, ok, "expected 204 response, got %T", resp)
}

func TestHandler_ListSemanticModelRelationships_MapsNestedRequest(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		semantics: &mockSemanticService{
			listRelationshipsForModelFn: func(_ context.Context, semanticModelID string) ([]domain.SemanticRelationship, error) {
				assert.Equal(t, "sm-sales", semanticModelID)
				return []domain.SemanticRelationship{{
					ID:               "rel-1",
					Name:             "sales_to_customers",
					FromSemanticID:   "sm-sales",
					ToSemanticID:     "sm-customers",
					RelationshipType: domain.RelationshipTypeManyToOne,
					JoinSQL:          "sales.customer_id = customers.customer_id",
					Cost:             1,
					MaxHops:          2,
				}}, nil
			},
		},
	}

	resp, err := h.ListSemanticModelRelationships(context.Background(), GenListSemanticModelRelationshipsRequest{
		SemanticModelId:   "sm-sales",
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenListSemanticModelRelationships200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.Len(t, okResp.Body.Data, 1)
	require.NotNil(t, okResp.Body.Data[0].Name)
	assert.Equal(t, "sales_to_customers", *okResp.Body.Data[0].Name)
	require.NotNil(t, okResp.Body.Data[0].RelationshipType)
	assert.Equal(t, SemanticRelationshipRelationshipTypeMANYTOONE, *okResp.Body.Data[0].RelationshipType)
}

func TestHandler_CreateSemanticModelRelationship_UsesPrincipalAndModelContext(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true, Type: "user"})
	cost := int32(3)
	maxHops := int32(1)

	h := &APIHandler{
		semantics: &mockSemanticService{
			createRelationshipForModelFn: func(_ context.Context, principal, semanticModelID string, req domain.CreateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
				assert.Equal(t, "alice", principal)
				assert.Equal(t, "sm-sales", semanticModelID)
				assert.Equal(t, "sales_to_customers", req.Name)
				assert.Equal(t, "sm-sales", req.FromSemanticID)
				assert.Equal(t, "sm-customers", req.ToSemanticID)
				assert.Equal(t, domain.RelationshipTypeManyToOne, req.RelationshipType)
				assert.Equal(t, "sales.customer_id = customers.customer_id", req.JoinSQL)
				assert.Equal(t, 3, req.Cost)
				assert.Equal(t, 1, req.MaxHops)
				return &domain.SemanticRelationship{
					ID:               "rel-1",
					Name:             req.Name,
					FromSemanticID:   req.FromSemanticID,
					ToSemanticID:     req.ToSemanticID,
					RelationshipType: req.RelationshipType,
					JoinSQL:          req.JoinSQL,
					Cost:             req.Cost,
					MaxHops:          req.MaxHops,
				}, nil
			},
		},
	}

	resp, err := h.CreateSemanticModelRelationship(ctx, GenCreateSemanticModelRelationshipRequest{
		SemanticModelId:   "sm-sales",
		Body: &GenCreateSemanticModelRelationshipJSONBody{
			Name:             "sales_to_customers",
			FromSemanticId:   "sm-sales",
			ToSemanticId:     "sm-customers",
			RelationshipType: SemanticRelationshipRelationshipTypeMANYTOONE,
			JoinSql:          "sales.customer_id = customers.customer_id",
			Cost:             &cost,
			MaxHops:          &maxHops,
		},
	})
	require.NoError(t, err)

	created, ok := resp.(GenCreateSemanticModelRelationship201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)
	require.NotNil(t, created.Body.ToSemanticId)
	assert.Equal(t, "sm-customers", *created.Body.ToSemanticId)
}

func TestHandler_UpdateSemanticModelRelationship_MapsNestedRequest(t *testing.T) {
	t.Parallel()

	cost := int32(7)
	maxHops := int32(4)
	relationshipType := SemanticRelationshipRelationshipTypeONETOMANY
	joinSQL := "customers.customer_id = sales.customer_id"

	h := &APIHandler{
		semantics: &mockSemanticService{
			updateRelationshipForModelFn: func(_ context.Context, semanticModelID, relationshipName string, req domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
				assert.Equal(t, "sm-sales", semanticModelID)
				assert.Equal(t, "sales_to_customers", relationshipName)
				require.NotNil(t, req.RelationshipType)
				assert.Equal(t, domain.RelationshipTypeOneToMany, *req.RelationshipType)
				require.NotNil(t, req.JoinSQL)
				assert.Equal(t, joinSQL, *req.JoinSQL)
				require.NotNil(t, req.Cost)
				assert.Equal(t, 7, *req.Cost)
				require.NotNil(t, req.MaxHops)
				assert.Equal(t, 4, *req.MaxHops)
				return &domain.SemanticRelationship{
					ID:               "rel-1",
					Name:             relationshipName,
					FromSemanticID:   "sm-customers",
					ToSemanticID:     "sm-sales",
					RelationshipType: *req.RelationshipType,
					JoinSQL:          *req.JoinSQL,
					Cost:             *req.Cost,
					MaxHops:          *req.MaxHops,
				}, nil
			},
		},
	}

	resp, err := h.UpdateSemanticModelRelationship(context.Background(), GenUpdateSemanticModelRelationshipRequest{
		SemanticModelId:   "sm-sales",
		RelationshipName:  "sales_to_customers",
		Body: &GenUpdateSemanticModelRelationshipJSONBody{
			RelationshipType: &relationshipType,
			JoinSql:          &joinSQL,
			Cost:             &cost,
			MaxHops:          &maxHops,
		},
	})
	require.NoError(t, err)

	updated, ok := resp.(GenUpdateSemanticModelRelationship200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, updated.Body.RelationshipType)
	assert.Equal(t, SemanticRelationshipRelationshipTypeONETOMANY, *updated.Body.RelationshipType)
}

func TestHandler_DeleteSemanticModelRelationship_MapsNestedRequest(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		semantics: &mockSemanticService{
			deleteRelationshipForModelFn: func(_ context.Context, semanticModelID, relationshipName string) error {
				assert.Equal(t, "sm-sales", semanticModelID)
				assert.Equal(t, "sales_to_customers", relationshipName)
				return nil
			},
		},
	}

	resp, err := h.DeleteSemanticModelRelationship(context.Background(), GenDeleteSemanticModelRelationshipRequest{
		SemanticModelId:   "sm-sales",
		RelationshipName:  "sales_to_customers",
	})
	require.NoError(t, err)
	_, ok := resp.(GenDeleteSemanticModelRelationship204Response)
	require.True(t, ok, "expected 204 response, got %T", resp)
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
				assert.Equal(t, "sm-sales", req.SemanticModelID)
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

	resp, err := h.ExplainMetricQuery(context.Background(), GenExplainMetricQueryRequest{SemanticModelId: "sm-sales", Body: &GenExplainMetricQueryJSONBody{
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
				assert.Equal(t, "sm-sales", req.SemanticModelID)
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

	resp, err := h.RunMetricQuery(ctx, GenRunMetricQueryRequest{SemanticModelId: "sm-sales", Body: &GenRunMetricQueryJSONBody{
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
	assert.Equal(t, []TabularColumn{{Name: "order_date"}, {Name: "total_revenue"}}, okResp.Body.Result.Columns)
}

func TestHandler_CheckMetricFreshness_ResolvesMetricAndReturnsFreshness(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		semantics: &mockSemanticService{
			listSemanticModelsFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SemanticModel, int64, error) {
				return []domain.SemanticModel{{ID: "sm-sales", Name: "sales", BaseModelRef: "analytics.fct_sales"}}, 1, nil
			},
			listMetricsFn: func(_ context.Context, semanticModelID string) ([]domain.SemanticMetric, error) {
				assert.Equal(t, "sm-sales", semanticModelID)
				return []domain.SemanticMetric{{Name: "total_revenue"}}, nil
			},
			explainMetricQueryFn: func(_ context.Context, req semantic.MetricQueryRequest) (*semantic.MetricQueryPlan, error) {
				assert.Equal(t, "sm-sales", req.SemanticModelID)
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
	require.NotNil(t, okResp.Body.SemanticModelId)
	assert.Equal(t, "sm-sales", *okResp.Body.SemanticModelId)
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
			listSemanticModelsFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SemanticModel, int64, error) {
				return []domain.SemanticModel{{ID: "sm-sales", Name: "sales"}, {ID: "sm-marketing", Name: "marketing"}}, 2, nil
			},
			listMetricsFn: func(_ context.Context, semanticModelID string) ([]domain.SemanticMetric, error) {
				if semanticModelID == "sm-sales" || semanticModelID == "sm-marketing" {
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
