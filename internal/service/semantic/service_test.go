package semantic

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
)

type fakeQueryExecutor struct {
	lastSQL string
}

func (f *fakeQueryExecutor) Execute(_ context.Context, _ string, sqlQuery string) (*query.QueryResult, error) {
	f.lastSQL = sqlQuery
	return &query.QueryResult{Columns: []string{"ok"}, Rows: [][]interface{}{{"ok"}}, RowCount: 1}, nil
}

func setupSemanticService(t *testing.T) *Service {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)

	return NewService(
		repository.NewSemanticModelRepo(writeDB),
		repository.NewSemanticMetricRepo(writeDB),
		repository.NewSemanticRelationshipRepo(writeDB),
		repository.NewSemanticPreAggregationRepo(writeDB),
	)
}

func TestService_CreateAndListSemanticModelResources(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	created, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		ProjectName:          "analytics",
		Name:                 "sales",
		Description:          "Sales semantics",
		BaseModelRef:         "analytics.fct_sales",
		DefaultTimeDimension: "order_date",
		Tags:                 []string{"core"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)

	metrics, err := svc.ListMetrics(ctx, "analytics", "sales")
	require.NoError(t, err)
	require.Empty(t, metrics)

	metric, err := svc.CreateMetric(ctx, "admin", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID:    "ignored-by-service",
		Name:               "total_revenue",
		Description:        "Total revenue",
		MetricType:         domain.MetricTypeSum,
		Expression:         "sum(amount)",
		ExpressionMode:     domain.MetricExpressionModeDSL,
		CertificationState: domain.CertificationDraft,
	})
	require.NoError(t, err)
	require.NotEmpty(t, metric.ID)

	metrics, err = svc.ListMetrics(ctx, "analytics", "sales")
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "total_revenue", metrics[0].Name)

	preAgg, err := svc.CreatePreAggregation(ctx, "admin", "analytics", "sales", domain.CreateSemanticPreAggregationRequest{
		SemanticModelID: "ignored-by-service",
		Name:            "daily_summary",
		MetricSet:       []string{"total_revenue"},
		DimensionSet:    []string{"order_date"},
		Grain:           "day",
		TargetRelation:  "analytics.daily_sales_summary",
		RefreshPolicy:   "hourly",
	})
	require.NoError(t, err)
	require.NotEmpty(t, preAgg.ID)

	preAggs, err := svc.ListPreAggregations(ctx, "analytics", "sales")
	require.NoError(t, err)
	require.Len(t, preAggs, 1)
	assert.Equal(t, "daily_summary", preAggs[0].Name)
}

func TestService_ExplainAndRunMetricQuery(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.fct_sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "customers",
		BaseModelRef: "analytics.dim_customers",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(sales.amount)",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, ptr("analytics"), domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	modelIDs := map[string]string{}
	for _, m := range models {
		modelIDs[m.Name] = m.ID
	}

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "sales_to_customers",
		FromSemanticID:   modelIDs["sales"],
		ToSemanticID:     modelIDs["customers"],
		RelationshipType: domain.RelationshipTypeManyToOne,
		JoinSQL:          "sales.customer_id = customers.customer_id",
		IsDefault:        true,
	})
	require.NoError(t, err)

	plan, err := svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		ProjectName:       "analytics",
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Dimensions:        []string{"customers.region"},
	})
	require.NoError(t, err)
	assert.Contains(t, plan.GeneratedSQL, "LEFT JOIN analytics.dim_customers AS customers")
	assert.Len(t, plan.JoinPath, 1)

	fake := &fakeQueryExecutor{}
	svc.SetQueryExecutor(fake)
	run, err := svc.RunMetricQuery(ctx, "alice", MetricQueryRequest{
		ProjectName:       "analytics",
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Dimensions:        []string{"customers.region"},
	})
	require.NoError(t, err)
	assert.NotNil(t, run.Result)
	assert.Equal(t, plan.GeneratedSQL, fake.lastSQL)
}

func TestService_ExplainMetricQuery_AmbiguousJoinPath(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	for _, name := range []string{"sales", "path_a", "path_b", "regions"} {
		_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
			ProjectName:  "analytics",
			Name:         name,
			BaseModelRef: "analytics." + name,
		})
		require.NoError(t, err)
	}

	_, err := svc.CreateMetric(ctx, "admin", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(sales.amount)",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, ptr("analytics"), domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	ids := map[string]string{}
	for _, m := range models {
		ids[m.Name] = m.ID
	}

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "sales_to_path_a",
		FromSemanticID:   ids["sales"],
		ToSemanticID:     ids["path_a"],
		RelationshipType: domain.RelationshipTypeManyToOne,
		JoinSQL:          "sales.customer_id = path_a.customer_id",
	})
	require.NoError(t, err)

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "path_a_to_regions",
		FromSemanticID:   ids["path_a"],
		ToSemanticID:     ids["regions"],
		RelationshipType: domain.RelationshipTypeManyToOne,
		JoinSQL:          "path_a.region_id = regions.id",
	})
	require.NoError(t, err)

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "sales_to_path_b",
		FromSemanticID:   ids["sales"],
		ToSemanticID:     ids["path_b"],
		RelationshipType: domain.RelationshipTypeManyToOne,
		JoinSQL:          "sales.customer_id = path_b.customer_id",
	})
	require.NoError(t, err)

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "path_b_to_regions",
		FromSemanticID:   ids["path_b"],
		ToSemanticID:     ids["regions"],
		RelationshipType: domain.RelationshipTypeManyToOne,
		JoinSQL:          "path_b.region_id = regions.id",
	})
	require.NoError(t, err)

	_, err = svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		ProjectName:       "analytics",
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Dimensions:        []string{"regions.name"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous join path")
}

func TestService_ExplainMetricQuery_RejectsDangerousSQLFragments(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		ProjectName:  "analytics",
		Name:         "sales",
		BaseModelRef: "analytics.fct_sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(amount)",
	})
	require.NoError(t, err)

	_, err = svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		ProjectName:       "analytics",
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Filters:           []string{"1=1; DROP TABLE sales"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not contain semicolons")

	_, err = svc.CreateMetric(ctx, "admin", "analytics", "sales", domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "external_read",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "read_parquet('s3://bucket/data.parquet')",
	})
	require.NoError(t, err)

	_, err = svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		ProjectName:       "analytics",
		SemanticModelName: "sales",
		Metrics:           []string{"external_read"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "forbidden token")
}

func ptr(s string) *string {
	return &s
}
