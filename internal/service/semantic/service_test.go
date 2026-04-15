//go:build integration

package semantic

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/db/repository"
	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/query"
)

type fakeQueryExecutor struct {
	lastSQL string
}

func (f *fakeQueryExecutor) Execute(_ context.Context, _ string, sqlQuery string) (*query.QueryResult, error) {
	f.lastSQL = sqlQuery
	return &query.QueryResult{Columns: []string{"ok"}, Rows: [][]interface{}{{"ok"}}, RowCount: 1}, nil
}

type fakeDDLExecutor struct {
	lastSQL string
}

func (f *fakeDDLExecutor) ExecContext(_ context.Context, query string) error {
	f.lastSQL = query
	return nil
}

func setupSemanticServiceDeps(t *testing.T) (*Service, *repository.ModelRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	modelRepo := repository.NewModelRepo(writeDB)

	return NewService(
		repository.NewSemanticModelRepo(writeDB),
		repository.NewSemanticMetricRepo(writeDB),
		repository.NewSemanticRelationshipRepo(writeDB),
		repository.NewSemanticPreAggregationRepo(writeDB),
		modelRepo,
	), modelRepo
}

func setupSemanticService(t *testing.T) *Service {
	t.Helper()
	svc, _ := setupSemanticServiceDeps(t)
	return svc
}

const testWorkspaceID = "analytics"

func mustSemanticModelID(t *testing.T, svc *Service, ctx context.Context, name string) string {
	t.Helper()
	model, err := svc.GetSemanticModelByName(ctx, testWorkspaceID, name)
	require.NoError(t, err)
	return model.ID
}

func TestService_CreateAndListSemanticModelResources(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	created, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:          testWorkspaceID,
		Name:                 "sales",
		Description:          "Sales semantics",
		BaseRelationRef:      "analytics.fct_sales",
		DefaultTimeDimension: "order_date",
		Tags:                 []string{"core"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)

	metrics, err := svc.ListMetrics(ctx, testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"))
	require.NoError(t, err)
	require.Empty(t, metrics)

	metric, err := svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
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

	metrics, err = svc.ListMetrics(ctx, testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"))
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	assert.Equal(t, "total_revenue", metrics[0].Name)

	preAgg, err := svc.CreatePreAggregation(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticPreAggregationRequest{
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

	preAggs, err := svc.ListPreAggregations(ctx, testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"))
	require.NoError(t, err)
	require.Len(t, preAggs, 1)
	assert.Equal(t, "daily_summary", preAggs[0].Name)
}

func TestService_ExplainAndRunMetricQuery(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.fct_sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "customers",
		BaseRelationRef: "analytics.dim_customers",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(sales.amount)",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, testWorkspaceID, domain.PageRequest{MaxResults: 100})
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
		WorkspaceID:       testWorkspaceID,
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
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Dimensions:        []string{"customers.region"},
	})
	require.NoError(t, err)
	assert.NotNil(t, run.Result)
	assert.Equal(t, plan.GeneratedSQL, fake.lastSQL)
}

func TestService_MaterializePreAggregation_RebuildsTargetWithoutSelfReference(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:          testWorkspaceID,
		Name:                 "sales",
		BaseRelationRef:      "analytics.fct_sales",
		DefaultTimeDimension: "sales.order_date",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(sales.amount)",
	})
	require.NoError(t, err)

	preAgg, err := svc.CreatePreAggregation(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticPreAggregationRequest{
		SemanticModelID: "placeholder",
		Name:            "daily_summary",
		MetricSet:       []string{"total_revenue"},
		DimensionSet:    []string{"sales.region"},
		Grain:           "day",
		TargetRelation:  "analytics.daily_sales_summary",
	})
	require.NoError(t, err)

	ddl := &fakeDDLExecutor{}
	svc.SetDDLExecutor(ddl)
	materialized, metadata, err := svc.MaterializePreAggregation(ctx, "alice", preAgg.ID)
	require.NoError(t, err)
	require.NotNil(t, materialized)
	require.NotNil(t, metadata)
	assert.Equal(t, preAgg.ID, materialized.ID)
	generatedSQL, _ := metadata["generated_sql"].(string)
	assert.Contains(t, generatedSQL, "FROM analytics.fct_sales AS sales")
	assert.NotContains(t, generatedSQL, "FROM analytics.daily_sales_summary")
	assert.Contains(t, ddl.lastSQL, `CREATE OR REPLACE TABLE "analytics"."daily_sales_summary" AS`)
	assert.Contains(t, ddl.lastSQL, generatedSQL)
}

func TestService_ExplainMetricQuery_AmbiguousJoinPath(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	for _, name := range []string{"sales", "path_a", "path_b", "regions"} {
		_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
			WorkspaceID:     testWorkspaceID,
			Name:            name,
			BaseRelationRef: "analytics." + name,
		})
		require.NoError(t, err)
	}

	_, err := svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(sales.amount)",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, testWorkspaceID, domain.PageRequest{MaxResults: 100})
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
		WorkspaceID:       testWorkspaceID,
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
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.fct_sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(amount)",
	})
	require.NoError(t, err)

	_, err = svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Filters:           []string{"1=1; DROP TABLE sales"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not contain semicolons")

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "external_read",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "read_parquet('s3://bucket/data.parquet')",
	})
	require.NoError(t, err)

	_, err = svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"external_read"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "forbidden token")
}

func TestService_ExplainMetricQuery_FilterSQLApplied(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.fct_sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "high_value_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(sales.amount)",
		FilterSQL:       "sales.amount > 100",
	})
	require.NoError(t, err)

	plan, err := svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"high_value_revenue"},
	})
	require.NoError(t, err)
	assert.Contains(t, plan.GeneratedSQL, "SUM(CASE WHEN sales.amount > 100 THEN sales.amount END)")
}

func TestService_RunMetricQuery_ReturnsPreAggregationReadinessError(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.fct_sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(revenue)",
	})
	require.NoError(t, err)

	_, err = svc.CreatePreAggregation(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticPreAggregationRequest{
		SemanticModelID: "ignored-by-service",
		Name:            "revenue_daily",
		MetricSet:       []string{"total_revenue"},
		TargetRelation:  "analytics.revenue_daily",
	})
	require.NoError(t, err)

	svc.SetQueryExecutor(&failingQueryExecutor{
		err: fmt.Errorf("catalog lookup for \"analytics.revenue_daily\": table \"revenue_daily\" not found in schema \"analytics\""),
	})

	_, err = svc.RunMetricQuery(ctx, "alice", MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
	})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
	assert.Contains(t, err.Error(), "pre-aggregation")
	assert.Contains(t, err.Error(), "not materialized yet")
}

func TestService_RunMetricQuery_ReturnsTransformationModelReadinessError(t *testing.T) {
	svc := setupSemanticService(t)
	ctx := context.Background()

	_, err := svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "customer_revenue",
		BaseRelationRef: "fct_customer_revenue",
	})
	require.NoError(t, err)

	modelRepo := svc.modelRepo
	_, err = modelRepo.Create(ctx, &domain.Model{
		WorkspaceID:     testWorkspaceID,
		Name:            "fct_customer_revenue",
		SQL:             "select 1 as revenue",
		Materialization: domain.MaterializationView,
		CreatedBy:       "admin",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "customer_revenue"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(revenue)",
	})
	require.NoError(t, err)

	svc.SetQueryExecutor(&failingQueryExecutor{
		err: fmt.Errorf("catalog lookup for \"fct_customer_revenue\": lookup table \"fct_customer_revenue\": no such table: ducklake_table"),
	})

	_, err = svc.RunMetricQuery(ctx, "alice", MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "customer_revenue",
		Metrics:           []string{"total_revenue"},
	})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
	assert.Contains(t, err.Error(), "transformation model")
	assert.Contains(t, err.Error(), "run the model first")
}

func TestService_ExplainMetricQuery_UnsafeJoinUsesBaseUniqueKey(t *testing.T) {
	svc, modelRepo := setupSemanticServiceDeps(t)
	ctx := context.Background()

	_, err := modelRepo.Create(ctx, &domain.Model{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		SQL:             "select 1 as sale_id, 42 as amount, 7 as customer_id",
		Materialization: domain.MaterializationView,
		Config:          domain.ModelConfig{UniqueKey: []string{"sale_id"}},
		CreatedBy:       "admin",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales_tags",
		BaseRelationRef: "analytics.sales_tags",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "total_revenue",
		MetricType:      domain.MetricTypeSum,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "SUM(sales.amount)",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, testWorkspaceID, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	modelIDs := map[string]string{}
	for _, m := range models {
		modelIDs[m.Name] = m.ID
	}

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "sales_to_tags",
		FromSemanticID:   modelIDs["sales"],
		ToSemanticID:     modelIDs["sales_tags"],
		RelationshipType: domain.RelationshipTypeOneToMany,
		JoinSQL:          "sales.sale_id = sales_tags.sale_id",
	})
	require.NoError(t, err)

	plan, err := svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"total_revenue"},
		Dimensions:        []string{"sales_tags.tag_name"},
	})
	require.NoError(t, err)
	assert.Contains(t, plan.GeneratedSQL, `WITH __metric_base AS`)
	assert.Contains(t, plan.GeneratedSQL, `GROUP BY sales.sale_id`)
	assert.Contains(t, plan.GeneratedSQL, `LEFT JOIN analytics.sales_tags AS sales_tags ON __metric_base.sale_id = sales_tags.sale_id`)
	assert.Contains(t, plan.GeneratedSQL, `SUM("__metric_total_revenue") AS "total_revenue"`)
}

func TestService_ExplainMetricQuery_UnsafeJoinSupportsCountDistinct(t *testing.T) {
	svc, modelRepo := setupSemanticServiceDeps(t)
	ctx := context.Background()

	_, err := modelRepo.Create(ctx, &domain.Model{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		SQL:             "select 1 as sale_id, 42 as amount, 7 as customer_id",
		Materialization: domain.MaterializationView,
		Config:          domain.ModelConfig{UniqueKey: []string{"sale_id"}},
		CreatedBy:       "admin",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales_tags",
		BaseRelationRef: "analytics.sales_tags",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "distinct_customers",
		MetricType:      domain.MetricTypeCountDistinct,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "COUNT(DISTINCT sales.customer_id)",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, testWorkspaceID, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	modelIDs := map[string]string{}
	for _, m := range models {
		modelIDs[m.Name] = m.ID
	}

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "sales_to_tags",
		FromSemanticID:   modelIDs["sales"],
		ToSemanticID:     modelIDs["sales_tags"],
		RelationshipType: domain.RelationshipTypeOneToMany,
		JoinSQL:          "sales.sale_id = sales_tags.sale_id",
	})
	require.NoError(t, err)

	plan, err := svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"distinct_customers"},
		Dimensions:        []string{"sales_tags.tag_name"},
	})
	require.NoError(t, err)
	assert.Contains(t, plan.GeneratedSQL, `ANY_VALUE(sales.customer_id) AS "__metric_distinct_customers_distinct"`)
	assert.Contains(t, plan.GeneratedSQL, `COUNT(DISTINCT "__metric_distinct_customers_distinct") AS "distinct_customers"`)
}

func TestService_ExplainMetricQuery_UnsafeJoinSupportsRatio(t *testing.T) {
	svc, modelRepo := setupSemanticServiceDeps(t)
	ctx := context.Background()

	_, err := modelRepo.Create(ctx, &domain.Model{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		SQL:             "select 1 as sale_id, 42 as amount, 7 as customer_id",
		Materialization: domain.MaterializationView,
		Config:          domain.ModelConfig{UniqueKey: []string{"sale_id"}},
		CreatedBy:       "admin",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales_tags",
		BaseRelationRef: "analytics.sales_tags",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "revenue_per_customer",
		MetricType:      domain.MetricTypeRatio,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "(SUM(sales.amount)) / (COUNT(DISTINCT sales.customer_id))",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, testWorkspaceID, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	modelIDs := map[string]string{}
	for _, m := range models {
		modelIDs[m.Name] = m.ID
	}

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "sales_to_tags",
		FromSemanticID:   modelIDs["sales"],
		ToSemanticID:     modelIDs["sales_tags"],
		RelationshipType: domain.RelationshipTypeOneToMany,
		JoinSQL:          "sales.sale_id = sales_tags.sale_id",
	})
	require.NoError(t, err)

	plan, err := svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"revenue_per_customer"},
		Dimensions:        []string{"sales_tags.tag_name"},
	})
	require.NoError(t, err)
	assert.Contains(t, plan.GeneratedSQL, `SUM("__metric_revenue_per_customer_lhs")`)
	assert.Contains(t, plan.GeneratedSQL, `COUNT(DISTINCT "__metric_revenue_per_customer_rhs_distinct")`)
	assert.Contains(t, plan.GeneratedSQL, `AS "revenue_per_customer"`)
}

func TestService_ExplainMetricQuery_UnsafeJoinSupportsDerivedArithmetic(t *testing.T) {
	svc, modelRepo := setupSemanticServiceDeps(t)
	ctx := context.Background()

	_, err := modelRepo.Create(ctx, &domain.Model{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		SQL:             "select 1 as sale_id, 42 as amount, 7 as customer_id, 2 as discount_amount",
		Materialization: domain.MaterializationView,
		Config:          domain.ModelConfig{UniqueKey: []string{"sale_id"}},
		CreatedBy:       "admin",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales_tags",
		BaseRelationRef: "analytics.sales_tags",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "net_revenue_per_customer",
		MetricType:      domain.MetricTypeRatio,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "((SUM(sales.amount) - SUM(sales.discount_amount)) / COUNT(DISTINCT sales.customer_id))",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, testWorkspaceID, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	modelIDs := map[string]string{}
	for _, m := range models {
		modelIDs[m.Name] = m.ID
	}

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "sales_to_tags",
		FromSemanticID:   modelIDs["sales"],
		ToSemanticID:     modelIDs["sales_tags"],
		RelationshipType: domain.RelationshipTypeOneToMany,
		JoinSQL:          "sales.sale_id = sales_tags.sale_id",
	})
	require.NoError(t, err)

	plan, err := svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"net_revenue_per_customer"},
		Dimensions:        []string{"sales_tags.tag_name"},
	})
	require.NoError(t, err)
	assert.Contains(t, plan.GeneratedSQL, `SUM("__metric_net_revenue_per_customer_lhs_lhs")`)
	assert.Contains(t, plan.GeneratedSQL, `SUM("__metric_net_revenue_per_customer_lhs_rhs")`)
	assert.Contains(t, plan.GeneratedSQL, `COUNT(DISTINCT "__metric_net_revenue_per_customer_rhs_distinct")`)
	assert.Contains(t, plan.GeneratedSQL, `AS "net_revenue_per_customer"`)
}

func TestService_ExplainMetricQuery_UnsafeJoinSupportsScalarWrappers(t *testing.T) {
	svc, modelRepo := setupSemanticServiceDeps(t)
	ctx := context.Background()

	_, err := modelRepo.Create(ctx, &domain.Model{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		SQL:             "select 1 as sale_id, 42 as amount, 7 as customer_id, 2 as discount_amount",
		Materialization: domain.MaterializationView,
		Config:          domain.ModelConfig{UniqueKey: []string{"sale_id"}},
		CreatedBy:       "admin",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales",
		BaseRelationRef: "analytics.sales",
	})
	require.NoError(t, err)

	_, err = svc.CreateSemanticModel(ctx, "admin", domain.CreateSemanticModelRequest{
		WorkspaceID:     testWorkspaceID,
		Name:            "sales_tags",
		BaseRelationRef: "analytics.sales_tags",
	})
	require.NoError(t, err)

	_, err = svc.CreateMetric(ctx, "admin", testWorkspaceID, mustSemanticModelID(t, svc, ctx, "sales"), domain.CreateSemanticMetricRequest{
		SemanticModelID: "placeholder",
		Name:            "rounded_revenue_per_customer",
		MetricType:      domain.MetricTypeRatio,
		ExpressionMode:  domain.MetricExpressionModeSQL,
		Expression:      "ROUND(COALESCE(SUM(sales.amount), 0) / COUNT(DISTINCT sales.customer_id), 2)",
	})
	require.NoError(t, err)

	models, _, err := svc.ListSemanticModels(ctx, testWorkspaceID, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	modelIDs := map[string]string{}
	for _, m := range models {
		modelIDs[m.Name] = m.ID
	}

	_, err = svc.CreateRelationship(ctx, "admin", domain.CreateSemanticRelationshipRequest{
		Name:             "sales_to_tags",
		FromSemanticID:   modelIDs["sales"],
		ToSemanticID:     modelIDs["sales_tags"],
		RelationshipType: domain.RelationshipTypeOneToMany,
		JoinSQL:          "sales.sale_id = sales_tags.sale_id",
	})
	require.NoError(t, err)

	plan, err := svc.ExplainMetricQuery(ctx, MetricQueryRequest{
		WorkspaceID:       testWorkspaceID,
		SemanticModelName: "sales",
		Metrics:           []string{"rounded_revenue_per_customer"},
		Dimensions:        []string{"sales_tags.tag_name"},
	})
	require.NoError(t, err)
	assert.Contains(t, plan.GeneratedSQL, `ROUND((COALESCE(`)
	assert.Contains(t, plan.GeneratedSQL, `COUNT(DISTINCT "__metric_rounded_revenue_per_customer_arg_0_rhs_distinct")`)
	assert.Contains(t, plan.GeneratedSQL, `, 2) AS "rounded_revenue_per_customer"`)
}

type failingQueryExecutor struct {
	err error
}

func (f *failingQueryExecutor) Execute(context.Context, string, string) (*query.QueryResult, error) {
	return nil, f.err
}
