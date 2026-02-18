package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupSemanticRepos(t *testing.T) (*SemanticModelRepo, *SemanticMetricRepo, *SemanticRelationshipRepo, *SemanticPreAggregationRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewSemanticModelRepo(writeDB), NewSemanticMetricRepo(writeDB), NewSemanticRelationshipRepo(writeDB), NewSemanticPreAggregationRepo(writeDB)
}

func TestSemanticRepos_EndToEndCRUD(t *testing.T) {
	modelRepo, metricRepo, relRepo, preAggRepo := setupSemanticRepos(t)
	ctx := context.Background()

	createdModel, err := modelRepo.Create(ctx, &domain.SemanticModel{
		ProjectName:          "analytics",
		Name:                 "sales",
		Description:          "Semantic model for sales analytics",
		Owner:                "data-team",
		BaseModelRef:         "analytics.fct_sales",
		DefaultTimeDimension: "order_date",
		Tags:                 []string{"finance", "core"},
		CreatedBy:            "admin",
	})
	require.NoError(t, err)
	require.NotEmpty(t, createdModel.ID)

	t.Run("semantic model update", func(t *testing.T) {
		desc := "Updated sales semantic model"
		updated, err := modelRepo.Update(ctx, createdModel.ID, domain.UpdateSemanticModelRequest{Description: &desc})
		require.NoError(t, err)
		assert.Equal(t, desc, updated.Description)
		assert.Equal(t, createdModel.BaseModelRef, updated.BaseModelRef)
	})

	createdMetric, err := metricRepo.Create(ctx, &domain.SemanticMetric{
		SemanticModelID:    createdModel.ID,
		Name:               "total_revenue",
		Description:        "Total revenue",
		MetricType:         domain.MetricTypeSum,
		ExpressionMode:     domain.MetricExpressionModeDSL,
		Expression:         "sum(amount)",
		DefaultTimeGrain:   "day",
		Format:             "currency",
		Owner:              "finance",
		CertificationState: domain.CertificationDraft,
		CreatedBy:          "admin",
	})
	require.NoError(t, err)
	require.NotEmpty(t, createdMetric.ID)

	t.Run("semantic metric list and update", func(t *testing.T) {
		metrics, err := metricRepo.ListByModel(ctx, createdModel.ID)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		assert.Equal(t, "total_revenue", metrics[0].Name)

		state := domain.CertificationCertified
		updated, err := metricRepo.Update(ctx, createdMetric.ID, domain.UpdateSemanticMetricRequest{CertificationState: &state})
		require.NoError(t, err)
		assert.Equal(t, domain.CertificationCertified, updated.CertificationState)
	})

	createdRel, err := relRepo.Create(ctx, &domain.SemanticRelationship{
		Name:             "sales_to_customers",
		FromSemanticID:   createdModel.ID,
		ToSemanticID:     createdModel.ID,
		RelationshipType: domain.RelationshipTypeManyToOne,
		JoinSQL:          "sales.customer_id = customers.customer_id",
		IsDefault:        true,
		Cost:             1,
		MaxHops:          2,
		CreatedBy:        "admin",
	})
	require.NoError(t, err)
	require.NotEmpty(t, createdRel.ID)

	t.Run("semantic relationship update", func(t *testing.T) {
		cost := 3
		updated, err := relRepo.Update(ctx, createdRel.ID, domain.UpdateSemanticRelationshipRequest{Cost: &cost})
		require.NoError(t, err)
		assert.Equal(t, 3, updated.Cost)
	})

	createdPreAgg, err := preAggRepo.Create(ctx, &domain.SemanticPreAggregation{
		SemanticModelID: createdModel.ID,
		Name:            "daily_sales_rollup",
		MetricSet:       []string{"total_revenue"},
		DimensionSet:    []string{"order_date", "region"},
		Grain:           "day",
		TargetRelation:  "analytics.daily_sales_summary",
		RefreshPolicy:   "hourly",
		CreatedBy:       "admin",
	})
	require.NoError(t, err)
	require.NotEmpty(t, createdPreAgg.ID)

	t.Run("semantic pre-aggregation list", func(t *testing.T) {
		preAggs, err := preAggRepo.ListByModel(ctx, createdModel.ID)
		require.NoError(t, err)
		require.Len(t, preAggs, 1)
		assert.Equal(t, "daily_sales_rollup", preAggs[0].Name)
		assert.Equal(t, []string{"total_revenue"}, preAggs[0].MetricSet)
	})
}
