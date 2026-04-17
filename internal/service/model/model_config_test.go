package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestResolveEffectiveModel_AppliesInlineConfig(t *testing.T) {
	model, warnings, err := resolveEffectiveModel(domain.Model{
		ProjectName:     "analytics",
		Name:            "fct_orders",
		Materialization: domain.MaterializationView,
		Tags:            []string{"published"},
		SQL: `
{{ config(materialized='incremental', schema='marts', tags=['finance', 'published'], unique_key='id', incremental_strategy='merge', on_schema_change='fail') }}
select * from upstream
`,
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Equal(t, domain.MaterializationIncremental, model.Materialization)
	assert.Equal(t, "incremental", model.Config.Materialized)
	assert.Equal(t, "marts", model.Config.Schema)
	assert.Equal(t, []string{"finance", "published"}, model.Tags)
	assert.Equal(t, []string{"finance", "published"}, model.Config.Tags)
	assert.Equal(t, []string{"id"}, model.Config.UniqueKey)
	assert.Equal(t, "merge", model.Config.IncrementalStrategy)
	assert.Equal(t, "fail", model.Config.OnSchemaChange)
	assert.Equal(t, "select * from upstream", model.SQL)
}

func TestResolveEffectiveModel_DisabledConfigDropsModel(t *testing.T) {
	model, warnings, err := resolveEffectiveModel(domain.Model{
		ProjectName:     "analytics",
		Name:            "scratch",
		Materialization: domain.MaterializationView,
		SQL:             `{{ config(enabled=false) }} select 1`,
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.NotNil(t, model.Config.Enabled)
	assert.False(t, *model.Config.Enabled)
	assert.False(t, modelEnabled(model))
}
