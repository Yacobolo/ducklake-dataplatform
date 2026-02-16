package model

import (
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompileModelSQL_Primitives(t *testing.T) {
	ctx := compileContext{
		targetCatalog: "memory",
		targetSchema:  "analytics",
		vars: map[string]string{
			"window_days": "7",
		},
		projectName: "analytics",
		modelName:   "fct_orders",
		materialize: domain.MaterializationTable,
		models: map[string]domain.Model{
			"analytics.stg_orders": {ProjectName: "analytics", Name: "stg_orders"},
		},
		byName: map[string][]domain.Model{
			"stg_orders": []domain.Model{{ProjectName: "analytics", Name: "stg_orders"}},
		},
	}

	compiled, err := compileModelSQL(`
select *
from {{ ref('stg_orders') }}
where days_between(current_date, order_date) <= {{ var('window_days') }}
`, ctx)
	require.NoError(t, err)
	assert.Contains(t, compiled.sql, `"memory"."analytics"."stg_orders"`)
	assert.Contains(t, compiled.sql, "<= 7")
	assert.Equal(t, []string{"analytics.stg_orders"}, compiled.dependsOn)
	assert.Equal(t, []string{"window_days"}, compiled.varsUsed)
	assert.NotEmpty(t, compiled.compiledHash)
}

func TestCompileModelSQL_IfIsIncremental(t *testing.T) {
	t.Run("incremental true", func(t *testing.T) {
		ctx := compileContext{
			targetSchema: "analytics",
			projectName:  "analytics",
			modelName:    "fct_orders",
			materialize:  domain.MaterializationIncremental,
			models: map[string]domain.Model{
				"analytics.stg_orders": {ProjectName: "analytics", Name: "stg_orders"},
			},
			byName: map[string][]domain.Model{
				"stg_orders": []domain.Model{{ProjectName: "analytics", Name: "stg_orders"}},
			},
		}

		compiled, err := compileModelSQL(`
{% if is_incremental() %}
select * from {{ ref('stg_orders') }} where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
{% else %}
select * from {{ ref('stg_orders') }}
{% endif %}
`, ctx)
		require.NoError(t, err)
		assert.Contains(t, compiled.sql, `from "analytics"."fct_orders"`)
		assert.Contains(t, compiled.sql, `where updated_at >`)
	})

	t.Run("full refresh false branch", func(t *testing.T) {
		ctx := compileContext{
			targetSchema: "analytics",
			projectName:  "analytics",
			modelName:    "fct_orders",
			materialize:  domain.MaterializationIncremental,
			fullRefresh:  true,
			models: map[string]domain.Model{
				"analytics.stg_orders": {ProjectName: "analytics", Name: "stg_orders"},
			},
			byName: map[string][]domain.Model{
				"stg_orders": []domain.Model{{ProjectName: "analytics", Name: "stg_orders"}},
			},
		}

		compiled, err := compileModelSQL(`{% if is_incremental() %}select 1{% else %}select * from {{ ref('stg_orders') }}{% endif %}`, ctx)
		require.NoError(t, err)
		assert.Equal(t, `select * from "analytics"."stg_orders"`, compiled.sql)
	})
}

func TestCompileModelSQL_ValidationErrors(t *testing.T) {
	ctx := compileContext{
		projectName: "analytics",
		modelName:   "fct_orders",
		materialize: domain.MaterializationTable,
		models:      map[string]domain.Model{},
		byName:      map[string][]domain.Model{},
	}

	t.Run("unknown ref", func(t *testing.T) {
		_, err := compileModelSQL(`select * from {{ ref('missing') }}`, ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown ref")
	})

	t.Run("missing var", func(t *testing.T) {
		_, err := compileModelSQL(`select {{ var('required_val') }}`, ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "required var")
	})
}

func TestCompileModelSQL_CapturesMacros(t *testing.T) {
	ctx := compileContext{
		projectName: "analytics",
		modelName:   "fct_orders",
		materialize: domain.MaterializationTable,
		models:      map[string]domain.Model{},
		byName:      map[string][]domain.Model{},
	}

	compiled, err := compileModelSQL(`select {{ utils.cents_to_dollars('amount') }} as amount_usd`, ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"utils.cents_to_dollars"}, compiled.macrosUsed)
	assert.Contains(t, compiled.sql, `utils.cents_to_dollars('amount')`)
}
