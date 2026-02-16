package model

import (
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractDependencies(t *testing.T) {
	allModels := []domain.Model{
		{ProjectName: "sales", Name: "stg_orders"},
		{ProjectName: "sales", Name: "stg_customers"},
		{ProjectName: "warehouse", Name: "raw_events"},
		{ProjectName: "warehouse", Name: "dim_products"},
	}

	tests := []struct {
		name     string
		sql      string
		project  string
		wantDeps []string
		wantErr  bool
	}{
		{
			name:     "no model deps",
			sql:      "SELECT * FROM raw_data.orders",
			project:  "sales",
			wantDeps: []string{},
		},
		{
			name:     "same-project dep",
			sql:      "SELECT * FROM stg_customers",
			project:  "sales",
			wantDeps: []string{"sales.stg_customers"},
		},
		{
			name:     "cross-project dep",
			sql:      "SELECT * FROM warehouse.raw_events",
			project:  "sales",
			wantDeps: []string{"warehouse.raw_events"},
		},
		{
			name:     "multiple deps",
			sql:      "SELECT o.*, c.name FROM stg_orders o JOIN stg_customers c ON o.cid = c.id",
			project:  "sales",
			wantDeps: []string{"sales.stg_customers", "sales.stg_orders"},
		},
		{
			name:     "no deps for physical table",
			sql:      "SELECT * FROM some_physical_table",
			project:  "sales",
			wantDeps: []string{},
		},
		{
			name:     "CTE does not appear as dep",
			sql:      "WITH cte AS (SELECT * FROM stg_orders) SELECT * FROM cte",
			project:  "sales",
			wantDeps: []string{"sales.stg_orders"},
		},
		{
			name:    "invalid SQL returns error",
			sql:     "NOT VALID SQL !!@#$",
			project: "sales",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps, err := ExtractDependencies(tt.sql, tt.project, allModels)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if len(tt.wantDeps) == 0 {
				assert.Empty(t, deps)
			} else {
				assert.Equal(t, tt.wantDeps, deps)
			}
		})
	}
}
