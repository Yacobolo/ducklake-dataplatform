package domain

import "testing"

import "github.com/stretchr/testify/require"

func TestDashboardWidgetSource_Validate(t *testing.T) {
	t.Run("sql query", func(t *testing.T) {
		err := (DashboardWidgetSource{
			Kind: DashboardWidgetSourceSQLQuery,
			SQLQuery: &DashboardSQLQuerySource{
				SQL: "select 1",
			},
		}).Validate()
		require.NoError(t, err)
	})

	t.Run("semantic query requires metrics", func(t *testing.T) {
		err := (DashboardWidgetSource{
			Kind: DashboardWidgetSourceSemanticQuery,
			SemanticQuery: &DashboardSemanticQuerySource{
				ProjectName:       "analytics",
				SemanticModelName: "sales",
			},
		}).Validate()
		require.Error(t, err)
	})
}
