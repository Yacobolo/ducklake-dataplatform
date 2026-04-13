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
				SemanticModelID: "semantic-sales",
			},
		}).Validate()
		require.Error(t, err)
	})
}

func TestDashboardComputePolicy_Validate(t *testing.T) {
	t.Run("defaults to auto", func(t *testing.T) {
		policy := DashboardComputePolicy{}
		require.NoError(t, policy.Validate())
		require.Equal(t, ComputeModeAuto, policy.Normalize().Mode)
	})

	t.Run("shared endpoint requires endpoint name", func(t *testing.T) {
		err := (DashboardComputePolicy{Mode: ComputeModeSharedEndpoint}).Validate()
		require.Error(t, err)
	})

	t.Run("byoc local rejects endpoint name", func(t *testing.T) {
		err := (DashboardComputePolicy{Mode: ComputeModeByocLocal, EndpointName: "analytics-xl"}).Validate()
		require.Error(t, err)
	})

	t.Run("shared endpoint accepts endpoint name", func(t *testing.T) {
		err := (DashboardComputePolicy{Mode: ComputeModeSharedEndpoint, EndpointName: "analytics-xl"}).Validate()
		require.NoError(t, err)
	})
}
