package domain

import "testing"

import "github.com/stretchr/testify/require"

func TestVisualSpec_Validate(t *testing.T) {
	t.Run("table", func(t *testing.T) {
		require.NoError(t, (&VisualSpec{Kind: VisualOutputTable}).Validate())
	})

	t.Run("metric requires value", func(t *testing.T) {
		err := (&VisualSpec{Kind: VisualOutputMetric}).Validate()
		require.Error(t, err)
	})

	t.Run("chart requires encodings", func(t *testing.T) {
		chartType := VisualChartBar
		err := (&VisualSpec{Kind: VisualOutputChart, ChartType: &chartType}).Validate()
		require.Error(t, err)
	})

	t.Run("pie accepts label value", func(t *testing.T) {
		chartType := VisualChartPie
		spec := &VisualSpec{
			Kind:      VisualOutputChart,
			ChartType: &chartType,
			Encodings: VisualEncodings{
				Label: &VisualFieldBinding{Field: "region"},
				Value: &VisualFieldBinding{Field: "revenue"},
			},
		}
		require.NoError(t, spec.Validate())
		require.NoError(t, spec.ValidateColumns([]string{"region", "revenue"}))
	})
}
