package model

import (
	"strings"
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildCompileManifest_DeterministicOrdering(t *testing.T) {
	selected := []domain.Model{
		{ID: "2", ProjectName: "analytics", Name: "fct_orders"},
		{ID: "1", ProjectName: "analytics", Name: "stg_orders"},
	}
	artifacts := map[string]compileResult{
		"1": {
			compiledHash: "sha256:a",
			dependsOn:    []string{"source:raw.orders"},
			varsUsed:     []string{"window_days"},
			macrosUsed:   []string{"utils.cast_money"},
		},
		"2": {
			compiledHash: "sha256:b",
			dependsOn:    []string{"analytics.stg_orders"},
		},
	}

	one, err := buildCompileManifest(selected, artifacts)
	require.NoError(t, err)
	two, err := buildCompileManifest(selected, artifacts)
	require.NoError(t, err)

	assert.Equal(t, one, two)
	assert.Contains(t, one, `"model_name":"analytics.fct_orders"`)
	assert.Contains(t, one, `"model_name":"analytics.stg_orders"`)
	assert.Less(t, stringsIndex(t, one, `"analytics.fct_orders"`), stringsIndex(t, one, `"analytics.stg_orders"`))
}

func TestBuildCompileDiagnostics_DedupesAndSorts(t *testing.T) {
	raw, err := buildCompileDiagnostics(
		[]string{"z warning", "a warning", "z warning"},
		[]string{"hard error", "hard error"},
	)
	require.NoError(t, err)

	d := diagnosticsFromJSONOrNil(raw)
	require.NotNil(t, d)
	assert.Equal(t, []string{"a warning", "z warning"}, d.Warnings)
	assert.Equal(t, []string{"hard error"}, d.Errors)
}

func stringsIndex(t *testing.T, haystack, needle string) int {
	t.Helper()
	i := strings.Index(haystack, needle)
	require.NotEqual(t, -1, i)
	return i
}
