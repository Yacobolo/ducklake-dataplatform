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

func TestModelHashByNameFromManifest(t *testing.T) {
	hashes, err := modelHashByNameFromManifest(`{"version":1,"models":[{"model_name":"analytics.stg_orders","compiled_hash":"sha256:aaa"},{"model_name":"analytics.fct_orders","compiled_hash":"sha256:bbb"}]}`)
	require.NoError(t, err)
	assert.Equal(t, "sha256:aaa", hashes["analytics.stg_orders"])
	assert.Equal(t, "sha256:bbb", hashes["analytics.fct_orders"])
}

func TestSelectStateModifiedModels(t *testing.T) {
	allModels := []domain.Model{
		{ID: "1", ProjectName: "analytics", Name: "stg_orders"},
		{ID: "2", ProjectName: "analytics", Name: "fct_orders"},
	}
	artifacts := map[string]compileResult{
		"1": {compiledHash: "sha256:old"},
		"2": {compiledHash: "sha256:new"},
	}
	baseline := map[string]string{
		"analytics.stg_orders": "sha256:old",
		"analytics.fct_orders": "sha256:old",
	}

	selected := selectStateModifiedModels(allModels, artifacts, baseline)
	require.Len(t, selected, 1)
	assert.Equal(t, "analytics.fct_orders", selected[0].QualifiedName())
}
