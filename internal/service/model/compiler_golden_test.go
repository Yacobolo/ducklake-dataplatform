package model

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompileModelSQL_GoldenArtifact(t *testing.T) {
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
			"stg_orders": {{ProjectName: "analytics", Name: "stg_orders"}},
		},
		macros: map[string]compileMacroDefinition{
			"utils.cents_to_dollars": {
				name:       "utils.cents_to_dollars",
				parameters: []string{"col"},
				body:       `"(" + col + " / 100.0)"`,
				starlark:   true,
				runtimeKey: "db",
			},
		},
	}
	runtime, err := newStarlarkMacroRuntime(ctx.macros)
	require.NoError(t, err)
	ctx.macroRuntimes = map[string]*starlarkMacroRuntime{"db": runtime}

	compiled, err := compileModelSQL(`
select {{ utils.cents_to_dollars('amount') }} as amount_usd
from {{ ref('stg_orders') }}
where days_between(current_date, order_date) <= {{ var('window_days') }}
`, ctx)
	require.NoError(t, err)

	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	err = enc.Encode(map[string]any{
		"sql":           compiled.sql,
		"depends_on":    compiled.dependsOn,
		"vars_used":     compiled.varsUsed,
		"macros_used":   compiled.macrosUsed,
		"compiled_hash": compiled.compiledHash,
	})
	require.NoError(t, err)

	expected, err := os.ReadFile(filepath.Join("testdata", "compile_model_artifact.golden.json"))
	require.NoError(t, err)
	assert.Equal(t, string(expected), b.String())
}

func TestBuildCompileManifest_Golden(t *testing.T) {
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

	manifest, err := buildCompileManifest(selected, artifacts)
	require.NoError(t, err)

	expected, err := os.ReadFile(filepath.Join("testdata", "compile_manifest.golden.json"))
	require.NoError(t, err)
	assert.Equal(t, string(expected), manifest+"\n")
}
