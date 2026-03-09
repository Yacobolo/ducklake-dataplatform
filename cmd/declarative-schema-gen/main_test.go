package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/declarative"
)

func TestApplyKindConstraints_GrantListAddsEnums(t *testing.T) {
	defs := map[string]map[string]interface{}{
		"GrantSpec": {
			"properties": map[string]interface{}{
				"principal_type": map[string]interface{}{"type": "string"},
				"securable_type": map[string]interface{}{"type": "string"},
				"privilege":      map[string]interface{}{"type": "string"},
			},
		},
	}

	applyKindConstraints(declarative.KindNameGrantList, defs)

	principalType := getDefProperty(defs, "GrantSpec", "principal_type")
	require.NotNil(t, principalType)
	assert.Contains(t, principalType["enum"], "user")
	assert.Contains(t, principalType["enum"], "group")

	securableType := getDefProperty(defs, "GrantSpec", "securable_type")
	require.NotNil(t, securableType)
	assert.Contains(t, securableType["enum"], "schema")
	assert.Contains(t, securableType["enum"], "table")

	privilege := getDefProperty(defs, "GrantSpec", "privilege")
	require.NotNil(t, privilege)
	assert.Contains(t, privilege["enum"], "SELECT")
	assert.Contains(t, privilege["enum"], "ALL_PRIVILEGES")
}

func TestApplyKindConstraints_TableAddsExternalConditional(t *testing.T) {
	defs := map[string]map[string]interface{}{
		"TableSpec": {
			"properties": map[string]interface{}{
				"table_type":  map[string]interface{}{"type": "string"},
				"source_path": map[string]interface{}{"type": "string"},
				"file_format": map[string]interface{}{"type": "string"},
			},
		},
	}

	applyKindConstraints(declarative.KindNameTable, defs)

	tableType := getDefProperty(defs, "TableSpec", "table_type")
	require.NotNil(t, tableType)
	assert.Contains(t, tableType["enum"], "EXTERNAL")

	tableSpec := defs["TableSpec"]
	rules, ok := tableSpec["allOf"].([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rules)
}

func TestApplyKindConstraints_APIKeyAddsExpiresAtFormat(t *testing.T) {
	defs := map[string]map[string]interface{}{
		"APIKeySpec": {
			"properties": map[string]interface{}{
				"expires_at": map[string]interface{}{"type": "string"},
			},
		},
	}

	applyKindConstraints(declarative.KindNameAPIKeyList, defs)

	expiresAt := getDefProperty(defs, "APIKeySpec", "expires_at")
	require.NotNil(t, expiresAt)
	assert.Equal(t, "date-time", expiresAt["format"])
}

func TestApplyKindConstraints_MacroAddsVisibilityRules(t *testing.T) {
	defs := map[string]map[string]interface{}{
		"MacroSpec": {
			"properties": map[string]interface{}{
				"macro_type":   map[string]interface{}{"type": "string"},
				"visibility":   map[string]interface{}{"type": "string"},
				"status":       map[string]interface{}{"type": "string"},
				"project_name": map[string]interface{}{"type": "string"},
				"catalog_name": map[string]interface{}{"type": "string"},
			},
		},
	}

	applyKindConstraints(declarative.KindNameMacro, defs)

	visibility := getDefProperty(defs, "MacroSpec", "visibility")
	require.NotNil(t, visibility)
	assert.Contains(t, visibility["enum"], "project")
	assert.Contains(t, visibility["enum"], "catalog_global")
	assert.Contains(t, visibility["enum"], "system")

	macroSpec := defs["MacroSpec"]
	rules, ok := macroSpec["allOf"].([]interface{})
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(rules), 3)
}

func TestRun_RemovesStaleKindSchemasBeforeGeneration(t *testing.T) {
	outDir := t.TempDir()
	kindsDir := filepath.Join(outDir, "kinds")
	require.NoError(t, os.MkdirAll(kindsDir, 0o755))

	staleSchema := filepath.Join(kindsDir, "pipeline.schema.json")
	require.NoError(t, os.WriteFile(staleSchema, []byte(`{"stale":true}`), 0o644))

	require.NoError(t, run(outDir))

	_, err := os.Stat(staleSchema)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestRun_IndexMatchesGeneratedSchemaArtifacts(t *testing.T) {
	outDir := t.TempDir()
	require.NoError(t, run(outDir))

	indexPath := filepath.Join(outDir, "index.json")
	data, err := os.ReadFile(indexPath)
	require.NoError(t, err)

	var index struct {
		Files map[string]string `json:"files"`
	}
	require.NoError(t, json.Unmarshal(data, &index))
	require.NotEmpty(t, index.Files)

	for relPath := range index.Files {
		if filepath.Ext(relPath) != ".json" {
			continue
		}
		_, err := os.Stat(filepath.Join(outDir, relPath))
		require.NoError(t, err, "indexed artifact should exist: %s", relPath)
	}

	kindEntries, err := os.ReadDir(filepath.Join(outDir, "kinds"))
	require.NoError(t, err)
	for _, entry := range kindEntries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		relPath := filepath.ToSlash(filepath.Join("kinds", entry.Name()))
		_, ok := index.Files[relPath]
		assert.True(t, ok, "schema artifact should be indexed: %s", relPath)
	}
}
