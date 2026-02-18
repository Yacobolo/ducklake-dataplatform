package declarative

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testdataDir returns the absolute path to testdata relative to this test file.
func testdataDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	return filepath.Join(filepath.Dir(filename), "testdata")
}

func TestLoader_FullConfig(t *testing.T) {
	dir := filepath.Join(testdataDir(t), "valid", "full")
	state, err := LoadDirectory(dir)
	require.NoError(t, err)

	t.Run("principals loaded", func(t *testing.T) {
		assert.Len(t, state.Principals, 3)
		assert.Equal(t, "admin-user", state.Principals[0].Name)
		assert.True(t, state.Principals[0].IsAdmin)
	})

	t.Run("groups loaded", func(t *testing.T) {
		assert.Len(t, state.Groups, 2)
		assert.Equal(t, "analysts", state.Groups[0].Name)
		assert.Len(t, state.Groups[0].Members, 1)
	})

	t.Run("grants loaded", func(t *testing.T) {
		assert.Len(t, state.Grants, 2)
	})

	t.Run("catalogs loaded", func(t *testing.T) {
		require.Len(t, state.Catalogs, 1)
		assert.Equal(t, "main", state.Catalogs[0].CatalogName)
		assert.True(t, state.Catalogs[0].DeletionProtection)
		assert.Equal(t, "sqlite", state.Catalogs[0].Spec.MetastoreType)
	})

	t.Run("schemas loaded", func(t *testing.T) {
		require.Len(t, state.Schemas, 1)
		assert.Equal(t, "main", state.Schemas[0].CatalogName)
		assert.Equal(t, "analytics", state.Schemas[0].SchemaName)
	})

	t.Run("tables loaded", func(t *testing.T) {
		require.Len(t, state.Tables, 1)
		assert.Equal(t, "orders", state.Tables[0].TableName)
		assert.True(t, state.Tables[0].DeletionProtection)
		assert.Len(t, state.Tables[0].Spec.Columns, 4)
	})

	t.Run("row filters loaded", func(t *testing.T) {
		require.Len(t, state.RowFilters, 1)
		assert.Len(t, state.RowFilters[0].Filters, 1)
		assert.Equal(t, "region-us", state.RowFilters[0].Filters[0].Name)
	})

	t.Run("column masks loaded", func(t *testing.T) {
		require.Len(t, state.ColumnMasks, 1)
		assert.Len(t, state.ColumnMasks[0].Masks, 1)
	})

	t.Run("views loaded", func(t *testing.T) {
		require.Len(t, state.Views, 1)
		assert.Equal(t, "monthly-revenue", state.Views[0].ViewName)
	})

	t.Run("volumes loaded", func(t *testing.T) {
		require.Len(t, state.Volumes, 1)
		assert.Equal(t, "raw-data", state.Volumes[0].VolumeName)
	})

	t.Run("tags loaded", func(t *testing.T) {
		assert.Len(t, state.Tags, 2)
		assert.Len(t, state.TagAssignments, 1)
	})

	t.Run("storage credentials loaded", func(t *testing.T) {
		require.Len(t, state.StorageCredentials, 1)
		assert.Equal(t, "test-s3", state.StorageCredentials[0].Name)
	})

	t.Run("external locations loaded", func(t *testing.T) {
		require.Len(t, state.ExternalLocations, 1)
	})

	t.Run("compute endpoints loaded", func(t *testing.T) {
		require.Len(t, state.ComputeEndpoints, 1)
		assert.Equal(t, "local-dev", state.ComputeEndpoints[0].Name)
	})

	t.Run("compute assignments loaded", func(t *testing.T) {
		require.Len(t, state.ComputeAssignments, 1)
	})
}

func TestLoader_MinimalConfig(t *testing.T) {
	dir := filepath.Join(testdataDir(t), "valid", "minimal")
	state, err := LoadDirectory(dir)
	require.NoError(t, err)

	assert.Len(t, state.Principals, 1)
	assert.Empty(t, state.Groups)
	assert.Empty(t, state.Grants)
	assert.Empty(t, state.Catalogs)
}

func TestLoader_SecurityPresetsAndBindings(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	secDir := filepath.Join(dir, "security")
	require.NoError(t, os.MkdirAll(secDir, 0o755))

	presetsYAML := []byte(`apiVersion: duck/v1
kind: PrivilegePresetList
presets:
  - name: reader
    privileges: [USE_CATALOG, USE_SCHEMA, SELECT]
`)
	require.NoError(t, os.WriteFile(filepath.Join(secDir, "privilege-presets.yaml"), presetsYAML, 0o644))

	bindingsYAML := []byte(`apiVersion: duck/v1
kind: BindingList
bindings:
  - principal: analysts
    principal_type: group
    preset: reader
    scope_type: schema
    scope: main.analytics
`)
	require.NoError(t, os.WriteFile(filepath.Join(secDir, "bindings.yaml"), bindingsYAML, 0o644))

	state, err := LoadDirectory(dir)
	require.NoError(t, err)
	require.Len(t, state.PrivilegePresets, 1)
	assert.Equal(t, "reader", state.PrivilegePresets[0].Name)
	require.Len(t, state.Bindings, 1)
	assert.Equal(t, "reader", state.Bindings[0].Preset)
}

func TestLoader_BadYAML(t *testing.T) {
	dir := filepath.Join(testdataDir(t), "invalid", "bad-yaml")
	_, err := LoadDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse")
}

func TestLoader_NameMismatch(t *testing.T) {
	dir := filepath.Join(testdataDir(t), "invalid", "name-mismatch")
	_, err := LoadDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name")
}

func TestLoader_DanglingRef(t *testing.T) {
	dir := filepath.Join(testdataDir(t), "invalid", "dangling-ref")
	state, err := LoadDirectory(dir)
	require.NoError(t, err, "LoadDirectory should succeed; validation is separate")

	// The loader succeeds but Validate catches the dangling references.
	errs := Validate(state)
	require.NotEmpty(t, errs)

	// Expect errors about both the unknown principal and the unknown catalog.
	var messages []string
	for _, e := range errs {
		messages = append(messages, e.Error())
	}

	// The grant references "nonexistent-user" which is not a declared principal.
	foundPrincipal := false
	foundSecurable := false
	for _, e := range errs {
		if e.Message == "principal \"nonexistent-user\" references unknown user" {
			foundPrincipal = true
		}
		if e.Message == "securable references unknown catalog \"main\"" {
			foundSecurable = true
		}
	}
	assert.True(t, foundPrincipal, "expected validation error about unknown principal; got: %v", messages)
	assert.True(t, foundSecurable, "expected validation error about unknown catalog; got: %v", messages)
}

func TestLoader_ModelsDirectory(t *testing.T) {
	// LoadDirectory expects <root>/models/<project>/... so we use a dedicated
	// fixture directory that contains only a models/ subtree.
	dir := filepath.Join(testdataDir(t), "valid", "models-only")
	state, err := LoadDirectory(dir)
	require.NoError(t, err)

	t.Run("models loaded from nested directories", func(t *testing.T) {
		require.Len(t, state.Models, 2)

		// Build a lookup by model name for order-independent assertions.
		byName := make(map[string]ModelResource, len(state.Models))
		for _, m := range state.Models {
			byName[m.ModelName] = m
		}

		stg, ok := byName["stg_orders"]
		require.True(t, ok, "expected stg_orders model")
		assert.Equal(t, "sales", stg.ProjectName)
		assert.Equal(t, "TABLE", stg.Spec.Materialization)
		assert.Equal(t, "Staged orders", stg.Spec.Description)
		assert.Equal(t, []string{"finance", "staging"}, stg.Spec.Tags)
		assert.Contains(t, stg.Spec.SQL, "SELECT order_id")

		fct, ok := byName["fct_orders"]
		require.True(t, ok, "expected fct_orders model")
		assert.Equal(t, "sales", fct.ProjectName)
		assert.Equal(t, "VIEW", fct.Spec.Materialization)
		assert.Equal(t, "Orders fact table", fct.Spec.Description)
	})
}

func TestLoader_NonexistentDir(t *testing.T) {
	_, err := LoadDirectory("/nonexistent/path")
	require.Error(t, err)
}

func TestLoader_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	state, err := LoadDirectory(dir)
	require.NoError(t, err)
	assert.Empty(t, state.Principals)
	assert.Empty(t, state.Catalogs)
}

func TestLoader_WrongAPIVersion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	secDir := filepath.Join(dir, "security")
	require.NoError(t, os.MkdirAll(secDir, 0o755))

	content := []byte(`apiVersion: v99
kind: PrincipalList
principals:
  - name: user1
    type: user
`)
	require.NoError(t, os.WriteFile(filepath.Join(secDir, "principals.yaml"), content, 0o644))

	_, err := LoadDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported apiVersion")
	assert.Contains(t, err.Error(), "v99")
}

func TestLoader_WrongKind(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	secDir := filepath.Join(dir, "security")
	require.NoError(t, os.MkdirAll(secDir, 0o755))

	content := []byte(`apiVersion: duck/v1
kind: WrongKind
principals:
  - name: user1
    type: user
`)
	require.NoError(t, os.WriteFile(filepath.Join(secDir, "principals.yaml"), content, 0o644))

	_, err := LoadDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected kind")
	assert.Contains(t, err.Error(), "WrongKind")
}

func TestLoader_PartialCatalogDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create catalogs/mycat/ directory without a catalog.yaml file inside.
	catDir := filepath.Join(dir, "catalogs", "mycat")
	require.NoError(t, os.MkdirAll(catDir, 0o755))

	state, err := LoadDirectory(dir)
	require.NoError(t, err, "should succeed without catalog.yaml — file is optional")
	assert.Empty(t, state.Catalogs, "no catalog should be loaded when catalog.yaml is missing")
}

func TestLoader_ModelAndMacroExtendedFields(t *testing.T) {
	dir := t.TempDir()

	modelDir := filepath.Join(dir, "models", "analytics")
	require.NoError(t, os.MkdirAll(modelDir, 0o755))
	modelYAML := `apiVersion: duck/v1
kind: Model
metadata:
  name: stg_orders
spec:
  materialization: INCREMENTAL
  sql: SELECT 1
  config:
    unique_key: [order_id]
    incremental_strategy: delete+insert
    on_schema_change: fail
`
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "stg_orders.yaml"), []byte(modelYAML), 0o644))

	macroDir := filepath.Join(dir, "macros")
	require.NoError(t, os.MkdirAll(macroDir, 0o755))
	macroYAML := `apiVersion: duck/v1
kind: Macro
metadata:
  name: fmt_money
spec:
  macro_type: SCALAR
  parameters: [amount]
  body: amount / 100.0
  catalog_name: main
  project_name: analytics
  visibility: catalog_global
  owner: data-team
  properties:
    team: finance
  tags: [finance, shared]
  status: DEPRECATED
`
	require.NoError(t, os.WriteFile(filepath.Join(macroDir, "fmt_money.yaml"), []byte(macroYAML), 0o644))

	state, err := LoadDirectory(dir)
	require.NoError(t, err)

	require.Len(t, state.Models, 1)
	assert.Equal(t, "fail", state.Models[0].Spec.Config.OnSchemaChange)
	assert.Equal(t, "delete+insert", state.Models[0].Spec.Config.IncrementalStrategy)

	require.Len(t, state.Macros, 1)
	macro := state.Macros[0]
	assert.Equal(t, "main", macro.Spec.CatalogName)
	assert.Equal(t, "analytics", macro.Spec.ProjectName)
	assert.Equal(t, "catalog_global", macro.Spec.Visibility)
	assert.Equal(t, "data-team", macro.Spec.Owner)
	assert.Equal(t, map[string]string{"team": "finance"}, macro.Spec.Properties)
	assert.Equal(t, []string{"finance", "shared"}, macro.Spec.Tags)
	assert.Equal(t, "DEPRECATED", macro.Spec.Status)
}

func TestLoader_StrictUnknownFields_DefaultRejectsUnknown(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "analytics")
	require.NoError(t, os.MkdirAll(modelDir, 0o755))

	content := `apiVersion: duck/v1
kind: Model
metadata:
  name: stg_orders
spec:
  sql: SELECT 1
  materialization: VIEW
  unknown_field: true
`
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "stg_orders.yaml"), []byte(content), 0o644))

	_, err := LoadDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field unknown_field not found")
}

func TestLoader_AllowUnknownFields_Option(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	modelDir := filepath.Join(dir, "models", "analytics")
	require.NoError(t, os.MkdirAll(modelDir, 0o755))

	content := `apiVersion: duck/v1
kind: Model
metadata:
  name: stg_orders
spec:
  sql: SELECT 1
  materialization: VIEW
  unknown_field: true
`
	require.NoError(t, os.WriteFile(filepath.Join(modelDir, "stg_orders.yaml"), []byte(content), 0o644))

	state, err := LoadDirectoryWithOptions(dir, LoadOptions{AllowUnknownFields: true})
	require.NoError(t, err)
	require.Len(t, state.Models, 1)
	assert.Equal(t, "stg_orders", state.Models[0].ModelName)
}

func TestLoader_NonYAMLFilesSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a proper catalog with a schema and views directory.
	viewsDir := filepath.Join(dir, "catalogs", "main", "schemas", "public", "views")
	require.NoError(t, os.MkdirAll(viewsDir, 0o755))

	// Write a valid catalog.yaml.
	catYAML := []byte(`apiVersion: duck/v1
kind: Catalog
metadata:
  name: main
spec:
  metastore_type: sqlite
  dsn: ":memory:"
  data_path: /tmp/data
`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "catalogs", "main", "catalog.yaml"), catYAML, 0o644))

	// Write a valid schema.yaml.
	schemaYAML := []byte(`apiVersion: duck/v1
kind: Schema
metadata:
  name: public
spec:
  comment: test schema
`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "catalogs", "main", "schemas", "public", "schema.yaml"), schemaYAML, 0o644))

	// Write a valid view YAML.
	viewYAML := []byte(`apiVersion: duck/v1
kind: View
metadata:
  name: my-view
spec:
  view_definition: "SELECT 1"
`)
	require.NoError(t, os.WriteFile(filepath.Join(viewsDir, "my-view.yaml"), viewYAML, 0o644))

	// Write a .txt file that should be skipped.
	require.NoError(t, os.WriteFile(filepath.Join(viewsDir, "notes.txt"), []byte("this is not yaml"), 0o644))

	state, err := LoadDirectory(dir)
	require.NoError(t, err, "non-YAML files should be silently skipped")
	require.Len(t, state.Views, 1, "should load only the .yaml view file")
	assert.Equal(t, "my-view", state.Views[0].ViewName)
}
