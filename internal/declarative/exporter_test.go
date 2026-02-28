package declarative

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExporter_RoundTrip(t *testing.T) {
	// Create a state with multiple resource types
	original := &DesiredState{
		Principals: []PrincipalSpec{
			{Name: "admin", Type: "user", IsAdmin: true},
			{Name: "analyst1", Type: "user", IsAdmin: false},
		},
		Groups: []GroupSpec{
			{Name: "analysts", Description: "Data analysts", Members: []MemberRef{
				{Name: "analyst1", Type: "user"},
			}},
		},
		Grants: []GrantSpec{
			{Principal: "analysts", PrincipalType: "group", SecurableType: "schema", Securable: "main.analytics", Privilege: "USAGE"},
		},
		Catalogs: []CatalogResource{
			{CatalogName: "main", DeletionProtection: true, Spec: CatalogSpec{
				MetastoreType: "sqlite",
				DSN:           "/data/meta.sqlite",
				DataPath:      "s3://bucket/main/",
				IsDefault:     true,
			}},
		},
		Schemas: []SchemaResource{
			{CatalogName: "main", SchemaName: "analytics", Spec: SchemaSpec{
				Comment: "Analytics schema",
				Owner:   "data-team",
			}},
		},
		Tables: []TableResource{
			{CatalogName: "main", SchemaName: "analytics", TableName: "orders", DeletionProtection: true, Spec: TableSpec{
				TableType: "MANAGED",
				Comment:   "Orders table",
				Columns:   []ColumnDef{{Name: "id", Type: "BIGINT"}, {Name: "amount", Type: "DECIMAL"}},
			}},
		},
		Views: []ViewResource{
			{CatalogName: "main", SchemaName: "analytics", ViewName: "summary", Spec: ViewSpec{
				ViewDefinition: "SELECT COUNT(*) FROM orders",
				Comment:        "Summary view",
			}},
		},
		Volumes: []VolumeResource{
			{CatalogName: "main", SchemaName: "analytics", VolumeName: "raw", Spec: VolumeSpec{
				VolumeType:      "EXTERNAL",
				StorageLocation: "s3://bucket/raw/",
			}},
		},
		StorageCredentials: []StorageCredentialSpec{
			{Name: "s3-creds", CredentialType: "S3", S3: &S3CredentialSpec{
				KeyIDFromEnv:  "AWS_KEY_ID",
				SecretFromEnv: "AWS_SECRET",
				Region:        "us-east-1",
			}},
		},
		ExternalLocations: []ExternalLocationSpec{
			{Name: "lake", URL: "s3://bucket/", CredentialName: "s3-creds"},
		},
		ComputeEndpoints: []ComputeEndpointSpec{
			{Name: "local", Type: "LOCAL"},
		},
		Tags: []TagSpec{
			{Key: "env", Value: strPtr("prod")},
		},
	}

	// Export to temp dir
	dir := t.TempDir()
	err := ExportDirectory(dir, original, false)
	require.NoError(t, err)

	// Verify files were created
	assertFileExists(t, filepath.Join(dir, "security", "principals.yaml"))
	assertFileExists(t, filepath.Join(dir, "security", "groups.yaml"))
	assertFileExists(t, filepath.Join(dir, "security", "grants.yaml"))
	assertFileExists(t, filepath.Join(dir, "catalogs", "main", "catalog.yaml"))
	assertFileExists(t, filepath.Join(dir, "catalogs", "main", "schemas", "analytics", "schema.yaml"))
	assertFileExists(t, filepath.Join(dir, "catalogs", "main", "schemas", "analytics", "tables", "orders", "table.yaml"))
	assertFileExists(t, filepath.Join(dir, "catalogs", "main", "schemas", "analytics", "views", "summary.yaml"))

	// Load back
	loaded, err := LoadDirectory(dir)
	require.NoError(t, err)

	// Compare
	assert.Len(t, loaded.Principals, len(original.Principals))
	assert.Equal(t, original.Principals[0].Name, loaded.Principals[0].Name)
	assert.Equal(t, original.Principals[0].IsAdmin, loaded.Principals[0].IsAdmin)

	assert.Len(t, loaded.Groups, len(original.Groups))
	assert.Equal(t, original.Groups[0].Name, loaded.Groups[0].Name)

	assert.Len(t, loaded.Grants, len(original.Grants))

	require.Len(t, loaded.Catalogs, 1)
	assert.Equal(t, original.Catalogs[0].CatalogName, loaded.Catalogs[0].CatalogName)
	assert.Equal(t, original.Catalogs[0].DeletionProtection, loaded.Catalogs[0].DeletionProtection)

	require.Len(t, loaded.Schemas, 1)
	assert.Equal(t, "analytics", loaded.Schemas[0].SchemaName)

	require.Len(t, loaded.Tables, 1)
	assert.Equal(t, "orders", loaded.Tables[0].TableName)
	assert.Len(t, loaded.Tables[0].Spec.Columns, 2)

	require.Len(t, loaded.Views, 1)
	assert.Equal(t, "summary", loaded.Views[0].ViewName)

	require.Len(t, loaded.Volumes, 1)
	assert.Equal(t, "raw", loaded.Volumes[0].VolumeName)
}

func TestExporter_EmptyState(t *testing.T) {
	dir := t.TempDir()
	err := ExportDirectory(dir, &DesiredState{}, false)
	require.NoError(t, err)
	// Should not create any files for empty state
	entries, _ := os.ReadDir(dir)
	assert.Empty(t, entries, "empty state should produce no files")
}

func TestExporter_OverwriteProtection(t *testing.T) {
	dir := t.TempDir()
	// Write something first
	err := os.WriteFile(filepath.Join(dir, "existing.txt"), []byte("data"), 0o644)
	require.NoError(t, err)

	// Export without overwrite should fail
	state := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user"}},
	}
	err = ExportDirectory(dir, state, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not empty")

	// Export with overwrite should succeed
	err = ExportDirectory(dir, state, true)
	require.NoError(t, err)
}

func TestExporter_PlanAfterExport(t *testing.T) {
	// Export a state, then diff exported vs original -> should be zero changes
	state := &DesiredState{
		Principals: []PrincipalSpec{
			{Name: "admin", Type: "user", IsAdmin: true},
		},
		Catalogs: []CatalogResource{
			{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/data/meta.sqlite", DataPath: "s3://test/"}},
		},
		Schemas: []SchemaResource{
			{CatalogName: "main", SchemaName: "analytics", Spec: SchemaSpec{Comment: "test"}},
		},
	}

	dir := t.TempDir()
	err := ExportDirectory(dir, state, false)
	require.NoError(t, err)

	loaded, err := LoadDirectory(dir)
	require.NoError(t, err)

	plan := Diff(loaded, state)
	assert.False(t, plan.HasChanges(), "plan after export should show no changes, got %d actions, %d errors", len(plan.Actions), len(plan.Errors))
}

func TestExporter_RoundTripModelsAndMacros(t *testing.T) {
	original := &DesiredState{
		Models: []ModelResource{{
			ProjectName: "analytics",
			ModelName:   "stg_orders",
			Spec: ModelSpec{
				Materialization: "INCREMENTAL",
				SQL:             "SELECT 1",
				Config: &ModelConfigSpec{
					UniqueKey:           []string{"order_id"},
					IncrementalStrategy: "delete+insert",
					OnSchemaChange:      "fail",
				},
			},
		}},
		Macros: []MacroResource{{
			Name: "fmt_money",
			Spec: MacroSpec{
				MacroType:   "SCALAR",
				Parameters:  []string{"amount"},
				Body:        "amount / 100.0",
				CatalogName: "main",
				ProjectName: "analytics",
				Visibility:  "catalog_global",
				Owner:       "data-team",
				Properties:  map[string]string{"team": "finance"},
				Tags:        []string{"finance", "shared"},
				Status:      "DEPRECATED",
			},
		}},
		SemanticModels: []SemanticModelResource{{
			ProjectName: "analytics",
			ModelName:   "sales",
			Spec: SemanticModelSpec{
				Description:          "Sales semantic model",
				BaseModelRef:         "analytics.fct_sales",
				DefaultTimeDimension: "order_date",
				Tags:                 []string{"finance"},
				Metrics: []SemanticMetricSpec{{
					Name:               "total_revenue",
					MetricType:         "SUM",
					ExpressionMode:     "SQL",
					Expression:         "sum(amount)",
					CertificationState: "CERTIFIED",
				}},
				Relationships: []SemanticRelationshipSpec{{
					Name:             "sales_to_customers",
					ToModel:          "analytics.customers",
					RelationshipType: "MANY_TO_ONE",
					JoinSQL:          "sales.customer_id = customers.id",
				}},
				PreAggregations: []SemanticPreAggSpec{{
					Name:           "daily_sales",
					MetricSet:      []string{"total_revenue"},
					DimensionSet:   []string{"order_date"},
					TargetRelation: "analytics.agg_daily_sales",
				}},
			},
		}},
	}

	dir := t.TempDir()
	err := ExportDirectory(dir, original, false)
	require.NoError(t, err)

	assertFileExists(t, filepath.Join(dir, "models", "analytics", "stg_orders.yaml"))
	assertFileExists(t, filepath.Join(dir, "macros", "fmt_money.yaml"))
	assertFileExists(t, filepath.Join(dir, "semantic_models", "analytics", "sales.yaml"))

	loaded, err := LoadDirectory(dir)
	require.NoError(t, err)

	require.Len(t, loaded.Models, 1)
	assert.Equal(t, "fail", loaded.Models[0].Spec.Config.OnSchemaChange)
	assert.Equal(t, "delete+insert", loaded.Models[0].Spec.Config.IncrementalStrategy)

	require.Len(t, loaded.Macros, 1)
	assert.Equal(t, original.Macros[0].Spec.CatalogName, loaded.Macros[0].Spec.CatalogName)
	assert.Equal(t, original.Macros[0].Spec.ProjectName, loaded.Macros[0].Spec.ProjectName)
	assert.Equal(t, original.Macros[0].Spec.Visibility, loaded.Macros[0].Spec.Visibility)
	assert.Equal(t, original.Macros[0].Spec.Owner, loaded.Macros[0].Spec.Owner)
	assert.Equal(t, original.Macros[0].Spec.Properties, loaded.Macros[0].Spec.Properties)
	assert.Equal(t, original.Macros[0].Spec.Tags, loaded.Macros[0].Spec.Tags)
	assert.Equal(t, original.Macros[0].Spec.Status, loaded.Macros[0].Spec.Status)

	require.Len(t, loaded.SemanticModels, 1)
	assert.Equal(t, original.SemanticModels[0].ProjectName, loaded.SemanticModels[0].ProjectName)
	assert.Equal(t, original.SemanticModels[0].ModelName, loaded.SemanticModels[0].ModelName)
	assert.Equal(t, original.SemanticModels[0].Spec.BaseModelRef, loaded.SemanticModels[0].Spec.BaseModelRef)
	require.Len(t, loaded.SemanticModels[0].Spec.Metrics, 1)
	assert.Equal(t, "total_revenue", loaded.SemanticModels[0].Spec.Metrics[0].Name)
}

// strPtr is defined in validator_test.go (same package).

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	assert.NoError(t, err, "file should exist: %s", path)
}
