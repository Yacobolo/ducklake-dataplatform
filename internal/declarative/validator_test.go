package declarative

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// containsStr is a helper wrapping strings.Contains for readability.
func containsStr(s, substr string) bool {
	return strings.Contains(s, substr)
}

// Helper to create *string.
func strPtr(s string) *string { return &s }

func testProductControlPlane(productSlug string) ([]DomainResource, []TeamResource, []DataProductResource) {
	return []DomainResource{{
			Name: "revenue",
			Spec: DomainSpec{Description: "Revenue domain"},
		}},
		[]TeamResource{{
			Name: "analytics-engineering",
			Spec: TeamSpec{DomainRef: "revenue", ContactChannel: "#rev-data"},
		}},
		[]DataProductResource{{
			Slug: productSlug,
			Spec: DataProductSpec{
				Name:             "Orders Product",
				DomainRef:        "revenue",
				OwnerTeamRef:     "analytics-engineering",
				StewardPrincipal: "alice",
				ContactChannel:   "#rev-data",
				Contract:         ProductContractSpec{DataGrain: "one row per order"},
				SLO:              ProductSLOSpec{FreshnessSLO: "60m"},
			},
		}}
}

func TestValidate_ValidFullState(t *testing.T) {
	// Build a valid DesiredState with all resource types populated correctly.
	// It should produce zero validation errors.
	state := &DesiredState{
		Principals: []PrincipalSpec{
			{Name: "admin", Type: "user", IsAdmin: true},
			{Name: "analyst1", Type: "user", IsAdmin: false},
			{Name: "bot", Type: "service_principal", IsAdmin: false},
		},
		Groups: []GroupSpec{
			{Name: "analysts", Description: "Analysts", Members: []MemberRef{{Name: "analyst1", Type: "user"}}},
		},
		Grants: []GrantSpec{
			{Principal: "analysts", PrincipalType: "group", SecurableType: "catalog", Securable: "main", Privilege: "USAGE"},
			{Principal: "analysts", PrincipalType: "group", SecurableType: "schema", Securable: "main.analytics", Privilege: "USAGE"},
			{Principal: "analyst1", PrincipalType: "user", SecurableType: "table", Securable: "main.analytics.orders", Privilege: "SELECT"},
		},
		PrivilegePresets: []PrivilegePresetSpec{
			{Name: "reader", Privileges: []string{"USE_CATALOG", "USE_SCHEMA", "SELECT"}},
		},
		Bindings: []BindingSpec{
			{Principal: "analysts", PrincipalType: "group", Preset: "reader", ScopeType: "schema", Scope: "main.analytics"},
		},
		Catalogs: []CatalogResource{
			{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/data/meta.sqlite", DataPath: "s3://bucket/"}},
		},
		Schemas: []SchemaResource{
			{CatalogName: "main", SchemaName: "analytics", Spec: SchemaSpec{Comment: "test"}},
		},
		Tables: []TableResource{
			{CatalogName: "main", SchemaName: "analytics", TableName: "orders", Spec: TableSpec{
				TableType: "MANAGED",
				Columns:   []ColumnDef{{Name: "order_id", Type: "BIGINT"}, {Name: "amount", Type: "DECIMAL"}},
			}},
		},
		Tags: []TagSpec{{Key: "classification", Value: strPtr("pii")}},
		StorageCredentials: []StorageCredentialSpec{
			{Name: "s3-creds", CredentialType: "S3", S3: &S3CredentialSpec{KeyIDFromEnv: "KEY", SecretFromEnv: "SECRET"}},
		},
		ExternalLocations: []ExternalLocationSpec{
			{Name: "lake", URL: "s3://bucket/", CredentialName: "s3-creds"},
		},
		ComputeEndpoints: []ComputeEndpointSpec{
			{Name: "local", Type: "LOCAL"},
		},
		ComputeAssignments: []ComputeAssignmentSpec{
			{Endpoint: "local", Principal: "analyst1", PrincipalType: "user"},
		},
	}

	errs := Validate(state)
	assert.Empty(t, errs, "valid state should have no errors: %v", errs)
}

func TestValidate_PresetAndBindingErrors(t *testing.T) {
	state := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user"}},
		Catalogs:   []CatalogResource{{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}}},
		PrivilegePresets: []PrivilegePresetSpec{
			{Name: "", Privileges: []string{"SELECT"}},
			{Name: "bad-priv", Privileges: []string{"NOT_REAL"}},
		},
		Bindings: []BindingSpec{
			{Principal: "user1", PrincipalType: "user", Preset: "missing", ScopeType: "catalog", Scope: "main"},
			{Principal: "ghost", PrincipalType: "user", Preset: "bad-priv", ScopeType: "banana", Scope: "main"},
		},
	}

	errs := Validate(state)
	require.NotEmpty(t, errs)

	messages := make([]string, 0, len(errs))
	for _, e := range errs {
		messages = append(messages, e.Error())
	}

	assert.Condition(t, func() bool {
		for _, m := range messages {
			if containsStr(m, "privilege_preset") && containsStr(m, "name is required") {
				return true
			}
		}
		return false
	})

	assert.Condition(t, func() bool {
		for _, m := range messages {
			if containsStr(m, "unknown privilege") && containsStr(m, "NOT_REAL") {
				return true
			}
		}
		return false
	})

	assert.Condition(t, func() bool {
		for _, m := range messages {
			if containsStr(m, "unknown preset") {
				return true
			}
		}
		return false
	})
}

func TestValidate_BindingPrivilegeScopeRules(t *testing.T) {
	state := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user"}},
		Catalogs:   []CatalogResource{{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}}},
		PrivilegePresets: []PrivilegePresetSpec{{
			Name:       "bad-catalog-reader",
			Privileges: []string{"SELECT"},
		}},
		Bindings: []BindingSpec{{
			Principal:     "user1",
			PrincipalType: "user",
			Preset:        "bad-catalog-reader",
			ScopeType:     "catalog",
			Scope:         "main",
		}},
	}

	errs := Validate(state)
	require.NotEmpty(t, errs)

	found := false
	for _, e := range errs {
		if containsStr(e.Error(), "not allowed on scope_type") && containsStr(e.Error(), "SELECT") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected binding scope privilege validation error, got %v", errs)
}

func TestValidate_DuplicateEffectiveGrantFromBinding(t *testing.T) {
	state := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user"}},
		Catalogs:   []CatalogResource{{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}}},
		Grants: []GrantSpec{{
			Principal:     "user1",
			PrincipalType: "user",
			SecurableType: "catalog",
			Securable:     "main",
			Privilege:     "USE_CATALOG",
		}},
		PrivilegePresets: []PrivilegePresetSpec{{
			Name:       "catalog-user",
			Privileges: []string{"USE_CATALOG"},
		}},
		Bindings: []BindingSpec{{
			Principal:     "user1",
			PrincipalType: "user",
			Preset:        "catalog-user",
			ScopeType:     "catalog",
			Scope:         "main",
		}},
	}

	errs := Validate(state)
	require.NotEmpty(t, errs)

	found := false
	for _, e := range errs {
		if containsStr(e.Error(), "duplicate effective grant") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected duplicate effective grant error, got %v", errs)
}

func TestValidate_PrincipalErrors(t *testing.T) {
	tests := []struct {
		name       string
		principals []PrincipalSpec
		wantErrs   int
		contains   string
	}{
		{"empty name", []PrincipalSpec{{Name: "", Type: "user"}}, 1, "name is required"},
		{"invalid type", []PrincipalSpec{{Name: "p1", Type: "invalid"}}, 1, "type"},
		{"duplicate", []PrincipalSpec{{Name: "p1", Type: "user"}, {Name: "p1", Type: "user"}}, 1, "duplicate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &DesiredState{Principals: tt.principals}
			errs := Validate(state)
			require.GreaterOrEqual(t, len(errs), tt.wantErrs)
			assert.Contains(t, errs[0].Error(), tt.contains)
		})
	}
}

func TestValidate_DashboardErrors(t *testing.T) {
	state := &DesiredState{
		Notebooks: []NotebookResource{{
			Name: "sales-kpis",
			Spec: NotebookSpec{
				Owner: "alice",
				Cells: []CellSpec{{Name: "zone_output", Type: "sql", Content: "select 1"}},
			},
		}},
		SemanticModels: []SemanticModelResource{{
			ModelName: "revenue",
			Spec:      SemanticModelSpec{BaseModelRef: "analytics.revenue"},
		}},
		Dashboards: []DashboardResource{{
			Name: "revenue-overview",
			Spec: DashboardSpec{
				Compute: &domain.DashboardComputePolicy{Mode: domain.ComputeModeSharedEndpoint},
				Widgets: []DashboardWidgetSpec{
					{
						Key:  "bad key",
						Name: "Revenue",
						Source: DashboardWidgetSourceSpec{
							Kind: domain.DashboardWidgetSourceNotebookCell,
							NotebookCell: &DashboardNotebookCellRefSpec{
								NotebookName: "sales-kpis",
								CellName:     "missing_cell",
							},
						},
						Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 6, H: 4},
					},
					{
						Key:  "bad key",
						Name: "Revenue Duplicate",
						Source: DashboardWidgetSourceSpec{
							Kind: domain.DashboardWidgetSourceSemanticQuery,
							SemanticQuery: &DashboardSemanticQuerySpec{
								Metrics: []string{"revenue"},
							},
						},
						Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 6, H: 4},
					},
				},
			},
		}},
	}

	errs := Validate(state)
	require.NotEmpty(t, errs)

	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		messages = append(messages, err.Error())
	}

	assert.Contains(t, strings.Join(messages, "\n"), "owner is required")
	assert.Contains(t, strings.Join(messages, "\n"), "endpoint_name is required")
	assert.Contains(t, strings.Join(messages, "\n"), "widget key must contain only lowercase letters, digits, and hyphens")
	assert.Contains(t, strings.Join(messages, "\n"), "duplicate widget key")
	assert.Contains(t, strings.Join(messages, "\n"), "unknown cell")
}

func TestValidate_DashboardValidNotebookAndSemanticBindings(t *testing.T) {
	state := &DesiredState{
		Notebooks: []NotebookResource{{
			Name: "sales-kpis",
			Spec: NotebookSpec{
				Owner: "alice",
				Cells: []CellSpec{{Name: "zone_output", Type: "sql", Content: "select 1"}},
			},
		}},
		SemanticModels: []SemanticModelResource{{
			ModelName: "revenue",
			Spec:      SemanticModelSpec{BaseModelRef: "analytics.revenue"},
		}},
		Dashboards: []DashboardResource{{
			Name: "revenue-overview",
			Spec: DashboardSpec{
				Owner:               "alice",
				SemanticProjectName: "analytics",
				SemanticModelName:   "revenue",
				Compute:             &domain.DashboardComputePolicy{Mode: domain.ComputeModeByocLocal},
				Widgets: []DashboardWidgetSpec{
					{
						Key:  "table-zones",
						Name: "Zone Detail",
						Source: DashboardWidgetSourceSpec{
							Kind: domain.DashboardWidgetSourceNotebookCell,
							NotebookCell: &DashboardNotebookCellRefSpec{
								NotebookName: "sales-kpis",
								CellName:     "zone_output",
							},
						},
						Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 6, H: 4},
					},
					{
						Key:  "chart-revenue",
						Name: "Revenue",
						Source: DashboardWidgetSourceSpec{
							Kind: domain.DashboardWidgetSourceSemanticQuery,
							SemanticQuery: &DashboardSemanticQuerySpec{
								Metrics: []string{"revenue"},
							},
						},
						Layout: domain.DashboardWidgetLayout{X: 0, Y: 0, W: 6, H: 4},
					},
				},
			},
		}},
	}

	assert.Empty(t, Validate(state))
}

func TestValidate_GroupErrors(t *testing.T) {
	tests := []struct {
		name    string
		state   *DesiredState
		wantErr string
	}{
		{
			"member references nonexistent principal",
			&DesiredState{
				Groups: []GroupSpec{{Name: "g1", Members: []MemberRef{{Name: "missing", Type: "user"}}}},
				// no principals defined
			},
			"references unknown",
		},
		{
			"circular group membership",
			&DesiredState{
				Groups: []GroupSpec{
					{Name: "g1", Members: []MemberRef{{Name: "g2", Type: "group"}}},
					{Name: "g2", Members: []MemberRef{{Name: "g1", Type: "group"}}},
				},
			},
			"circular",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := Validate(tt.state)
			require.NotEmpty(t, errs)
			found := false
			for _, e := range errs {
				if containsStr(e.Error(), tt.wantErr) {
					found = true
					break
				}
			}
			assert.True(t, found, "expected error containing %q, got %v", tt.wantErr, errs)
		})
	}
}

func TestValidate_GrantErrors(t *testing.T) {
	// Base state with principals and a catalog to allow valid references where needed.
	basePrincipals := []PrincipalSpec{{Name: "user1", Type: "user"}}
	baseCatalogs := []CatalogResource{
		{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}},
	}
	baseSchemas := []SchemaResource{
		{CatalogName: "main", SchemaName: "analytics"},
	}
	baseTables := []TableResource{
		{CatalogName: "main", SchemaName: "analytics", TableName: "orders", Spec: TableSpec{TableType: "MANAGED"}},
	}

	tests := []struct {
		name    string
		state   *DesiredState
		wantErr string
	}{
		{
			"grant references nonexistent principal",
			&DesiredState{
				Grants: []GrantSpec{
					{Principal: "ghost", PrincipalType: "user", SecurableType: "catalog", Securable: "main", Privilege: "USAGE"},
				},
				Catalogs: baseCatalogs,
			},
			"references unknown user",
		},
		{
			"invalid securable_type",
			&DesiredState{
				Principals: basePrincipals,
				Grants: []GrantSpec{
					{Principal: "user1", PrincipalType: "user", SecurableType: "banana", Securable: "main", Privilege: "USAGE"},
				},
			},
			"securable_type must be one of",
		},
		{
			"invalid privilege",
			&DesiredState{
				Principals: basePrincipals,
				Catalogs:   baseCatalogs,
				Grants: []GrantSpec{
					{Principal: "user1", PrincipalType: "user", SecurableType: "catalog", Securable: "main", Privilege: "FLY"},
				},
			},
			"unknown privilege",
		},
		{
			"schema securable path format mismatch",
			&DesiredState{
				Principals: basePrincipals,
				Grants: []GrantSpec{
					{Principal: "user1", PrincipalType: "user", SecurableType: "schema", Securable: "main", Privilege: "USAGE"},
				},
			},
			"schema securable must be",
		},
		{
			"duplicate grant",
			&DesiredState{
				Principals: basePrincipals,
				Catalogs:   baseCatalogs,
				Schemas:    baseSchemas,
				Tables:     baseTables,
				Grants: []GrantSpec{
					{Principal: "user1", PrincipalType: "user", SecurableType: "table", Securable: "main.analytics.orders", Privilege: "SELECT"},
					{Principal: "user1", PrincipalType: "user", SecurableType: "table", Securable: "main.analytics.orders", Privilege: "SELECT"},
				},
			},
			"duplicate grant",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := Validate(tt.state)
			require.NotEmpty(t, errs)
			found := false
			for _, e := range errs {
				if containsStr(e.Error(), tt.wantErr) {
					found = true
					break
				}
			}
			assert.True(t, found, "expected error containing %q, got %v", tt.wantErr, errs)
		})
	}
}

func TestValidate_CatalogErrors(t *testing.T) {
	tests := []struct {
		name    string
		state   *DesiredState
		wantErr string
	}{
		{
			"missing name",
			&DesiredState{
				Catalogs: []CatalogResource{
					{CatalogName: "", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}},
				},
			},
			"name is required",
		},
		{
			"missing dsn",
			&DesiredState{
				Catalogs: []CatalogResource{
					{CatalogName: "c1", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "", DataPath: "/data"}},
				},
			},
			"dsn is required",
		},
		{
			"invalid metastore_type",
			&DesiredState{
				Catalogs: []CatalogResource{
					{CatalogName: "c1", Spec: CatalogSpec{MetastoreType: "mysql", DSN: "/db", DataPath: "/data"}},
				},
			},
			"metastore_type must be",
		},
		{
			"duplicate catalog",
			&DesiredState{
				Catalogs: []CatalogResource{
					{CatalogName: "c1", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}},
					{CatalogName: "c1", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db2", DataPath: "/data2"}},
				},
			},
			"duplicate catalog",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := Validate(tt.state)
			require.NotEmpty(t, errs)
			found := false
			for _, e := range errs {
				if containsStr(e.Error(), tt.wantErr) {
					found = true
					break
				}
			}
			assert.True(t, found, "expected error containing %q, got %v", tt.wantErr, errs)
		})
	}
}

func TestValidate_TableErrors(t *testing.T) {
	baseCatalogs := []CatalogResource{
		{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}},
	}
	baseSchemas := []SchemaResource{
		{CatalogName: "main", SchemaName: "analytics"},
	}

	tests := []struct {
		name    string
		state   *DesiredState
		wantErr string
	}{
		{
			"schema doesn't exist",
			&DesiredState{
				Catalogs: baseCatalogs,
				Tables: []TableResource{
					{CatalogName: "main", SchemaName: "missing_schema", TableName: "t1", Spec: TableSpec{TableType: "MANAGED"}},
				},
			},
			"references unknown schema",
		},
		{
			"EXTERNAL missing source_path",
			&DesiredState{
				Catalogs: baseCatalogs,
				Schemas:  baseSchemas,
				Tables: []TableResource{
					{CatalogName: "main", SchemaName: "analytics", TableName: "ext_table", Spec: TableSpec{
						TableType: "EXTERNAL",
						// SourcePath intentionally missing
					}},
				},
			},
			"source_path is required",
		},
		{
			"duplicate column names",
			&DesiredState{
				Catalogs: baseCatalogs,
				Schemas:  baseSchemas,
				Tables: []TableResource{
					{CatalogName: "main", SchemaName: "analytics", TableName: "t1", Spec: TableSpec{
						TableType: "MANAGED",
						Columns: []ColumnDef{
							{Name: "id", Type: "BIGINT"},
							{Name: "id", Type: "VARCHAR"},
						},
					}},
				},
			},
			"duplicate column name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := Validate(tt.state)
			require.NotEmpty(t, errs)
			found := false
			for _, e := range errs {
				if containsStr(e.Error(), tt.wantErr) {
					found = true
					break
				}
			}
			assert.True(t, found, "expected error containing %q, got %v", tt.wantErr, errs)
		})
	}
}

func TestValidate_StorageCredentialErrors(t *testing.T) {
	tests := []struct {
		name    string
		state   *DesiredState
		wantErr string
	}{
		{
			"missing credential_type",
			&DesiredState{
				StorageCredentials: []StorageCredentialSpec{
					{Name: "cred1", CredentialType: ""},
				},
			},
			"credential_type must be",
		},
		{
			"missing S3 sub-spec",
			&DesiredState{
				StorageCredentials: []StorageCredentialSpec{
					{Name: "cred1", CredentialType: "S3", S3: nil},
				},
			},
			"s3 spec is required",
		},
		{
			"missing required env var refs",
			&DesiredState{
				StorageCredentials: []StorageCredentialSpec{
					{Name: "cred1", CredentialType: "S3", S3: &S3CredentialSpec{KeyIDFromEnv: "", SecretFromEnv: "SECRET"}},
				},
			},
			"key_id_from_env is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := Validate(tt.state)
			require.NotEmpty(t, errs)
			found := false
			for _, e := range errs {
				if containsStr(e.Error(), tt.wantErr) {
					found = true
					break
				}
			}
			assert.True(t, found, "expected error containing %q, got %v", tt.wantErr, errs)
		})
	}
}

func TestValidate_ModelErrors(t *testing.T) {
	tests := []struct {
		name    string
		state   *DesiredState
		wantErr string
	}{
		{
			"missing project_name",
			&DesiredState{
				Models: []ModelResource{
					{ProjectName: "", ModelName: "stg_orders", Spec: ModelSpec{SQL: "SELECT 1", Materialization: "VIEW"}},
				},
			},
			"project_name is required",
		},
		{
			"missing model_name",
			&DesiredState{
				Models: []ModelResource{
					{ProjectName: "sales", ModelName: "", Spec: ModelSpec{SQL: "SELECT 1", Materialization: "VIEW"}},
				},
			},
			"model_name is required",
		},
		{
			"missing sql",
			&DesiredState{
				Models: []ModelResource{
					{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{SQL: "", Materialization: "VIEW"}},
				},
			},
			"sql is required",
		},
		{
			"invalid materialization",
			&DesiredState{
				Models: []ModelResource{
					{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{SQL: "SELECT 1", Materialization: "BANANA"}},
				},
			},
			"materialization must be one of",
		},
		{
			"duplicate model",
			&DesiredState{
				Models: []ModelResource{
					{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{SQL: "SELECT 1", Materialization: "VIEW"}},
					{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{SQL: "SELECT 2", Materialization: "TABLE"}},
				},
			},
			"duplicate model",
		},
		{
			"invalid incremental strategy",
			&DesiredState{
				Models: []ModelResource{
					{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{SQL: "SELECT 1", Materialization: "INCREMENTAL", Config: &ModelConfigSpec{IncrementalStrategy: "upsert"}}},
				},
			},
			"config.incremental_strategy must be one of",
		},
		{
			"invalid on_schema_change",
			&DesiredState{
				Models: []ModelResource{
					{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{SQL: "SELECT 1", Materialization: "INCREMENTAL", Config: &ModelConfigSpec{OnSchemaChange: "replace"}}},
				},
			},
			"config.on_schema_change must be one of",
		},
		{
			"relationships to_model must exist",
			&DesiredState{
				Models: []ModelResource{
					{
						ProjectName: "sales",
						ModelName:   "stg_orders",
						Spec: ModelSpec{
							SQL:             "SELECT 1 AS order_id",
							Materialization: "VIEW",
							Tests: []TestSpec{{
								Name:     "rel_orders",
								Type:     "relationships",
								Column:   "order_id",
								ToModel:  "missing_model",
								ToColumn: "order_id",
							}},
						},
					},
				},
			},
			"references unknown to_model",
		},
		{
			"relationships to_column must exist when target has contract",
			&DesiredState{
				Models: []ModelResource{
					{
						ProjectName: "sales",
						ModelName:   "stg_orders",
						Spec: ModelSpec{
							SQL:             "SELECT 1 AS order_id",
							Materialization: "VIEW",
							Contract:        &ContractSpec{Columns: []ContractColumnSpec{{Name: "order_id", Type: "BIGINT"}}},
							Tests: []TestSpec{{
								Name:     "rel_orders",
								Type:     "relationships",
								Column:   "order_id",
								ToModel:  "dim_orders",
								ToColumn: "missing_col",
							}},
						},
					},
					{
						ProjectName: "sales",
						ModelName:   "dim_orders",
						Spec: ModelSpec{
							SQL:             "SELECT 1 AS order_id",
							Materialization: "VIEW",
							Contract:        &ContractSpec{Columns: []ContractColumnSpec{{Name: "order_id", Type: "BIGINT"}}},
						},
					},
				},
			},
			"references unknown to_column",
		},
		{
			"column test must reference declared contract column",
			&DesiredState{
				Models: []ModelResource{{
					ProjectName: "sales",
					ModelName:   "stg_orders",
					Spec: ModelSpec{
						SQL:             "SELECT 1 AS order_id",
						Materialization: "VIEW",
						Contract:        &ContractSpec{Columns: []ContractColumnSpec{{Name: "order_id", Type: "BIGINT"}}},
						Tests: []TestSpec{{
							Name:   "nn_status",
							Type:   "not_null",
							Column: "status",
						}},
					},
				}},
			},
			"is not declared in model contract",
		},
		{
			"sql macro reference must exist",
			&DesiredState{
				Models: []ModelResource{{
					ProjectName: "sales",
					ModelName:   "stg_orders",
					Spec: ModelSpec{
						SQL:             "SELECT {{ missing_macro(amount) }} AS amount",
						Materialization: "VIEW",
					},
				}},
			},
			"references unknown macro",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := Validate(tt.state)
			require.NotEmpty(t, errs)
			found := false
			for _, e := range errs {
				if containsStr(e.Error(), tt.wantErr) {
					found = true
					break
				}
			}
			assert.True(t, found, "expected error containing %q, got %v", tt.wantErr, errs)
		})
	}
}

func TestValidate_ModelValid(t *testing.T) {
	state := &DesiredState{
		Macros: []MacroResource{{Name: "fmt_money", Spec: MacroSpec{Body: "amount / 100.0"}}},
		Models: []ModelResource{
			{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{
				SQL:             "SELECT {{ fmt_money(amount) }} AS amt, order_id FROM raw_data.orders",
				Materialization: "INCREMENTAL",
				Description:     "Staged orders",
				Tags:            []string{"finance"},
				Config: &ModelConfigSpec{
					UniqueKey:           []string{"order_id"},
					IncrementalStrategy: "delete+insert",
					OnSchemaChange:      "fail",
				},
			}},
			{ProjectName: "sales", ModelName: "fct_orders", Spec: ModelSpec{
				SQL:             "SELECT * FROM stg_orders",
				Materialization: "VIEW",
			}},
		},
	}

	errs := Validate(state)
	// Filter to only model-related errors.
	var modelErrs []ValidationError
	for _, e := range errs {
		if containsStr(e.Path, "model") {
			modelErrs = append(modelErrs, e)
		}
	}
	assert.Empty(t, modelErrs, "valid models should have no model errors: %v", modelErrs)
}

func TestValidate_MacroErrors(t *testing.T) {
	tests := []struct {
		name    string
		state   *DesiredState
		wantErr string
	}{
		{
			"invalid macro visibility",
			&DesiredState{Macros: []MacroResource{{
				Name: "fmt_money",
				Spec: MacroSpec{Body: "x", Visibility: "workspace"},
			}}},
			"visibility must be one of",
		},
		{
			"invalid macro status",
			&DesiredState{Macros: []MacroResource{{
				Name: "fmt_money",
				Spec: MacroSpec{Body: "x", Status: "DISABLED"},
			}}},
			"status must be one of",
		},
		{
			"project visibility requires project_name",
			&DesiredState{Macros: []MacroResource{{
				Name: "fmt_money",
				Spec: MacroSpec{Body: "x", Visibility: "project"},
			}}},
			"project_name is required when visibility is project",
		},
		{
			"catalog_global visibility requires catalog_name",
			&DesiredState{Macros: []MacroResource{{
				Name: "fmt_money",
				Spec: MacroSpec{Body: "x", Visibility: "catalog_global"},
			}}},
			"catalog_name is required when visibility is catalog_global",
		},
		{
			"system visibility forbids project and catalog",
			&DesiredState{Macros: []MacroResource{{
				Name: "fmt_money",
				Spec: MacroSpec{Body: "x", Visibility: "system", ProjectName: "analytics", CatalogName: "main"},
			}}},
			"must be empty when visibility is system",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := Validate(tt.state)
			require.NotEmpty(t, errs)
			found := false
			for _, e := range errs {
				if containsStr(e.Error(), tt.wantErr) {
					found = true
					break
				}
			}
			assert.True(t, found, "expected error containing %q, got %v", tt.wantErr, errs)
		})
	}
}

func TestValidate_MacroValid(t *testing.T) {
	state := &DesiredState{Macros: []MacroResource{{
		Name: "fmt_money",
		Spec: MacroSpec{
			MacroType:   "SCALAR",
			Body:        "amount / 100.0",
			Visibility:  "project",
			Status:      "ACTIVE",
			CatalogName: "main",
			ProjectName: "analytics",
			Owner:       "data-team",
			Properties:  map[string]string{"team": "analytics"},
			Tags:        []string{"finance"},
		},
	}}}

	errs := Validate(state)
	var macroErrs []ValidationError
	for _, e := range errs {
		if containsStr(e.Path, "macro") {
			macroErrs = append(macroErrs, e)
		}
	}
	assert.Empty(t, macroErrs, "valid macros should have no macro errors: %v", macroErrs)
}

func TestValidate_AssetPolicyRules(t *testing.T) {
	positiveLag := int64(60)
	positiveInterval := int64(30)
	domains, teams, products := testProductControlPlane("orders-product")

	valid := &DesiredState{
		Domains:      domains,
		Teams:        teams,
		DataProducts: products,
		Assets: []AssetResource{{
			Name: "orders_asset",
			Spec: AssetSpec{
				ProductRef: "orders-product",
				DependsOn:  []string{},
				IOProfile:  "warehouse-default",
				PartitionDefinition: &AssetPartitionDefinitionSpec{
					Type:       "static",
					StaticKeys: []string{"us", "eu"},
				},
				AutoMaterializePolicy: &AssetAutoMaterializePolicySpec{
					Mode:               "scheduled",
					MinIntervalSeconds: &positiveInterval,
				},
				FreshnessPolicy: &AssetFreshnessPolicySpec{
					MaxLagSeconds: &positiveLag,
				},
				MaterializationPolicy: &AssetMaterializationPolicySpec{
					Mode: "incremental",
				},
				CheckDefinitions: []AssetCheckSpec{{Name: "row_count", CheckType: "threshold"}},
			},
		}},
	}

	err := Validate(valid)
	var assetErrs []ValidationError
	for _, e := range err {
		if strings.HasPrefix(e.Path, "asset[") {
			assetErrs = append(assetErrs, e)
		}
	}
	assert.Empty(t, assetErrs, "valid asset should have no asset errors: %v", assetErrs)

	minusOne := int64(-1)
	zero := int64(0)
	invalid := &DesiredState{
		Domains:      domains,
		Teams:        teams,
		DataProducts: products,
		Assets: []AssetResource{{
			Name: "broken_asset",
			Spec: AssetSpec{
				ProductRef:          "orders-product",
				IOProfile:           " ",
				PartitionDefinition: &AssetPartitionDefinitionSpec{Type: "static", StaticKeys: []string{"   "}},
				AutoMaterializePolicy: &AssetAutoMaterializePolicySpec{
					MinIntervalSeconds: &zero,
				},
				FreshnessPolicy: &AssetFreshnessPolicySpec{
					MaxLagSeconds: &minusOne,
				},
				MaterializationPolicy: &AssetMaterializationPolicySpec{Mode: " "},
				MaxLagSeconds:         &zero,
			},
		}},
	}

	err = Validate(invalid)
	require.NotEmpty(t, err)

	contains := []string{
		"io_profile must not be blank",
		"static partition requires at least one non-empty static_keys entry",
		"auto_materialize_policy.min_interval_seconds must be > 0",
		"freshness_policy.max_lag_seconds must be > 0",
		"materialization_policy.mode must not be blank",
		"max_lag_seconds must be > 0",
	}
	for _, needle := range contains {
		found := false
		for _, e := range err {
			if containsStr(e.Error(), needle) {
				found = true
				break
			}
		}
		assert.True(t, found, "expected error containing %q, got %v", needle, err)
	}
}

func TestValidate_AssetPartitionDefinitionRules(t *testing.T) {
	domains, teams, products := testProductControlPlane("orders-product")
	state := &DesiredState{
		Domains:      domains,
		Teams:        teams,
		DataProducts: products,
		Assets: []AssetResource{
			{
				Name: "invalid_type",
				Spec: AssetSpec{
					ProductRef:          "orders-product",
					PartitionDefinition: &AssetPartitionDefinitionSpec{Type: "weekly"},
				},
			},
			{
				Name: "missing_dynamic_group",
				Spec: AssetSpec{
					ProductRef:          "orders-product",
					PartitionDefinition: &AssetPartitionDefinitionSpec{Type: "dynamic"},
				},
			},
		},
	}

	err := Validate(state)
	require.NotEmpty(t, err)

	foundTypeErr := false
	foundDynamicGroupErr := false
	for _, e := range err {
		if containsStr(e.Error(), "partition_definition.type must be one of") && containsStr(e.Error(), "weekly") {
			foundTypeErr = true
		}
		if containsStr(e.Error(), "dynamic partition requires dynamic_group") {
			foundDynamicGroupErr = true
		}
	}

	assert.True(t, foundTypeErr, "expected partition_definition.type enum error, got %v", err)
	assert.True(t, foundDynamicGroupErr, "expected dynamic_group required error, got %v", err)
}

func TestValidate_ProductControlPlaneRefs(t *testing.T) {
	valid := &DesiredState{
		Domains: []DomainResource{{
			Name: "revenue",
			Spec: DomainSpec{Description: "Revenue domain"},
		}},
		Teams: []TeamResource{{
			Name: "analytics-engineering",
			Spec: TeamSpec{DomainRef: "revenue", ContactChannel: "#rev-data"},
		}},
		DataProducts: []DataProductResource{
			{
				Slug: "upstream-orders",
				Spec: DataProductSpec{
					Name:             "Upstream Orders",
					DomainRef:        "revenue",
					OwnerTeamRef:     "analytics-engineering",
					StewardPrincipal: "alice",
					ContactChannel:   "#rev-data",
					Contract:         ProductContractSpec{DataGrain: "one row per order"},
					SLO:              ProductSLOSpec{FreshnessSLO: "60m"},
				},
			},
			{
				Slug: "daily-orders",
				Spec: DataProductSpec{
					Name:                "Daily Orders",
					DomainRef:           "revenue",
					OwnerTeamRef:        "analytics-engineering",
					StewardPrincipal:    "alice",
					ContactChannel:      "#rev-data",
					Contract:            ProductContractSpec{DataGrain: "one row per order"},
					SLO:                 ProductSLOSpec{FreshnessSLO: "60m"},
					Outputs:             []string{"daily_orders_asset"},
					SemanticEntrypoints: []string{"orders"},
					Dependencies:        []string{"upstream-orders"},
					Versions: []DataProductVersionSpec{{
						Version:             1,
						ReleaseState:        "PUBLISHED",
						CompatibilityLevel:  "BACKWARD_COMPATIBLE",
						Outputs:             []string{"daily_orders_asset"},
						SemanticEntrypoints: []string{"orders"},
					}},
				},
			},
		},
		Assets: []AssetResource{{
			Name: "daily_orders_asset",
			Spec: AssetSpec{AssetType: "table", ProductRef: "daily-orders"},
		}},
		SemanticModels: []SemanticModelResource{{
			ModelName: "orders",
			Spec:      SemanticModelSpec{BaseModelRef: "sales.orders"},
		}},
	}

	errs := Validate(valid)
	assert.Empty(t, errs)

	invalid := &DesiredState{
		Domains: []DomainResource{{Name: "revenue", Spec: DomainSpec{}}},
		Teams:   []TeamResource{{Name: "analytics-engineering", Spec: TeamSpec{DomainRef: "missing-domain"}}},
		DataProducts: []DataProductResource{{
			Slug: "broken-product",
			Spec: DataProductSpec{
				DomainRef:           "revenue",
				OwnerTeamRef:        "missing-team",
				Outputs:             []string{"missing-asset"},
				SemanticEntrypoints: []string{"missing.project"},
				Dependencies:        []string{"missing-product"},
				PublicationIntent:   "INVALID",
				Versions: []DataProductVersionSpec{{
					Version:             0,
					ReleaseState:        "BAD",
					CompatibilityLevel:  "BAD",
					Outputs:             []string{"missing-asset"},
					SemanticEntrypoints: []string{"missing.project"},
				}},
			},
		}},
		Assets: []AssetResource{{
			Name: "orphaned-asset",
			Spec: AssetSpec{AssetType: "table", ProductRef: "missing-product"},
		}},
	}

	errs = Validate(invalid)
	require.NotEmpty(t, errs)

	expected := []string{
		"domain_ref references unknown domain",
		"owner_team_ref references unknown team",
		"output references unknown asset",
		"semantic_entrypoints references unknown semantic model",
		"dependency references unknown data product",
		"publication_intent must be DRAFT or PUBLISHED",
		"version must be > 0",
		"release_state must be DRAFT, PUBLISHED, DEPRECATED, or RETIRED",
		"compatibility_level must be BACKWARD_COMPATIBLE or BREAKING",
		"product_ref references unknown data product",
	}
	for _, needle := range expected {
		found := false
		for _, err := range errs {
			if containsStr(err.Error(), needle) {
				found = true
				break
			}
		}
		assert.True(t, found, "expected validation error containing %q, got %v", needle, errs)
	}
}

func TestValidate_AuthoringCoreRefs(t *testing.T) {
	t.Parallel()

	valid := &DesiredState{
		Workspaces: []WorkspaceResource{{
			Name: "personal",
			Spec: WorkspaceSpec{
				Kind:                  "personal",
				OwnerPrincipal:        "alice",
				DefaultProjectRef:     "personal/core",
				DefaultEnvironmentRef: "personal/core/dev",
			},
		}},
		Folders: []FolderResource{{
			Name: "analysis",
			Spec: FolderSpec{
				WorkspaceRef:          "personal",
				DefaultProjectRef:     "personal/core",
				DefaultEnvironmentRef: "personal/core/dev",
			},
		}},
		Projects: []ProjectResource{{
			Name: "core",
			Spec: ProjectSpec{
				WorkspaceRef:  "personal",
				Kind:          "personal",
				DefaultBranch: "main",
			},
		}},
		Environments: []EnvironmentResource{{
			Name: "dev",
			Spec: EnvironmentSpec{
				ProjectRef:    "personal/core",
				Kind:          "development",
				TargetCatalog: "main",
				TargetSchema:  "analytics",
			},
		}},
		Notebooks: []NotebookResource{{
			Name: "orders",
			Spec: NotebookSpec{
				WorkspaceRef:   "personal",
				FolderRef:      "personal/analysis",
				ProjectRef:     "personal/core",
				EnvironmentRef: "personal/core/dev",
				Cells: []CellSpec{{
					Type:    "sql",
					Name:    "output",
					Role:    "output",
					Content: "select 1",
				}},
			},
		}},
	}

	assert.Empty(t, Validate(valid))

	invalid := &DesiredState{
		Workspaces: []WorkspaceResource{{
			Name: "shared",
			Spec: WorkspaceSpec{Kind: "shared"},
		}},
		Folders: []FolderResource{{
			Name: "analysis",
			Spec: FolderSpec{
				WorkspaceRef:      "shared",
				ParentFolderRef:   "personal/root",
				DefaultProjectRef: "personal/core",
			},
		}},
		Projects: []ProjectResource{{
			Name: "core",
			Spec: ProjectSpec{
				WorkspaceRef: "missing",
				Kind:         "shared",
			},
		}},
		Environments: []EnvironmentResource{{
			Name: "prod",
			Spec: EnvironmentSpec{
				ProjectRef: "shared/core",
				Kind:       "production",
			},
		}},
		Notebooks: []NotebookResource{{
			Name: "broken",
			Spec: NotebookSpec{
				WorkspaceRef:   "shared",
				FolderRef:      "personal/root",
				ProjectRef:     "personal/core",
				EnvironmentRef: "personal/core/dev",
				Cells: []CellSpec{{
					Type:    "sql",
					Name:    "output",
					Role:    "output",
					Content: "select 1",
				}},
			},
		}},
	}

	errs := Validate(invalid)
	require.NotEmpty(t, errs)

	expected := []string{
		"owner_team_id is required for shared workspaces",
		"parent_folder_ref must be in the same workspace",
		"default_project_ref references unknown project",
		"workspace_ref references unknown workspace",
		"target_catalog is required",
		"folder_ref references unknown folder",
	}
	for _, needle := range expected {
		found := false
		for _, err := range errs {
			if containsStr(err.Error(), needle) {
				found = true
				break
			}
		}
		assert.True(t, found, "expected validation error containing %q, got %v", needle, errs)
	}
}
