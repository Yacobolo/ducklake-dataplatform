package declarative

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// containsStr is a helper wrapping strings.Contains for readability.
func containsStr(s, substr string) bool {
	return strings.Contains(s, substr)
}

// Helper to create *string.
func strPtr(s string) *string { return &s }

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

func TestValidate_PipelineJobCycleError(t *testing.T) {
	state := &DesiredState{
		Notebooks: []NotebookResource{{Name: "nb1", Spec: NotebookSpec{}}},
		Pipelines: []PipelineResource{{
			Name: "p1",
			Spec: PipelineSpec{
				Jobs: []PipelineJobSpec{
					{Name: "j1", Notebook: "nb1", DependsOn: []string{"j2"}},
					{Name: "j2", Notebook: "nb1", DependsOn: []string{"j1"}},
				},
			},
		}},
	}
	errs := Validate(state)
	require.NotEmpty(t, errs)
	found := false
	for _, e := range errs {
		if containsStr(e.Error(), "circular") || containsStr(e.Error(), "cycle") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected error containing 'circular' or 'cycle', got %v", errs)
}
