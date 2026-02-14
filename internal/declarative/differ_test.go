package declarative

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiff_EmptyStates(t *testing.T) {
	plan := Diff(&DesiredState{}, &DesiredState{})
	assert.False(t, plan.HasChanges())
	assert.Empty(t, plan.Actions)
}

func TestDiff_CreatePrincipals(t *testing.T) {
	desired := &DesiredState{
		Principals: []PrincipalSpec{
			{Name: "new-user", Type: "user"},
		},
	}
	actual := &DesiredState{}

	plan := Diff(desired, actual)
	require.Len(t, plan.Actions, 1)
	assert.Equal(t, OpCreate, plan.Actions[0].Operation)
	assert.Equal(t, KindPrincipal, plan.Actions[0].ResourceKind)
	assert.Equal(t, "new-user", plan.Actions[0].ResourceName)
}

func TestDiff_DeletePrincipals(t *testing.T) {
	desired := &DesiredState{}
	actual := &DesiredState{
		Principals: []PrincipalSpec{
			{Name: "old-user", Type: "user"},
		},
	}

	plan := Diff(desired, actual)
	require.Len(t, plan.Actions, 1)
	assert.Equal(t, OpDelete, plan.Actions[0].Operation)
	assert.Equal(t, "old-user", plan.Actions[0].ResourceName)
}

func TestDiff_UpdatePrincipalAdmin(t *testing.T) {
	desired := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user", IsAdmin: true}},
	}
	actual := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user", IsAdmin: false}},
	}

	plan := Diff(desired, actual)
	require.Len(t, plan.Actions, 1)
	assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
	assert.NotEmpty(t, plan.Actions[0].Changes)
}

func TestDiff_NoPrincipalChanges(t *testing.T) {
	state := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user", IsAdmin: false}},
	}

	plan := Diff(state, state)
	assert.False(t, plan.HasChanges())
}

func TestDiff_CreateAndDeleteGrants(t *testing.T) {
	desired := &DesiredState{
		Grants: []GrantSpec{
			{Principal: "user1", PrincipalType: "user", SecurableType: "catalog", Securable: "main", Privilege: "USAGE"},
		},
	}
	actual := &DesiredState{
		Grants: []GrantSpec{
			{Principal: "user1", PrincipalType: "user", SecurableType: "catalog", Securable: "main", Privilege: "SELECT"},
		},
	}

	plan := Diff(desired, actual)
	require.Len(t, plan.Actions, 2)
	// One create (USAGE) and one delete (SELECT)
	ops := map[Operation]int{}
	for _, a := range plan.Actions {
		ops[a.Operation]++
	}
	assert.Equal(t, 1, ops[OpCreate])
	assert.Equal(t, 1, ops[OpDelete])
}

func TestDiff_DeletionProtection(t *testing.T) {
	// Catalog exists on server with deletion_protection but missing from desired
	desired := &DesiredState{}
	actual := &DesiredState{
		Catalogs: []CatalogResource{
			{CatalogName: "protected", DeletionProtection: true, Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "x", DataPath: "y"}},
		},
	}

	plan := Diff(desired, actual)
	assert.Empty(t, plan.Actions, "should not create delete action for protected resource")
	require.Len(t, plan.Errors, 1, "should have one plan error for protected resource")
	assert.Contains(t, plan.Errors[0].Message, "deletion_protection")
}

func TestDiff_SortOrder(t *testing.T) {
	// Create plan with resources at different layers and verify sorting.
	desired := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user"}},
		Groups:     []GroupSpec{{Name: "group1"}},
		Grants:     []GrantSpec{{Principal: "user1", PrincipalType: "user", SecurableType: "catalog", Securable: "cat1", Privilege: "USAGE"}},
	}
	actual := &DesiredState{}

	plan := Diff(desired, actual)
	// Principal (layer 0) + Group (layer 1) + Grant (layer 5) = 3 actions
	require.Len(t, plan.Actions, 3)

	// Layer 0 (principal) should come before layer 1 (group) which should come before layer 5 (grant)
	assert.Equal(t, KindPrincipal, plan.Actions[0].ResourceKind)
	assert.Equal(t, KindGroup, plan.Actions[1].ResourceKind)
	assert.Equal(t, KindPrivilegeGrant, plan.Actions[2].ResourceKind)
}

func TestDiff_DeleteReverseOrder(t *testing.T) {
	// When deleting, higher layers should be deleted first
	desired := &DesiredState{}
	actual := &DesiredState{
		Principals: []PrincipalSpec{{Name: "user1", Type: "user"}},
		Grants:     []GrantSpec{{Principal: "user1", PrincipalType: "user", SecurableType: "catalog", Securable: "cat1", Privilege: "USAGE"}},
	}

	plan := Diff(desired, actual)
	require.Len(t, plan.Actions, 2)

	// Grant (layer 5) should be deleted before principal (layer 0)
	assert.Equal(t, KindPrivilegeGrant, plan.Actions[0].ResourceKind)
	assert.Equal(t, OpDelete, plan.Actions[0].Operation)
	assert.Equal(t, KindPrincipal, plan.Actions[1].ResourceKind)
	assert.Equal(t, OpDelete, plan.Actions[1].Operation)
}

func TestDiff_MixedCreateAndDelete(t *testing.T) {
	// Creates should come before deletes
	desired := &DesiredState{
		Principals: []PrincipalSpec{{Name: "new-user", Type: "user"}},
	}
	actual := &DesiredState{
		Principals: []PrincipalSpec{{Name: "old-user", Type: "user"}},
	}

	plan := Diff(desired, actual)
	require.Len(t, plan.Actions, 2)
	assert.Equal(t, OpCreate, plan.Actions[0].Operation)
	assert.Equal(t, OpDelete, plan.Actions[1].Operation)
}

func TestDiff_UpdateGroup(t *testing.T) {
	desired := &DesiredState{
		Groups: []GroupSpec{{Name: "g1", Description: "new desc"}},
	}
	actual := &DesiredState{
		Groups: []GroupSpec{{Name: "g1", Description: "old desc"}},
	}

	plan := Diff(desired, actual)
	hasUpdate := false
	for _, a := range plan.Actions {
		if a.Operation == OpUpdate && a.ResourceKind == KindGroup {
			hasUpdate = true
			assert.NotEmpty(t, a.Changes)
		}
	}
	assert.True(t, hasUpdate, "should have an update action for the group")
}

func TestDiff_TableColumnDiff(t *testing.T) {
	tests := []struct {
		name       string
		desired    TableSpec
		actual     TableSpec
		wantOp     Operation
		wantChange string
		wantError  bool
	}{
		{
			name: "add column",
			desired: TableSpec{
				TableType: "MANAGED",
				Columns:   []ColumnDef{{Name: "id", Type: "BIGINT"}, {Name: "name", Type: "VARCHAR"}},
			},
			actual: TableSpec{
				TableType: "MANAGED",
				Columns:   []ColumnDef{{Name: "id", Type: "BIGINT"}},
			},
			wantOp:     OpUpdate,
			wantChange: "columns.name",
		},
		{
			name: "remove column",
			desired: TableSpec{
				TableType: "MANAGED",
				Columns:   []ColumnDef{{Name: "id", Type: "BIGINT"}},
			},
			actual: TableSpec{
				TableType: "MANAGED",
				Columns:   []ColumnDef{{Name: "id", Type: "BIGINT"}, {Name: "name", Type: "VARCHAR"}},
			},
			wantOp:     OpUpdate,
			wantChange: "columns.name",
		},
		{
			name: "column type change produces error",
			desired: TableSpec{
				TableType: "MANAGED",
				Columns:   []ColumnDef{{Name: "id", Type: "VARCHAR"}},
			},
			actual: TableSpec{
				TableType: "MANAGED",
				Columns:   []ColumnDef{{Name: "id", Type: "BIGINT"}},
			},
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desired := &DesiredState{
				Tables: []TableResource{
					{CatalogName: "c", SchemaName: "s", TableName: "t", Spec: tt.desired},
				},
			}
			actual := &DesiredState{
				Tables: []TableResource{
					{CatalogName: "c", SchemaName: "s", TableName: "t", Spec: tt.actual},
				},
			}
			plan := Diff(desired, actual)

			if tt.wantError {
				require.NotEmpty(t, plan.Errors, "expected a plan error for column type change")
				assert.Contains(t, plan.Errors[0].Message, "cannot change type")
			} else {
				require.NotEmpty(t, plan.Actions)
				action := plan.Actions[0]
				assert.Equal(t, tt.wantOp, action.Operation)
				found := false
				for _, c := range action.Changes {
					if containsStr(c.Field, tt.wantChange) {
						found = true
						break
					}
				}
				assert.True(t, found, "expected change field containing %q, got %v", tt.wantChange, action.Changes)
			}
		})
	}
}

func TestDiff_SchemaUpdate(t *testing.T) {
	desired := &DesiredState{
		Schemas: []SchemaResource{
			{CatalogName: "main", SchemaName: "analytics", Spec: SchemaSpec{Comment: "new comment", Owner: "admin"}},
		},
	}
	actual := &DesiredState{
		Schemas: []SchemaResource{
			{CatalogName: "main", SchemaName: "analytics", Spec: SchemaSpec{Comment: "old comment", Owner: "admin"}},
		},
	}

	plan := Diff(desired, actual)
	require.NotEmpty(t, plan.Actions)
	assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
	assert.Equal(t, KindSchema, plan.Actions[0].ResourceKind)
	assert.Equal(t, "main.analytics", plan.Actions[0].ResourceName)

	found := false
	for _, c := range plan.Actions[0].Changes {
		if c.Field == "comment" {
			assert.Equal(t, "old comment", c.OldValue)
			assert.Equal(t, "new comment", c.NewValue)
			found = true
		}
	}
	assert.True(t, found, "expected a comment field diff")
}

func TestDiff_StorageCredentialUpdate(t *testing.T) {
	desired := &DesiredState{
		StorageCredentials: []StorageCredentialSpec{
			{Name: "cred1", CredentialType: "S3", Comment: "updated",
				S3: &S3CredentialSpec{KeyIDFromEnv: "K", SecretFromEnv: "S", Region: "us-east-1"}},
		},
	}
	actual := &DesiredState{
		StorageCredentials: []StorageCredentialSpec{
			{Name: "cred1", CredentialType: "S3", Comment: "original",
				S3: &S3CredentialSpec{KeyIDFromEnv: "K", SecretFromEnv: "S", Region: "eu-west-1"}},
		},
	}

	plan := Diff(desired, actual)
	require.NotEmpty(t, plan.Actions)
	assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
	assert.Equal(t, KindStorageCredential, plan.Actions[0].ResourceKind)

	fields := map[string]bool{}
	for _, c := range plan.Actions[0].Changes {
		fields[c.Field] = true
	}
	assert.True(t, fields["comment"], "expected comment field diff")
	assert.True(t, fields["s3.region"], "expected s3.region field diff")
}
