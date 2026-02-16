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

func TestDiff_PrincipalTypeChangeIgnored(t *testing.T) {
	// Principal type is immutable; changing it should NOT produce an update.
	desired := &DesiredState{
		Principals: []PrincipalSpec{{Name: "svc1", Type: "service_principal", IsAdmin: false}},
	}
	actual := &DesiredState{
		Principals: []PrincipalSpec{{Name: "svc1", Type: "user", IsAdmin: false}},
	}

	plan := Diff(desired, actual)
	assert.False(t, plan.HasChanges(), "type-only change should not produce an update")
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

func TestDiff_Views(t *testing.T) {
	t.Parallel()
	t.Run("create view", func(t *testing.T) {
		desired := &DesiredState{
			Views: []ViewResource{
				{CatalogName: "c", SchemaName: "s", ViewName: "v1",
					Spec: ViewSpec{ViewDefinition: "SELECT 1", Comment: "test"}},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindView, plan.Actions[0].ResourceKind)
		assert.Equal(t, "c.s.v1", plan.Actions[0].ResourceName)
	})

	t.Run("update view SQL", func(t *testing.T) {
		desired := &DesiredState{
			Views: []ViewResource{
				{CatalogName: "c", SchemaName: "s", ViewName: "v1",
					Spec: ViewSpec{ViewDefinition: "SELECT 2"}},
			},
		}
		actual := &DesiredState{
			Views: []ViewResource{
				{CatalogName: "c", SchemaName: "s", ViewName: "v1",
					Spec: ViewSpec{ViewDefinition: "SELECT 1"}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		assert.Equal(t, KindView, plan.Actions[0].ResourceKind)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "view_definition" {
				assert.Equal(t, "SELECT 1", c.OldValue)
				assert.Equal(t, "SELECT 2", c.NewValue)
				found = true
			}
		}
		assert.True(t, found, "expected view_definition field diff")
	})

	t.Run("delete view", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			Views: []ViewResource{
				{CatalogName: "c", SchemaName: "s", ViewName: "v1",
					Spec: ViewSpec{ViewDefinition: "SELECT 1"}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindView, plan.Actions[0].ResourceKind)
		assert.Equal(t, "c.s.v1", plan.Actions[0].ResourceName)
	})
}

func TestDiff_Volumes(t *testing.T) {
	t.Parallel()
	t.Run("create volume", func(t *testing.T) {
		desired := &DesiredState{
			Volumes: []VolumeResource{
				{CatalogName: "c", SchemaName: "s", VolumeName: "vol1",
					Spec: VolumeSpec{VolumeType: "MANAGED", Comment: "v"}},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindVolume, plan.Actions[0].ResourceKind)
		assert.Equal(t, "c.s.vol1", plan.Actions[0].ResourceName)
	})

	t.Run("update volume comment", func(t *testing.T) {
		desired := &DesiredState{
			Volumes: []VolumeResource{
				{CatalogName: "c", SchemaName: "s", VolumeName: "vol1",
					Spec: VolumeSpec{VolumeType: "MANAGED", Comment: "new"}},
			},
		}
		actual := &DesiredState{
			Volumes: []VolumeResource{
				{CatalogName: "c", SchemaName: "s", VolumeName: "vol1",
					Spec: VolumeSpec{VolumeType: "MANAGED", Comment: "old"}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "comment" {
				assert.Equal(t, "old", c.OldValue)
				assert.Equal(t, "new", c.NewValue)
				found = true
			}
		}
		assert.True(t, found, "expected comment field diff")
	})

	t.Run("delete volume", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			Volumes: []VolumeResource{
				{CatalogName: "c", SchemaName: "s", VolumeName: "vol1",
					Spec: VolumeSpec{VolumeType: "MANAGED"}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindVolume, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_RowFilters(t *testing.T) {
	t.Parallel()
	t.Run("create row filter with bindings", func(t *testing.T) {
		desired := &DesiredState{
			RowFilters: []RowFilterResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Filters: []RowFilterSpec{
						{Name: "rf1", FilterSQL: "region = 'US'",
							Bindings: []FilterBindingRef{{Principal: "user1", PrincipalType: "user"}}},
					}},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		// Should create: the filter + its binding
		require.Len(t, plan.Actions, 2)
		ops := map[ResourceKind]int{}
		for _, a := range plan.Actions {
			assert.Equal(t, OpCreate, a.Operation)
			ops[a.ResourceKind]++
		}
		assert.Equal(t, 1, ops[KindRowFilter])
		assert.Equal(t, 1, ops[KindRowFilterBinding])
	})

	t.Run("update filter SQL", func(t *testing.T) {
		desired := &DesiredState{
			RowFilters: []RowFilterResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Filters: []RowFilterSpec{
						{Name: "rf1", FilterSQL: "region = 'EU'"},
					}},
			},
		}
		actual := &DesiredState{
			RowFilters: []RowFilterResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Filters: []RowFilterSpec{
						{Name: "rf1", FilterSQL: "region = 'US'"},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		assert.Equal(t, KindRowFilter, plan.Actions[0].ResourceKind)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "filter_sql" {
				found = true
			}
		}
		assert.True(t, found, "expected filter_sql field diff")
	})

	t.Run("delete row filter", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			RowFilters: []RowFilterResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Filters: []RowFilterSpec{
						{Name: "rf1", FilterSQL: "1=1"},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindRowFilter, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_RowFilterBindings(t *testing.T) {
	t.Parallel()
	t.Run("add binding", func(t *testing.T) {
		desired := &DesiredState{
			RowFilters: []RowFilterResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Filters: []RowFilterSpec{
						{Name: "rf1", FilterSQL: "1=1",
							Bindings: []FilterBindingRef{
								{Principal: "user1", PrincipalType: "user"},
								{Principal: "user2", PrincipalType: "user"},
							}},
					}},
			},
		}
		actual := &DesiredState{
			RowFilters: []RowFilterResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Filters: []RowFilterSpec{
						{Name: "rf1", FilterSQL: "1=1",
							Bindings: []FilterBindingRef{
								{Principal: "user1", PrincipalType: "user"},
							}},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindRowFilterBinding, plan.Actions[0].ResourceKind)
		assert.Contains(t, plan.Actions[0].ResourceName, "user2")
	})

	t.Run("remove binding", func(t *testing.T) {
		desired := &DesiredState{
			RowFilters: []RowFilterResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Filters: []RowFilterSpec{
						{Name: "rf1", FilterSQL: "1=1",
							Bindings: []FilterBindingRef{}},
					}},
			},
		}
		actual := &DesiredState{
			RowFilters: []RowFilterResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Filters: []RowFilterSpec{
						{Name: "rf1", FilterSQL: "1=1",
							Bindings: []FilterBindingRef{
								{Principal: "user1", PrincipalType: "user"},
							}},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindRowFilterBinding, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_ColumnMasks(t *testing.T) {
	t.Parallel()
	t.Run("create column mask", func(t *testing.T) {
		desired := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'",
							Bindings: []MaskBindingRef{{Principal: "user1", PrincipalType: "user", SeeOriginal: true}}},
					}},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		// Should create: the mask + its binding
		require.Len(t, plan.Actions, 2)
		kinds := map[ResourceKind]int{}
		for _, a := range plan.Actions {
			assert.Equal(t, OpCreate, a.Operation)
			kinds[a.ResourceKind]++
		}
		assert.Equal(t, 1, kinds[KindColumnMask])
		assert.Equal(t, 1, kinds[KindColumnMaskBinding])
	})

	t.Run("update mask expression", func(t *testing.T) {
		desired := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'XXX'"},
					}},
			},
		}
		actual := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'"},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		assert.Equal(t, KindColumnMask, plan.Actions[0].ResourceKind)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "mask_expression" {
				found = true
			}
		}
		assert.True(t, found, "expected mask_expression field diff")
	})

	t.Run("delete column mask", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'"},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindColumnMask, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_ColumnMaskBindings(t *testing.T) {
	t.Parallel()
	t.Run("add binding", func(t *testing.T) {
		desired := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'",
							Bindings: []MaskBindingRef{
								{Principal: "user1", PrincipalType: "user"},
								{Principal: "user2", PrincipalType: "user"},
							}},
					}},
			},
		}
		actual := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'",
							Bindings: []MaskBindingRef{
								{Principal: "user1", PrincipalType: "user"},
							}},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindColumnMaskBinding, plan.Actions[0].ResourceKind)
		assert.Contains(t, plan.Actions[0].ResourceName, "user2")
	})

	t.Run("remove binding", func(t *testing.T) {
		desired := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'",
							Bindings: []MaskBindingRef{}},
					}},
			},
		}
		actual := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'",
							Bindings: []MaskBindingRef{
								{Principal: "user1", PrincipalType: "user"},
							}},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindColumnMaskBinding, plan.Actions[0].ResourceKind)
	})

	t.Run("update SeeOriginal", func(t *testing.T) {
		desired := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'",
							Bindings: []MaskBindingRef{
								{Principal: "user1", PrincipalType: "user", SeeOriginal: true},
							}},
					}},
			},
		}
		actual := &DesiredState{
			ColumnMasks: []ColumnMaskResource{
				{CatalogName: "c", SchemaName: "s", TableName: "t",
					Masks: []ColumnMaskSpec{
						{Name: "m1", ColumnName: "ssn", MaskExpression: "'***'",
							Bindings: []MaskBindingRef{
								{Principal: "user1", PrincipalType: "user", SeeOriginal: false},
							}},
					}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		assert.Equal(t, KindColumnMaskBinding, plan.Actions[0].ResourceKind)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "see_original" {
				found = true
			}
		}
		assert.True(t, found, "expected see_original field diff")
	})
}

func TestDiff_Tags(t *testing.T) {
	t.Parallel()
	t.Run("create tag", func(t *testing.T) {
		desired := &DesiredState{
			Tags: []TagSpec{{Key: "pii", Value: strPtr("true")}},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindTag, plan.Actions[0].ResourceKind)
		assert.Equal(t, "pii:true", plan.Actions[0].ResourceName)
	})

	t.Run("delete tag", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			Tags: []TagSpec{{Key: "pii"}},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindTag, plan.Actions[0].ResourceKind)
		assert.Equal(t, "pii", plan.Actions[0].ResourceName)
	})
}

func TestDiff_TagAssignments(t *testing.T) {
	t.Parallel()
	t.Run("add assignment", func(t *testing.T) {
		desired := &DesiredState{
			TagAssignments: []TagAssignmentSpec{
				{Tag: "pii:true", SecurableType: "table", Securable: "main.s.t"},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindTagAssignment, plan.Actions[0].ResourceKind)
	})

	t.Run("remove assignment", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			TagAssignments: []TagAssignmentSpec{
				{Tag: "pii:true", SecurableType: "table", Securable: "main.s.t"},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindTagAssignment, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_ExternalLocations(t *testing.T) {
	t.Parallel()
	t.Run("create", func(t *testing.T) {
		desired := &DesiredState{
			ExternalLocations: []ExternalLocationSpec{
				{Name: "loc1", URL: "s3://bucket/path", CredentialName: "cred1", StorageType: "S3"},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindExternalLocation, plan.Actions[0].ResourceKind)
		assert.Equal(t, "loc1", plan.Actions[0].ResourceName)
	})

	t.Run("update URL", func(t *testing.T) {
		desired := &DesiredState{
			ExternalLocations: []ExternalLocationSpec{
				{Name: "loc1", URL: "s3://bucket/new-path", CredentialName: "cred1"},
			},
		}
		actual := &DesiredState{
			ExternalLocations: []ExternalLocationSpec{
				{Name: "loc1", URL: "s3://bucket/old-path", CredentialName: "cred1"},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		assert.Equal(t, KindExternalLocation, plan.Actions[0].ResourceKind)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "url" {
				found = true
			}
		}
		assert.True(t, found, "expected url field diff")
	})

	t.Run("delete", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			ExternalLocations: []ExternalLocationSpec{
				{Name: "loc1", URL: "s3://bucket/path", CredentialName: "cred1"},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindExternalLocation, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_ComputeEndpoints(t *testing.T) {
	t.Parallel()
	t.Run("create", func(t *testing.T) {
		desired := &DesiredState{
			ComputeEndpoints: []ComputeEndpointSpec{
				{Name: "ep1", Type: "LOCAL", Size: "small"},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindComputeEndpoint, plan.Actions[0].ResourceKind)
		assert.Equal(t, "ep1", plan.Actions[0].ResourceName)
	})

	t.Run("update size", func(t *testing.T) {
		desired := &DesiredState{
			ComputeEndpoints: []ComputeEndpointSpec{
				{Name: "ep1", Type: "LOCAL", Size: "large"},
			},
		}
		actual := &DesiredState{
			ComputeEndpoints: []ComputeEndpointSpec{
				{Name: "ep1", Type: "LOCAL", Size: "small"},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "size" {
				found = true
			}
		}
		assert.True(t, found, "expected size field diff")
	})

	t.Run("delete", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			ComputeEndpoints: []ComputeEndpointSpec{
				{Name: "ep1", Type: "LOCAL"},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindComputeEndpoint, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_ComputeAssignments(t *testing.T) {
	t.Parallel()
	t.Run("create", func(t *testing.T) {
		desired := &DesiredState{
			ComputeAssignments: []ComputeAssignmentSpec{
				{Endpoint: "ep1", Principal: "user1", PrincipalType: "user", IsDefault: true},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindComputeAssignment, plan.Actions[0].ResourceKind)
		assert.Equal(t, "ep1->user:user1", plan.Actions[0].ResourceName)
	})

	t.Run("delete", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			ComputeAssignments: []ComputeAssignmentSpec{
				{Endpoint: "ep1", Principal: "user1", PrincipalType: "user"},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindComputeAssignment, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_APIKeys(t *testing.T) {
	t.Parallel()
	t.Run("create", func(t *testing.T) {
		desired := &DesiredState{
			APIKeys: []APIKeySpec{
				{Name: "key1", Principal: "user1"},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindAPIKey, plan.Actions[0].ResourceKind)
		assert.Equal(t, "key1", plan.Actions[0].ResourceName)
	})

	t.Run("delete", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			APIKeys: []APIKeySpec{
				{Name: "key1", Principal: "user1"},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindAPIKey, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_GroupMembers(t *testing.T) {
	t.Parallel()
	t.Run("add member", func(t *testing.T) {
		desired := &DesiredState{
			Groups: []GroupSpec{
				{Name: "g1", Members: []MemberRef{
					{Name: "user1", Type: "user"},
					{Name: "user2", Type: "user"},
				}},
			},
		}
		actual := &DesiredState{
			Groups: []GroupSpec{
				{Name: "g1", Members: []MemberRef{
					{Name: "user1", Type: "user"},
				}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindGroupMembership, plan.Actions[0].ResourceKind)
		assert.Contains(t, plan.Actions[0].ResourceName, "user2")
	})

	t.Run("remove member", func(t *testing.T) {
		desired := &DesiredState{
			Groups: []GroupSpec{
				{Name: "g1", Members: []MemberRef{}},
			},
		}
		actual := &DesiredState{
			Groups: []GroupSpec{
				{Name: "g1", Members: []MemberRef{
					{Name: "user1", Type: "user"},
				}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindGroupMembership, plan.Actions[0].ResourceKind)
		assert.Contains(t, plan.Actions[0].ResourceName, "user1")
	})
}

func TestDiff_Notebooks(t *testing.T) {
	t.Parallel()
	t.Run("create", func(t *testing.T) {
		desired := &DesiredState{
			Notebooks: []NotebookResource{
				{Name: "nb1", Spec: NotebookSpec{
					Description: "test notebook",
					Cells:       []CellSpec{{Type: "sql", Content: "SELECT 1"}},
				}},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpCreate, plan.Actions[0].Operation)
		assert.Equal(t, KindNotebook, plan.Actions[0].ResourceKind)
		assert.Equal(t, "nb1", plan.Actions[0].ResourceName)
	})

	t.Run("update cells differ", func(t *testing.T) {
		desired := &DesiredState{
			Notebooks: []NotebookResource{
				{Name: "nb1", Spec: NotebookSpec{
					Description: "test",
					Cells: []CellSpec{
						{Type: "sql", Content: "SELECT 1"},
						{Type: "sql", Content: "SELECT 2"},
					},
				}},
			},
		}
		actual := &DesiredState{
			Notebooks: []NotebookResource{
				{Name: "nb1", Spec: NotebookSpec{
					Description: "test",
					Cells: []CellSpec{
						{Type: "sql", Content: "SELECT 1"},
					},
				}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		assert.Equal(t, KindNotebook, plan.Actions[0].ResourceKind)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "cells" {
				found = true
			}
		}
		assert.True(t, found, "expected cells field diff")
	})

	t.Run("delete", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			Notebooks: []NotebookResource{
				{Name: "nb1", Spec: NotebookSpec{Description: "test"}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpDelete, plan.Actions[0].Operation)
		assert.Equal(t, KindNotebook, plan.Actions[0].ResourceKind)
	})
}

func TestDiff_Pipelines(t *testing.T) {
	t.Parallel()
	t.Run("create with jobs", func(t *testing.T) {
		desired := &DesiredState{
			Pipelines: []PipelineResource{
				{Name: "pl1", Spec: PipelineSpec{
					Description:  "test pipeline",
					ScheduleCron: "0 * * * *",
					Jobs: []PipelineJobSpec{
						{Name: "job1", Notebook: "nb1"},
					},
				}},
			},
		}
		actual := &DesiredState{}
		plan := Diff(desired, actual)
		// Should create: the pipeline + its job
		require.Len(t, plan.Actions, 2)
		kinds := map[ResourceKind]int{}
		for _, a := range plan.Actions {
			assert.Equal(t, OpCreate, a.Operation)
			kinds[a.ResourceKind]++
		}
		assert.Equal(t, 1, kinds[KindPipeline])
		assert.Equal(t, 1, kinds[KindPipelineJob])
	})

	t.Run("update schedule", func(t *testing.T) {
		desired := &DesiredState{
			Pipelines: []PipelineResource{
				{Name: "pl1", Spec: PipelineSpec{
					Description:  "test",
					ScheduleCron: "0 0 * * *",
				}},
			},
		}
		actual := &DesiredState{
			Pipelines: []PipelineResource{
				{Name: "pl1", Spec: PipelineSpec{
					Description:  "test",
					ScheduleCron: "0 * * * *",
				}},
			},
		}
		plan := Diff(desired, actual)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, OpUpdate, plan.Actions[0].Operation)
		assert.Equal(t, KindPipeline, plan.Actions[0].ResourceKind)
		found := false
		for _, c := range plan.Actions[0].Changes {
			if c.Field == "schedule_cron" {
				found = true
			}
		}
		assert.True(t, found, "expected schedule_cron field diff")
	})

	t.Run("delete with jobs", func(t *testing.T) {
		desired := &DesiredState{}
		actual := &DesiredState{
			Pipelines: []PipelineResource{
				{Name: "pl1", Spec: PipelineSpec{
					Jobs: []PipelineJobSpec{
						{Name: "job1", Notebook: "nb1"},
					},
				}},
			},
		}
		plan := Diff(desired, actual)
		// Should delete: the job + the pipeline
		require.Len(t, plan.Actions, 2)
		kinds := map[ResourceKind]int{}
		for _, a := range plan.Actions {
			assert.Equal(t, OpDelete, a.Operation)
			kinds[a.ResourceKind]++
		}
		assert.Equal(t, 1, kinds[KindPipeline])
		assert.Equal(t, 1, kinds[KindPipelineJob])
	})
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

func TestDiff_CreateModels(t *testing.T) {
	desired := &DesiredState{
		Models: []ModelResource{
			{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{
				Materialization: "TABLE",
				SQL:             "SELECT 1",
			}},
		},
	}
	actual := &DesiredState{}

	plan := Diff(desired, actual)
	require.Len(t, plan.Actions, 1)
	assert.Equal(t, OpCreate, plan.Actions[0].Operation)
	assert.Equal(t, KindModel, plan.Actions[0].ResourceKind)
	assert.Equal(t, "sales.stg_orders", plan.Actions[0].ResourceName)
}

func TestDiff_DeleteModels(t *testing.T) {
	desired := &DesiredState{}
	actual := &DesiredState{
		Models: []ModelResource{
			{ProjectName: "sales", ModelName: "old_model", Spec: ModelSpec{
				Materialization: "VIEW",
				SQL:             "SELECT 1",
			}},
		},
	}

	plan := Diff(desired, actual)
	require.Len(t, plan.Actions, 1)
	assert.Equal(t, OpDelete, plan.Actions[0].Operation)
	assert.Equal(t, KindModel, plan.Actions[0].ResourceKind)
	assert.Equal(t, "sales.old_model", plan.Actions[0].ResourceName)
}

func TestDiff_UpdateModel(t *testing.T) {
	desired := &DesiredState{
		Models: []ModelResource{
			{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{
				Materialization: "TABLE",
				Description:     "updated description",
				SQL:             "SELECT 2",
				Tags:            []string{"v2"},
			}},
		},
	}
	actual := &DesiredState{
		Models: []ModelResource{
			{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{
				Materialization: "VIEW",
				Description:     "original",
				SQL:             "SELECT 1",
				Tags:            []string{"v1"},
			}},
		},
	}

	plan := Diff(desired, actual)
	require.NotEmpty(t, plan.Actions)
	action := plan.Actions[0]
	assert.Equal(t, OpUpdate, action.Operation)
	assert.Equal(t, KindModel, action.ResourceKind)

	fields := map[string]bool{}
	for _, c := range action.Changes {
		fields[c.Field] = true
	}
	assert.True(t, fields["materialization"], "expected materialization diff")
	assert.True(t, fields["description"], "expected description diff")
	assert.True(t, fields["sql"], "expected sql diff")
	assert.True(t, fields["tags"], "expected tags diff")
}

func TestDiff_NoModelChanges(t *testing.T) {
	state := &DesiredState{
		Models: []ModelResource{
			{ProjectName: "sales", ModelName: "stg_orders", Spec: ModelSpec{
				Materialization: "VIEW",
				SQL:             "SELECT 1",
			}},
		},
	}

	plan := Diff(state, state)
	// No model actions expected.
	for _, a := range plan.Actions {
		assert.NotEqual(t, KindModel, a.ResourceKind, "expected no model actions")
	}
}

func TestDiff_GroupMembershipDeleteEmptyName(t *testing.T) {
	t.Parallel()

	desired := &DesiredState{
		Groups: []GroupSpec{
			{Name: "admins"}, // no members desired
		},
	}
	actual := &DesiredState{
		Groups: []GroupSpec{
			{
				Name: "admins",
				Members: []MemberRef{
					{Name: "", Type: "user", MemberID: "orphan-uuid"},
				},
			},
		},
	}

	plan := Diff(desired, actual)

	var found bool
	for _, a := range plan.Actions {
		if a.ResourceKind == KindGroupMembership && a.Operation == OpDelete {
			found = true
			member, ok := a.Actual.(MemberRef)
			require.True(t, ok)
			assert.Empty(t, member.Name)
			assert.Equal(t, "orphan-uuid", member.MemberID)
			assert.Equal(t, "user", member.Type)
		}
	}
	assert.True(t, found, "expected a delete action for the empty-name membership")
}
