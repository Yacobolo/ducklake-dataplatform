package policy

import (
	"sync"
	"testing"
)

func defaultStore() *PolicyStore {
	store := NewPolicyStore()

	store.UpdateRole(&Role{
		Name:          "admin",
		AllowedTables: []string{"*"},
	})

	store.UpdateRole(&Role{
		Name:          "first_class_analyst",
		AllowedTables: []string{"titanic"},
		RLSRules: []RLSRule{
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
		},
	})

	store.UpdateRole(&Role{
		Name:          "survivor_researcher",
		AllowedTables: []string{"titanic"},
		RLSRules: []RLSRule{
			{Table: "titanic", Column: "Survived", Operator: OpEqual, Value: int64(1)},
		},
	})

	store.UpdateRole(&Role{
		Name:          "no_access",
		AllowedTables: []string{},
	})

	store.UpdateRole(&Role{
		Name:          "deny_override",
		AllowedTables: []string{"*"},
		DeniedTables:  []string{"secret_data"},
	})

	return store
}

func TestAdminCanAccessAnyTable(t *testing.T) {
	store := defaultStore()
	role, err := store.GetRole("admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, table := range []string{"titanic", "secret_data", "anything"} {
		if !role.CanAccess(table) {
			t.Errorf("admin should be able to access %q", table)
		}
	}
}

func TestRoleCanAccessAllowedTable(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("first_class_analyst")

	if !role.CanAccess("titanic") {
		t.Error("first_class_analyst should be able to access titanic")
	}
}

func TestRoleDeniedUnlistedTable(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("first_class_analyst")

	if role.CanAccess("secret_data") {
		t.Error("first_class_analyst should NOT be able to access secret_data")
	}
}

func TestDenyOverridesAllow(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("deny_override")

	if role.CanAccess("secret_data") {
		t.Error("deny should override wildcard allow for secret_data")
	}
	if !role.CanAccess("titanic") {
		t.Error("deny_override should still access titanic (not in deny list)")
	}
}

func TestGetRLSRulesForTable(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("first_class_analyst")

	rules := role.RLSRulesForTable("titanic")
	if len(rules) != 1 {
		t.Fatalf("expected 1 RLS rule, got %d", len(rules))
	}
	if rules[0].Column != "Pclass" {
		t.Errorf("expected column Pclass, got %s", rules[0].Column)
	}
	if rules[0].Operator != OpEqual {
		t.Errorf("expected operator eq, got %s", rules[0].Operator)
	}
	if rules[0].Value != int64(1) {
		t.Errorf("expected value 1, got %v", rules[0].Value)
	}
}

func TestGetRLSRulesForUnrelatedTable(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("first_class_analyst")

	rules := role.RLSRulesForTable("other_table")
	if len(rules) != 0 {
		t.Errorf("expected 0 RLS rules for other_table, got %d", len(rules))
	}
}

func TestNoRLSForAdmin(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("admin")

	rules := role.RLSRulesForTable("titanic")
	if len(rules) != 0 {
		t.Errorf("admin should have no RLS rules, got %d", len(rules))
	}
}

func TestUnknownRoleReturnsError(t *testing.T) {
	store := defaultStore()
	_, err := store.GetRole("nonexistent")
	if err == nil {
		t.Error("expected error for unknown role")
	}
}

func TestEmptyRoleHasNoAccess(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("no_access")

	if role.CanAccess("titanic") {
		t.Error("no_access role should not be able to access titanic")
	}
	if role.CanAccess("anything") {
		t.Error("no_access role should not be able to access anything")
	}
}

func TestCheckAccessMultipleTables(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("first_class_analyst")

	// Should pass with only allowed tables
	if err := role.CheckAccess([]string{"titanic"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should fail with a disallowed table
	if err := role.CheckAccess([]string{"titanic", "secret_data"}); err == nil {
		t.Error("expected error when accessing secret_data")
	}
}

func TestCheckAccessEmptyTableList(t *testing.T) {
	store := defaultStore()
	role, _ := store.GetRole("first_class_analyst")
	if err := role.CheckAccess([]string{}); err != nil {
		t.Errorf("empty table list should succeed, got: %v", err)
	}
}

func TestAddRoleDuplicateReturnsError(t *testing.T) {
	store := NewPolicyStore()
	err := store.AddRole(&Role{Name: "test", AllowedTables: []string{"*"}})
	if err != nil {
		t.Fatalf("first AddRole should succeed: %v", err)
	}
	err = store.AddRole(&Role{Name: "test", AllowedTables: []string{"other"}})
	if err == nil {
		t.Error("second AddRole with same name should return error")
	}
}

func TestUpdateRoleOverwrites(t *testing.T) {
	store := NewPolicyStore()
	store.UpdateRole(&Role{Name: "test", AllowedTables: []string{"a"}})
	store.UpdateRole(&Role{Name: "test", AllowedTables: []string{"b"}})

	role, err := store.GetRole("test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(role.AllowedTables) != 1 || role.AllowedTables[0] != "b" {
		t.Errorf("expected AllowedTables=[b], got %v", role.AllowedTables)
	}
}

func TestRemoveRole(t *testing.T) {
	store := NewPolicyStore()
	store.UpdateRole(&Role{Name: "removable", AllowedTables: []string{"*"}})

	err := store.RemoveRole("removable")
	if err != nil {
		t.Fatalf("RemoveRole should succeed: %v", err)
	}

	_, err = store.GetRole("removable")
	if err == nil {
		t.Error("GetRole should fail after RemoveRole")
	}
}

func TestRemoveNonexistentRoleReturnsError(t *testing.T) {
	store := NewPolicyStore()
	err := store.RemoveRole("ghost")
	if err == nil {
		t.Error("RemoveRole for nonexistent role should return error")
	}
}

func TestConcurrentPolicyAccess(t *testing.T) {
	store := NewPolicyStore()

	// Pre-populate with some roles
	for i := 0; i < 10; i++ {
		store.UpdateRole(&Role{
			Name:          "role_" + string(rune('a'+i)),
			AllowedTables: []string{"*"},
		})
	}

	// Run concurrent reads and writes
	var wg sync.WaitGroup
	const goroutines = 50

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if id%3 == 0 {
				// Write
				store.UpdateRole(&Role{
					Name:          "concurrent_role",
					AllowedTables: []string{"*"},
				})
			} else {
				// Read
				store.GetRole("role_a")
			}
		}(i)
	}

	wg.Wait()
	// If we get here without panic, the mutex is working
}

func TestRLSRulesWithDifferentOperators(t *testing.T) {
	role := &Role{
		Name:          "analyst",
		AllowedTables: []string{"data"},
		RLSRules: []RLSRule{
			{Table: "data", Column: "age", Operator: OpGreaterThan, Value: int64(18)},
			{Table: "data", Column: "status", Operator: OpNotEqual, Value: "inactive"},
			{Table: "data", Column: "score", Operator: OpLessEqual, Value: int64(100)},
		},
	}

	rules := role.RLSRulesForTable("data")
	if len(rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rules))
	}
	if rules[0].Operator != OpGreaterThan {
		t.Errorf("expected gt, got %s", rules[0].Operator)
	}
	if rules[1].Operator != OpNotEqual {
		t.Errorf("expected neq, got %s", rules[1].Operator)
	}
	if rules[2].Operator != OpLessEqual {
		t.Errorf("expected lte, got %s", rules[2].Operator)
	}
}
