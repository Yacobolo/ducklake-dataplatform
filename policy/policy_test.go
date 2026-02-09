package policy

import (
	"testing"
)

func defaultStore() *PolicyStore {
	store := NewPolicyStore()

	store.AddRole(&Role{
		Name:          "admin",
		AllowedTables: []string{"*"},
	})

	store.AddRole(&Role{
		Name:          "first_class_analyst",
		AllowedTables: []string{"titanic"},
		RLSRules: []RLSRule{
			{Table: "titanic", Column: "Pclass", Operator: OpEqual, Value: int64(1)},
		},
	})

	store.AddRole(&Role{
		Name:          "survivor_researcher",
		AllowedTables: []string{"titanic"},
		RLSRules: []RLSRule{
			{Table: "titanic", Column: "Survived", Operator: OpEqual, Value: int64(1)},
		},
	})

	store.AddRole(&Role{
		Name:          "no_access",
		AllowedTables: []string{},
	})

	store.AddRole(&Role{
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
