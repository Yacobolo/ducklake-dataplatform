package catalog

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	dbstore "duck-demo/db/catalog"
)

// setupTestService creates a CatalogService with a temporary SQLite DB,
// runs migrations, and creates mock DuckLake metadata tables.
func setupTestService(t *testing.T) (*CatalogService, context.Context) {
	t.Helper()

	tmpDir := t.TempDir()
	db, err := sql.Open("sqlite3", tmpDir+"/test.sqlite?_foreign_keys=on")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if err := RunMigrations(db); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Create mock DuckLake catalog tables
	_, err = db.Exec(`
		CREATE TABLE ducklake_schema (
			schema_id INTEGER PRIMARY KEY,
			schema_uuid TEXT,
			begin_snapshot INTEGER,
			end_snapshot INTEGER,
			schema_name TEXT,
			path TEXT,
			path_is_relative INTEGER
		);
		CREATE TABLE ducklake_table (
			table_id INTEGER,
			table_uuid TEXT,
			begin_snapshot INTEGER,
			end_snapshot INTEGER,
			schema_id INTEGER,
			table_name TEXT,
			path TEXT,
			path_is_relative INTEGER
		);
		INSERT INTO ducklake_schema (schema_id, schema_name, begin_snapshot)
		VALUES (0, 'main', 0);
		INSERT INTO ducklake_table (table_id, table_name, schema_id, begin_snapshot)
		VALUES (1, 'titanic', 0, 1);
		INSERT INTO ducklake_table (table_id, table_name, schema_id, begin_snapshot)
		VALUES (2, 'orders', 0, 1);
	`)
	if err != nil {
		t.Fatalf("create mock tables: %v", err)
	}

	return NewCatalogService(db), context.Background()
}

func TestAdminBypassesAllChecks(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	if err != nil {
		t.Fatalf("create admin: %v", err)
	}

	// Admin should have access to anything without explicit grants
	ok, err := cat.CheckPrivilege(ctx, "admin", SecurableTable, 1, PrivSelect)
	if err != nil {
		t.Fatalf("check privilege: %v", err)
	}
	if !ok {
		t.Error("admin should bypass all privilege checks")
	}
}

func TestUserWithNoGrantsDenied(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "nobody", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	ok, err := cat.CheckPrivilege(ctx, "nobody", SecurableTable, 1, PrivSelect)
	if err != nil {
		t.Fatalf("check privilege: %v", err)
	}
	if ok {
		t.Error("user with no grants should be denied")
	}
}

func TestDirectTableGrant(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})

	// Grant USAGE on schema (required gate)
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: 0,
		Privilege: PrivUsage,
	})

	// Grant SELECT on table
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableTable, SecurableID: 1,
		Privilege: PrivSelect,
	})

	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, 1, PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("user with direct SELECT grant should have access")
	}
}

func TestUsageGateRequired(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})

	// Grant SELECT on table but NOT USAGE on schema
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableTable, SecurableID: 1,
		Privilege: PrivSelect,
	})

	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, 1, PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if ok {
		t.Error("user without USAGE on schema should be denied even with SELECT on table")
	}
}

func TestSchemaLevelInheritance(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})

	// Grant USAGE + SELECT on schema (should cascade to tables)
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: 0,
		Privilege: PrivUsage,
	})
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: 0,
		Privilege: PrivSelect,
	})

	// Should inherit SELECT on any table within the schema
	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, 1, PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("SELECT on schema should cascade to tables within it")
	}

	// Also check table 2 (orders)
	ok, err = cat.CheckPrivilege(ctx, "analyst", SecurableTable, 2, PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("SELECT on schema should cascade to all tables within it")
	}
}

func TestCatalogLevelInheritance(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "superuser", Type: "user", IsAdmin: 0,
	})

	// Grant ALL_PRIVILEGES at catalog level
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableCatalog, SecurableID: CatalogID,
		Privilege: PrivAllPrivileges,
	})

	// Should inherit to schema USAGE and table SELECT
	ok, err := cat.CheckPrivilege(ctx, "superuser", SecurableTable, 1, PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("ALL_PRIVILEGES at catalog level should cascade to table SELECT")
	}
}

func TestGroupMembership(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})

	group, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})
	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: group.ID, MemberType: "user", MemberID: user.ID,
	})

	// Grant to group
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: group.ID, PrincipalType: "group",
		SecurableType: SecurableSchema, SecurableID: 0,
		Privilege: PrivUsage,
	})
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: group.ID, PrincipalType: "group",
		SecurableType: SecurableTable, SecurableID: 1,
		Privilege: PrivSelect,
	})

	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, 1, PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("user in group with SELECT should have access")
	}
}

func TestNestedGroupMembership(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})

	innerGroup, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})
	outerGroup, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "data_team"})

	// user -> analysts -> data_team
	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: innerGroup.ID, MemberType: "user", MemberID: user.ID,
	})
	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: outerGroup.ID, MemberType: "group", MemberID: innerGroup.ID,
	})

	// Grant to outer group
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: outerGroup.ID, PrincipalType: "group",
		SecurableType: SecurableCatalog, SecurableID: CatalogID,
		Privilege: PrivAllPrivileges,
	})

	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, 1, PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("user in nested group should inherit privileges from outer group")
	}
}

func TestRowFilterForPrincipal(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})

	// Create and bind row filter
	filter, _ := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:   1,
		FilterSql: `"Pclass" = 1`,
	})
	q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: filter.ID, PrincipalID: user.ID, PrincipalType: "user",
	})

	result, err := cat.GetEffectiveRowFilter(ctx, "analyst", 1)
	if err != nil {
		t.Fatalf("get row filter: %v", err)
	}
	if result == nil {
		t.Fatal("expected row filter, got nil")
	}
	if *result != `"Pclass" = 1` {
		t.Errorf("expected filter \"Pclass\" = 1, got %q", *result)
	}
}

func TestRowFilterForGroupMember(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	group, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})
	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: group.ID, MemberType: "user", MemberID: user.ID,
	})

	filter, _ := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:   1,
		FilterSql: `"Pclass" = 1`,
	})
	q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: filter.ID, PrincipalID: group.ID, PrincipalType: "group",
	})

	result, err := cat.GetEffectiveRowFilter(ctx, "analyst", 1)
	if err != nil {
		t.Fatalf("get row filter: %v", err)
	}
	if result == nil {
		t.Fatal("expected row filter via group, got nil")
	}
}

func TestAdminNoRowFilter(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	_, _ = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin", Type: "user", IsAdmin: 1,
	})

	// Even if there's a filter, admin should bypass
	filter, _ := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:   1,
		FilterSql: `"Pclass" = 1`,
	})
	q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: filter.ID, PrincipalID: 1, PrincipalType: "user",
	})

	result, err := cat.GetEffectiveRowFilter(ctx, "admin", 1)
	if err != nil {
		t.Fatalf("get row filter: %v", err)
	}
	if result != nil {
		t.Error("admin should have no row filter")
	}
}

func TestColumnMaskForPrincipal(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})

	mask, _ := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		TableID:        1,
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: mask.ID, PrincipalID: user.ID,
		PrincipalType: "user", SeeOriginal: 0,
	})

	masks, err := cat.GetEffectiveColumnMasks(ctx, "analyst", 1)
	if err != nil {
		t.Fatalf("get column masks: %v", err)
	}
	if masks == nil {
		t.Fatal("expected column masks, got nil")
	}
	if masks["Name"] != "'***'" {
		t.Errorf("expected Name mask = '***', got %q", masks["Name"])
	}
}

func TestColumnMaskSeeOriginal(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	user, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst", Type: "user", IsAdmin: 0,
	})

	mask, _ := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		TableID:        1,
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	// see_original = 1 means user sees the real value
	q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: mask.ID, PrincipalID: user.ID,
		PrincipalType: "user", SeeOriginal: 1,
	})

	masks, err := cat.GetEffectiveColumnMasks(ctx, "analyst", 1)
	if err != nil {
		t.Fatalf("get column masks: %v", err)
	}
	if masks != nil {
		t.Errorf("user with see_original=1 should have no masks, got %v", masks)
	}
}

func TestAdminNoColumnMasks(t *testing.T) {
	cat, ctx := setupTestService(t)
	q := cat.Queries()

	_, _ = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin", Type: "user", IsAdmin: 1,
	})

	mask, _ := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		TableID:        1,
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: mask.ID, PrincipalID: 1,
		PrincipalType: "user", SeeOriginal: 0,
	})

	masks, err := cat.GetEffectiveColumnMasks(ctx, "admin", 1)
	if err != nil {
		t.Fatalf("get column masks: %v", err)
	}
	if masks != nil {
		t.Error("admin should have no column masks")
	}
}

func TestUnknownPrincipalError(t *testing.T) {
	cat, ctx := setupTestService(t)

	_, err := cat.CheckPrivilege(ctx, "nobody", SecurableTable, 1, PrivSelect)
	if err == nil {
		t.Error("expected error for unknown principal")
	}
}

func TestLookupTableID(t *testing.T) {
	cat, ctx := setupTestService(t)

	tableID, schemaID, err := cat.LookupTableID(ctx, "titanic")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if tableID != 1 {
		t.Errorf("expected tableID=1, got %d", tableID)
	}
	if schemaID != 0 {
		t.Errorf("expected schemaID=0, got %d", schemaID)
	}
}

func TestLookupTableIDNotFound(t *testing.T) {
	cat, ctx := setupTestService(t)

	_, _, err := cat.LookupTableID(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestLookupSchemaID(t *testing.T) {
	cat, ctx := setupTestService(t)

	schemaID, err := cat.LookupSchemaID(ctx, "main")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if schemaID != 0 {
		t.Errorf("expected schemaID=0, got %d", schemaID)
	}
}
