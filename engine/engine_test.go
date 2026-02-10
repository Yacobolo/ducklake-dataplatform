package engine

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mattn/go-sqlite3"

	"duck-demo/catalog"
	dbstore "duck-demo/db/catalog"
)

// setupTestCatalog creates a temporary SQLite metastore with demo permissions.
// It also creates the DuckLake-like metadata tables that the catalog service queries.
func setupTestCatalog(t *testing.T) *catalog.CatalogService {
	t.Helper()

	tmpDir := t.TempDir()
	metaPath := tmpDir + "/test_meta.sqlite"

	metaDB, err := sql.Open("sqlite3", metaPath+"?_foreign_keys=on")
	if err != nil {
		t.Fatalf("open metastore: %v", err)
	}
	t.Cleanup(func() { metaDB.Close() })

	// Run permission migrations
	if err := catalog.RunMigrations(metaDB); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// Create mock DuckLake catalog tables that the service queries
	_, err = metaDB.Exec(`
		CREATE TABLE IF NOT EXISTS ducklake_schema (
			schema_id INTEGER PRIMARY KEY,
			schema_uuid TEXT,
			begin_snapshot INTEGER,
			end_snapshot INTEGER,
			schema_name TEXT,
			path TEXT,
			path_is_relative INTEGER
		);
		CREATE TABLE IF NOT EXISTS ducklake_table (
			table_id INTEGER,
			table_uuid TEXT,
			begin_snapshot INTEGER,
			end_snapshot INTEGER,
			schema_id INTEGER,
			table_name TEXT,
			path TEXT,
			path_is_relative INTEGER
		);
		INSERT OR IGNORE INTO ducklake_schema (schema_id, schema_name, begin_snapshot)
		VALUES (0, 'main', 0);
		INSERT OR IGNORE INTO ducklake_table (table_id, table_name, schema_id, begin_snapshot)
		VALUES (1, 'titanic', 0, 1);
	`)
	if err != nil {
		t.Fatalf("create mock ducklake tables: %v", err)
	}

	cat := catalog.NewCatalogService(metaDB)
	ctx := context.Background()
	q := cat.Queries()

	// Create principals
	adminUser, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	analyst, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "first_class_analyst", Type: "user", IsAdmin: 0,
	})
	survivor, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "survivor_researcher", Type: "user", IsAdmin: 0,
	})
	_, _ = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "no_access", Type: "user", IsAdmin: 0,
	})

	// Create groups
	analystsGroup, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})
	survivorGroup, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "survivors"})

	// Add members to groups
	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst.ID,
	})
	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: survivorGroup.ID, MemberType: "user", MemberID: survivor.ID,
	})

	// Grant privileges
	// Admin: ALL_PRIVILEGES on catalog
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminUser.ID, PrincipalType: "user",
		SecurableType: "catalog", SecurableID: 0,
		Privilege: "ALL_PRIVILEGES",
	})

	// Analysts: USAGE on schema + SELECT on titanic
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: 0,
		Privilege: "USAGE",
	})
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: 1,
		Privilege: "SELECT",
	})

	// Survivors: USAGE on schema + SELECT on titanic
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: 0,
		Privilege: "USAGE",
	})
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: 1,
		Privilege: "SELECT",
	})

	// Row filter: Pclass = 1 for analysts
	firstClassFilter, _ := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:   1,
		FilterSql: `"Pclass" = 1`,
	})
	q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: firstClassFilter.ID, PrincipalID: analystsGroup.ID, PrincipalType: "group",
	})

	return cat
}

// setupEngine creates a SecureEngine with a real DuckDB connection and test catalog.
func setupEngine(t *testing.T) *SecureEngine {
	t.Helper()

	if _, err := os.Stat("../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found, skipping integration test")
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec("CREATE TABLE titanic AS SELECT * FROM '../titanic.parquet'"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	cat := setupTestCatalog(t)
	return NewSecureEngine(db, cat)
}

func TestAdminSeesAllRows(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	rows, err := eng.Query(ctx, "admin", "SELECT * FROM titanic LIMIT 10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 10 {
		t.Errorf("admin should see 10 rows, got %d", count)
	}
}

func TestFirstClassAnalystOnlySeesClass1(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	rows, err := eng.Query(ctx, "first_class_analyst", `SELECT "Pclass" FROM titanic`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var pclass int64
		if err := rows.Scan(&pclass); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if pclass != 1 {
			t.Errorf("expected Pclass=1, got %d", pclass)
		}
		count++
	}
	if count == 0 {
		t.Error("expected at least one row")
	}
	t.Logf("first_class_analyst saw %d rows (all Pclass=1)", count)
}

func TestSurvivorResearcherSeesAll(t *testing.T) {
	// survivor_researcher has no row filter (only analysts have the Pclass filter)
	eng := setupEngine(t)
	ctx := context.Background()

	rows, err := eng.Query(ctx, "survivor_researcher", `SELECT "Survived" FROM titanic`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Error("expected at least one row")
	}
	t.Logf("survivor_researcher saw %d rows (no row filter)", count)
}

func TestNoAccessRoleDenied(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	_, err := eng.Query(ctx, "no_access", "SELECT * FROM titanic LIMIT 10")
	if err == nil {
		t.Error("expected access denied error for no_access user")
	}
	t.Logf("no_access error: %v", err)
}

func TestAccessToDeniedTableFails(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	_, err := eng.Query(ctx, "first_class_analyst", "SELECT * FROM secret_data LIMIT 10")
	if err == nil {
		t.Error("expected error when accessing unauthorized table")
	}
	t.Logf("access denied error: %v", err)
}

func TestModifiedPlanExecutesCorrectly(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	rows, err := eng.Query(ctx, "first_class_analyst", `SELECT "PassengerId", "Name", "Pclass" FROM titanic LIMIT 5`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	t.Logf("columns: %v", cols)

	count := 0
	for rows.Next() {
		var id int64
		var name string
		var pclass int64
		if err := rows.Scan(&id, &name, &pclass); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if pclass != 1 {
			t.Errorf("expected Pclass=1, got %d", pclass)
		}
		t.Logf("  row: id=%d name=%q pclass=%d", id, name, pclass)
		count++
	}
	if count == 0 {
		t.Error("expected at least one row")
	}
}

func TestRowCountReducedByRLS(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	// Admin count
	adminRows, err := eng.Query(ctx, "admin", "SELECT count(*) FROM titanic")
	if err != nil {
		t.Fatalf("admin query: %v", err)
	}
	var adminCount int64
	if !adminRows.Next() {
		t.Fatal("expected a row from admin count query")
	}
	if err := adminRows.Scan(&adminCount); err != nil {
		t.Fatalf("admin scan: %v", err)
	}
	adminRows.Close()

	// First class count
	fcRows, err := eng.Query(ctx, "first_class_analyst", "SELECT count(*) FROM titanic")
	if err != nil {
		t.Fatalf("first_class query: %v", err)
	}
	var fcCount int64
	if !fcRows.Next() {
		t.Fatal("expected a row from first_class count query")
	}
	if err := fcRows.Scan(&fcCount); err != nil {
		t.Fatalf("first_class scan: %v", err)
	}
	fcRows.Close()

	t.Logf("admin sees %d rows, first_class_analyst sees %d rows", adminCount, fcCount)

	if fcCount >= adminCount {
		t.Errorf("first_class_analyst (%d) should see fewer rows than admin (%d)", fcCount, adminCount)
	}
	if fcCount == 0 {
		t.Error("first_class_analyst should see at least some rows")
	}
}

func TestUnknownRoleReturnsError(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	_, err := eng.Query(ctx, "nonexistent_role", "SELECT * FROM titanic LIMIT 10")
	if err == nil {
		t.Error("expected error for unknown role")
	}
}

func TestDDLBlocked(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	_, err := eng.Query(ctx, "first_class_analyst", "DROP TABLE titanic")
	if err == nil {
		t.Error("expected DDL to be blocked")
	}
	t.Logf("DDL blocked: %v", err)
}

func TestInsertRequiresPrivilege(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	// first_class_analyst only has SELECT, not INSERT
	_, err := eng.Query(ctx, "first_class_analyst", `INSERT INTO titanic ("PassengerId") VALUES (9999)`)
	if err == nil {
		t.Error("expected INSERT to be denied for user without INSERT privilege")
	}
	t.Logf("INSERT denied: %v", err)
}

func TestUpdateRequiresPrivilege(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	_, err := eng.Query(ctx, "first_class_analyst", `UPDATE titanic SET "Name" = 'test' WHERE "PassengerId" = 1`)
	if err == nil {
		t.Error("expected UPDATE to be denied for user without UPDATE privilege")
	}
	t.Logf("UPDATE denied: %v", err)
}

func TestDeleteRequiresPrivilege(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	_, err := eng.Query(ctx, "first_class_analyst", `DELETE FROM titanic WHERE "PassengerId" = 1`)
	if err == nil {
		t.Error("expected DELETE to be denied for user without DELETE privilege")
	}
	t.Logf("DELETE denied: %v", err)
}
