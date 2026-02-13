package engine_test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/engine"
	"duck-demo/internal/service/security"
)

// ctx is a package-level background context used by setup helpers.
var ctx = context.Background()

// setupTestCatalog creates a temporary SQLite metastore with demo permissions.
// It also creates the DuckLake-like metadata tables that the catalog service queries.
func setupTestCatalog(t *testing.T) *security.AuthorizationService {
	t.Helper()

	metaDB, _ := internaldb.OpenTestSQLite(t)

	// Create mock DuckLake catalog tables that the service queries
	_, err := metaDB.ExecContext(ctx, `
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

	cat := security.NewAuthorizationService(
		repository.NewPrincipalRepo(metaDB),
		repository.NewGroupRepo(metaDB),
		repository.NewGrantRepo(metaDB),
		repository.NewRowFilterRepo(metaDB),
		repository.NewColumnMaskRepo(metaDB),
		repository.NewIntrospectionRepo(metaDB),
		nil,
	)
	setupCtx := context.Background()
	q := dbstore.New(metaDB)

	// Create principals
	adminUser, err := q.CreatePrincipal(setupCtx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "admin", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)
	analyst, err := q.CreatePrincipal(setupCtx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "first_class_analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)
	survivor, err := q.CreatePrincipal(setupCtx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "survivor_researcher", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)
	_, err = q.CreatePrincipal(setupCtx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "no_access", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Create groups
	analystsGroup, err := q.CreateGroup(setupCtx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "analysts"})
	require.NoError(t, err)
	survivorGroup, err := q.CreateGroup(setupCtx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "survivors"})
	require.NoError(t, err)

	// Add members to groups
	err = q.AddGroupMember(setupCtx, dbstore.AddGroupMemberParams{
		GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst.ID,
	})
	require.NoError(t, err)
	err = q.AddGroupMember(setupCtx, dbstore.AddGroupMemberParams{
		GroupID: survivorGroup.ID, MemberType: "user", MemberID: survivor.ID,
	})
	require.NoError(t, err)

	// Grant privileges
	_, err = q.GrantPrivilege(setupCtx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: adminUser.ID, PrincipalType: "user",
		SecurableType: "catalog", SecurableID: "0",
		Privilege: "ALL_PRIVILEGES",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(setupCtx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0",
		Privilege: "USAGE",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(setupCtx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1",
		Privilege: "SELECT",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(setupCtx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0",
		Privilege: "USAGE",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(setupCtx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1",
		Privilege: "SELECT",
	})
	require.NoError(t, err)

	// Row filter: Pclass = 1 for analysts
	firstClassFilter, err := q.CreateRowFilter(setupCtx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "1",
		FilterSql: `"Pclass" = 1`,
	})
	require.NoError(t, err)
	err = q.BindRowFilter(setupCtx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: firstClassFilter.ID, PrincipalID: analystsGroup.ID, PrincipalType: "group",
	})
	require.NoError(t, err)

	return cat
}

// queryAndClose calls eng.Query and closes the returned rows. It is a helper
// for tests that only care about the error, not the result set.
func queryAndClose(t *testing.T, eng *engine.SecureEngine, principal, sqlStr string) error {
	t.Helper()
	r, err := eng.Query(ctx, principal, sqlStr) //nolint:rowserrcheck // closes rows immediately; no iteration
	if r != nil {
		r.Close() //nolint:errcheck,sqlclosecheck
	}
	return err
}

// setupEngine creates a SecureEngine with a real DuckDB connection and test catalog.
func setupEngine(t *testing.T) *engine.SecureEngine {
	t.Helper()

	if _, err := os.Stat("../../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found, skipping integration test")
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.ExecContext(ctx, "CREATE TABLE titanic AS SELECT * FROM '../../titanic.parquet'"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	cat := setupTestCatalog(t)
	return engine.NewSecureEngine(db, cat, nil, nil, slog.New(slog.DiscardHandler))
}

func TestAdminSeesAllRows(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	rows, err := eng.Query(ctx, "admin", "SELECT * FROM titanic LIMIT 10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rows.Close() //nolint:errcheck

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
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
	defer rows.Close() //nolint:errcheck

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
	require.NoError(t, rows.Err())
	if count == 0 {
		t.Error("expected at least one row")
	}
	t.Logf("first_class_analyst saw %d rows (all Pclass=1)", count)
}

func TestSurvivorResearcherSeesAll(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	rows, err := eng.Query(ctx, "survivor_researcher", `SELECT "Survived" FROM titanic`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rows.Close() //nolint:errcheck

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	if count == 0 {
		t.Error("expected at least one row")
	}
	t.Logf("survivor_researcher saw %d rows (no row filter)", count)
}

func TestNoAccessRoleDenied(t *testing.T) {
	eng := setupEngine(t)

	err := queryAndClose(t, eng, "no_access", "SELECT * FROM titanic LIMIT 10")
	if err == nil {
		t.Error("expected access denied error for no_access user")
	}
	t.Logf("no_access error: %v", err)
}

func TestAccessToDeniedTableFails(t *testing.T) {
	eng := setupEngine(t)

	err := queryAndClose(t, eng, "first_class_analyst", "SELECT * FROM secret_data LIMIT 10")
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
	defer rows.Close() //nolint:errcheck

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
	require.NoError(t, rows.Err())
	if count == 0 {
		t.Error("expected at least one row")
	}
}

func TestRowCountReducedByRLS(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	adminRows, err := eng.Query(ctx, "admin", "SELECT count(*) FROM titanic")
	if err != nil {
		t.Fatalf("admin query: %v", err)
	}
	defer adminRows.Close() //nolint:errcheck
	var adminCount int64
	if !adminRows.Next() {
		t.Fatal("expected a row from admin count query")
	}
	if err := adminRows.Scan(&adminCount); err != nil {
		t.Fatalf("admin scan: %v", err)
	}
	require.NoError(t, adminRows.Err())

	fcRows, err := eng.Query(ctx, "first_class_analyst", "SELECT count(*) FROM titanic")
	if err != nil {
		t.Fatalf("first_class query: %v", err)
	}
	defer fcRows.Close() //nolint:errcheck
	var fcCount int64
	if !fcRows.Next() {
		t.Fatal("expected a row from first_class count query")
	}
	if err := fcRows.Scan(&fcCount); err != nil {
		t.Fatalf("first_class scan: %v", err)
	}
	require.NoError(t, fcRows.Err())

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

	err := queryAndClose(t, eng, "nonexistent_role", "SELECT * FROM titanic LIMIT 10")
	if err == nil {
		t.Error("expected error for unknown role")
	}
}

func TestDDLBlocked(t *testing.T) {
	eng := setupEngine(t)

	err := queryAndClose(t, eng, "first_class_analyst", "DROP TABLE titanic")
	if err == nil {
		t.Error("expected DDL to be blocked")
	}
	t.Logf("DDL blocked: %v", err)
}

func TestInsertRequiresPrivilege(t *testing.T) {
	eng := setupEngine(t)

	err := queryAndClose(t, eng, "first_class_analyst", `INSERT INTO titanic ("PassengerId") VALUES (9999)`)
	if err == nil {
		t.Error("expected INSERT to be denied for user without INSERT privilege")
	}
	t.Logf("INSERT denied: %v", err)
}

func TestUpdateRequiresPrivilege(t *testing.T) {
	eng := setupEngine(t)

	err := queryAndClose(t, eng, "first_class_analyst", `UPDATE titanic SET "Name" = 'test' WHERE "PassengerId" = 1`)
	if err == nil {
		t.Error("expected UPDATE to be denied for user without UPDATE privilege")
	}
	t.Logf("UPDATE denied: %v", err)
}

func TestDeleteRequiresPrivilege(t *testing.T) {
	eng := setupEngine(t)

	err := queryAndClose(t, eng, "first_class_analyst", `DELETE FROM titanic WHERE "PassengerId" = 1`)
	if err == nil {
		t.Error("expected DELETE to be denied for user without DELETE privilege")
	}
	t.Logf("DELETE denied: %v", err)
}

func TestMultiStatementBlocked(t *testing.T) {
	eng := setupEngine(t)

	tests := []struct {
		name string
		sql  string
	}{
		{"select_then_drop", "SELECT 1; DROP TABLE titanic"},
		{"select_then_insert", "SELECT 1; INSERT INTO titanic (\"PassengerId\") VALUES (9999)"},
		{"two_selects", "SELECT 1; SELECT * FROM titanic"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := queryAndClose(t, eng, "admin", tc.sql)
			if err == nil {
				t.Error("expected multi-statement query to be blocked")
			}
		})
	}
}

func TestTablelessStatementRequiresAuth(t *testing.T) {
	eng := setupEngine(t)
	ctx := context.Background()

	t.Run("admin_allowed", func(t *testing.T) {
		rows, err := eng.Query(ctx, "admin", "SELECT 1 + 1")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck
		require.NoError(t, rows.Err())
	})

	t.Run("non_privileged_denied", func(t *testing.T) {
		err := queryAndClose(t, eng, "first_class_analyst", "SELECT 1 + 1")
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "access denied") || strings.Contains(err.Error(), "privilege"),
			"expected access denied error, got: %v", err)
	})

	t.Run("no_access_denied", func(t *testing.T) {
		err := queryAndClose(t, eng, "no_access", "SELECT 1 + 1")
		require.Error(t, err)
	})
}

func TestDDLVariantsBlocked(t *testing.T) {
	eng := setupEngine(t)

	tests := []struct {
		name string
		sql  string
	}{
		{"create_table", "CREATE TABLE evil (id INT)"},
		{"drop_table", "DROP TABLE titanic"},
		{"alter_table", `ALTER TABLE titanic ADD COLUMN evil INT`},
		{"truncate", "TRUNCATE TABLE titanic"},
		{"create_schema", "CREATE SCHEMA evil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := queryAndClose(t, eng, "first_class_analyst", tc.sql)
			require.Error(t, err, "expected DDL %q to be blocked", tc.name)
		})
	}
}

func TestMalformedSQLReturnsError(t *testing.T) {
	eng := setupEngine(t)

	tests := []struct {
		name string
		sql  string
	}{
		{"garbage", "NOT VALID SQL AT ALL"},
		{"incomplete", "SELECT FROM"},
		{"empty", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := queryAndClose(t, eng, "admin", tc.sql)
			// Should return a parse error, not panic
			if tc.sql == "" {
				// Empty SQL may or may not error depending on parser
				return
			}
			require.Error(t, err, "expected parse error for malformed SQL")
		})
	}
}
