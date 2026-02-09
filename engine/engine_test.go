package engine

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/marcboeker/go-duckdb"

	"duck-demo/policy"
)

// testPolicyStore returns a PolicyStore with all test roles.
func testPolicyStore() *policy.PolicyStore {
	store := policy.NewPolicyStore()

	store.AddRole(&policy.Role{
		Name:          "admin",
		AllowedTables: []string{"*"},
	})

	store.AddRole(&policy.Role{
		Name:          "first_class_analyst",
		AllowedTables: []string{"titanic"},
		RLSRules: []policy.RLSRule{
			{Table: "titanic", Column: "Pclass", Operator: policy.OpEqual, Value: int64(1)},
		},
	})

	store.AddRole(&policy.Role{
		Name:          "survivor_researcher",
		AllowedTables: []string{"titanic"},
		RLSRules: []policy.RLSRule{
			{Table: "titanic", Column: "Survived", Operator: policy.OpEqual, Value: int64(1)},
		},
	})

	store.AddRole(&policy.Role{
		Name:          "no_access",
		AllowedTables: []string{},
	})

	return store
}

// setupEngine creates a SecureEngine with a real DuckDB connection.
func setupEngine(t *testing.T) *SecureEngine {
	t.Helper()

	// Check titanic.parquet exists
	if _, err := os.Stat("../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found, skipping integration test")
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Install substrait extension
	if _, err := db.Exec("INSTALL substrait; LOAD substrait;"); err != nil {
		t.Fatalf("install substrait: %v", err)
	}

	// Register parquet as a table
	if _, err := db.Exec("CREATE TABLE titanic AS SELECT * FROM '../titanic.parquet'"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	return NewSecureEngine(db, testPolicyStore())
}

func TestAdminSeesAllRows(t *testing.T) {
	eng := setupEngine(t)

	rows, err := eng.Query("admin", "SELECT * FROM titanic LIMIT 10")
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

	rows, err := eng.Query("first_class_analyst", "SELECT \"Pclass\" FROM titanic")
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

func TestSurvivorResearcherOnlySeesSurvivors(t *testing.T) {
	eng := setupEngine(t)

	rows, err := eng.Query("survivor_researcher", "SELECT \"Survived\" FROM titanic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var survived int64
		if err := rows.Scan(&survived); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if survived != 1 {
			t.Errorf("expected Survived=1, got %d", survived)
		}
		count++
	}
	if count == 0 {
		t.Error("expected at least one row")
	}
	t.Logf("survivor_researcher saw %d rows (all Survived=1)", count)
}

func TestNoAccessRoleDenied(t *testing.T) {
	eng := setupEngine(t)

	_, err := eng.Query("no_access", "SELECT * FROM titanic LIMIT 10")
	if err == nil {
		t.Error("expected access denied error for no_access role")
	}
	t.Logf("no_access error: %v", err)
}

func TestAccessToDeniedTableFails(t *testing.T) {
	eng := setupEngine(t)

	// first_class_analyst can only access "titanic"
	// Try to query a non-existent table that they don't have access to
	_, err := eng.Query("first_class_analyst", "SELECT * FROM secret_data LIMIT 10")
	if err == nil {
		t.Error("expected error when accessing unauthorized table")
	}
	t.Logf("access denied error: %v", err)
}

func TestModifiedPlanExecutesCorrectly(t *testing.T) {
	eng := setupEngine(t)

	// First class analyst should get valid results with correct schema
	rows, err := eng.Query("first_class_analyst", "SELECT \"PassengerId\", \"Name\", \"Pclass\" FROM titanic LIMIT 5")
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

	// Admin count
	adminRows, err := eng.Query("admin", "SELECT count(*) FROM titanic")
	if err != nil {
		t.Fatalf("admin query: %v", err)
	}
	var adminCount int64
	adminRows.Next()
	adminRows.Scan(&adminCount)
	adminRows.Close()

	// First class count
	fcRows, err := eng.Query("first_class_analyst", "SELECT count(*) FROM titanic")
	if err != nil {
		t.Fatalf("first_class query: %v", err)
	}
	var fcCount int64
	fcRows.Next()
	fcRows.Scan(&fcCount)
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

	_, err := eng.Query("nonexistent_role", "SELECT * FROM titanic LIMIT 10")
	if err == nil {
		t.Error("expected error for unknown role")
	}
}
