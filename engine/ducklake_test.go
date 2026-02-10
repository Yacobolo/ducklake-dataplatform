package engine

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"duck-demo/config"
)

// TestDuckLakeWithHetznerSetup tests the full SetupDuckLake flow with real
// Hetzner S3 credentials. Skipped if .env is not present or credentials are missing.
func TestDuckLakeWithHetznerSetup(t *testing.T) {
	// Check for .env file
	if err := config.LoadDotEnv("../.env"); err != nil {
		t.Skipf("could not load .env: %v", err)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		t.Skipf("missing config: %v", err)
	}

	// Check titanic.parquet exists
	if _, err := os.Stat("../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found, skipping integration test")
	}

	tmpDir := t.TempDir()
	cfg.MetaDBPath = tmpDir + "/test_meta.sqlite"
	cfg.ParquetPath = "../titanic.parquet"

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := SetupDuckLake(ctx, db, cfg); err != nil {
		t.Skipf("SetupDuckLake failed (S3 bucket may not exist): %v", err)
	}

	// Verify we can query the titanic table through the lake
	var rowCount int64
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM titanic").Scan(&rowCount)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	t.Logf("titanic table has %d rows in DuckLake", rowCount)

	if rowCount == 0 {
		t.Error("expected rows in titanic table")
	}
}

// TestDuckLakeRBACIntegration tests the full RBAC + RLS flow through DuckLake.
func TestDuckLakeRBACIntegration(t *testing.T) {
	// Check for .env file
	if err := config.LoadDotEnv("../.env"); err != nil {
		t.Skipf("could not load .env: %v", err)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		t.Skipf("missing config: %v", err)
	}

	if _, err := os.Stat("../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found")
	}

	tmpDir := t.TempDir()
	cfg.MetaDBPath = tmpDir + "/test_meta.sqlite"
	cfg.ParquetPath = "../titanic.parquet"

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := SetupDuckLake(ctx, db, cfg); err != nil {
		t.Skipf("SetupDuckLake failed (S3 bucket may not exist): %v", err)
	}

	store := testPolicyStore()
	eng := NewSecureEngine(db, store)

	// Test admin access
	t.Run("AdminAccess", func(t *testing.T) {
		rows, err := eng.Query(ctx, "admin", "SELECT * FROM titanic LIMIT 10")
		if err != nil {
			t.Fatalf("admin query: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		if count != 10 {
			t.Errorf("admin should see 10 rows, got %d", count)
		}
		t.Logf("admin saw %d rows", count)
	})

	// Test RLS filtering — first_class_analyst
	t.Run("FirstClassRLS", func(t *testing.T) {
		rows, err := eng.Query(ctx, "first_class_analyst", `SELECT "Pclass" FROM titanic`)
		if err != nil {
			t.Fatalf("first_class query: %v", err)
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
	})

	// Test RLS filtering — survivor_researcher
	t.Run("SurvivorRLS", func(t *testing.T) {
		rows, err := eng.Query(ctx, "survivor_researcher", `SELECT "Survived" FROM titanic`)
		if err != nil {
			t.Fatalf("survivor query: %v", err)
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
	})

	// Test no_access denied
	t.Run("NoAccessDenied", func(t *testing.T) {
		_, err := eng.Query(ctx, "no_access", "SELECT * FROM titanic LIMIT 10")
		if err == nil {
			t.Error("expected access denied error for no_access role")
		}
		t.Logf("no_access error: %v", err)
	})
}
