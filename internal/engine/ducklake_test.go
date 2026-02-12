package engine_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/engine"
	"duck-demo/internal/service"
)

// TestDuckLakeWithHetznerSetup tests the full DuckLake setup flow with real
// Hetzner S3 credentials. Skipped if .env is not present or credentials are missing.
func TestDuckLakeWithHetznerSetup(t *testing.T) {
	if err := config.LoadDotEnv("../../.env"); err != nil {
		t.Skipf("could not load .env: %v", err)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		t.Skipf("config load failed: %v", err)
	}
	if !cfg.HasS3Config() {
		t.Skip("S3 config not available, skipping")
	}

	if _, err := os.Stat("../../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found, skipping integration test")
	}

	tmpDir := t.TempDir()
	cfg.MetaDBPath = tmpDir + "/test_meta.sqlite"

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close() //nolint:errcheck

	ctx := context.Background()

	// Use the new split functions
	if err := engine.InstallExtensions(ctx, db); err != nil {
		t.Fatalf("install extensions: %v", err)
	}
	if err := engine.CreateS3Secret(ctx, db, "hetzner_s3",
		*cfg.S3KeyID, *cfg.S3Secret, *cfg.S3Endpoint, *cfg.S3Region, "path"); err != nil {
		t.Fatalf("create S3 secret: %v", err)
	}
	bucket := "duck-demo"
	if cfg.S3Bucket != nil {
		bucket = *cfg.S3Bucket
	}
	dataPath := "s3://" + bucket + "/lake_data/"
	if err := engine.AttachDuckLake(ctx, db, cfg.MetaDBPath, dataPath); err != nil {
		t.Skipf("AttachDuckLake failed (S3 bucket may not exist): %v", err)
	}

	// Seed titanic data
	createSQL := "CREATE TABLE IF NOT EXISTS lake.main.titanic AS SELECT * FROM '../../titanic.parquet'"
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create titanic table: %v", err)
	}

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
	if err := config.LoadDotEnv("../../.env"); err != nil {
		t.Skipf("could not load .env: %v", err)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		t.Skipf("config load failed: %v", err)
	}
	if !cfg.HasS3Config() {
		t.Skip("S3 config not available, skipping")
	}

	if _, err := os.Stat("../../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found")
	}

	tmpDir := t.TempDir()
	cfg.MetaDBPath = tmpDir + "/test_meta.sqlite"

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close() //nolint:errcheck

	ctx := context.Background()

	// Use the new split functions
	if err := engine.InstallExtensions(ctx, db); err != nil {
		t.Fatalf("install extensions: %v", err)
	}
	if err := engine.CreateS3Secret(ctx, db, "hetzner_s3",
		*cfg.S3KeyID, *cfg.S3Secret, *cfg.S3Endpoint, *cfg.S3Region, "path"); err != nil {
		t.Fatalf("create S3 secret: %v", err)
	}
	bucket := "duck-demo"
	if cfg.S3Bucket != nil {
		bucket = *cfg.S3Bucket
	}
	dataPath := "s3://" + bucket + "/lake_data/"
	if err := engine.AttachDuckLake(ctx, db, cfg.MetaDBPath, dataPath); err != nil {
		t.Skipf("AttachDuckLake failed (S3 bucket may not exist): %v", err)
	}

	// Seed titanic data
	createSQL := "CREATE TABLE IF NOT EXISTS lake.main.titanic AS SELECT * FROM '../../titanic.parquet'"
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create titanic table: %v", err)
	}

	// Set up catalog using the DuckLake metastore (hardened pools)
	metaDB, err := internaldb.OpenSQLite(cfg.MetaDBPath, "write", 0)
	if err != nil {
		t.Fatalf("open metastore: %v", err)
	}
	defer metaDB.Close() //nolint:errcheck

	if err := internaldb.RunMigrations(metaDB); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	cat := service.NewAuthorizationService(
		repository.NewPrincipalRepo(metaDB),
		repository.NewGroupRepo(metaDB),
		repository.NewGrantRepo(metaDB),
		repository.NewRowFilterRepo(metaDB),
		repository.NewColumnMaskRepo(metaDB),
		repository.NewIntrospectionRepo(metaDB),
	)
	q := dbstore.New(metaDB)

	// Seed principals and grants
	adminUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	if err != nil {
		t.Fatalf("create admin: %v", err)
	}

	analyst, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "first_class_analyst", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create analyst: %v", err)
	}
	_, err = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "no_access", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create no_access: %v", err)
	}

	// Lookup DuckLake IDs
	schemaID, err := cat.LookupSchemaID(ctx, "main")
	if err != nil {
		t.Fatalf("lookup schema: %v", err)
	}
	titanicID, _, _, err := cat.LookupTableID(ctx, "titanic")
	if err != nil {
		t.Fatalf("lookup table: %v", err)
	}

	// Create group for analysts
	analystsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst.ID,
	})
	if err != nil {
		t.Fatalf("add group member: %v", err)
	}

	// Grant admin ALL_PRIVILEGES on catalog
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminUser.ID, PrincipalType: "user",
		SecurableType: "catalog", SecurableID: 0,
		Privilege: "ALL_PRIVILEGES",
	})
	if err != nil {
		t.Fatalf("grant admin: %v", err)
	}

	// Grant analysts USAGE + SELECT
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: schemaID,
		Privilege: "USAGE",
	})
	if err != nil {
		t.Fatalf("grant usage: %v", err)
	}
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: titanicID,
		Privilege: "SELECT",
	})
	if err != nil {
		t.Fatalf("grant select: %v", err)
	}

	// Row filter
	filter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:   titanicID,
		FilterSql: `"Pclass" = 1`,
	})
	if err != nil {
		t.Fatalf("create row filter: %v", err)
	}
	err = q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: filter.ID, PrincipalID: analystsGroup.ID, PrincipalType: "group",
	})
	if err != nil {
		t.Fatalf("bind row filter: %v", err)
	}

	eng := engine.NewSecureEngine(db, cat, slog.New(slog.NewTextHandler(io.Discard, nil)))

	t.Run("AdminAccess", func(t *testing.T) {
		rows, err := eng.Query(ctx, "admin", "SELECT * FROM titanic LIMIT 10")
		if err != nil {
			t.Fatalf("admin query: %v", err)
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
		t.Logf("admin saw %d rows", count)
	})

	t.Run("FirstClassRLS", func(t *testing.T) {
		rows, err := eng.Query(ctx, "first_class_analyst", `SELECT "Pclass" FROM titanic`)
		if err != nil {
			t.Fatalf("first_class query: %v", err)
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
	})

	t.Run("NoAccessDenied", func(t *testing.T) {
		err := queryAndClose(t, eng, "no_access", "SELECT * FROM titanic LIMIT 10")
		if err == nil {
			t.Error("expected access denied error for no_access user")
		}
		t.Logf("no_access error: %v", err)
	})
}
