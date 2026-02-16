//go:build integration

package integration

import (
	"database/sql"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/service/catalog"
)

// setupCatalogRegistrationEnv creates the components needed to test multi-catalog
// registration: a DuckDB instance, a control plane SQLite, and the service layer.
type catalogRegTestEnv struct {
	DuckDB  *sql.DB
	MetaDB  *sql.DB
	TmpDir  string
	Service *catalog.CatalogRegistrationService
	Repo    domain.CatalogRegistrationRepository
}

func setupCatalogRegistrationEnv(t *testing.T) *catalogRegTestEnv {
	t.Helper()

	tmpDir := t.TempDir()
	controlPlaneDBPath := filepath.Join(tmpDir, "control_plane.sqlite")

	// Open control plane SQLite
	metaDB, err := internaldb.OpenSQLite(controlPlaneDBPath, "write", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = metaDB.Close() })

	// Run migrations to create the catalogs table
	require.NoError(t, internaldb.RunMigrations(metaDB))

	// Open DuckDB in-memory
	duckDB, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = duckDB.Close() })

	// Install ducklake + sqlite extensions
	for _, stmt := range []string{
		"INSTALL ducklake", "LOAD ducklake",
		"INSTALL sqlite", "LOAD sqlite",
	} {
		if _, err := duckDB.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s failed: %v", stmt, err)
		}
	}

	// Build repositories
	catalogRegRepo := repository.NewCatalogRegistrationRepo(metaDB)
	secretMgr := engine.NewDuckDBSecretManager(duckDB)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Build service
	svc := catalog.NewCatalogRegistrationService(catalog.RegistrationServiceDeps{
		Repo:               catalogRegRepo,
		Attacher:           secretMgr,
		ControlPlaneDBPath: controlPlaneDBPath,
		Logger:             logger,
	})

	return &catalogRegTestEnv{
		DuckDB:  duckDB,
		MetaDB:  metaDB,
		TmpDir:  tmpDir,
		Service: svc,
		Repo:    catalogRegRepo,
	}
}

// createSQLiteCatalog creates a new SQLite-backed DuckLake catalog directory.
func createSQLiteCatalog(t *testing.T, tmpDir, name string) (metaPath, dataPath string) {
	t.Helper()
	catDir := filepath.Join(tmpDir, name)
	require.NoError(t, os.MkdirAll(catDir, 0o755))

	metaPath = filepath.Join(catDir, "meta.sqlite")
	dataPath = filepath.Join(catDir, "data") + "/"
	require.NoError(t, os.MkdirAll(filepath.Join(catDir, "data"), 0o755))
	return metaPath, dataPath
}

// TestCatalogRegistration_RegisterAndAttach registers a catalog and verifies it's attached.
func TestCatalogRegistration_RegisterAndAttach(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)
	metaPath, dataPath := createSQLiteCatalog(t, env.TmpDir, "test_lake")

	reg, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name:          "test_lake",
		MetastoreType: "sqlite",
		DSN:           metaPath,
		DataPath:      dataPath,
	})

	require.NoError(t, err)
	assert.Equal(t, "test_lake", reg.Name)
	assert.Equal(t, domain.CatalogStatusActive, reg.Status)
	assert.Equal(t, domain.MetastoreTypeSQLite, reg.MetastoreType)

	// Verify DuckDB can see it
	var attached int
	err = env.DuckDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'test_lake'").Scan(&attached)
	require.NoError(t, err)
	assert.Equal(t, 1, attached, "catalog should be attached in DuckDB")
}

// TestCatalogRegistration_TwoCatalogs registers two catalogs and verifies cross-catalog schemas.
func TestCatalogRegistration_TwoCatalogs(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)

	// Register catalog 1
	meta1, data1 := createSQLiteCatalog(t, env.TmpDir, "lake1")
	reg1, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name:          "lake1",
		MetastoreType: "sqlite",
		DSN:           meta1,
		DataPath:      data1,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.CatalogStatusActive, reg1.Status)

	// Register catalog 2
	meta2, data2 := createSQLiteCatalog(t, env.TmpDir, "lake2")
	reg2, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name:          "lake2",
		MetastoreType: "sqlite",
		DSN:           meta2,
		DataPath:      data2,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.CatalogStatusActive, reg2.Status)

	// Create schema in catalog 1
	_, err = env.DuckDB.ExecContext(ctx, `CREATE SCHEMA "lake1"."analytics"`)
	require.NoError(t, err)

	// Create schema in catalog 2
	_, err = env.DuckDB.ExecContext(ctx, `CREATE SCHEMA "lake2"."staging"`)
	require.NoError(t, err)

	// Verify schemas are in the right catalogs
	var schemaCount int
	err = env.DuckDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM duckdb_schemas() WHERE database_name = 'lake1' AND schema_name = 'analytics'`).Scan(&schemaCount)
	require.NoError(t, err)
	assert.Equal(t, 1, schemaCount, "analytics schema should exist in lake1")

	err = env.DuckDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM duckdb_schemas() WHERE database_name = 'lake2' AND schema_name = 'staging'`).Scan(&schemaCount)
	require.NoError(t, err)
	assert.Equal(t, 1, schemaCount, "staging schema should exist in lake2")

	// List catalogs
	catalogs, total, err := env.Service.List(ctx, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, catalogs, 2)
}

// TestCatalogRegistration_DuplicateName verifies that registering a duplicate name fails.
func TestCatalogRegistration_DuplicateName(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)
	meta1, data1 := createSQLiteCatalog(t, env.TmpDir, "dupe")

	_, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name: "dupe", MetastoreType: "sqlite", DSN: meta1, DataPath: data1,
	})
	require.NoError(t, err)

	meta2, data2 := createSQLiteCatalog(t, env.TmpDir, "dupe2")
	_, err = env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name: "dupe", MetastoreType: "sqlite", DSN: meta2, DataPath: data2,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

// TestCatalogRegistration_DeleteCleansUp deletes a catalog and verifies DuckDB detachment.
func TestCatalogRegistration_DeleteCleansUp(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)
	metaPath, dataPath := createSQLiteCatalog(t, env.TmpDir, "to_delete")

	_, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name:          "to_delete",
		MetastoreType: "sqlite",
		DSN:           metaPath,
		DataPath:      dataPath,
	})
	require.NoError(t, err)

	// Verify attached
	var attached int
	err = env.DuckDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'to_delete'").Scan(&attached)
	require.NoError(t, err)
	assert.Equal(t, 1, attached)

	// Delete
	err = env.Service.Delete(ctx, "to_delete")
	require.NoError(t, err)

	// Verify detached
	err = env.DuckDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'to_delete'").Scan(&attached)
	require.NoError(t, err)
	assert.Equal(t, 0, attached, "catalog should be detached after delete")

	// Verify not in DB
	_, err = env.Repo.GetByName(ctx, "to_delete")
	require.Error(t, err)
}

// TestCatalogRegistration_SetDefault sets a catalog as default and verifies USE.
func TestCatalogRegistration_SetDefault(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)

	meta1, data1 := createSQLiteCatalog(t, env.TmpDir, "primary")
	_, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name: "primary", MetastoreType: "sqlite", DSN: meta1, DataPath: data1,
	})
	require.NoError(t, err)

	meta2, data2 := createSQLiteCatalog(t, env.TmpDir, "secondary")
	_, err = env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name: "secondary", MetastoreType: "sqlite", DSN: meta2, DataPath: data2,
	})
	require.NoError(t, err)

	// Set primary as default
	result, err := env.Service.SetDefault(ctx, "primary")
	require.NoError(t, err)
	assert.True(t, result.IsDefault)

	// Verify in DB
	defCat, err := env.Repo.GetDefault(ctx)
	require.NoError(t, err)
	assert.Equal(t, "primary", defCat.Name)

	// Switch default to secondary
	result, err = env.Service.SetDefault(ctx, "secondary")
	require.NoError(t, err)
	assert.True(t, result.IsDefault)

	// Verify old default is no longer default
	defCat, err = env.Repo.GetDefault(ctx)
	require.NoError(t, err)
	assert.Equal(t, "secondary", defCat.Name)
}

// TestCatalogRegistration_AttachAllOnStartup registers catalogs, then verifies AttachAll.
func TestCatalogRegistration_AttachAllOnStartup(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)

	// Register two catalogs
	meta1, data1 := createSQLiteCatalog(t, env.TmpDir, "startup1")
	_, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name: "startup1", MetastoreType: "sqlite", DSN: meta1, DataPath: data1,
	})
	require.NoError(t, err)

	meta2, data2 := createSQLiteCatalog(t, env.TmpDir, "startup2")
	_, err = env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name: "startup2", MetastoreType: "sqlite", DSN: meta2, DataPath: data2,
	})
	require.NoError(t, err)

	// Set one as default
	_, err = env.Service.SetDefault(ctx, "startup1")
	require.NoError(t, err)

	// Simulate restart: create a new DuckDB and service
	newDuckDB, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = newDuckDB.Close() })

	for _, stmt := range []string{
		"INSTALL ducklake", "LOAD ducklake",
		"INSTALL sqlite", "LOAD sqlite",
	} {
		_, err := newDuckDB.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	newSecretMgr := engine.NewDuckDBSecretManager(newDuckDB)
	catalogRegRepo := repository.NewCatalogRegistrationRepo(env.MetaDB)
	newSvc := catalog.NewCatalogRegistrationService(catalog.RegistrationServiceDeps{
		Repo:     catalogRegRepo,
		Attacher: newSecretMgr,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	// AttachAll
	err = newSvc.AttachAll(ctx)
	require.NoError(t, err)

	// Verify both catalogs are attached
	var count int
	err = newDuckDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM duckdb_databases() WHERE database_name IN ('startup1', 'startup2')").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "both catalogs should be re-attached after startup")

	// Verify catalogs are ACTIVE
	catalogs, _, err := catalogRegRepo.List(ctx, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	for _, c := range catalogs {
		assert.Equal(t, domain.CatalogStatusActive, c.Status, "catalog %s should be ACTIVE", c.Name)
	}
}

// TestCatalogRegistration_ControlPlaneSeparation ensures the control plane DB
// cannot be used as a catalog metastore DSN.
func TestCatalogRegistration_ControlPlaneSeparation(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)
	controlPlaneDBPath := filepath.Join(env.TmpDir, "control_plane.sqlite")

	_, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name:          "bad_catalog",
		MetastoreType: "sqlite",
		DSN:           controlPlaneDBPath,
		DataPath:      filepath.Join(env.TmpDir, "data") + "/",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "control plane")
}

// TestCatalogRegistration_InvalidName verifies that invalid catalog names are rejected.
func TestCatalogRegistration_InvalidName(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)
	metaPath, dataPath := createSQLiteCatalog(t, env.TmpDir, "valid")

	cases := []struct {
		name string
		err  string
	}{
		{"invalid-name!", "invalid catalog name"},
		{"", "invalid catalog name"},
		{"DROP TABLE; --", "invalid catalog name"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
				Name:          tc.name,
				MetastoreType: "sqlite",
				DSN:           metaPath,
				DataPath:      dataPath,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
		})
	}
}

// TestCatalogRegistration_CannotDeleteDefault verifies that the default catalog cannot be deleted.
func TestCatalogRegistration_CannotDeleteDefault(t *testing.T) {
	env := setupCatalogRegistrationEnv(t)
	metaPath, dataPath := createSQLiteCatalog(t, env.TmpDir, "my_default")

	_, err := env.Service.Register(ctx, domain.CreateCatalogRequest{
		Name: "my_default", MetastoreType: "sqlite", DSN: metaPath, DataPath: dataPath,
	})
	require.NoError(t, err)

	_, err = env.Service.SetDefault(ctx, "my_default")
	require.NoError(t, err)

	err = env.Service.Delete(ctx, "my_default")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default catalog")
}
