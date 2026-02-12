//go:build integration

// Package integration contains end-to-end HTTP integration tests.
package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-chi/chi/v5"
	_ "github.com/mattn/go-sqlite3"

	"github.com/golang-jwt/jwt/v5"

	"duck-demo/internal/agent"
	"duck-demo/internal/api"
	"duck-demo/internal/compute"
	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/crypto"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/engine"
	"duck-demo/internal/middleware"
	"duck-demo/internal/service"
)

// ctx is a package-level background context used by setup helpers.
var ctx = context.Background()

// ---------------------------------------------------------------------------
// TestMain — shared setup for all integration tests
// ---------------------------------------------------------------------------

// sharedCatalogEnv is initialized once in TestMain and shared by all catalog
// tests. This avoids 6 redundant DuckDB instance + extension install + migration
// cycles (one per TestCatalog_* function).
var sharedCatalogEnv *catalogTestEnv
var sharedCatalogCleanup func()

func TestMain(m *testing.M) {
	env, cleanup, err := setupSharedDuckLake()
	if err != nil {
		// DuckLake not available — catalog tests will skip, extension tests unaffected
		fmt.Fprintf(os.Stderr, "DuckLake shared setup skipped: %v\n", err)
	} else {
		sharedCatalogEnv = env
		sharedCatalogCleanup = cleanup
	}

	code := m.Run()

	if sharedCatalogCleanup != nil {
		sharedCatalogCleanup()
	}
	os.Exit(code)
}

// setupSharedDuckLake creates a single DuckLake instance for all catalog tests.
// Unlike setupLocalDuckLake, it does not require *testing.T and returns an
// explicit cleanup function + error (for use in TestMain).
func setupSharedDuckLake() (*catalogTestEnv, func(), error) {
	tmpDir, err := os.MkdirTemp("", "ducklake-integration-*")
	if err != nil {
		return nil, nil, err
	}
	metaPath := filepath.Join(tmpDir, "meta.sqlite")
	dataPath := filepath.Join(tmpDir, "lake_data") + "/"
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, nil, err
	}

	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, nil, err
	}

	for _, stmt := range []string{
		"INSTALL ducklake", "LOAD ducklake",
		"INSTALL sqlite", "LOAD sqlite",
	} {
		if _, err := duckDB.ExecContext(ctx, stmt); err != nil {
			_ = duckDB.Close()
			_ = os.RemoveAll(tmpDir)
			return nil, nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}

	attachSQL := fmt.Sprintf(
		`ATTACH 'ducklake:sqlite:%s' AS lake (DATA_PATH '%s')`,
		metaPath, dataPath,
	)
	if _, err := duckDB.ExecContext(ctx, attachSQL); err != nil {
		_ = duckDB.Close()
		_ = os.RemoveAll(tmpDir)
		return nil, nil, fmt.Errorf("attach ducklake: %w", err)
	}
	if _, err := duckDB.ExecContext(ctx, "USE lake"); err != nil {
		_ = duckDB.Close()
		_ = os.RemoveAll(tmpDir)
		return nil, nil, fmt.Errorf("use lake: %w", err)
	}

	metaDB, err := internaldb.OpenSQLite(metaPath, "write", 0)
	if err != nil {
		_ = duckDB.Close()
		_ = os.RemoveAll(tmpDir)
		return nil, nil, err
	}

	if err := internaldb.RunMigrations(metaDB); err != nil {
		_ = metaDB.Close()
		_ = duckDB.Close()
		_ = os.RemoveAll(tmpDir)
		return nil, nil, fmt.Errorf("migrations: %w", err)
	}

	cleanup := func() {
		_ = metaDB.Close()
		_ = duckDB.Close()
		_ = os.RemoveAll(tmpDir)
	}

	return &catalogTestEnv{DuckDB: duckDB, MetaDB: metaDB}, cleanup, nil
}

// requireCatalogEnv returns the shared DuckLake environment or fails the test.
func requireCatalogEnv(t *testing.T) *catalogTestEnv {
	t.Helper()
	if sharedCatalogEnv == nil {
		t.Fatal("DuckLake extensions not available — cannot run catalog test")
	}
	return sharedCatalogEnv
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

// projectRoot returns the absolute path to the repository root.
// Derived from this file's location: test/integration/helpers_test.go → up 2 dirs.
func projectRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

func extensionPath() string {
	return filepath.Join(projectRoot(),
		"extension", "duck_access", "build", "release",
		"extension", "duck_access", "duck_access.duckdb_extension")
}

func duckdbCLIPath() string {
	return filepath.Join(projectRoot(),
		"extension", "duck_access", "build", "release", "duckdb")
}

func dotEnvPath() string {
	return filepath.Join(projectRoot(), ".env")
}

// ---------------------------------------------------------------------------
// Prerequisites
// ---------------------------------------------------------------------------

// checkExtensionBinaries skips the test if the DuckDB CLI or duck_access
// extension binary are not present on disk. Does NOT check S3 credentials.
func checkExtensionBinaries(t *testing.T) {
	t.Helper()

	if _, err := os.Stat(extensionPath()); err != nil {
		t.Skipf("extension binary not found at %s — build with: cd extension/duck_access && make", extensionPath())
	}
	if _, err := os.Stat(duckdbCLIPath()); err != nil {
		t.Skipf("DuckDB CLI not found at %s", duckdbCLIPath())
	}
}

// checkPrerequisites skips the test if the extension binary, DuckDB CLI, or
// S3 credentials are missing. Use for tests that require real S3 connectivity.
func checkPrerequisites(t *testing.T) {
	t.Helper()

	checkExtensionBinaries(t)

	// Load .env and check S3 credentials
	_ = config.LoadDotEnv(dotEnvPath())
	for _, envVar := range []string{"KEY_ID", "SECRET", "ENDPOINT", "REGION"} {
		if os.Getenv(envVar) == "" {
			t.Skipf("required env var %s not set (check .env)", envVar)
		}
	}
}

// ---------------------------------------------------------------------------
// Crypto helper
// ---------------------------------------------------------------------------

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// ---------------------------------------------------------------------------
// DuckLake metadata seeding
// ---------------------------------------------------------------------------

// ducklakeDataFileName is the parquet file name used in seeded DuckLake metadata.
// This must match the name used by setupLocalExtensionServer when copying the
// testdata/titanic.parquet fixture into the local data directory.
const ducklakeDataFileName = "ducklake-019c4727-c55c-7e4d-ab38-e01a2920253c.parquet"

// seedDuckLakeMetadata creates the DuckLake catalog tables in the temp SQLite
// and inserts hardcoded production values. The dataPath parameter controls the
// data_path metadata value (e.g., "s3://yacobolo/lake_data/" for S3 tests or a
// local temp directory path for local tests).
func seedDuckLakeMetadata(t *testing.T, db *sql.DB, dataPath string) {
	t.Helper()

	const ddl = `
	CREATE TABLE IF NOT EXISTS ducklake_metadata(
		"key" VARCHAR NOT NULL,
		"value" VARCHAR NOT NULL,
		"scope" VARCHAR,
		scope_id BIGINT
	);
	CREATE TABLE IF NOT EXISTS ducklake_schema(
		schema_id BIGINT PRIMARY KEY,
		schema_uuid VARCHAR,
		begin_snapshot BIGINT,
		end_snapshot BIGINT,
		schema_name VARCHAR,
		path VARCHAR,
		path_is_relative BIGINT
	);
	CREATE TABLE IF NOT EXISTS ducklake_table(
		table_id BIGINT,
		table_uuid VARCHAR,
		begin_snapshot BIGINT,
		end_snapshot BIGINT,
		schema_id BIGINT,
		table_name VARCHAR,
		path VARCHAR,
		path_is_relative BIGINT
	);
	CREATE TABLE IF NOT EXISTS ducklake_column(
		column_id BIGINT,
		begin_snapshot BIGINT,
		end_snapshot BIGINT,
		table_id BIGINT,
		column_order BIGINT,
		column_name VARCHAR,
		column_type VARCHAR,
		initial_default VARCHAR,
		default_value VARCHAR,
		nulls_allowed BIGINT,
		parent_column BIGINT
	);
	CREATE TABLE IF NOT EXISTS ducklake_data_file(
		data_file_id BIGINT PRIMARY KEY,
		table_id BIGINT,
		begin_snapshot BIGINT,
		end_snapshot BIGINT,
		file_order BIGINT,
		path VARCHAR,
		path_is_relative BIGINT,
		file_format VARCHAR,
		record_count BIGINT,
		file_size_bytes BIGINT,
		footer_size BIGINT,
		row_id_start BIGINT,
		partition_id BIGINT,
		encryption_key VARCHAR,
		partial_file_info VARCHAR,
		mapping_id BIGINT
	);`

	const staticData = `
	-- ducklake_metadata (excluding data_path — inserted separately)
	INSERT INTO ducklake_metadata("key", "value") VALUES ('version', '0.3');
	INSERT INTO ducklake_metadata("key", "value") VALUES ('created_by', 'DuckDB 6ddac802ff');
	INSERT INTO ducklake_metadata("key", "value") VALUES ('encrypted', 'false');

	-- ducklake_schema (main)
	INSERT INTO ducklake_schema VALUES (
		0, '38632316-cb07-4c65-96e1-767465b56bcf', 0, NULL,
		'main', 'main/', 1
	);

	-- ducklake_table (titanic)
	INSERT INTO ducklake_table VALUES (
		1, '019c4727-c55b-79e9-90bb-28f40bacf385', 1, NULL,
		0, 'titanic', 'titanic/', 1
	);

	-- ducklake_column (all 12 columns of titanic)
	INSERT INTO ducklake_column VALUES (1,  1, NULL, 1, 1,  'PassengerId', 'int64',   NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (2,  1, NULL, 1, 2,  'Survived',    'int64',   NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (3,  1, NULL, 1, 3,  'Pclass',      'int64',   NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (4,  1, NULL, 1, 4,  'Name',        'varchar', NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (5,  1, NULL, 1, 5,  'Sex',         'varchar', NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (6,  1, NULL, 1, 6,  'Age',         'float64', NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (7,  1, NULL, 1, 7,  'SibSp',       'int64',   NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (8,  1, NULL, 1, 8,  'Parch',       'int64',   NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (9,  1, NULL, 1, 9,  'Ticket',      'varchar', NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (10, 1, NULL, 1, 10, 'Fare',        'float64', NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (11, 1, NULL, 1, 11, 'Cabin',       'varchar', NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (12, 1, NULL, 1, 12, 'Embarked',    'varchar', NULL, NULL, 1, NULL);

	-- ducklake_data_file (single parquet file, 891 rows)
	INSERT INTO ducklake_data_file VALUES (
		0, 1, 1, NULL, NULL,
		'ducklake-019c4727-c55c-7e4d-ab38-e01a2920253c.parquet',
		1, 'parquet', 891, 36014, 1332, 0, NULL, NULL, NULL, NULL
	);`

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("create ducklake DDL: %v", err)
	}
	if _, err := db.ExecContext(ctx, staticData); err != nil {
		t.Fatalf("seed ducklake static data: %v", err)
	}
	dataPathSQL := fmt.Sprintf(
		`INSERT INTO ducklake_metadata("key", "value") VALUES ('data_path', '%s');`,
		dataPath,
	)
	if _, err := db.ExecContext(ctx, dataPathSQL); err != nil {
		t.Fatalf("seed ducklake data_path: %v", err)
	}
}

// ---------------------------------------------------------------------------
// RBAC seeding
// ---------------------------------------------------------------------------

// apiKeys holds the plaintext API keys for each test role.
type apiKeys struct {
	Admin      string
	Analyst    string
	Researcher string
	NoAccess   string
}

// seedRBAC creates principals, groups, grants, row filters, column masks,
// and API keys in the temp SQLite. Returns plaintext API key strings.
func seedRBAC(t *testing.T, db *sql.DB) apiKeys {
	t.Helper()

	q := dbstore.New(db)

	// --- Principals ---
	adminUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin_user", Type: "user", IsAdmin: 1,
	})
	if err != nil {
		t.Fatalf("create admin_user: %v", err)
	}

	analyst1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create analyst1: %v", err)
	}

	researcher1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "researcher1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create researcher1: %v", err)
	}

	noAccessUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "no_access_user", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create no_access_user: %v", err)
	}

	// --- Groups ---
	adminsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "admins"})
	if err != nil {
		t.Fatalf("create admins group: %v", err)
	}

	analystsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})
	if err != nil {
		t.Fatalf("create analysts group: %v", err)
	}

	researchersGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "researchers"})
	if err != nil {
		t.Fatalf("create researchers group: %v", err)
	}

	// --- Group memberships ---
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID,
	}); err != nil {
		t.Fatalf("add admin to admins: %v", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst1.ID,
	}); err != nil {
		t.Fatalf("add analyst to analysts: %v", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: researchersGroup.ID, MemberType: "user", MemberID: researcher1.ID,
	}); err != nil {
		t.Fatalf("add researcher to researchers: %v", err)
	}

	// --- Grants ---
	// admins → ALL_PRIVILEGES on catalog
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: "catalog", SecurableID: 0, Privilege: "ALL_PRIVILEGES",
	}); err != nil {
		t.Fatalf("grant admins ALL_PRIVILEGES: %v", err)
	}

	// analysts → USAGE on schema + SELECT on table
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: 0, Privilege: "USAGE",
	}); err != nil {
		t.Fatalf("grant analysts USAGE: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: 1, Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant analysts SELECT: %v", err)
	}

	// researchers → USAGE on schema + SELECT on table
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: researchersGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: 0, Privilege: "USAGE",
	}); err != nil {
		t.Fatalf("grant researchers USAGE: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: researchersGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: 1, Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant researchers SELECT: %v", err)
	}

	// --- Row Filters ---
	// analysts: "Pclass" = 1 on titanic (table_id=1)
	filter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID: 1, FilterSql: `"Pclass" = 1`,
	})
	if err != nil {
		t.Fatalf("create row filter: %v", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: filter.ID, PrincipalID: analystsGroup.ID, PrincipalType: "group",
	}); err != nil {
		t.Fatalf("bind row filter to analysts: %v", err)
	}

	// --- Column Masks ---
	// Name → '***' on titanic (table_id=1)
	nameMask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		TableID: 1, ColumnName: "Name", MaskExpression: `'***'`,
	})
	if err != nil {
		t.Fatalf("create column mask: %v", err)
	}
	// analysts: see_original=0 (Name is masked)
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: nameMask.ID, PrincipalID: analystsGroup.ID,
		PrincipalType: "group", SeeOriginal: 0,
	}); err != nil {
		t.Fatalf("bind mask to analysts: %v", err)
	}
	// researchers: see_original=1 (Name is visible)
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: nameMask.ID, PrincipalID: researchersGroup.ID,
		PrincipalType: "group", SeeOriginal: 1,
	}); err != nil {
		t.Fatalf("bind mask to researchers: %v", err)
	}

	// --- API Keys (stored as SHA-256 hashes) ---
	keys := apiKeys{
		Admin:      "test-admin-key",
		Analyst:    "test-analyst-key",
		Researcher: "test-researcher-key",
		NoAccess:   "test-noaccess-key",
	}

	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		KeyHash: sha256Hex(keys.Admin), PrincipalID: adminUser.ID, Name: "admin-test",
	}); err != nil {
		t.Fatalf("create admin API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		KeyHash: sha256Hex(keys.Analyst), PrincipalID: analyst1.ID, Name: "analyst-test",
	}); err != nil {
		t.Fatalf("create analyst API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		KeyHash: sha256Hex(keys.Researcher), PrincipalID: researcher1.ID, Name: "researcher-test",
	}); err != nil {
		t.Fatalf("create researcher API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		KeyHash: sha256Hex(keys.NoAccess), PrincipalID: noAccessUser.ID, Name: "noaccess-test",
	}); err != nil {
		t.Fatalf("create noaccess API key: %v", err)
	}

	return keys
}

// ---------------------------------------------------------------------------
// Server setup
// ---------------------------------------------------------------------------

// testEnv bundles the shared test server and API keys.
type testEnv struct {
	Server *httptest.Server
	Keys   apiKeys
}

// setupIntegrationServer creates a fully-wired in-process Go API server with
// real auth middleware, real S3 presigner, and seeded RBAC + DuckLake metadata.
func setupIntegrationServer(t *testing.T) *testEnv {
	t.Helper()

	// Load S3 config from .env
	_ = config.LoadDotEnv(dotEnvPath())
	cfg, err := config.LoadFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// Temp SQLite with hardened connection (WAL, busy_timeout, etc.)
	metaDB, _ := internaldb.OpenTestSQLite(t)

	// Seed DuckLake catalog metadata + RBAC data
	seedDuckLakeMetadata(t, metaDB, "s3://yacobolo/lake_data/")
	keys := seedRBAC(t, metaDB)

	// Build repositories
	principalRepo := repository.NewPrincipalRepo(metaDB)
	groupRepo := repository.NewGroupRepo(metaDB)
	grantRepo := repository.NewGrantRepo(metaDB)
	rowFilterRepo := repository.NewRowFilterRepo(metaDB)
	columnMaskRepo := repository.NewColumnMaskRepo(metaDB)
	auditRepo := repository.NewAuditRepo(metaDB)
	introspectionRepo := repository.NewIntrospectionRepo(metaDB)
	apiKeyRepo := repository.NewAPIKeyRepo(metaDB)
	tagRepo := repository.NewTagRepo(metaDB)
	lineageRepo := repository.NewLineageRepo(metaDB)
	searchRepo := repository.NewSearchRepo(metaDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(metaDB)
	viewRepo := repository.NewViewRepo(metaDB)

	// Build services
	authSvc := service.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
	)

	presigner, err := service.NewS3Presigner(cfg)
	if err != nil {
		t.Fatalf("create presigner: %v", err)
	}

	manifestSvc := service.NewManifestService(
		metaDB, authSvc, presigner, introspectionRepo, auditRepo,
	)

	// Remaining services (querySvc gets nil engine — we never hit /v1/query)
	querySvc := service.NewQueryService(nil, auditRepo, nil)
	principalSvc := service.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := service.NewGroupService(groupRepo, auditRepo)
	grantSvc := service.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := service.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := service.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := service.NewAuditService(auditRepo)
	tagSvc := service.NewTagService(tagRepo, auditRepo)
	lineageSvc := service.NewLineageService(lineageRepo)
	searchSvc := service.NewSearchService(searchRepo)
	queryHistorySvc := service.NewQueryHistoryService(queryHistoryRepo)
	catalogRepo := repository.NewCatalogRepo(metaDB, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	viewSvc := service.NewViewService(viewRepo, catalogRepo, authSvc, auditRepo)

	handler := api.NewHandler(
		querySvc, principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, auditSvc,
		manifestSvc, nil, // catalogSvc=nil — integration tests only hit /v1/manifest
		queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc,
		nil,                     // ingestionSvc
		nil, nil, nil, nil, nil, // storageCredSvc, extLocationSvc, volumeSvc, computeEndpointSvc, apiKeySvc
	)
	strictHandler := api.NewStrictHandler(handler, nil)

	// Router with REAL auth middleware (API key via SHA-256 hash lookup)
	r := chi.NewRouter()
	r.Use(middleware.AuthMiddleware([]byte("test-jwt-secret"), apiKeyRepo))
	r.Route("/v1", func(r chi.Router) {
		api.HandlerFromMux(strictHandler, r)
	})

	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	return &testEnv{Server: srv, Keys: keys}
}

// ---------------------------------------------------------------------------
// Local extension server setup (no S3 — uses local parquet files)
// ---------------------------------------------------------------------------

// localPresigner returns file paths as-is (no S3 presigning).
// Used by extension tests that run against local parquet files.
type localPresigner struct{}

func (p *localPresigner) PresignGetObject(_ context.Context, path string, _ time.Duration) (string, error) {
	return path, nil
}

// copyFile copies a file from src to dst. Fails the test on error.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

// testdataPath returns the path to a file in the test/integration/testdata directory.
func testdataPath(name string) string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "testdata", name)
}

// setupLocalExtensionServer creates a fully-wired in-process Go API server for
// extension tests using local filesystem instead of S3. The manifest endpoint
// returns local file paths to parquet files, which DuckDB's read_parquet() reads
// directly. Requires extension binary + DuckDB CLI, but NOT S3 credentials.
func setupLocalExtensionServer(t *testing.T) *testEnv {
	t.Helper()

	// Create temp directory for local data files
	tmpDir := t.TempDir()
	dataPath := filepath.Join(tmpDir, "lake_data") + "/"
	if err := os.MkdirAll(filepath.Join(tmpDir, "lake_data"), 0o755); err != nil {
		t.Fatalf("mkdir lake_data: %v", err)
	}

	// Copy testdata/titanic.parquet into the expected ducklake data file location.
	// The seed metadata references this filename as a relative path under data_path.
	copyFile(t,
		testdataPath("titanic.parquet"),
		filepath.Join(tmpDir, "lake_data", ducklakeDataFileName),
	)

	// Temp SQLite with hardened connection (WAL, busy_timeout, etc.)
	metaDB, _ := internaldb.OpenTestSQLite(t)

	// Seed DuckLake metadata with LOCAL data_path
	seedDuckLakeMetadata(t, metaDB, dataPath)
	keys := seedRBAC(t, metaDB)

	// Build repositories
	principalRepo := repository.NewPrincipalRepo(metaDB)
	groupRepo := repository.NewGroupRepo(metaDB)
	grantRepo := repository.NewGrantRepo(metaDB)
	rowFilterRepo := repository.NewRowFilterRepo(metaDB)
	columnMaskRepo := repository.NewColumnMaskRepo(metaDB)
	auditRepo := repository.NewAuditRepo(metaDB)
	introspectionRepo := repository.NewIntrospectionRepo(metaDB)
	apiKeyRepo := repository.NewAPIKeyRepo(metaDB)
	tagRepo := repository.NewTagRepo(metaDB)
	lineageRepo := repository.NewLineageRepo(metaDB)
	searchRepo := repository.NewSearchRepo(metaDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(metaDB)
	viewRepo := repository.NewViewRepo(metaDB)

	// Build services
	authSvc := service.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
	)

	// LOCAL presigner — returns paths as-is (no S3 presigning)
	manifestSvc := service.NewManifestService(
		metaDB, authSvc, &localPresigner{}, introspectionRepo, auditRepo,
	)

	// Remaining services (querySvc gets nil engine — we never hit /v1/query)
	querySvc := service.NewQueryService(nil, auditRepo, nil)
	principalSvc := service.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := service.NewGroupService(groupRepo, auditRepo)
	grantSvc := service.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := service.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := service.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := service.NewAuditService(auditRepo)
	tagSvc := service.NewTagService(tagRepo, auditRepo)
	lineageSvc := service.NewLineageService(lineageRepo)
	searchSvc := service.NewSearchService(searchRepo)
	queryHistorySvc := service.NewQueryHistoryService(queryHistoryRepo)
	catalogRepo := repository.NewCatalogRepo(metaDB, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	viewSvc := service.NewViewService(viewRepo, catalogRepo, authSvc, auditRepo)

	handler := api.NewHandler(
		querySvc, principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, auditSvc,
		manifestSvc, nil, // catalogSvc=nil — extension tests only hit /v1/manifest
		queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc,
		nil,                     // ingestionSvc
		nil, nil, nil, nil, nil, // storageCredSvc, extLocationSvc, volumeSvc, computeEndpointSvc, apiKeySvc
	)
	strictHandler := api.NewStrictHandler(handler, nil)

	// Router with REAL auth middleware (API key via SHA-256 hash lookup)
	r := chi.NewRouter()
	r.Use(middleware.AuthMiddleware([]byte("test-jwt-secret"), apiKeyRepo))
	r.Route("/v1", func(r chi.Router) {
		api.HandlerFromMux(strictHandler, r)
	})

	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	return &testEnv{Server: srv, Keys: keys}
}

// ---------------------------------------------------------------------------
// DuckDB CLI runner
// ---------------------------------------------------------------------------

// duckDBResult represents rows of DuckDB JSON output.
// DuckDB -json outputs [{"col":"val"}, ...].
type duckDBResult []map[string]interface{}

// runDuckDBQuery executes SQL through the DuckDB CLI with the duck_access
// extension loaded and a secret configured for the given API key/server.
func runDuckDBQuery(t *testing.T, serverURL, apiKey, query string) (duckDBResult, string, error) {
	t.Helper()

	absExtPath := extensionPath()

	// Build the SQL preamble + query.
	// - allow_unsigned_extensions must be a CLI flag (cannot SET while DB is running)
	// - httpfs is required for read_parquet with https:// presigned URLs
	// - Use autoload instead of INSTALL (avoids lock contention when parallel
	//   tests all try to INSTALL httpfs simultaneously)
	sqlText := fmt.Sprintf(`SET autoinstall_known_extensions=true;
SET autoload_known_extensions=true;
LOAD '%s';
CREATE SECRET my_platform (TYPE duck_access, API_URL '%s/v1', API_KEY '%s');
%s
`, absExtPath, serverURL, apiKey, query)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, duckdbCLIPath(), "-json", "-unsigned")
	cmd.Stdin = strings.NewReader(sqlText)

	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	// DuckDB -json outputs one JSON array per SQL statement. Multi-row results
	// span multiple lines. We want the LAST top-level JSON array (the query result).
	// Strategy: find the last ']' in stdout, then scan backwards for its matching '['.
	var result duckDBResult
	outStr := strings.TrimSpace(stdout.String())
	if outStr != "" {
		lastJSON := extractLastJSONArray(outStr)
		if lastJSON != "" && lastJSON != "[]" {
			if jsonErr := json.Unmarshal([]byte(lastJSON), &result); jsonErr != nil {
				t.Logf("DuckDB JSON parse failed: %v\nraw: %s", jsonErr, lastJSON)
			}
		}
	}

	return result, stderr.String(), err
}

// extractLastJSONArray finds the last top-level [...] block in the output.
// DuckDB -json may output multiple arrays (one per statement), potentially
// spanning multiple lines for multi-row results. We scan backwards from the
// end to find the last balanced [...] pair, skipping brackets inside JSON
// string literals to avoid being confused by values like "[a,b]".
func extractLastJSONArray(s string) string {
	// Find the last ']'
	end := strings.LastIndex(s, "]")
	if end == -1 {
		return ""
	}

	// Scan backwards to find the matching '['.
	// Track whether we're inside a JSON string to skip embedded brackets.
	depth := 0
	inString := false
	for i := end; i >= 0; i-- {
		ch := s[i]

		// Detect string boundaries (scanning backwards).
		// A '"' toggles inString unless it's escaped (preceded by '\').
		if ch == '"' {
			escaped := false
			if i > 0 && s[i-1] == '\\' {
				// Count consecutive backslashes before this quote
				backslashes := 0
				for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
					backslashes++
				}
				escaped = backslashes%2 == 1
			}
			if !escaped {
				inString = !inString
			}
			continue
		}

		if inString {
			continue
		}

		switch ch {
		case ']':
			depth++
		case '[':
			depth--
			if depth == 0 {
				return s[i : end+1]
			}
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Local DuckLake setup (no S3, no credentials — for catalog repo tests)
// ---------------------------------------------------------------------------

// catalogTestEnv bundles the DuckDB + SQLite connections for catalog repo tests.
type catalogTestEnv struct {
	DuckDB *sql.DB
	MetaDB *sql.DB
}

// setupLocalDuckLake creates a local DuckLake instance using filesystem storage.
// It opens an in-memory DuckDB, installs the ducklake + sqlite extensions,
// attaches a DuckLake catalog backed by a temp SQLite file, opens a separate
// SQLite connection for metaDB reads, and runs app migrations (catalog_metadata).
// Fails the test if DuckLake extensions are unavailable.
func setupLocalDuckLake(t *testing.T) *catalogTestEnv {
	t.Helper()

	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta.sqlite")
	dataPath := filepath.Join(tmpDir, "lake_data") + "/"
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatalf("mkdir lake_data: %v", err)
	}

	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = duckDB.Close() })

	// Install and load DuckLake + SQLite extensions (fail if unavailable)
	for _, stmt := range []string{
		"INSTALL ducklake", "LOAD ducklake",
		"INSTALL sqlite", "LOAD sqlite",
	} {
		if _, err := duckDB.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s failed (extension not available): %v", stmt, err)
		}
	}

	// Attach DuckLake catalog with local filesystem storage
	attachSQL := fmt.Sprintf(
		`ATTACH 'ducklake:sqlite:%s' AS lake (DATA_PATH '%s')`,
		metaPath, dataPath,
	)
	if _, err := duckDB.ExecContext(ctx, attachSQL); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if _, err := duckDB.ExecContext(ctx, "USE lake"); err != nil {
		t.Fatalf("use lake: %v", err)
	}

	// Open same SQLite for metaDB with hardened connection settings
	metaDB, err := internaldb.OpenSQLite(metaPath, "write", 0)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = metaDB.Close() })

	// Run app migrations (creates catalog_metadata table)
	if err := internaldb.RunMigrations(metaDB); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	return &catalogTestEnv{DuckDB: duckDB, MetaDB: metaDB}
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

// getScalarInt extracts a single integer value from a 1-row result.
// DuckDB JSON outputs numbers as float64 in Go's json.Unmarshal.
func getScalarInt(t *testing.T, result duckDBResult, column string) int {
	t.Helper()
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	v, ok := result[0][column]
	if !ok {
		t.Fatalf("column %q not found in result: %v", column, result[0])
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		t.Fatalf("column %q: expected number, got %T (%v)", column, v, v)
		return 0
	}
}

// containsAny returns true if s contains any of the substrings (case-insensitive).
func containsAny(s string, subs ...string) bool {
	lower := strings.ToLower(s)
	for _, sub := range subs {
		if strings.Contains(lower, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

// titanicColumns lists the expected column names in the titanic dataset.
var titanicColumns = []string{
	"PassengerId", "Survived", "Pclass", "Name", "Sex", "Age",
	"SibSp", "Parch", "Ticket", "Fare", "Cabin", "Embarked",
}

// ---------------------------------------------------------------------------
// Generic HTTP test server setup (no S3 required)
// ---------------------------------------------------------------------------

// httpTestOpts configures which optional services to wire into the test server.
type httpTestOpts struct {
	// WithDuckLake wires a real CatalogService backed by local DuckLake.
	// Requires ducklake+sqlite DuckDB extensions (test skips if unavailable).
	WithDuckLake bool
	// SeedDuckLakeMetadata seeds the ducklake_* tables in SQLite with hardcoded
	// titanic dataset metadata. Use when DuckLake extensions are NOT available
	// but you still need schema/table/column IDs for RBAC operations.
	SeedDuckLakeMetadata bool
	// JWTSecret overrides the default "test-jwt-secret".
	JWTSecret []byte
	// WithStorageCredentials wires StorageCredentialService and ExternalLocationService.
	WithStorageCredentials bool
	// WithComputeEndpoints wires ComputeEndpointService, a full resolver, and a
	// SecureEngine so that /v1/query routes through the compute resolver.
	WithComputeEndpoints bool
	// WithAuthenticator uses the full Authenticator struct with JIT provisioning
	// instead of the legacy AuthMiddleware wrapper.
	WithAuthenticator bool
	// BootstrapAdmin is the external ID (sub) of the bootstrap admin user.
	// Only used when WithAuthenticator is true.
	BootstrapAdmin string
	// NameClaim overrides the JWT claim used for principal name resolution.
	// Only used when WithAuthenticator is true.
	NameClaim string
	// WithAPIKeyService wires the APIKeyService into the handler (enables API key
	// management endpoints). When false, the handler gets nil for apiKeySvc.
	WithAPIKeyService bool
}

// httpTestEnv bundles the test server, API keys, and direct DB access.
type httpTestEnv struct {
	Server         *httptest.Server
	Keys           apiKeys
	MetaDB         *sql.DB
	DuckDB         *sql.DB                          // nil unless WithDuckLake
	ExtLocationSvc *service.ExternalLocationService // nil unless WithStorageCredentials
}

// setupHTTPServer creates a fully-wired in-process HTTP server with real auth
// middleware and real SQLite repositories. Does NOT require S3 credentials.
func setupHTTPServer(t *testing.T, opts httpTestOpts) *httpTestEnv {
	t.Helper()

	jwtSecret := opts.JWTSecret
	if jwtSecret == nil {
		jwtSecret = []byte("test-jwt-secret")
	}

	// Temp SQLite with hardened connection (WAL, busy_timeout, etc.)
	metaDB, _ := internaldb.OpenTestSQLite(t)

	// Optionally seed DuckLake metadata (without DuckLake extensions)
	if opts.SeedDuckLakeMetadata {
		seedDuckLakeMetadata(t, metaDB, "s3://yacobolo/lake_data/")
	}

	// Seed RBAC data (principals, groups, grants, row filters, column masks, API keys)
	keys := seedRBAC(t, metaDB)

	// Build repositories
	principalRepo := repository.NewPrincipalRepo(metaDB)
	groupRepo := repository.NewGroupRepo(metaDB)
	grantRepo := repository.NewGrantRepo(metaDB)
	rowFilterRepo := repository.NewRowFilterRepo(metaDB)
	columnMaskRepo := repository.NewColumnMaskRepo(metaDB)
	auditRepo := repository.NewAuditRepo(metaDB)
	introspectionRepo := repository.NewIntrospectionRepo(metaDB)
	apiKeyRepo := repository.NewAPIKeyRepo(metaDB)
	tagRepo := repository.NewTagRepo(metaDB)
	lineageRepo := repository.NewLineageRepo(metaDB)
	searchRepo := repository.NewSearchRepo(metaDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(metaDB)
	viewRepo := repository.NewViewRepo(metaDB)

	// Build authorization service unconditionally (needed by viewSvc)
	authSvc := service.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
	)

	// Build services
	principalSvc := service.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := service.NewGroupService(groupRepo, auditRepo)
	grantSvc := service.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := service.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := service.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := service.NewAuditService(auditRepo)
	tagSvc := service.NewTagService(tagRepo, auditRepo)
	lineageSvc := service.NewLineageService(lineageRepo)
	searchSvc := service.NewSearchService(searchRepo)
	queryHistorySvc := service.NewQueryHistoryService(queryHistoryRepo)

	// querySvc gets nil engine — no /v1/query support unless WithDuckLake+engine
	querySvc := service.NewQueryService(nil, auditRepo, nil)

	// catalogRepo with duckDB=nil is safe — GetSchema only reads ducklake_schema from metaDB
	catalogRepo := repository.NewCatalogRepo(metaDB, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	viewSvc := service.NewViewService(viewRepo, catalogRepo, authSvc, auditRepo)

	var duckDB *sql.DB
	var manifestSvc *service.ManifestService
	tableStatsRepo := repository.NewTableStatisticsRepo(metaDB)
	catalogSvc := service.NewCatalogService(catalogRepo, authSvc, auditRepo, tagRepo, tableStatsRepo)

	if opts.WithDuckLake {
		env := setupLocalDuckLake(t)
		duckDB = env.DuckDB
		// The local DuckLake setup creates its own SQLite; but we need RBAC in the
		// same DB. Re-run migrations and re-seed into the DuckLake metaDB.
		// Actually, we need to use the DuckLake metaDB for everything.
		// Close the original metaDB and replace it.
		_ = metaDB.Close()
		metaDB = env.MetaDB

		// Re-seed RBAC data into the DuckLake metaDB (migrations already ran in setupLocalDuckLake)
		keys = seedRBAC(t, metaDB)

		// Rebuild repos on the new metaDB
		principalRepo = repository.NewPrincipalRepo(metaDB)
		groupRepo = repository.NewGroupRepo(metaDB)
		grantRepo = repository.NewGrantRepo(metaDB)
		rowFilterRepo = repository.NewRowFilterRepo(metaDB)
		columnMaskRepo = repository.NewColumnMaskRepo(metaDB)
		auditRepo = repository.NewAuditRepo(metaDB)
		introspectionRepo = repository.NewIntrospectionRepo(metaDB)
		apiKeyRepo = repository.NewAPIKeyRepo(metaDB)
		tagRepo = repository.NewTagRepo(metaDB)
		lineageRepo = repository.NewLineageRepo(metaDB)
		searchRepo = repository.NewSearchRepo(metaDB)
		queryHistoryRepo = repository.NewQueryHistoryRepo(metaDB)
		viewRepo = repository.NewViewRepo(metaDB)

		// Rebuild services on new repos
		authSvc = service.NewAuthorizationService(
			principalRepo, groupRepo, grantRepo,
			rowFilterRepo, columnMaskRepo, introspectionRepo,
		)
		principalSvc = service.NewPrincipalService(principalRepo, auditRepo)
		groupSvc = service.NewGroupService(groupRepo, auditRepo)
		grantSvc = service.NewGrantService(grantRepo, auditRepo)
		rowFilterSvc = service.NewRowFilterService(rowFilterRepo, auditRepo)
		columnMaskSvc = service.NewColumnMaskService(columnMaskRepo, auditRepo)
		auditSvc = service.NewAuditService(auditRepo)
		querySvc = service.NewQueryService(nil, auditRepo, nil)
		tagSvc = service.NewTagService(tagRepo, auditRepo)
		lineageSvc = service.NewLineageService(lineageRepo)
		searchSvc = service.NewSearchService(searchRepo)
		queryHistorySvc = service.NewQueryHistoryService(queryHistoryRepo)

		catalogRepo = repository.NewCatalogRepo(metaDB, duckDB, slog.New(slog.NewTextHandler(io.Discard, nil)))
		viewSvc = service.NewViewService(viewRepo, catalogRepo, authSvc, auditRepo)
		tableStatsRepo = repository.NewTableStatisticsRepo(metaDB)
		catalogSvc = service.NewCatalogService(catalogRepo, authSvc, auditRepo, tagRepo, tableStatsRepo)

		// manifestSvc needs S3 presigner — leave nil for non-S3 tests
	}

	// Optionally wire storage credential and external location services
	var storageCredSvc *service.StorageCredentialService
	var extLocationSvc *service.ExternalLocationService

	if opts.WithStorageCredentials {
		testEncKey := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		enc, err := crypto.NewEncryptor(testEncKey)
		if err != nil {
			t.Fatalf("create encryptor: %v", err)
		}
		storageCredRepo := repository.NewStorageCredentialRepo(metaDB, enc)
		extLocationRepo := repository.NewExternalLocationRepo(metaDB)
		storageCredSvc = service.NewStorageCredentialService(storageCredRepo, authSvc, auditRepo)

		// ExternalLocationService needs a DuckDB for CREATE SECRET / DROP SECRET.
		// If duckDB is nil (no WithDuckLake), open a plain in-memory DuckDB.
		extDuckDB := duckDB
		if extDuckDB == nil {
			extDuckDB, err = sql.Open("duckdb", "")
			if err != nil {
				t.Fatalf("open duckdb for ext locations: %v", err)
			}
			t.Cleanup(func() { _ = extDuckDB.Close() })
		}
		extLocationSvc = service.NewExternalLocationService(
			extLocationRepo, storageCredRepo, authSvc, auditRepo,
			extDuckDB, "", slog.New(slog.NewTextHandler(io.Discard, nil)),
		)
	}

	// Optionally wire compute endpoints with full resolver + engine
	var computeEndpointSvc *service.ComputeEndpointService

	if opts.WithComputeEndpoints {
		testEncKey := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		enc, encErr := crypto.NewEncryptor(testEncKey)
		if encErr != nil {
			t.Fatalf("create encryptor for compute: %v", encErr)
		}

		// Need a DuckDB for the engine
		engineDuckDB := duckDB
		if engineDuckDB == nil {
			var openErr error
			engineDuckDB, openErr = sql.Open("duckdb", "")
			if openErr != nil {
				t.Fatalf("open duckdb for compute engine: %v", openErr)
			}
			t.Cleanup(func() { _ = engineDuckDB.Close() })
			duckDB = engineDuckDB
		}

		computeEndpointRepo := repository.NewComputeEndpointRepo(metaDB, enc)
		computeEndpointSvc = service.NewComputeEndpointService(computeEndpointRepo, authSvc, auditRepo)

		// Build full resolver: local executor + compute repo + principal/group repos
		localExec := compute.NewLocalExecutor(engineDuckDB)
		remoteCache := compute.NewRemoteCache(engineDuckDB)
		resolver := compute.NewResolver(
			localExec, computeEndpointRepo, principalRepo, groupRepo,
			remoteCache, slog.New(slog.NewTextHandler(io.Discard, nil)),
		)

		// Build SecureEngine with the resolver
		eng := engine.NewSecureEngine(engineDuckDB, authSvc, resolver,
			slog.New(slog.NewTextHandler(io.Discard, nil)))

		// Rebuild querySvc with the real engine
		querySvc = service.NewQueryService(eng, auditRepo, lineageRepo)
	}

	// Optionally wire APIKeyService
	var apiKeySvc *service.APIKeyService
	if opts.WithAPIKeyService {
		apiKeySvc = service.NewAPIKeyService(apiKeyRepo, auditRepo)
	}

	handler := api.NewHandler(
		querySvc, principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, auditSvc,
		manifestSvc, catalogSvc,
		queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc,
		nil,                                 // ingestionSvc
		storageCredSvc, extLocationSvc, nil, // volumeSvc
		computeEndpointSvc,
		apiKeySvc,
	)
	strictHandler := api.NewStrictHandler(handler, nil)

	r := chi.NewRouter()

	// Always use the full Authenticator for correct IsAdmin resolution.
	// JIT provisioning is enabled when WithAuthenticator is set.
	nameClaim := opts.NameClaim
	if nameClaim == "" {
		nameClaim = "sub"
	}
	authCfg := config.AuthConfig{
		SharedSecret:   string(jwtSecret),
		APIKeyEnabled:  true,
		APIKeyHeader:   "X-API-Key",
		NameClaim:      nameClaim,
		BootstrapAdmin: opts.BootstrapAdmin,
	}
	validator := middleware.NewSharedSecretValidator(string(jwtSecret))
	var provisioner middleware.PrincipalProvisioner
	if opts.WithAuthenticator {
		provisioner = principalSvc
	}
	authenticator := middleware.NewAuthenticator(
		validator,
		apiKeyRepo,
		principalRepo,
		provisioner,
		authCfg,
		nil, // logger
	)
	r.Use(authenticator.Middleware())
	r.Route("/v1", func(r chi.Router) {
		api.HandlerFromMux(strictHandler, r)
	})

	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	return &httpTestEnv{
		Server:         srv,
		Keys:           keys,
		MetaDB:         metaDB,
		DuckDB:         duckDB,
		ExtLocationSvc: extLocationSvc,
	}
}

// ---------------------------------------------------------------------------
// HTTP request helpers
// ---------------------------------------------------------------------------

// doRequest makes an HTTP request with JSON body and optional API key header.
func doRequest(t *testing.T, method, url, apiKey string, body interface{}) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		bodyReader = strings.NewReader(string(b))
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("execute request: %v", err)
	}
	return resp
}

// doRequestWithBearer makes an HTTP request with Authorization: Bearer header.
func doRequestWithBearer(t *testing.T, method, url, token string, body interface{}) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		bodyReader = strings.NewReader(string(b))
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("execute request: %v", err)
	}
	return resp
}

// readBody reads and returns the response body, closing it afterwards.
func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return b
}

// decodeJSON decodes a JSON response body into the given target.
func decodeJSON(t *testing.T, resp *http.Response, target interface{}) {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		t.Fatalf("decode response JSON: %v", err)
	}
}

// generateJWT creates a signed HS256 JWT token with the given subject and expiry.
// Includes an "iss" claim for compatibility with JIT provisioning (which looks
// up by (issuer, external_id) — without iss, the issuer is stored as NULL
// and SQL NULL=NULL comparisons fail on subsequent lookups).
func generateJWT(t *testing.T, secret []byte, subject string, expiry time.Time) string {
	t.Helper()
	claims := jwt.MapClaims{
		"sub": subject,
		"iss": "test-issuer",
		"exp": expiry.Unix(),
		"iat": time.Now().Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}
	return signed
}

// fetchAuditLogs calls GET /v1/audit-logs on the test server and returns
// the parsed entries. Used to verify the manifest endpoint writes audit records.
func fetchAuditLogs(t *testing.T, serverURL, apiKey string) []map[string]interface{} {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, "GET", serverURL+"/v1/audit-logs", nil)
	if err != nil {
		t.Fatalf("create audit request: %v", err)
	}
	req.Header.Set("X-API-Key", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetch audit logs: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("audit logs returned %d: %s", resp.StatusCode, body)
	}

	var parsed struct {
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode audit response: %v", err)
	}
	return parsed.Data
}

// ---------------------------------------------------------------------------
// Compute endpoint test helpers
// ---------------------------------------------------------------------------

// agentTestEnv holds the in-process compute agent and its auth token.
type agentTestEnv struct {
	Server     *httptest.Server
	AgentToken string
}

// startTestAgent starts an in-process compute agent backed by a plain in-memory
// DuckDB. No extensions, no S3, no DuckLake — just raw SQL execution. Returns
// the agent's httptest.Server URL and the auth token for X-Agent-Token.
func startTestAgent(t *testing.T) *agentTestEnv {
	t.Helper()

	agentToken := "test-agent-secret-token"

	agentDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open agent duckdb: %v", err)
	}
	t.Cleanup(func() { _ = agentDB.Close() })

	handler := agent.NewHandler(agent.HandlerConfig{
		DB:         agentDB,
		AgentToken: agentToken,
		StartTime:  time.Now(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return &agentTestEnv{Server: srv, AgentToken: agentToken}
}

// lookupPrincipalID finds a principal by name via the API and returns its ID.
func lookupPrincipalID(t *testing.T, env *httpTestEnv, name string) float64 {
	t.Helper()
	resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	if resp.StatusCode != 200 {
		body := readBody(t, resp)
		t.Fatalf("list principals returned %d: %s", resp.StatusCode, body)
	}

	var result map[string]interface{}
	decodeJSON(t, resp, &result)

	data, ok := result["data"].([]interface{})
	if !ok {
		t.Fatalf("unexpected principals response shape: %v", result)
	}
	for _, p := range data {
		pm, _ := p.(map[string]interface{})
		if pm["name"] == name {
			id, _ := pm["id"].(float64)
			return id
		}
	}
	t.Fatalf("principal %q not found", name)
	return 0
}

// setupRemoteEndpoint creates and activates a REMOTE compute endpoint pointing
// to the given agent. Uses the admin API key.
func setupRemoteEndpoint(t *testing.T, env *httpTestEnv, agentEnv *agentTestEnv, name string) {
	t.Helper()

	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin,
		map[string]interface{}{
			"name":       name,
			"url":        agentEnv.Server.URL,
			"type":       "REMOTE",
			"auth_token": agentEnv.AgentToken,
		})
	if resp.StatusCode != 201 {
		body := readBody(t, resp)
		t.Fatalf("create endpoint returned %d: %s", resp.StatusCode, body)
	}
	_ = resp.Body.Close()

	resp = doRequest(t, "PATCH", env.Server.URL+"/v1/compute-endpoints/"+name,
		env.Keys.Admin, map[string]interface{}{"status": "ACTIVE"})
	if resp.StatusCode != 200 {
		body := readBody(t, resp)
		t.Fatalf("activate endpoint returned %d: %s", resp.StatusCode, body)
	}
	_ = resp.Body.Close()
}

// assignToEndpoint creates a default compute assignment for the given principal
// to the named endpoint. Uses the admin API key.
func assignToEndpoint(t *testing.T, env *httpTestEnv, endpointName string, principalID float64, principalType string) {
	t.Helper()

	resp := doRequest(t, "POST",
		env.Server.URL+"/v1/compute-endpoints/"+endpointName+"/assignments",
		env.Keys.Admin, map[string]interface{}{
			"principal_id":   principalID,
			"principal_type": principalType,
			"is_default":     true,
		})
	if resp.StatusCode != 201 {
		body := readBody(t, resp)
		t.Fatalf("assign endpoint returned %d: %s", resp.StatusCode, body)
	}
	_ = resp.Body.Close()
}
