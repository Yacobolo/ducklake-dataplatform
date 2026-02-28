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
	"net"
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
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"

	"duck-demo/internal/agent"
	"duck-demo/internal/api"
	"duck-demo/internal/compute"
	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/crypto"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/middleware"
	"duck-demo/internal/service/catalog"
	svccompute "duck-demo/internal/service/compute"
	"duck-demo/internal/service/governance"
	"duck-demo/internal/service/macro"
	svcmodel "duck-demo/internal/service/model"
	svcnotebook "duck-demo/internal/service/notebook"
	svcpipeline "duck-demo/internal/service/pipeline"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/security"
	svcsemantic "duck-demo/internal/service/semantic"
	"duck-demo/internal/service/storage"
)

// ctx is a package-level background context used by setup helpers.
var ctx = context.Background()

type noOpCatalogAttacher struct{}

func (noOpCatalogAttacher) Attach(_ context.Context, _ domain.CatalogRegistration) error { return nil }
func (noOpCatalogAttacher) Detach(_ context.Context, _ string) error                     { return nil }
func (noOpCatalogAttacher) SetDefaultCatalog(_ context.Context, _ string) error          { return nil }

type testHS256Validator struct {
	secret []byte
}

func (v *testHS256Validator) Validate(_ context.Context, tokenString string) (*middleware.JWTClaims, error) {
	token, err := jwt.Parse(tokenString, func(_ *jwt.Token) (interface{}, error) {
		return v.secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		return nil, fmt.Errorf("jwt parse: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	mapClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}

	claims := &middleware.JWTClaims{Raw: map[string]interface{}(mapClaims)}
	if sub, ok := mapClaims["sub"].(string); ok {
		claims.Subject = sub
	}
	if iss, ok := mapClaims["iss"].(string); ok {
		claims.Issuer = iss
	}
	if email, ok := mapClaims["email"].(string); ok {
		claims.Email = &email
	}
	if name, ok := mapClaims["name"].(string); ok {
		claims.Name = &name
	}
	if aud, ok := mapClaims["aud"].(string); ok {
		claims.Audience = []string{aud}
	}
	return claims, nil
}

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

	return &catalogTestEnv{DuckDB: duckDB, MetaDB: metaDB, MetaPath: metaPath}, cleanup, nil
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
		ID: uuid.New().String(), Name: "admin_user", Type: "user", IsAdmin: 1,
	})
	if err != nil {
		t.Fatalf("create admin_user: %v", err)
	}

	analyst1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "analyst1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create analyst1: %v", err)
	}

	researcher1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "researcher1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create researcher1: %v", err)
	}

	noAccessUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "no_access_user", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create no_access_user: %v", err)
	}

	// --- Groups ---
	adminsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "admins"})
	if err != nil {
		t.Fatalf("create admins group: %v", err)
	}

	analystsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "analysts"})
	if err != nil {
		t.Fatalf("create analysts group: %v", err)
	}

	researchersGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "researchers"})
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
		ID: uuid.New().String(), PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: "catalog", SecurableID: "0", Privilege: "ALL_PRIVILEGES",
	}); err != nil {
		t.Fatalf("grant admins ALL_PRIVILEGES: %v", err)
	}

	// analysts → USAGE on schema + SELECT on table
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0", Privilege: "USAGE",
	}); err != nil {
		t.Fatalf("grant analysts USAGE: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1", Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant analysts SELECT: %v", err)
	}

	// researchers → USAGE on schema + SELECT on table
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: researchersGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0", Privilege: "USAGE",
	}); err != nil {
		t.Fatalf("grant researchers USAGE: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: researchersGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1", Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant researchers SELECT: %v", err)
	}

	// --- Row Filters ---
	// analysts: "Pclass" = 1 on titanic (table_id=1)
	filter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "1", FilterSql: `"Pclass" = 1`,
	})
	if err != nil {
		t.Fatalf("create row filter: %v", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: filter.ID, PrincipalID: analystsGroup.ID, PrincipalType: "group",
	}); err != nil {
		t.Fatalf("bind row filter to analysts: %v", err)
	}

	// --- Column Masks ---
	// Name → '***' on titanic (table_id=1)
	nameMask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "1", ColumnName: "Name", MaskExpression: `'***'`,
	})
	if err != nil {
		t.Fatalf("create column mask: %v", err)
	}
	// analysts: see_original=0 (Name is masked)
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: nameMask.ID, PrincipalID: analystsGroup.ID,
		PrincipalType: "group", SeeOriginal: 0,
	}); err != nil {
		t.Fatalf("bind mask to analysts: %v", err)
	}
	// researchers: see_original=1 (Name is visible)
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: nameMask.ID, PrincipalID: researchersGroup.ID,
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
		ID: uuid.New().String(), KeyHash: sha256Hex(keys.Admin), PrincipalID: adminUser.ID, Name: "admin-test",
	}); err != nil {
		t.Fatalf("create admin API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		ID: uuid.New().String(), KeyHash: sha256Hex(keys.Analyst), PrincipalID: analyst1.ID, Name: "analyst-test",
	}); err != nil {
		t.Fatalf("create analyst API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		ID: uuid.New().String(), KeyHash: sha256Hex(keys.Researcher), PrincipalID: researcher1.ID, Name: "researcher-test",
	}); err != nil {
		t.Fatalf("create researcher API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		ID: uuid.New().String(), KeyHash: sha256Hex(keys.NoAccess), PrincipalID: noAccessUser.ID, Name: "noaccess-test",
	}); err != nil {
		t.Fatalf("create noaccess API key: %v", err)
	}

	return keys
}

// registerTestCatalog inserts a catalog registration for "lake" into the
// control-plane DB so that CatalogRepoFactory.ForCatalog("lake") succeeds.
func registerTestCatalog(t *testing.T, db *sql.DB, metaPath string) {
	t.Helper()
	q := dbstore.New(db)
	_, err := q.CreateCatalog(ctx, dbstore.CreateCatalogParams{
		ID:            uuid.New().String(),
		Name:          "lake",
		MetastoreType: "sqlite",
		Dsn:           metaPath,
		DataPath:      "s3://yacobolo/lake_data/",
		Status:        "ACTIVE",
		IsDefault:     1,
	})
	if err != nil {
		t.Fatalf("register test catalog: %v", err)
	}
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
	searchRepo := repository.NewSearchRepo(metaDB, metaDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(metaDB)
	viewRepo := repository.NewViewRepo(metaDB)

	// Build services
	authSvc := security.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
		nil,
	)

	presigner, err := query.NewS3Presigner(cfg)
	if err != nil {
		t.Fatalf("create presigner: %v", err)
	}

	manifestSvc := query.NewManifestService(
		nil, authSvc, presigner, introspectionRepo, auditRepo,
		nil, nil,
	)

	// Remaining services (querySvc gets nil engine — we never hit /v1/query)
	querySvc := query.NewQueryService(nil, auditRepo, nil)
	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := security.NewGroupService(groupRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)
	tagSvc := governance.NewTagService(tagRepo, auditRepo)
	lineageSvc := governance.NewLineageService(lineageRepo, nil)
	searchSvc := catalog.NewSearchService(searchRepo, nil)
	queryHistorySvc := governance.NewQueryHistoryService(queryHistoryRepo)
	catalogRegRepo := repository.NewCatalogRegistrationRepo(metaDB)
	catalogRepoFactory := repository.NewCatalogRepoFactory(catalogRegRepo, metaDB, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	viewSvc := catalog.NewViewService(viewRepo, catalogRepoFactory, authSvc, auditRepo)

	handler := api.NewHandler(
		querySvc, principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, auditSvc,
		manifestSvc, nil, nil, // catalogSvc=nil, catalogRegSvc=nil — integration tests only hit /v1/manifest
		queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc,
		nil,                     // ingestionSvc
		nil, nil, nil, nil, nil, // storageCredSvc, extLocationSvc, volumeSvc, computeEndpointSvc, apiKeySvc
		nil, nil, nil, // notebookSvc, sessionSvc, gitRepoSvc
		nil, // pipelineSvc
		nil, // modelSvc
		nil, // macroSvc
		nil, // semanticSvc
	)
	strictHandler := api.NewStrictHandler(handler, nil)

	// Router with auth middleware (API key via SHA-256 hash lookup)
	r := chi.NewRouter()
	validator := &testHS256Validator{secret: []byte("test-jwt-secret")}
	authenticator := middleware.NewAuthenticator(validator, apiKeyRepo, principalRepo, nil, config.AuthConfig{
		APIKeyEnabled: true,
		APIKeyHeader:  "X-API-Key",
		NameClaim:     "sub",
	}, nil)
	r.Use(authenticator.Middleware())
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
	searchRepo := repository.NewSearchRepo(metaDB, metaDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(metaDB)
	viewRepo := repository.NewViewRepo(metaDB)

	// Build services
	authSvc := security.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
		nil,
	)

	// LOCAL presigner — returns paths as-is (no S3 presigning)
	manifestSvc := query.NewManifestService(
		nil, authSvc, &localPresigner{}, introspectionRepo, auditRepo,
		nil, nil,
	)

	// Remaining services (querySvc gets nil engine — we never hit /v1/query)
	querySvc := query.NewQueryService(nil, auditRepo, nil)
	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := security.NewGroupService(groupRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)
	tagSvc := governance.NewTagService(tagRepo, auditRepo)
	lineageSvc := governance.NewLineageService(lineageRepo, nil)
	searchSvc := catalog.NewSearchService(searchRepo, nil)
	queryHistorySvc := governance.NewQueryHistoryService(queryHistoryRepo)
	catalogRegRepo := repository.NewCatalogRegistrationRepo(metaDB)
	catalogRepoFactory := repository.NewCatalogRepoFactory(catalogRegRepo, metaDB, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	viewSvc := catalog.NewViewService(viewRepo, catalogRepoFactory, authSvc, auditRepo)

	handler := api.NewHandler(
		querySvc, principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, auditSvc,
		manifestSvc, nil, nil, // catalogSvc=nil, catalogRegSvc=nil — extension tests only hit /v1/manifest
		queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc,
		nil,                     // ingestionSvc
		nil, nil, nil, nil, nil, // storageCredSvc, extLocationSvc, volumeSvc, computeEndpointSvc, apiKeySvc
		nil, nil, nil, // notebookSvc, sessionSvc, gitRepoSvc
		nil, // pipelineSvc
		nil, // modelSvc
		nil, // macroSvc
		nil, // semanticSvc
	)
	strictHandler := api.NewStrictHandler(handler, nil)

	// Router with auth middleware (API key via SHA-256 hash lookup)
	r := chi.NewRouter()
	validator := &testHS256Validator{secret: []byte("test-jwt-secret")}
	authenticator := middleware.NewAuthenticator(validator, apiKeyRepo, principalRepo, nil, config.AuthConfig{
		APIKeyEnabled: true,
		APIKeyHeader:  "X-API-Key",
		NameClaim:     "sub",
	}, nil)
	r.Use(authenticator.Middleware())
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
	DuckDB   *sql.DB
	MetaDB   *sql.DB
	MetaPath string // filesystem path to the metastore SQLite file
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

	return &catalogTestEnv{DuckDB: duckDB, MetaDB: metaDB, MetaPath: metaPath}
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
	// CatalogAttached marks the DuckLake catalog as attached at construction time.
	// Used when tests need the catalog to be attached without going through full setup.
	CatalogAttached bool
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
	// WithModels wires ModelService and MacroService into the handler (enables
	// model CRUD, DAG, run, test, freshness, and macro endpoints).
	WithModels bool
	// WithSemantic wires SemanticService into the handler (enables semantic
	// models, metrics, relationships, pre-aggregations, explain, and run endpoints).
	WithSemantic bool
}

// httpTestEnv bundles the test server, API keys, and direct DB access.
type httpTestEnv struct {
	Server         *httptest.Server
	Keys           apiKeys
	MetaDB         *sql.DB
	DuckDB         *sql.DB                          // nil unless WithDuckLake
	ExtLocationSvc *storage.ExternalLocationService // nil unless WithStorageCredentials
}

type integrationSessionEngine struct{}

func (integrationSessionEngine) Query(ctx context.Context, _ string, sqlQuery string) (*sql.Rows, error) {
	return nil, fmt.Errorf("integration session engine requires pinned connection for query %q", sqlQuery)
}

func (integrationSessionEngine) QueryOnConn(ctx context.Context, conn *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
	return conn.QueryContext(ctx, sqlQuery)
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
	// We create the DB at a known path so we can register the catalog later.
	metaPath := filepath.Join(t.TempDir(), "test.sqlite")
	metaDB, err := internaldb.OpenSQLite(metaPath, "write", 0)
	if err != nil {
		t.Fatalf("open test sqlite: %v", err)
	}
	t.Cleanup(func() { _ = metaDB.Close() })
	if err := internaldb.RunMigrations(metaDB); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Optionally seed DuckLake metadata (without DuckLake extensions)
	if opts.SeedDuckLakeMetadata {
		seedDuckLakeMetadata(t, metaDB, "s3://yacobolo/lake_data/")
		// Register the "lake" catalog so ForCatalog("lake") can find it.
		registerTestCatalog(t, metaDB, metaPath)
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
	searchRepo := repository.NewSearchRepo(metaDB, metaDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(metaDB)
	viewRepo := repository.NewViewRepo(metaDB)

	// Build authorization service unconditionally (needed by viewSvc)
	authSvc := security.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
		nil,
	)

	// Build services
	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := security.NewGroupService(groupRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)
	tagSvc := governance.NewTagService(tagRepo, auditRepo)
	lineageSvc := governance.NewLineageService(lineageRepo, nil)
	searchSvc := catalog.NewSearchService(searchRepo, nil)
	queryHistorySvc := governance.NewQueryHistoryService(queryHistoryRepo)

	// querySvc gets nil engine — no /v1/query support unless WithDuckLake+engine
	querySvc := query.NewQueryService(nil, auditRepo, nil)

	// catalogRepoFactory with duckDB=nil is safe — GetSchema only reads ducklake_schema from metaDB
	catalogRegRepo := repository.NewCatalogRegistrationRepo(metaDB)
	catalogRepoFactory := repository.NewCatalogRepoFactory(catalogRegRepo, metaDB, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	catalogRegSvc := catalog.NewCatalogRegistrationService(catalog.RegistrationServiceDeps{
		Repo:               catalogRegRepo,
		Attacher:           noOpCatalogAttacher{},
		ControlPlaneDBPath: metaPath,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	viewSvc := catalog.NewViewService(viewRepo, catalogRepoFactory, authSvc, auditRepo)

	var duckDB *sql.DB
	var manifestSvc *query.ManifestService
	tableStatsRepo := repository.NewTableStatisticsRepo(metaDB)
	catalogSvc := catalog.NewCatalogService(catalogRepoFactory, authSvc, auditRepo, tagRepo, tableStatsRepo, nil)

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

		// Register the "lake" catalog so ForCatalog("lake") can find it.
		registerTestCatalog(t, metaDB, env.MetaPath)

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
		searchRepo = repository.NewSearchRepo(metaDB, metaDB)
		queryHistoryRepo = repository.NewQueryHistoryRepo(metaDB)
		viewRepo = repository.NewViewRepo(metaDB)

		// Rebuild services on new repos
		authSvc = security.NewAuthorizationService(
			principalRepo, groupRepo, grantRepo,
			rowFilterRepo, columnMaskRepo, introspectionRepo,
			nil,
		)
		principalSvc = security.NewPrincipalService(principalRepo, auditRepo)
		groupSvc = security.NewGroupService(groupRepo, auditRepo)
		grantSvc = security.NewGrantService(grantRepo, auditRepo)
		rowFilterSvc = security.NewRowFilterService(rowFilterRepo, auditRepo)
		columnMaskSvc = security.NewColumnMaskService(columnMaskRepo, auditRepo)
		auditSvc = governance.NewAuditService(auditRepo)
		querySvc = query.NewQueryService(nil, auditRepo, nil)
		tagSvc = governance.NewTagService(tagRepo, auditRepo)
		lineageSvc = governance.NewLineageService(lineageRepo, nil)
		searchSvc = catalog.NewSearchService(searchRepo, nil)
		queryHistorySvc = governance.NewQueryHistoryService(queryHistoryRepo)

		catalogRegRepo = repository.NewCatalogRegistrationRepo(metaDB)
		catalogRepoFactory = repository.NewCatalogRepoFactory(catalogRegRepo, metaDB, duckDB, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		catalogRegSvc = catalog.NewCatalogRegistrationService(catalog.RegistrationServiceDeps{
			Repo:               catalogRegRepo,
			Attacher:           noOpCatalogAttacher{},
			ControlPlaneDBPath: env.MetaPath,
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		viewSvc = catalog.NewViewService(viewRepo, catalogRepoFactory, authSvc, auditRepo)
		tableStatsRepo = repository.NewTableStatisticsRepo(metaDB)
		catalogSvc = catalog.NewCatalogService(catalogRepoFactory, authSvc, auditRepo, tagRepo, tableStatsRepo, nil)

		// manifestSvc needs S3 presigner — leave nil for non-S3 tests
	}

	// Optionally wire storage credential and external location services
	var storageCredSvc *storage.StorageCredentialService
	var extLocationSvc *storage.ExternalLocationService

	{
		testEncKey := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		enc, err := crypto.NewEncryptor(testEncKey)
		if err != nil {
			t.Fatalf("create encryptor: %v", err)
		}
		storageCredRepo := repository.NewStorageCredentialRepo(metaDB, enc)
		extLocationRepo := repository.NewExternalLocationRepo(metaDB)
		storageCredSvc = storage.NewStorageCredentialService(storageCredRepo, authSvc, auditRepo)

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
		secretMgr := engine.NewDuckDBSecretManager(extDuckDB)
		extLocationSvc = storage.NewExternalLocationService(
			extLocationRepo, storageCredRepo, authSvc, auditRepo,
			secretMgr, slog.New(slog.NewTextHandler(io.Discard, nil)),
		)
	}

	// Optionally wire compute endpoints with full resolver + engine
	var computeEndpointSvc *svccompute.ComputeEndpointService

	{
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
		computeEndpointSvc = svccompute.NewComputeEndpointService(computeEndpointRepo, authSvc, auditRepo)

		// Build full resolver: local executor + compute repo + principal/group repos
		localExec := compute.NewLocalExecutor(engineDuckDB)
		remoteCache := compute.NewRemoteCache(engineDuckDB)
		resolver := compute.NewResolver(
			localExec, computeEndpointRepo, principalRepo, groupRepo,
			remoteCache, slog.New(slog.NewTextHandler(io.Discard, nil)),
		)

		// Build SecureEngine with the resolver
		eng := engine.NewSecureEngine(engineDuckDB, authSvc, resolver, nil,
			slog.New(slog.NewTextHandler(io.Discard, nil)))

		// Rebuild querySvc with the real engine
		querySvc = query.NewQueryService(eng, auditRepo, lineageRepo)
	}

	// Wire notebook, git repo, and pipeline services so declarative export can
	// read these resources without endpoint panics.
	notebookRepo := repository.NewNotebookRepo(metaDB)
	notebookSvc := svcnotebook.New(notebookRepo, auditRepo)
	notebookProvider := svcpipeline.NewDBNotebookProvider(notebookRepo)
	gitRepoRepo := repository.NewGitRepoRepo(metaDB)
	gitRepoSvc := svcnotebook.NewGitService(gitRepoRepo, auditRepo)
	pipelineRepo := repository.NewPipelineRepo(metaDB)
	pipelineRunRepo := repository.NewPipelineRunRepo(metaDB)
	pipelineSvc := svcpipeline.NewService(
		pipelineRepo,
		pipelineRunRepo,
		auditRepo,
		notebookProvider,
		nil,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	// Wire APIKeyService by default so API key endpoints are always available
	// in integration test servers.
	apiKeySvc := security.NewAPIKeyService(apiKeyRepo, auditRepo)

	// Optionally wire Model + Macro services
	var modelSvc *svcmodel.Service
	var macroSvc *macro.Service
	if opts.WithModels {
		if duckDB == nil {
			duckDB, err = sql.Open("duckdb", "")
			if err != nil {
				t.Fatalf("open duckdb for model service: %v", err)
			}
			t.Cleanup(func() { _ = duckDB.Close() })
		}
		_, err = duckDB.Exec("CREATE SCHEMA IF NOT EXISTS analytics")
		if err != nil {
			t.Fatalf("create analytics schema for model service: %v", err)
		}

		modelRepo := repository.NewModelRepo(metaDB)
		modelRunRepo := repository.NewModelRunRepo(metaDB)
		modelTestRepo := repository.NewModelTestRepo(metaDB)
		modelTestResultRepo := repository.NewModelTestResultRepo(metaDB)
		colLineageRepo := repository.NewColumnLineageRepo(metaDB)
		modelSvc = svcmodel.NewService(
			modelRepo, modelRunRepo, modelTestRepo, modelTestResultRepo, auditRepo,
			lineageRepo, colLineageRepo,
			integrationSessionEngine{}, duckDB,
			slog.New(slog.NewTextHandler(io.Discard, nil)),
		)
		macroRepo := repository.NewMacroRepo(metaDB)
		macroSvc = macro.NewService(macroRepo, auditRepo)
		modelSvc.SetMacroRepo(macroRepo)
	}

	// Optionally wire Semantic service.
	var semanticSvc *svcsemantic.Service
	if opts.WithSemantic {
		semanticModelRepo := repository.NewSemanticModelRepo(metaDB)
		semanticMetricRepo := repository.NewSemanticMetricRepo(metaDB)
		semanticRelationshipRepo := repository.NewSemanticRelationshipRepo(metaDB)
		semanticPreAggRepo := repository.NewSemanticPreAggregationRepo(metaDB)
		semanticSvc = svcsemantic.NewService(
			semanticModelRepo,
			semanticMetricRepo,
			semanticRelationshipRepo,
			semanticPreAggRepo,
		)
		semanticSvc.SetQueryExecutor(querySvc)
	}

	handler := api.NewHandler(
		querySvc, principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, auditSvc,
		manifestSvc, catalogSvc, catalogRegSvc,
		queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc,
		nil,                                 // ingestionSvc
		storageCredSvc, extLocationSvc, nil, // volumeSvc
		computeEndpointSvc,
		apiKeySvc,
		notebookSvc, nil, gitRepoSvc,
		pipelineSvc,
		modelSvc, // modelSvc
		macroSvc, // macroSvc
		semanticSvc,
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
		APIKeyEnabled:  true,
		APIKeyHeader:   "X-API-Key",
		NameClaim:      nameClaim,
		BootstrapAdmin: opts.BootstrapAdmin,
	}
	validator := &testHS256Validator{secret: jwtSecret}
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
	EndpointURL string
	AgentToken  string
}

// startTestAgent starts an in-process compute agent backed by a plain in-memory
// DuckDB. No extensions, no S3, no DuckLake — just raw SQL execution. Returns
// the agent's gRPC endpoint URL and the auth token for X-Agent-Token.
func startTestAgent(t *testing.T) *agentTestEnv {
	t.Helper()

	agentToken := "test-agent-secret-token"

	agentDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open agent duckdb: %v", err)
	}
	t.Cleanup(func() { _ = agentDB.Close() })

	grpcServer := agent.NewComputeGRPCServer(agent.HandlerConfig{
		DB:         agentDB,
		AgentToken: agentToken,
		StartTime:  time.Now(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc agent: %v", err)
	}

	server := grpc.NewServer()
	agent.RegisterComputeWorkerGRPCServer(server, grpcServer)

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	return &agentTestEnv{EndpointURL: "grpc://" + listener.Addr().String(), AgentToken: agentToken}
}

// lookupPrincipalID finds a principal by name via the API and returns its ID.
func lookupPrincipalID(t *testing.T, env *httpTestEnv, name string) string {
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
			id, _ := pm["id"].(string)
			return id
		}
	}
	t.Fatalf("principal %q not found", name)
	return ""
}

// setupRemoteEndpoint creates and activates a REMOTE compute endpoint pointing
// to the given agent. Uses the admin API key.
func setupRemoteEndpoint(t *testing.T, env *httpTestEnv, agentEnv *agentTestEnv, name string) {
	t.Helper()

	resp := doRequest(t, "POST", env.Server.URL+"/v1/compute-endpoints", env.Keys.Admin,
		map[string]interface{}{
			"name":       name,
			"url":        agentEnv.EndpointURL,
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

// ---------------------------------------------------------------------------
// Multi-table test infrastructure
// ---------------------------------------------------------------------------

// ducklakeDeptDataFileName is the parquet file name for the departments table.
const ducklakeDeptDataFileName = "ducklake-departments-test-fixture.parquet"

// multiTableKeys extends apiKeys with additional test principals.
type multiTableKeys struct {
	apiKeys
	DeptViewer      string
	USOnlyViewer    string
	MaskedViewer    string
	MultiFilterUser string
}

// multiTableTestEnv bundles the test server and multi-table API keys.
type multiTableTestEnv struct {
	Server *httptest.Server
	Keys   multiTableKeys
}

// seedMultiTableMetadata seeds both the titanic and departments tables.
func seedMultiTableMetadata(t *testing.T, db *sql.DB, dataPath string) {
	t.Helper()
	seedDuckLakeMetadata(t, db, dataPath)

	const deptData = `
	INSERT INTO ducklake_table VALUES (
		2, 'dept-test-uuid', 1, NULL,
		0, 'departments', 'departments/', 1
	);
	INSERT INTO ducklake_column VALUES (13, 1, NULL, 2, 1, 'dept_id',     'int64',   NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (14, 1, NULL, 2, 2, 'dept_name',   'varchar', NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (15, 1, NULL, 2, 3, 'region',      'varchar', NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (16, 1, NULL, 2, 4, 'avg_salary',  'int64',   NULL, NULL, 1, NULL);
	INSERT INTO ducklake_column VALUES (17, 1, NULL, 2, 5, 'headcount',   'int64',   NULL, NULL, 1, NULL);

	INSERT INTO ducklake_data_file VALUES (
		1, 2, 1, NULL, NULL,
		'ducklake-departments-test-fixture.parquet',
		1, 'parquet', 10, 2048, 512, 0, NULL, NULL, NULL, NULL
	);`

	if _, err := db.ExecContext(ctx, deptData); err != nil {
		t.Fatalf("seed department metadata: %v", err)
	}
}

// seedMultiTableRBAC seeds base RBAC plus 4 additional principals with
// table-specific grants, row filters, and column masks.
func seedMultiTableRBAC(t *testing.T, db *sql.DB) multiTableKeys {
	t.Helper()
	base := seedRBAC(t, db)
	q := dbstore.New(db)

	// --- dept_viewer: SELECT on departments only ---
	deptViewer, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "dept_viewer", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create dept_viewer: %v", err)
	}
	deptViewerGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "dept_viewers"})
	if err != nil {
		t.Fatalf("create dept_viewers group: %v", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: deptViewerGroup.ID, MemberType: "user", MemberID: deptViewer.ID,
	}); err != nil {
		t.Fatalf("add dept_viewer to group: %v", err)
	}
	// USAGE on schema + SELECT on departments (table_id=2) only
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: deptViewerGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0", Privilege: "USAGE",
	}); err != nil {
		t.Fatalf("grant dept_viewers USAGE: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: deptViewerGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "2", Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant dept_viewers SELECT on departments: %v", err)
	}

	// --- us_only_viewer: SELECT on both tables, RLS region='US' on departments, "Embarked"='S' on titanic ---
	usOnlyViewer, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "us_only_viewer", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create us_only_viewer: %v", err)
	}
	usGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "us_only"})
	if err != nil {
		t.Fatalf("create us_only group: %v", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: usGroup.ID, MemberType: "user", MemberID: usOnlyViewer.ID,
	}); err != nil {
		t.Fatalf("add us_only_viewer to group: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: usGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0", Privilege: "USAGE",
	}); err != nil {
		t.Fatalf("grant us_only USAGE: %v", err)
	}
	// SELECT on both tables
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: usGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1", Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant us_only SELECT titanic: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: usGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "2", Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant us_only SELECT departments: %v", err)
	}
	// RLS: region = 'US' on departments (table_id=2)
	deptFilter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "2", FilterSql: `"region" = 'US'`,
	})
	if err != nil {
		t.Fatalf("create dept region filter: %v", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: deptFilter.ID, PrincipalID: usGroup.ID, PrincipalType: "group",
	}); err != nil {
		t.Fatalf("bind dept filter to us_only: %v", err)
	}
	// RLS: "Embarked" = 'S' on titanic (table_id=1)
	titanicFilter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "1", FilterSql: `"Embarked" = 'S'`,
	})
	if err != nil {
		t.Fatalf("create titanic embarked filter: %v", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: titanicFilter.ID, PrincipalID: usGroup.ID, PrincipalType: "group",
	}); err != nil {
		t.Fatalf("bind titanic filter to us_only: %v", err)
	}

	// --- masked_viewer: SELECT on both, column masks on titanic + departments ---
	maskedViewer, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "masked_viewer", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create masked_viewer: %v", err)
	}
	maskedGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "masked_viewers"})
	if err != nil {
		t.Fatalf("create masked_viewers group: %v", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: maskedGroup.ID, MemberType: "user", MemberID: maskedViewer.ID,
	}); err != nil {
		t.Fatalf("add masked_viewer to group: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: maskedGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0", Privilege: "USAGE",
	}); err != nil {
		t.Fatalf("grant masked_viewers USAGE: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: maskedGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1", Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant masked_viewers SELECT titanic: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: maskedGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "2", Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant masked_viewers SELECT departments: %v", err)
	}
	// Column mask: Fare → ROUND("Fare"/10)*10 on titanic
	fareMask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "1", ColumnName: "Fare", MaskExpression: `ROUND("Fare"/10)*10`,
	})
	if err != nil {
		t.Fatalf("create fare mask: %v", err)
	}
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: fareMask.ID, PrincipalID: maskedGroup.ID,
		PrincipalType: "group", SeeOriginal: 0,
	}); err != nil {
		t.Fatalf("bind fare mask to masked_viewers: %v", err)
	}
	// Column mask: Name → '***' on titanic
	nameMaskForMasked, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "1", ColumnName: "Name", MaskExpression: `'***'`,
	})
	if err != nil {
		t.Fatalf("create name mask for masked_viewers: %v", err)
	}
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: nameMaskForMasked.ID, PrincipalID: maskedGroup.ID,
		PrincipalType: "group", SeeOriginal: 0,
	}); err != nil {
		t.Fatalf("bind name mask to masked_viewers: %v", err)
	}
	// Column mask: avg_salary → 0 on departments
	salaryMask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "2", ColumnName: "avg_salary", MaskExpression: `0`,
	})
	if err != nil {
		t.Fatalf("create salary mask: %v", err)
	}
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: salaryMask.ID, PrincipalID: maskedGroup.ID,
		PrincipalType: "group", SeeOriginal: 0,
	}); err != nil {
		t.Fatalf("bind salary mask to masked_viewers: %v", err)
	}

	// --- multi_filter_user: SELECT on titanic, TWO RLS filters (Pclass=1 AND Survived=1) ---
	multiFilterUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "multi_filter_user", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create multi_filter_user: %v", err)
	}
	multiFilterGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "multi_filter"})
	if err != nil {
		t.Fatalf("create multi_filter group: %v", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: multiFilterGroup.ID, MemberType: "user", MemberID: multiFilterUser.ID,
	}); err != nil {
		t.Fatalf("add multi_filter_user to group: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: multiFilterGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0", Privilege: "USAGE",
	}); err != nil {
		t.Fatalf("grant multi_filter USAGE: %v", err)
	}
	if _, err := q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: multiFilterGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1", Privilege: "SELECT",
	}); err != nil {
		t.Fatalf("grant multi_filter SELECT titanic: %v", err)
	}
	// Two RLS filters on titanic
	pclassFilter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "1", FilterSql: `"Pclass" = 1`,
	})
	if err != nil {
		t.Fatalf("create pclass filter: %v", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: pclassFilter.ID, PrincipalID: multiFilterGroup.ID, PrincipalType: "group",
	}); err != nil {
		t.Fatalf("bind pclass filter to multi_filter: %v", err)
	}
	survivedFilter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "1", FilterSql: `"Survived" = 1`,
	})
	if err != nil {
		t.Fatalf("create survived filter: %v", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: survivedFilter.ID, PrincipalID: multiFilterGroup.ID, PrincipalType: "group",
	}); err != nil {
		t.Fatalf("bind survived filter to multi_filter: %v", err)
	}

	// --- API Keys for new principals ---
	deptViewerKey := "test-deptviewer-key"
	usOnlyKey := "test-usonly-key"
	maskedKey := "test-masked-key"
	multiFilterKey := "test-multifilter-key"

	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		ID: uuid.New().String(), KeyHash: sha256Hex(deptViewerKey), PrincipalID: deptViewer.ID, Name: "deptviewer-test",
	}); err != nil {
		t.Fatalf("create dept_viewer API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		ID: uuid.New().String(), KeyHash: sha256Hex(usOnlyKey), PrincipalID: usOnlyViewer.ID, Name: "usonly-test",
	}); err != nil {
		t.Fatalf("create us_only_viewer API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		ID: uuid.New().String(), KeyHash: sha256Hex(maskedKey), PrincipalID: maskedViewer.ID, Name: "masked-test",
	}); err != nil {
		t.Fatalf("create masked_viewer API key: %v", err)
	}
	if _, err := q.CreateAPIKey(ctx, dbstore.CreateAPIKeyParams{
		ID: uuid.New().String(), KeyHash: sha256Hex(multiFilterKey), PrincipalID: multiFilterUser.ID, Name: "multifilter-test",
	}); err != nil {
		t.Fatalf("create multi_filter_user API key: %v", err)
	}

	return multiTableKeys{
		apiKeys:         base,
		DeptViewer:      deptViewerKey,
		USOnlyViewer:    usOnlyKey,
		MaskedViewer:    maskedKey,
		MultiFilterUser: multiFilterKey,
	}
}

// generateDepartmentsParquet creates a 10-row departments parquet file using
// an in-memory DuckDB instance.
func generateDepartmentsParquet(t *testing.T, destPath string) {
	t.Helper()

	// Open a separate in-memory DuckDB to generate test data
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb for parquet gen: %v", err)
	}
	defer duckDB.Close()

	query := fmt.Sprintf(`COPY (
		SELECT * FROM (VALUES
			(1, 'Engineering', 'US', 150000, 50),
			(2, 'Engineering', 'UK', 120000, 30),
			(3, 'Sales', 'US', 100000, 40),
			(4, 'Sales', 'EU', 95000, 25),
			(5, 'HR', 'US', 90000, 15),
			(6, 'HR', 'EU', 85000, 10),
			(7, 'Finance', 'US', 130000, 20),
			(8, 'Finance', 'UK', 125000, 12),
			(9, 'Legal', 'US', 140000, 8),
			(10, 'Legal', 'EU', 135000, 6)
		) AS t(dept_id, dept_name, region, avg_salary, headcount)
	) TO '%s' (FORMAT PARQUET)`, strings.ReplaceAll(destPath, "'", "''"))

	if _, err := duckDB.ExecContext(ctx, query); err != nil {
		t.Fatalf("generate departments parquet: %v", err)
	}
}

// setupMultiTableLocalServer creates a fully-wired in-process Go API server
// with two tables (titanic + departments), extended RBAC, and local filesystem
// storage. Similar to setupLocalExtensionServer but with multi-table data.
func setupMultiTableLocalServer(t *testing.T) *multiTableTestEnv {
	t.Helper()

	tmpDir := t.TempDir()
	dataPath := filepath.Join(tmpDir, "lake_data") + "/"
	if err := os.MkdirAll(filepath.Join(tmpDir, "lake_data"), 0o755); err != nil {
		t.Fatalf("mkdir lake_data: %v", err)
	}

	// Copy titanic parquet
	copyFile(t,
		testdataPath("titanic.parquet"),
		filepath.Join(tmpDir, "lake_data", ducklakeDataFileName),
	)

	// Generate departments parquet inline
	generateDepartmentsParquet(t, filepath.Join(tmpDir, "lake_data", ducklakeDeptDataFileName))

	// SQLite + metadata + RBAC
	metaDB, _ := internaldb.OpenTestSQLite(t)
	seedMultiTableMetadata(t, metaDB, dataPath)
	keys := seedMultiTableRBAC(t, metaDB)

	// Build repos and services (same pattern as setupLocalExtensionServer)
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
	searchRepo := repository.NewSearchRepo(metaDB, metaDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(metaDB)
	viewRepo := repository.NewViewRepo(metaDB)

	authSvc := security.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
		nil,
	)

	manifestSvc := query.NewManifestService(
		nil, authSvc, &localPresigner{}, introspectionRepo, auditRepo,
		nil, nil,
	)

	querySvc := query.NewQueryService(nil, auditRepo, nil)
	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := security.NewGroupService(groupRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)
	tagSvc := governance.NewTagService(tagRepo, auditRepo)
	lineageSvc := governance.NewLineageService(lineageRepo, nil)
	searchSvc := catalog.NewSearchService(searchRepo, nil)
	queryHistorySvc := governance.NewQueryHistoryService(queryHistoryRepo)
	catalogRegRepo := repository.NewCatalogRegistrationRepo(metaDB)
	catalogRepoFactory := repository.NewCatalogRepoFactory(catalogRegRepo, metaDB, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	viewSvc := catalog.NewViewService(viewRepo, catalogRepoFactory, authSvc, auditRepo)

	handler := api.NewHandler(
		querySvc, principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, auditSvc,
		manifestSvc, nil, nil, // catalogSvc=nil, catalogRegSvc=nil
		queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc,
		nil,
		nil, nil, nil, nil, nil,
		nil, nil, nil, // notebookSvc, sessionSvc, gitRepoSvc
		nil, // pipelineSvc
		nil, // modelSvc
		nil, // macroSvc
		nil, // semanticSvc
	)
	strictHandler := api.NewStrictHandler(handler, nil)

	r := chi.NewRouter()
	validator := &testHS256Validator{secret: []byte("test-jwt-secret")}
	authenticator := middleware.NewAuthenticator(validator, apiKeyRepo, principalRepo, nil, config.AuthConfig{
		APIKeyEnabled: true,
		APIKeyHeader:  "X-API-Key",
		NameClaim:     "sub",
	}, nil)
	r.Use(authenticator.Middleware())
	r.Route("/v1", func(r chi.Router) {
		api.HandlerFromMux(strictHandler, r)
	})

	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	return &multiTableTestEnv{Server: srv, Keys: keys}
}

// assignToEndpoint creates a default compute assignment for the given principal
// to the named endpoint. Uses the admin API key.
func assignToEndpoint(t *testing.T, env *httpTestEnv, endpointName string, principalID string, principalType string) {
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
