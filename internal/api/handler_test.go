package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mattn/go-sqlite3"

	internaldb "duck-demo/internal/db"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/service/catalog"
	"duck-demo/internal/service/governance"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/security"
)

// ctx is a package-level background context used by setup helpers.
var ctx = context.Background()

// setupTestServer creates a fully wired test HTTP server with real DuckDB + SQLite.
func setupTestServer(t *testing.T, principalName string) *httptest.Server {
	t.Helper()

	if _, err := os.Stat("../../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found, skipping integration test")
	}

	// DuckDB
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = duckDB.Close() })

	if _, err := duckDB.ExecContext(ctx, "CREATE TABLE titanic AS SELECT * FROM '../../titanic.parquet'"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// SQLite metastore (hardened write/read pools; tests use writeDB for everything)
	metaDB, _ := internaldb.OpenTestSQLite(t)

	// Create mock DuckLake tables
	_, err = metaDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS ducklake_schema (
			schema_id INTEGER PRIMARY KEY, schema_uuid TEXT,
			begin_snapshot INTEGER, end_snapshot INTEGER,
			schema_name TEXT, path TEXT, path_is_relative INTEGER
		);
		CREATE TABLE IF NOT EXISTS ducklake_table (
			table_id INTEGER, table_uuid TEXT,
			begin_snapshot INTEGER, end_snapshot INTEGER,
			schema_id INTEGER, table_name TEXT,
			path TEXT, path_is_relative INTEGER
		);
		INSERT OR IGNORE INTO ducklake_schema (schema_id, schema_name, begin_snapshot) VALUES (0, 'main', 0);
		INSERT OR IGNORE INTO ducklake_table (table_id, table_name, schema_id, begin_snapshot) VALUES (1, 'titanic', 0, 1);
	`)
	if err != nil {
		t.Fatalf("create mock tables: %v", err)
	}

	// Seed test data
	ctx := context.Background()
	q := dbstore.New(metaDB)

	adminUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(), Name: "admin_user", Type: "user", IsAdmin: 1})
	require.NoError(t, err)
	analyst, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(), Name: "analyst1", Type: "user", IsAdmin: 0})
	require.NoError(t, err)
	_, err = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(), Name: "no_access_user", Type: "user", IsAdmin: 0})
	require.NoError(t, err)

	adminsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "admins"})
	require.NoError(t, err)
	analystsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "analysts"})
	require.NoError(t, err)

	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst.ID})
	require.NoError(t, err)

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: "catalog", SecurableID: "0", Privilege: "ALL_PRIVILEGES",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0", Privilege: "USAGE",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1", Privilege: "SELECT",
	})
	require.NoError(t, err)

	// Row filter for analysts
	filter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{ID: uuid.New().String(), TableID: "1", FilterSql: `"Pclass" = 1`})
	require.NoError(t, err)
	err = q.BindRowFilter(ctx, dbstore.BindRowFilterParams{ID: uuid.New().String(), RowFilterID: filter.ID, PrincipalID: analystsGroup.ID, PrincipalType: "group"})
	require.NoError(t, err)

	// Build repositories and services
	auditRepo := repository.NewAuditRepo(metaDB)
	principalRepo := repository.NewPrincipalRepo(metaDB)
	groupRepo := repository.NewGroupRepo(metaDB)
	grantRepo := repository.NewGrantRepo(metaDB)
	rowFilterRepo := repository.NewRowFilterRepo(metaDB)
	columnMaskRepo := repository.NewColumnMaskRepo(metaDB)
	introspectionRepo := repository.NewIntrospectionRepo(metaDB)

	cat := security.NewAuthorizationService(principalRepo, groupRepo, grantRepo, rowFilterRepo, columnMaskRepo, introspectionRepo, nil)
	eng := engine.NewSecureEngine(duckDB, cat, nil, nil, slog.New(slog.DiscardHandler))

	lineageRepo := repository.NewLineageRepo(metaDB)
	querySvc := query.NewQueryService(eng, auditRepo, lineageRepo)
	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := security.NewGroupService(groupRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)

	catalogRepoFactory := repository.NewCatalogRepoFactory(metaDB, duckDB, nil, slog.New(slog.DiscardHandler))
	tagRepo := repository.NewTagRepo(metaDB)
	catalogSvc := catalog.NewCatalogService(catalogRepoFactory, cat, auditRepo, tagRepo, nil, nil)

	queryHistorySvc := governance.NewQueryHistoryService(repository.NewQueryHistoryRepo(metaDB))
	lineageSvc := governance.NewLineageService(lineageRepo)
	searchSvc := catalog.NewSearchService(repository.NewSearchRepo(metaDB, metaDB), nil)
	tagSvc := governance.NewTagService(repository.NewTagRepo(metaDB), auditRepo)
	viewSvc := catalog.NewViewService(repository.NewViewRepo(metaDB), catalogRepoFactory, cat, auditRepo)

	handler := NewHandler(querySvc, principalSvc, groupSvc, grantSvc, rowFilterSvc, columnMaskSvc, auditSvc, nil, catalogSvc, nil, queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc, nil, nil, nil, nil, nil, nil)
	strictHandler := NewStrictHandler(handler, nil)

	// Lookup principal to get admin status for context injection
	p, err := principalRepo.GetByName(context.Background(), principalName)
	isAdmin := false
	if err == nil {
		isAdmin = p.IsAdmin
	}

	// Setup router with fixed auth (test principal)
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := domain.WithPrincipal(req.Context(), domain.ContextPrincipal{
				Name:    principalName,
				IsAdmin: isAdmin,
				Type:    "user",
			})
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	HandlerFromMux(strictHandler, r)

	return httptest.NewServer(r)
}

func TestAPI_ListPrincipals(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/principals", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result PaginatedPrincipals
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	if result.Data == nil || len(*result.Data) < 2 {
		t.Errorf("expected at least 2 principals, got %v", result.Data)
	}
	t.Logf("got %d principals", len(*result.Data))
}

func TestAPI_ExecuteQuery_Admin(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	body := `{"sql": "SELECT count(*) FROM titanic"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/query", bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result QueryResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	if result.RowCount == nil || *result.RowCount == 0 {
		t.Error("expected non-zero row count")
	}
	t.Logf("admin query returned %d rows", *result.RowCount)
}

func TestAPI_ExecuteQuery_NoAccess(t *testing.T) {
	srv := setupTestServer(t, "no_access_user")
	defer srv.Close()

	body := `{"sql": "SELECT * FROM titanic LIMIT 5"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/query", bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != 403 {
		t.Errorf("expected 403 for no_access_user, got %d", resp.StatusCode)
	}
}

func TestAPI_CreateAndDeletePrincipal(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	// Create
	body := `{"name": "test_user", "type": "user"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/principals", bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != 201 {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}

	var p Principal
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&p))
	if p.Name == nil || *p.Name != "test_user" {
		t.Errorf("expected name=test_user, got %v", p.Name)
	}

	// Delete
	req, err = http.NewRequestWithContext(ctx, "DELETE", srv.URL+"/principals/"+*p.Id, nil)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close() //nolint:errcheck

	if resp2.StatusCode != 204 {
		t.Errorf("expected 204, got %d", resp2.StatusCode)
	}
}

func TestAPI_AuditLogs(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	// Execute a query first to generate audit entries
	body := `{"sql": "SELECT 1"}`
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/query", bytes.NewBufferString(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	postResp, err := http.DefaultClient.Do(postReq)
	require.NoError(t, err)
	_ = postResp.Body.Close()

	// List audit logs
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/audit-logs", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var auditResult PaginatedAuditLogs
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&auditResult))
	if auditResult.Data != nil {
		t.Logf("got %d audit entries", len(*auditResult.Data))
	}
}

// setupCatalogTestServer creates a test server wired with a mockCatalogRepo.
// It has the same RBAC seed data as setupTestServer (admin_user with ALL_PRIVILEGES,
// analyst1 with USAGE/SELECT, no_access_user with nothing) but uses a mock catalog
// repo instead of a real DuckLake-attached DuckDB for catalog DDL operations.
func setupCatalogTestServer(t *testing.T, principalName string, mockRepo *mockCatalogRepo) *httptest.Server {
	t.Helper()

	if _, err := os.Stat("../../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found, skipping integration test")
	}

	// DuckDB (needed for query service and engine)
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = duckDB.Close() })

	if _, err := duckDB.ExecContext(ctx, "CREATE TABLE titanic AS SELECT * FROM '../../titanic.parquet'"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// SQLite metastore (hardened write/read pools; tests use writeDB for everything)
	metaDB, _ := internaldb.OpenTestSQLite(t)

	// Create mock DuckLake tables (for introspection/authorization)
	_, err = metaDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS ducklake_schema (
			schema_id INTEGER PRIMARY KEY, schema_uuid TEXT,
			begin_snapshot INTEGER, end_snapshot INTEGER,
			schema_name TEXT, path TEXT, path_is_relative INTEGER
		);
		CREATE TABLE IF NOT EXISTS ducklake_table (
			table_id INTEGER, table_uuid TEXT,
			begin_snapshot INTEGER, end_snapshot INTEGER,
			schema_id INTEGER, table_name TEXT,
			path TEXT, path_is_relative INTEGER
		);
		INSERT OR IGNORE INTO ducklake_schema (schema_id, schema_name, begin_snapshot) VALUES (0, 'main', 0);
		INSERT OR IGNORE INTO ducklake_table (table_id, table_name, schema_id, begin_snapshot) VALUES (1, 'titanic', 0, 1);
	`)
	if err != nil {
		t.Fatalf("create mock tables: %v", err)
	}

	// Seed RBAC data
	ctx := context.Background()
	q := dbstore.New(metaDB)

	adminUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(), Name: "admin_user", Type: "user", IsAdmin: 1})
	require.NoError(t, err)
	analyst, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(), Name: "analyst1", Type: "user", IsAdmin: 0})
	require.NoError(t, err)
	_, err = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(), Name: "no_access_user", Type: "user", IsAdmin: 0})
	require.NoError(t, err)

	adminsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "admins"})
	require.NoError(t, err)
	analystsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "analysts"})
	require.NoError(t, err)

	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst.ID})
	require.NoError(t, err)

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: "catalog", SecurableID: "0", Privilege: "ALL_PRIVILEGES",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: "0", Privilege: "USAGE",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: "1", Privilege: "SELECT",
	})
	require.NoError(t, err)

	// Build repositories and services
	auditRepo := repository.NewAuditRepo(metaDB)
	principalRepo := repository.NewPrincipalRepo(metaDB)
	groupRepo := repository.NewGroupRepo(metaDB)
	grantRepo := repository.NewGrantRepo(metaDB)
	rowFilterRepo := repository.NewRowFilterRepo(metaDB)
	columnMaskRepo := repository.NewColumnMaskRepo(metaDB)
	introspectionRepo := repository.NewIntrospectionRepo(metaDB)

	cat := security.NewAuthorizationService(principalRepo, groupRepo, grantRepo, rowFilterRepo, columnMaskRepo, introspectionRepo, nil)
	eng := engine.NewSecureEngine(duckDB, cat, nil, nil, slog.New(slog.DiscardHandler))

	lineageRepo2 := repository.NewLineageRepo(metaDB)
	querySvc := query.NewQueryService(eng, auditRepo, lineageRepo2)
	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := security.NewGroupService(groupRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)

	// Use the mock catalog repo instead of real DuckLake
	tagRepo2 := repository.NewTagRepo(metaDB)
	mockFactory := &mockCatalogRepoFactory{repo: mockRepo}
	catalogSvc := catalog.NewCatalogService(mockFactory, cat, auditRepo, tagRepo2, nil, nil)

	queryHistorySvc := governance.NewQueryHistoryService(repository.NewQueryHistoryRepo(metaDB))
	lineageSvc := governance.NewLineageService(lineageRepo2)
	searchSvc := catalog.NewSearchService(repository.NewSearchRepo(metaDB, metaDB), nil)
	tagSvc := governance.NewTagService(repository.NewTagRepo(metaDB), auditRepo)
	viewSvc := catalog.NewViewService(repository.NewViewRepo(metaDB), mockFactory, cat, auditRepo)

	handler := NewHandler(querySvc, principalSvc, groupSvc, grantSvc, rowFilterSvc, columnMaskSvc, auditSvc, nil, catalogSvc, nil, queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc, nil, nil, nil, nil, nil, nil)
	strictHandler := NewStrictHandler(handler, nil)

	// Lookup principal to get admin status for context injection
	p2, err := principalRepo.GetByName(context.Background(), principalName)
	isAdmin2 := false
	if err == nil {
		isAdmin2 = p2.IsAdmin
	}

	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := domain.WithPrincipal(req.Context(), domain.ContextPrincipal{
				Name:    principalName,
				IsAdmin: isAdmin2,
				Type:    "user",
			})
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	HandlerFromMux(strictHandler, r)

	return httptest.NewServer(r)
}

// doRequest is a test helper that sends an HTTP request and returns the response.
func doRequest(t *testing.T, method, url string, body string) *http.Response {
	t.Helper()
	var reqBody *bytes.Buffer
	if body != "" {
		reqBody = bytes.NewBufferString(body)
	} else {
		reqBody = bytes.NewBuffer(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("execute request: %v", err)
	}
	return resp
}

// decodeJSON is a test helper that decodes a JSON response body.
func decodeJSON[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck
	var result T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return result
}

func TestAPI_GetCatalog(t *testing.T) {
	mock := newMockCatalogRepo()
	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	resp := doRequest(t, "GET", srv.URL+"/catalogs/lake/info", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	result := decodeJSON[CatalogInfo](t, resp)
	if result.Name == nil || *result.Name != "lake" {
		t.Errorf("expected name=lake, got %v", result.Name)
	}
}

func TestAPI_GetMetastoreSummary(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("main")
	mock.addSchema("analytics")
	mock.addTable("main", "users", []domain.ColumnDetail{{Name: "id", Type: "INTEGER"}})

	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	resp := doRequest(t, "GET", srv.URL+"/catalogs/lake/metastore/summary", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	result := decodeJSON[MetastoreSummary](t, resp)

	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"catalog_name", ptrVal(result.CatalogName), "lake"},
		{"schema_count", ptrVal(result.SchemaCount), int64(2)},
		{"table_count", ptrVal(result.TableCount), int64(1)},
		{"metastore_type", ptrVal(result.MetastoreType), "DuckLake (SQLite)"},
		{"storage_backend", ptrVal(result.StorageBackend), "S3"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, tt.got, tt.want)
		}
	}
}

func TestAPI_SchemaCRUD(t *testing.T) {
	mock := newMockCatalogRepo()
	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	type step struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		check      func(t *testing.T, resp *http.Response)
	}

	steps := []step{
		{
			name:       "create schema",
			method:     "POST",
			path:       "/catalogs/lake/schemas",
			body:       `{"name":"test_schema","comment":"a test schema"}`,
			wantStatus: 201,
			check: func(t *testing.T, resp *http.Response) {
				r := decodeJSON[SchemaDetail](t, resp)
				if r.Name == nil || *r.Name != "test_schema" {
					t.Errorf("name: got %v, want test_schema", r.Name)
				}
				if r.Comment == nil || *r.Comment != "a test schema" {
					t.Errorf("comment: got %v, want 'a test schema'", r.Comment)
				}
				if r.Owner == nil || *r.Owner != "admin_user" {
					t.Errorf("owner: got %v, want admin_user", r.Owner)
				}
				if r.CatalogName == nil || *r.CatalogName != "lake" {
					t.Errorf("catalog_name: got %v, want lake", r.CatalogName)
				}
			},
		},
		{
			name:       "get schema by name",
			method:     "GET",
			path:       "/catalogs/lake/schemas/test_schema",
			wantStatus: 200,
			check: func(t *testing.T, resp *http.Response) {
				r := decodeJSON[SchemaDetail](t, resp)
				if r.Name == nil || *r.Name != "test_schema" {
					t.Errorf("name: got %v, want test_schema", r.Name)
				}
			},
		},
		{
			name:       "list schemas includes new schema",
			method:     "GET",
			path:       "/catalogs/lake/schemas",
			wantStatus: 200,
			check: func(t *testing.T, resp *http.Response) {
				r := decodeJSON[PaginatedSchemaDetails](t, resp)
				if r.Data == nil || len(*r.Data) != 1 {
					t.Errorf("expected 1 schema, got %v", r.Data)
				}
			},
		},
		{
			name:       "update schema metadata",
			method:     "PATCH",
			path:       "/catalogs/lake/schemas/test_schema",
			body:       `{"comment":"updated comment","properties":{"env":"test"}}`,
			wantStatus: 200,
			check: func(t *testing.T, resp *http.Response) {
				r := decodeJSON[SchemaDetail](t, resp)
				if r.Comment == nil || *r.Comment != "updated comment" {
					t.Errorf("comment: got %v, want 'updated comment'", r.Comment)
				}
				if r.Properties == nil || (*r.Properties)["env"] != "test" {
					t.Errorf("properties: got %v, want {env:test}", r.Properties)
				}
			},
		},
		{
			name:       "delete schema",
			method:     "DELETE",
			path:       "/catalogs/lake/schemas/test_schema",
			wantStatus: 204,
		},
		{
			name:       "get deleted schema returns 404",
			method:     "GET",
			path:       "/catalogs/lake/schemas/test_schema",
			wantStatus: 404,
		},
	}

	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			resp := doRequest(t, s.method, srv.URL+s.path, s.body)
			if resp.StatusCode != s.wantStatus {
				t.Fatalf("status: got %d, want %d", resp.StatusCode, s.wantStatus)
			}
			if s.check != nil {
				s.check(t, resp)
			} else {
				_ = resp.Body.Close()
			}
		})
	}
}

func TestAPI_TableCRUD(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("test_schema")
	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	type step struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		check      func(t *testing.T, resp *http.Response)
	}

	steps := []step{
		{
			name:       "create table",
			method:     "POST",
			path:       "/catalogs/lake/schemas/test_schema/tables",
			body:       `{"name":"users","columns":[{"name":"id","type":"INTEGER"},{"name":"name","type":"VARCHAR"}],"comment":"user table"}`,
			wantStatus: 201,
			check: func(t *testing.T, resp *http.Response) {
				r := decodeJSON[TableDetail](t, resp)
				if r.Name == nil || *r.Name != "users" {
					t.Errorf("name: got %v, want users", r.Name)
				}
				if r.SchemaName == nil || *r.SchemaName != "test_schema" {
					t.Errorf("schema_name: got %v, want test_schema", r.SchemaName)
				}
				if r.Comment == nil || *r.Comment != "user table" {
					t.Errorf("comment: got %v, want 'user table'", r.Comment)
				}
				if r.Columns == nil || len(*r.Columns) != 2 {
					t.Errorf("columns: got %d, want 2", len(*r.Columns))
				}
				if r.Owner == nil || *r.Owner != "admin_user" {
					t.Errorf("owner: got %v, want admin_user", r.Owner)
				}
				if r.TableType == nil || *r.TableType != "MANAGED" {
					t.Errorf("table_type: got %v, want MANAGED", r.TableType)
				}
			},
		},
		{
			name:       "get table by name",
			method:     "GET",
			path:       "/catalogs/lake/schemas/test_schema/tables/users",
			wantStatus: 200,
			check: func(t *testing.T, resp *http.Response) {
				r := decodeJSON[TableDetail](t, resp)
				if r.Columns == nil || len(*r.Columns) != 2 {
					t.Fatalf("columns: got %d, want 2", len(*r.Columns))
				}
				cols := *r.Columns
				if cols[0].Name == nil || *cols[0].Name != "id" {
					t.Errorf("col[0].name: got %v, want id", cols[0].Name)
				}
				if cols[1].Type == nil || *cols[1].Type != "VARCHAR" {
					t.Errorf("col[1].type: got %v, want VARCHAR", cols[1].Type)
				}
				// Verify nullable field is present in column response
				if cols[0].Nullable == nil {
					t.Error("col[0].nullable should not be nil")
				} else if *cols[0].Nullable != false {
					t.Errorf("col[0].nullable: got %v, want false", *cols[0].Nullable)
				}
			},
		},
		{
			name:       "list tables",
			method:     "GET",
			path:       "/catalogs/lake/schemas/test_schema/tables",
			wantStatus: 200,
			check: func(t *testing.T, resp *http.Response) {
				r := decodeJSON[PaginatedTableDetails](t, resp)
				if r.Data == nil || len(*r.Data) != 1 {
					t.Errorf("expected 1 table, got %v", r.Data)
				}
			},
		},
		{
			name:       "list columns",
			method:     "GET",
			path:       "/catalogs/lake/schemas/test_schema/tables/users/columns",
			wantStatus: 200,
			check: func(t *testing.T, resp *http.Response) {
				r := decodeJSON[PaginatedColumnDetails](t, resp)
				if r.Data == nil || len(*r.Data) != 2 {
					t.Fatalf("expected 2 columns, got %v", r.Data)
				}
				cols := *r.Data
				if cols[0].Name == nil || *cols[0].Name != "id" {
					t.Errorf("col[0].name: got %v, want id", cols[0].Name)
				}
				if cols[0].Position == nil || *cols[0].Position != 0 {
					t.Errorf("col[0].position: got %v, want 0", cols[0].Position)
				}
			},
		},
		{
			name:       "delete table",
			method:     "DELETE",
			path:       "/catalogs/lake/schemas/test_schema/tables/users",
			wantStatus: 204,
		},
		{
			name:       "get deleted table returns 404",
			method:     "GET",
			path:       "/catalogs/lake/schemas/test_schema/tables/users",
			wantStatus: 404,
		},
	}

	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			resp := doRequest(t, s.method, srv.URL+s.path, s.body)
			if resp.StatusCode != s.wantStatus {
				t.Fatalf("status: got %d, want %d", resp.StatusCode, s.wantStatus)
			}
			if s.check != nil {
				s.check(t, resp)
			} else {
				_ = resp.Body.Close()
			}
		})
	}
}

func TestAPI_Schema_Authorization(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("main")

	tests := []struct {
		name       string
		principal  string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		// no_access_user has no privileges — mutating ops should be 403
		{"create schema denied", "no_access_user", "POST", "/catalogs/lake/schemas", `{"name":"forbidden"}`, 403},
		{"update schema denied", "no_access_user", "PATCH", "/catalogs/lake/schemas/main", `{"comment":"nope"}`, 403},
		{"delete schema denied", "no_access_user", "DELETE", "/catalogs/lake/schemas/main", "", 403},
		// read ops should succeed regardless of privileges
		{"list schemas allowed", "no_access_user", "GET", "/catalogs/lake/schemas", "", 200},
		{"get schema allowed", "no_access_user", "GET", "/catalogs/lake/schemas/main", "", 200},
		{"get catalog allowed", "no_access_user", "GET", "/catalogs/lake/info", "", 200},
		// admin_user has ALL_PRIVILEGES via admins group — all ops should succeed
		{"admin create schema", "admin_user", "POST", "/catalogs/lake/schemas", `{"name":"new_schema"}`, 201},
		{"admin update schema", "admin_user", "PATCH", "/catalogs/lake/schemas/main", `{"comment":"updated"}`, 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each sub-test gets its own server since principal is baked into middleware
			srv := setupCatalogTestServer(t, tt.principal, mock)
			defer srv.Close()

			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status: got %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestAPI_Table_Authorization(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("main")
	mock.addTable("main", "titanic", []domain.ColumnDetail{{Name: "id", Type: "INTEGER"}})

	tests := []struct {
		name       string
		principal  string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"create table denied", "no_access_user", "POST", "/catalogs/lake/schemas/main/tables",
			`{"name":"forbidden","columns":[{"name":"x","type":"INT"}]}`, 403},
		{"delete table denied", "no_access_user", "DELETE", "/catalogs/lake/schemas/main/tables/titanic", "", 403},
		// read ops succeed
		{"list tables allowed", "no_access_user", "GET", "/catalogs/lake/schemas/main/tables", "", 200},
		{"get table allowed", "no_access_user", "GET", "/catalogs/lake/schemas/main/tables/titanic", "", 200},
		{"list columns allowed", "no_access_user", "GET", "/catalogs/lake/schemas/main/tables/titanic/columns", "", 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := setupCatalogTestServer(t, tt.principal, mock)
			defer srv.Close()

			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status: got %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestAPI_Schema_Validation(t *testing.T) {
	mock := newMockCatalogRepo()
	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"empty name", "POST", "/catalogs/lake/schemas", `{"name":""}`, 400},
		{"invalid name with space", "POST", "/catalogs/lake/schemas", `{"name":"bad name"}`, 400},
		{"invalid name with special chars", "POST", "/catalogs/lake/schemas", `{"name":"bad!name"}`, 400},
		{"get nonexistent schema", "GET", "/catalogs/lake/schemas/nonexistent", "", 404},
		{"delete nonexistent schema", "DELETE", "/catalogs/lake/schemas/nonexistent", "", 404},
		{"create table in nonexistent schema", "POST", "/catalogs/lake/schemas/nonexistent/tables",
			`{"name":"t","columns":[{"name":"x","type":"INT"}]}`, 400},
		{"create table with no columns", "POST", "/catalogs/lake/schemas/nonexistent/tables",
			`{"name":"t","columns":[]}`, 400},
		{"get table in nonexistent schema", "GET", "/catalogs/lake/schemas/nonexistent/tables/foo", "", 404},
		{"list tables in nonexistent schema", "GET", "/catalogs/lake/schemas/nonexistent/tables", "", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status: got %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestAPI_Schema_Conflict(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("existing")
	mock.addTable("existing", "some_table", []domain.ColumnDetail{{Name: "id", Type: "INT"}})

	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"duplicate schema", "POST", "/catalogs/lake/schemas", `{"name":"existing"}`, 409},
		// delete non-empty schema without force -> conflict
		{"non-empty schema delete without force", "DELETE", "/catalogs/lake/schemas/existing", "", 409},
		// delete non-empty schema with force -> success
		{"non-empty schema delete with force", "DELETE", "/catalogs/lake/schemas/existing?force=true", "", 204},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status: got %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestAPI_CatalogPagination(t *testing.T) {
	mock := newMockCatalogRepo()
	// Prepopulate with 5 schemas (alphabetically: alpha, bravo, charlie, delta, echo)
	for _, name := range []string{"charlie", "alpha", "echo", "bravo", "delta"} {
		mock.addSchema(name)
	}

	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	tests := []struct {
		name          string
		path          string
		wantCount     int
		wantHasToken  bool
		wantFirstName string
	}{
		{"page 1", "/catalogs/lake/schemas?max_results=2", 2, true, "alpha"},
		// We'll chain tokens dynamically below
	}

	// Page 1
	t.Run(tests[0].name, func(t *testing.T) {
		resp := doRequest(t, "GET", srv.URL+tests[0].path, "")
		r := decodeJSON[PaginatedSchemaDetails](t, resp)
		if r.Data == nil || len(*r.Data) != tests[0].wantCount {
			t.Fatalf("count: got %d, want %d", len(*r.Data), tests[0].wantCount)
		}
		if (*r.Data)[0].Name == nil || *(*r.Data)[0].Name != tests[0].wantFirstName {
			t.Errorf("first: got %v, want %s", (*r.Data)[0].Name, tests[0].wantFirstName)
		}
		if tests[0].wantHasToken && r.NextPageToken == nil {
			t.Fatal("expected next_page_token, got nil")
		}
	})

	// Full pagination chain: collect all schema names across pages
	t.Run("full pagination chain", func(t *testing.T) {
		var allNames []string
		path := "/catalogs/lake/schemas?max_results=2"
		pages := 0

		for {
			pages++
			if pages > 10 {
				t.Fatal("pagination loop detected")
			}

			resp := doRequest(t, "GET", srv.URL+path, "")
			r := decodeJSON[PaginatedSchemaDetails](t, resp)

			if r.Data == nil {
				t.Fatal("data is nil")
			}
			for _, s := range *r.Data {
				if s.Name != nil {
					allNames = append(allNames, *s.Name)
				}
			}

			if r.NextPageToken == nil {
				break
			}
			path = fmt.Sprintf("/catalogs/lake/schemas?max_results=2&page_token=%s", url.QueryEscape(*r.NextPageToken))
		}

		if len(allNames) != 5 {
			t.Errorf("total schemas: got %d, want 5", len(allNames))
		}
		if pages != 3 {
			t.Errorf("pages: got %d, want 3", pages)
		}
		// Verify sorted order
		wantOrder := []string{"alpha", "bravo", "charlie", "delta", "echo"}
		for i, name := range wantOrder {
			if i >= len(allNames) || allNames[i] != name {
				t.Errorf("order[%d]: got %v, want %s", i, allNames[i], name)
			}
		}
	})
}

func TestAPI_ExistingPagination(t *testing.T) {
	// Uses the real setupTestServer which seeds 3 principals
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	t.Run("principals pagination", func(t *testing.T) {
		var allNames []string
		path := "/principals?max_results=1"
		pages := 0

		for {
			pages++
			if pages > 10 {
				t.Fatal("pagination loop detected")
			}

			resp := doRequest(t, "GET", srv.URL+path, "")
			r := decodeJSON[PaginatedPrincipals](t, resp)

			if r.Data == nil {
				t.Fatal("data is nil")
			}
			for _, p := range *r.Data {
				if p.Name != nil {
					allNames = append(allNames, *p.Name)
				}
			}

			if r.NextPageToken == nil {
				break
			}
			path = fmt.Sprintf("/principals?max_results=1&page_token=%s", url.QueryEscape(*r.NextPageToken))
		}

		if len(allNames) != 3 {
			t.Errorf("total principals: got %d, want 3", len(allNames))
		}
		if pages != 3 {
			t.Errorf("pages: got %d, want 3", pages)
		}
	})

	t.Run("groups pagination", func(t *testing.T) {
		var count int
		path := "/groups?max_results=1"
		pages := 0

		for {
			pages++
			if pages > 10 {
				t.Fatal("pagination loop detected")
			}

			resp := doRequest(t, "GET", srv.URL+path, "")
			if resp.StatusCode != 200 {
				_ = resp.Body.Close()
				t.Fatalf("page %d: expected 200, got %d for path %s", pages, resp.StatusCode, path)
			}
			r := decodeJSON[PaginatedGroups](t, resp)

			if r.Data != nil {
				count += len(*r.Data)
			}

			if r.NextPageToken == nil {
				break
			}
			path = fmt.Sprintf("/groups?max_results=1&page_token=%s", url.QueryEscape(*r.NextPageToken))
		}

		if count != 2 {
			t.Errorf("total groups: got %d, want 2", count)
		}
		if pages != 2 {
			t.Errorf("pages: got %d, want 2", pages)
		}
	})
}

// ptrVal safely dereferences a pointer, returning the zero value if nil.
func ptrVal[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// === UpdateTable ===

func TestAPI_UpdateTable(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("main")
	mock.addTable("main", "users", []domain.ColumnDetail{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "VARCHAR"},
	})
	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	t.Run("update comment", func(t *testing.T) {
		resp := doRequest(t, "PATCH", srv.URL+"/catalogs/lake/schemas/main/tables/users", `{"comment":"updated table comment"}`)
		if resp.StatusCode != 200 {
			t.Fatalf("status: got %d, want 200", resp.StatusCode)
		}
		r := decodeJSON[TableDetail](t, resp)
		if r.Comment == nil || *r.Comment != "updated table comment" {
			t.Errorf("comment: got %v, want 'updated table comment'", r.Comment)
		}
	})

	t.Run("update properties", func(t *testing.T) {
		resp := doRequest(t, "PATCH", srv.URL+"/catalogs/lake/schemas/main/tables/users", `{"properties":{"team":"data"}}`)
		if resp.StatusCode != 200 {
			t.Fatalf("status: got %d, want 200", resp.StatusCode)
		}
		r := decodeJSON[TableDetail](t, resp)
		if r.Properties == nil || (*r.Properties)["team"] != "data" {
			t.Errorf("properties: got %v, want {team:data}", r.Properties)
		}
	})

	t.Run("not found", func(t *testing.T) {
		resp := doRequest(t, "PATCH", srv.URL+"/catalogs/lake/schemas/main/tables/nonexistent", `{"comment":"nope"}`)
		defer resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != 404 {
			t.Errorf("status: got %d, want 404", resp.StatusCode)
		}
	})
}

// === UpdateColumn ===

func TestAPI_UpdateColumn(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("main")
	mock.addTable("main", "users", []domain.ColumnDetail{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "VARCHAR"},
	})
	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	t.Run("update column comment", func(t *testing.T) {
		resp := doRequest(t, "PATCH", srv.URL+"/catalogs/lake/schemas/main/tables/users/columns/id", `{"comment":"primary key"}`)
		if resp.StatusCode != 200 {
			t.Fatalf("status: got %d, want 200", resp.StatusCode)
		}
		r := decodeJSON[ColumnDetail](t, resp)
		if r.Comment == nil || *r.Comment != "primary key" {
			t.Errorf("comment: got %v, want 'primary key'", r.Comment)
		}
	})

	t.Run("column not found", func(t *testing.T) {
		resp := doRequest(t, "PATCH", srv.URL+"/catalogs/lake/schemas/main/tables/users/columns/nonexistent", `{"comment":"nope"}`)
		defer resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != 404 {
			t.Errorf("status: got %d, want 404", resp.StatusCode)
		}
	})
}

// === Authorization for new update endpoints ===

func TestAPI_UpdateEndpoints_Authorization(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("main")
	mock.addTable("main", "users", []domain.ColumnDetail{{Name: "id", Type: "INTEGER"}})

	tests := []struct {
		name       string
		principal  string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"update table denied", "no_access_user", "PATCH", "/catalogs/lake/schemas/main/tables/users", `{"comment":"nope"}`, 403},
		{"update column denied", "no_access_user", "PATCH", "/catalogs/lake/schemas/main/tables/users/columns/id", `{"comment":"nope"}`, 403},
		{"admin update table", "admin_user", "PATCH", "/catalogs/lake/schemas/main/tables/users", `{"comment":"ok"}`, 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := setupCatalogTestServer(t, tt.principal, mock)
			defer srv.Close()

			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status: got %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestAPI_ColumnNullable(t *testing.T) {
	mock := newMockCatalogRepo()
	mock.addSchema("main")
	mock.addTable("main", "events", []domain.ColumnDetail{
		{Name: "id", Type: "INTEGER", Nullable: false},
		{Name: "name", Type: "VARCHAR", Nullable: true},
	})
	srv := setupCatalogTestServer(t, "admin_user", mock)
	defer srv.Close()

	resp := doRequest(t, "GET", srv.URL+"/catalogs/lake/schemas/main/tables/events", "")
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	r := decodeJSON[TableDetail](t, resp)
	if r.Columns == nil || len(*r.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(*r.Columns))
	}
	cols := *r.Columns
	if cols[0].Nullable == nil || *cols[0].Nullable != false {
		t.Errorf("col[0] (id) nullable: got %v, want false", cols[0].Nullable)
	}
	if cols[1].Nullable == nil || *cols[1].Nullable != true {
		t.Errorf("col[1] (name) nullable: got %v, want true", cols[1].Nullable)
	}
}

// === Security-focused tests (no DuckDB / titanic.parquet needed) ===

// setupSecurityTestServer creates a lightweight test server that only needs SQLite.
// It wires security services (principals, groups, grants, API keys) without DuckDB.
func setupSecurityTestServer(t *testing.T, principalName string, isAdmin bool) *httptest.Server {
	t.Helper()

	metaDB, _ := internaldb.OpenTestSQLite(t)

	auditRepo := repository.NewAuditRepo(metaDB)
	principalRepo := repository.NewPrincipalRepo(metaDB)
	groupRepo := repository.NewGroupRepo(metaDB)
	grantRepo := repository.NewGrantRepo(metaDB)
	rowFilterRepo := repository.NewRowFilterRepo(metaDB)
	columnMaskRepo := repository.NewColumnMaskRepo(metaDB)
	apiKeyRepo := repository.NewAPIKeyRepo(metaDB)

	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := security.NewGroupService(groupRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)
	apiKeySvc := security.NewAPIKeyService(apiKeyRepo, auditRepo)

	handler := NewHandler(
		nil, // querySvc (not needed for security tests)
		principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, auditSvc,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, // computeEndpointSvc
		apiKeySvc,
	)
	strictHandler := NewStrictHandler(handler, nil)

	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := domain.WithPrincipal(req.Context(), domain.ContextPrincipal{
				Name:    principalName,
				IsAdmin: isAdmin,
				Type:    "user",
			})
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	HandlerFromMux(strictHandler, r)

	return httptest.NewServer(r)
}

func TestAPI_AdminGuards(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{
			name:   "create principal requires admin",
			method: http.MethodPost,
			path:   "/principals",
			body:   `{"name":"test","type":"user"}`,
			want:   http.StatusForbidden,
		},
		{
			name:   "delete principal requires admin",
			method: http.MethodDelete,
			path:   "/principals/999",
			want:   http.StatusForbidden,
		},
		{
			name:   "update principal admin requires admin",
			method: http.MethodPut,
			path:   "/principals/999/admin",
			body:   `{"is_admin":true}`,
			want:   http.StatusForbidden,
		},
		{
			name:   "create group requires admin",
			method: http.MethodPost,
			path:   "/groups",
			body:   `{"name":"test-group"}`,
			want:   http.StatusForbidden,
		},
		{
			name:   "delete group requires admin",
			method: http.MethodDelete,
			path:   "/groups/999",
			want:   http.StatusForbidden,
		},
		{
			name:   "create grant requires admin",
			method: http.MethodPost,
			path:   "/grants",
			body:   `{"principal_id":"1","principal_type":"user","privilege":"SELECT","securable_type":"table","securable_id":"1"}`,
			want:   http.StatusForbidden,
		},
		{
			name:   "cleanup expired keys requires admin",
			method: http.MethodPost,
			path:   "/api-keys/cleanup",
			want:   http.StatusForbidden,
		},
	}

	srv := setupSecurityTestServer(t, "non-admin-user", false)
	defer srv.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close() //nolint:errcheck
			if resp.StatusCode != tt.want {
				t.Errorf("got status %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestAPI_AdminGuards_Allowed(t *testing.T) {
	srv := setupSecurityTestServer(t, "admin-user", true)
	defer srv.Close()

	// Admin can create a principal.
	resp := doRequest(t, http.MethodPost, srv.URL+"/principals", `{"name":"created-by-admin","type":"user"}`)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("admin create principal: got status %d, want 201", resp.StatusCode)
	}

	// Admin can create a group.
	resp2 := doRequest(t, http.MethodPost, srv.URL+"/groups", `{"name":"admin-group"}`)
	defer resp2.Body.Close() //nolint:errcheck
	if resp2.StatusCode != http.StatusCreated {
		t.Errorf("admin create group: got status %d, want 201", resp2.StatusCode)
	}

	// Admin can cleanup expired keys.
	resp3 := doRequest(t, http.MethodPost, srv.URL+"/api-keys/cleanup", "")
	defer resp3.Body.Close() //nolint:errcheck
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("admin cleanup keys: got status %d, want 200", resp3.StatusCode)
	}
}

func TestAPI_APIKey_CRUD(t *testing.T) {
	srv := setupSecurityTestServer(t, "admin-user", true)
	defer srv.Close()

	// Create a principal first.
	resp := doRequest(t, http.MethodPost, srv.URL+"/principals", `{"name":"key-owner","type":"user"}`)
	p := decodeJSON[Principal](t, resp)
	require.NotNil(t, p.Id)

	// Create API key.
	body := fmt.Sprintf(`{"principal_id":"%s","name":"test-key"}`, *p.Id)
	resp2 := doRequest(t, http.MethodPost, srv.URL+"/api-keys", body)
	require.Equal(t, http.StatusCreated, resp2.StatusCode)
	keyResp := decodeJSON[CreateAPIKeyResponse](t, resp2)
	require.NotNil(t, keyResp.Key, "raw key should be returned on create")
	require.NotEmpty(t, *keyResp.Key)
	require.NotNil(t, keyResp.Id)

	// List API keys.
	listURL := fmt.Sprintf("%s/api-keys?principal_id=%s", srv.URL, *p.Id)
	resp3 := doRequest(t, http.MethodGet, listURL, "")
	require.Equal(t, http.StatusOK, resp3.StatusCode)
	listResp := decodeJSON[PaginatedAPIKeys](t, resp3)
	require.NotNil(t, listResp.Data)
	require.Len(t, *listResp.Data, 1)
	// Raw key should NOT be in list response.
	item := (*listResp.Data)[0]
	require.NotNil(t, item.Name)
	require.Equal(t, "test-key", *item.Name)

	// Delete API key.
	deleteURL := fmt.Sprintf("%s/api-keys/%s", srv.URL, *keyResp.Id)
	resp4 := doRequest(t, http.MethodDelete, deleteURL, "")
	defer resp4.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusNoContent, resp4.StatusCode)

	// Verify deleted - list should be empty.
	resp5 := doRequest(t, http.MethodGet, listURL, "")
	listResp2 := decodeJSON[PaginatedAPIKeys](t, resp5)
	if listResp2.Data != nil {
		require.Empty(t, *listResp2.Data)
	}
}

func TestAPI_ReadEndpoints_NoAdminRequired(t *testing.T) {
	srv := setupSecurityTestServer(t, "non-admin-user", false)
	defer srv.Close()

	// Non-admin can list principals.
	resp := doRequest(t, http.MethodGet, srv.URL+"/principals", "")
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Errorf("list principals: got status %d, want 200", resp.StatusCode)
	}

	// Non-admin can list groups.
	resp2 := doRequest(t, http.MethodGet, srv.URL+"/groups", "")
	defer resp2.Body.Close() //nolint:errcheck
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("list groups: got status %d, want 200", resp2.StatusCode)
	}
}
