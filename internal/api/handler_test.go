package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/go-chi/chi/v5"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mattn/go-sqlite3"

	internaldb "duck-demo/internal/db"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/middleware"
	"duck-demo/internal/service"
)

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
	t.Cleanup(func() { duckDB.Close() })

	if _, err := duckDB.Exec("CREATE TABLE titanic AS SELECT * FROM '../../titanic.parquet'"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// SQLite metastore (hardened write/read pools; tests use writeDB for everything)
	metaDB, _ := internaldb.OpenTestSQLite(t)

	// Create mock DuckLake tables
	_, err = metaDB.Exec(`
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

	adminUser, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "admin_user", Type: "user", IsAdmin: 1})
	analyst, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "analyst1", Type: "user", IsAdmin: 0})
	q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "no_access_user", Type: "user", IsAdmin: 0})

	adminsGroup, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "admins"})
	analystsGroup, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})

	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID})
	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst.ID})

	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: "catalog", SecurableID: 0, Privilege: "ALL_PRIVILEGES",
	})
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: 0, Privilege: "USAGE",
	})
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: 1, Privilege: "SELECT",
	})

	// Row filter for analysts
	filter, _ := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{TableID: 1, FilterSql: `"Pclass" = 1`})
	q.BindRowFilter(ctx, dbstore.BindRowFilterParams{RowFilterID: filter.ID, PrincipalID: analystsGroup.ID, PrincipalType: "group"})

	// Build repositories and services
	auditRepo := repository.NewAuditRepo(metaDB)
	principalRepo := repository.NewPrincipalRepo(metaDB)
	groupRepo := repository.NewGroupRepo(metaDB)
	grantRepo := repository.NewGrantRepo(metaDB)
	rowFilterRepo := repository.NewRowFilterRepo(metaDB)
	columnMaskRepo := repository.NewColumnMaskRepo(metaDB)
	introspectionRepo := repository.NewIntrospectionRepo(metaDB)

	cat := service.NewAuthorizationService(principalRepo, groupRepo, grantRepo, rowFilterRepo, columnMaskRepo, introspectionRepo)
	eng := engine.NewSecureEngine(duckDB, cat)

	lineageRepo := repository.NewLineageRepo(metaDB)
	querySvc := service.NewQueryService(eng, auditRepo, lineageRepo)
	principalSvc := service.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := service.NewGroupService(groupRepo, auditRepo)
	grantSvc := service.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := service.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := service.NewColumnMaskService(columnMaskRepo, auditRepo)
	introspectionSvc := service.NewIntrospectionService(introspectionRepo)
	auditSvc := service.NewAuditService(auditRepo)

	catalogRepo := repository.NewCatalogRepo(metaDB, duckDB)
	catalogSvc := service.NewCatalogService(catalogRepo, cat, auditRepo)

	queryHistorySvc := service.NewQueryHistoryService(repository.NewQueryHistoryRepo(metaDB))
	lineageSvc := service.NewLineageService(lineageRepo)
	searchSvc := service.NewSearchService(repository.NewSearchRepo(metaDB))
	tagSvc := service.NewTagService(repository.NewTagRepo(metaDB), auditRepo)
	viewSvc := service.NewViewService(repository.NewViewRepo(metaDB), catalogRepo, cat, auditRepo)

	handler := NewHandler(querySvc, principalSvc, groupSvc, grantSvc, rowFilterSvc, columnMaskSvc, introspectionSvc, auditSvc, nil, catalogSvc, queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc, nil)
	strictHandler := NewStrictHandler(handler, nil)

	// Setup router with fixed auth (test principal)
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := middleware.WithPrincipal(req.Context(), principalName)
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	HandlerFromMux(strictHandler, r)

	return httptest.NewServer(r)
}

func TestAPI_ListPrincipals(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/principals")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result PaginatedPrincipals
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Data == nil || len(*result.Data) < 2 {
		t.Errorf("expected at least 2 principals, got %v", result.Data)
	}
	t.Logf("got %d principals", len(*result.Data))
}

func TestAPI_ExecuteQuery_Admin(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	body := `{"sql": "SELECT count(*) FROM titanic"}`
	resp, err := http.Post(srv.URL+"/query", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.RowCount == nil || *result.RowCount == 0 {
		t.Error("expected non-zero row count")
	}
	t.Logf("admin query returned %d rows", *result.RowCount)
}

func TestAPI_ExecuteQuery_NoAccess(t *testing.T) {
	srv := setupTestServer(t, "no_access_user")
	defer srv.Close()

	body := `{"sql": "SELECT * FROM titanic LIMIT 5"}`
	resp, err := http.Post(srv.URL+"/query", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 403 {
		t.Errorf("expected 403 for no_access_user, got %d", resp.StatusCode)
	}
}

func TestAPI_CreateAndDeletePrincipal(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	// Create
	body := `{"name": "test_user", "type": "user"}`
	resp, err := http.Post(srv.URL+"/principals", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}

	var p Principal
	json.NewDecoder(resp.Body).Decode(&p)
	if p.Name == nil || *p.Name != "test_user" {
		t.Errorf("expected name=test_user, got %v", p.Name)
	}

	// Delete
	req, _ := http.NewRequest("DELETE", srv.URL+"/principals/"+itoa(*p.Id), nil)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != 204 {
		t.Errorf("expected 204, got %d", resp2.StatusCode)
	}
}

func TestAPI_ListSchemas(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/schemas")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var schemasResult PaginatedSchemas
	json.NewDecoder(resp.Body).Decode(&schemasResult)
	if schemasResult.Data == nil || len(*schemasResult.Data) == 0 {
		t.Error("expected at least one schema")
	}
	t.Logf("got %d schemas", len(*schemasResult.Data))
}

func TestAPI_AuditLogs(t *testing.T) {
	srv := setupTestServer(t, "admin_user")
	defer srv.Close()

	// Execute a query first to generate audit entries
	body := `{"sql": "SELECT 1"}`
	http.Post(srv.URL+"/query", "application/json", bytes.NewBufferString(body))

	// List audit logs
	resp, err := http.Get(srv.URL + "/audit-logs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var auditResult PaginatedAuditLogs
	json.NewDecoder(resp.Body).Decode(&auditResult)
	if auditResult.Data != nil {
		t.Logf("got %d audit entries", len(*auditResult.Data))
	}
}

func itoa(i int64) string {
	return fmt.Sprintf("%d", i)
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
	t.Cleanup(func() { duckDB.Close() })

	if _, err := duckDB.Exec("CREATE TABLE titanic AS SELECT * FROM '../../titanic.parquet'"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// SQLite metastore (hardened write/read pools; tests use writeDB for everything)
	metaDB, _ := internaldb.OpenTestSQLite(t)

	// Create mock DuckLake tables (for introspection/authorization)
	_, err = metaDB.Exec(`
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

	adminUser, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "admin_user", Type: "user", IsAdmin: 1})
	analyst, _ := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "analyst1", Type: "user", IsAdmin: 0})
	q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "no_access_user", Type: "user", IsAdmin: 0})

	adminsGroup, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "admins"})
	analystsGroup, _ := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})

	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID})
	q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst.ID})

	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: "catalog", SecurableID: 0, Privilege: "ALL_PRIVILEGES",
	})
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: 0, Privilege: "USAGE",
	})
	q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: 1, Privilege: "SELECT",
	})

	// Build repositories and services
	auditRepo := repository.NewAuditRepo(metaDB)
	principalRepo := repository.NewPrincipalRepo(metaDB)
	groupRepo := repository.NewGroupRepo(metaDB)
	grantRepo := repository.NewGrantRepo(metaDB)
	rowFilterRepo := repository.NewRowFilterRepo(metaDB)
	columnMaskRepo := repository.NewColumnMaskRepo(metaDB)
	introspectionRepo := repository.NewIntrospectionRepo(metaDB)

	cat := service.NewAuthorizationService(principalRepo, groupRepo, grantRepo, rowFilterRepo, columnMaskRepo, introspectionRepo)
	eng := engine.NewSecureEngine(duckDB, cat)

	lineageRepo2 := repository.NewLineageRepo(metaDB)
	querySvc := service.NewQueryService(eng, auditRepo, lineageRepo2)
	principalSvc := service.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := service.NewGroupService(groupRepo, auditRepo)
	grantSvc := service.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := service.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := service.NewColumnMaskService(columnMaskRepo, auditRepo)
	introspectionSvc := service.NewIntrospectionService(introspectionRepo)
	auditSvc := service.NewAuditService(auditRepo)

	// Use the mock catalog repo instead of real DuckLake
	catalogSvc := service.NewCatalogService(mockRepo, cat, auditRepo)

	queryHistorySvc := service.NewQueryHistoryService(repository.NewQueryHistoryRepo(metaDB))
	lineageSvc := service.NewLineageService(lineageRepo2)
	searchSvc := service.NewSearchService(repository.NewSearchRepo(metaDB))
	tagSvc := service.NewTagService(repository.NewTagRepo(metaDB), auditRepo)
	viewSvc := service.NewViewService(repository.NewViewRepo(metaDB), mockRepo, cat, auditRepo)

	handler := NewHandler(querySvc, principalSvc, groupSvc, grantSvc, rowFilterSvc, columnMaskSvc, introspectionSvc, auditSvc, nil, catalogSvc, queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc, nil)
	strictHandler := NewStrictHandler(handler, nil)

	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := middleware.WithPrincipal(req.Context(), principalName)
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
	req, err := http.NewRequest(method, url, reqBody)
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
	defer resp.Body.Close()
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

	resp := doRequest(t, "GET", srv.URL+"/catalog", "")
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

	resp := doRequest(t, "GET", srv.URL+"/metastore/summary", "")
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
			path:       "/catalog/schemas",
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
			path:       "/catalog/schemas/test_schema",
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
			path:       "/catalog/schemas",
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
			path:       "/catalog/schemas/test_schema",
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
			path:       "/catalog/schemas/test_schema",
			wantStatus: 204,
		},
		{
			name:       "get deleted schema returns 404",
			method:     "GET",
			path:       "/catalog/schemas/test_schema",
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
				resp.Body.Close()
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
			path:       "/catalog/schemas/test_schema/tables",
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
			path:       "/catalog/schemas/test_schema/tables/users",
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
			},
		},
		{
			name:       "list tables",
			method:     "GET",
			path:       "/catalog/schemas/test_schema/tables",
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
			path:       "/catalog/schemas/test_schema/tables/users/columns",
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
			path:       "/catalog/schemas/test_schema/tables/users",
			wantStatus: 204,
		},
		{
			name:       "get deleted table returns 404",
			method:     "GET",
			path:       "/catalog/schemas/test_schema/tables/users",
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
				resp.Body.Close()
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
		{"create schema denied", "no_access_user", "POST", "/catalog/schemas", `{"name":"forbidden"}`, 403},
		{"update schema denied", "no_access_user", "PATCH", "/catalog/schemas/main", `{"comment":"nope"}`, 403},
		{"delete schema denied", "no_access_user", "DELETE", "/catalog/schemas/main", "", 403},
		// read ops should succeed regardless of privileges
		{"list schemas allowed", "no_access_user", "GET", "/catalog/schemas", "", 200},
		{"get schema allowed", "no_access_user", "GET", "/catalog/schemas/main", "", 200},
		{"get catalog allowed", "no_access_user", "GET", "/catalog", "", 200},
		// admin_user has ALL_PRIVILEGES via admins group — all ops should succeed
		{"admin create schema", "admin_user", "POST", "/catalog/schemas", `{"name":"new_schema"}`, 201},
		{"admin update schema", "admin_user", "PATCH", "/catalog/schemas/main", `{"comment":"updated"}`, 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each sub-test gets its own server since principal is baked into middleware
			srv := setupCatalogTestServer(t, tt.principal, mock)
			defer srv.Close()

			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close()
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
		{"create table denied", "no_access_user", "POST", "/catalog/schemas/main/tables",
			`{"name":"forbidden","columns":[{"name":"x","type":"INT"}]}`, 403},
		{"delete table denied", "no_access_user", "DELETE", "/catalog/schemas/main/tables/titanic", "", 403},
		// read ops succeed
		{"list tables allowed", "no_access_user", "GET", "/catalog/schemas/main/tables", "", 200},
		{"get table allowed", "no_access_user", "GET", "/catalog/schemas/main/tables/titanic", "", 200},
		{"list columns allowed", "no_access_user", "GET", "/catalog/schemas/main/tables/titanic/columns", "", 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := setupCatalogTestServer(t, tt.principal, mock)
			defer srv.Close()

			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close()
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
		{"empty name", "POST", "/catalog/schemas", `{"name":""}`, 400},
		{"invalid name with space", "POST", "/catalog/schemas", `{"name":"bad name"}`, 400},
		{"invalid name with special chars", "POST", "/catalog/schemas", `{"name":"bad!name"}`, 400},
		{"get nonexistent schema", "GET", "/catalog/schemas/nonexistent", "", 404},
		{"delete nonexistent schema", "DELETE", "/catalog/schemas/nonexistent", "", 404},
		{"create table in nonexistent schema", "POST", "/catalog/schemas/nonexistent/tables",
			`{"name":"t","columns":[{"name":"x","type":"INT"}]}`, 400},
		{"create table with no columns", "POST", "/catalog/schemas/nonexistent/tables",
			`{"name":"t","columns":[]}`, 400},
		{"get table in nonexistent schema", "GET", "/catalog/schemas/nonexistent/tables/foo", "", 404},
		{"list tables in nonexistent schema", "GET", "/catalog/schemas/nonexistent/tables", "", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close()
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
		{"duplicate schema", "POST", "/catalog/schemas", `{"name":"existing"}`, 409},
		// delete non-empty schema without force -> conflict (handler maps to 403)
		{"non-empty schema delete without force", "DELETE", "/catalog/schemas/existing", "", 403},
		// delete non-empty schema with force -> success
		{"non-empty schema delete with force", "DELETE", "/catalog/schemas/existing?force=true", "", 204},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doRequest(t, tt.method, srv.URL+tt.path, tt.body)
			defer resp.Body.Close()
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
		{"page 1", "/catalog/schemas?max_results=2", 2, true, "alpha"},
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
		path := "/catalog/schemas?max_results=2"
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
			path = fmt.Sprintf("/catalog/schemas?max_results=2&page_token=%s", url.QueryEscape(*r.NextPageToken))
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
				resp.Body.Close()
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
