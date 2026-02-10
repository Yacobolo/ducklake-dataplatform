package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-chi/chi/v5"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mattn/go-sqlite3"

	internaldb "duck-demo/internal/db"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
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

	// SQLite metastore
	tmpDir := t.TempDir()
	metaDB, err := sql.Open("sqlite3", tmpDir+"/test.sqlite?_foreign_keys=on")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { metaDB.Close() })

	if err := internaldb.RunMigrations(metaDB); err != nil {
		t.Fatalf("migrations: %v", err)
	}

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

	querySvc := service.NewQueryService(eng, auditRepo)
	principalSvc := service.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := service.NewGroupService(groupRepo, auditRepo)
	grantSvc := service.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := service.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := service.NewColumnMaskService(columnMaskRepo, auditRepo)
	introspectionSvc := service.NewIntrospectionService(introspectionRepo)
	auditSvc := service.NewAuditService(auditRepo)

	handler := NewHandler(querySvc, principalSvc, groupSvc, grantSvc, rowFilterSvc, columnMaskSvc, introspectionSvc, auditSvc)
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

	var principals []Principal
	if err := json.NewDecoder(resp.Body).Decode(&principals); err != nil {
		t.Fatal(err)
	}
	if len(principals) < 2 {
		t.Errorf("expected at least 2 principals, got %d", len(principals))
	}
	t.Logf("got %d principals", len(principals))
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

	var schemas []Schema
	json.NewDecoder(resp.Body).Decode(&schemas)
	if len(schemas) == 0 {
		t.Error("expected at least one schema")
	}
	t.Logf("got %d schemas", len(schemas))
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

	var result AuditLogResponse
	json.NewDecoder(resp.Body).Decode(&result)
	t.Logf("got %d audit entries (total: %v)", len(*result.Data), result.Total)
}

func itoa(i int64) string {
	return fmt.Sprintf("%d", i)
}
