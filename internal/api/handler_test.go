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

// === Shared test helpers ===

// createMockDuckLakeTables creates the ducklake_schema and ducklake_table tables
// in the given metaDB and inserts a default "main" schema and "titanic" table.
func createMockDuckLakeTables(t *testing.T, metaDB *sql.DB) {
	t.Helper()
	_, err := metaDB.ExecContext(ctx, `
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
}

// testRBACData holds entity IDs created by seedTestRBAC.
type testRBACData struct {
	adminUser     dbstore.Principal
	analyst       dbstore.Principal
	noAccessUser  dbstore.Principal
	adminsGroup   dbstore.Group
	analystsGroup dbstore.Group
}

// seedTestRBAC creates principals, groups, and grants in the given metaDB.
// It does NOT create row filters (callers can add those separately).
func seedTestRBAC(t *testing.T, metaDB *sql.DB) testRBACData {
	t.Helper()
	ctx := context.Background()
	q := dbstore.New(metaDB)

	adminUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "admin_user", Type: "user", IsAdmin: 1})
	require.NoError(t, err)
	analyst, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "analyst1", Type: "user", IsAdmin: 0})
	require.NoError(t, err)
	noAccessUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{Name: "no_access_user", Type: "user", IsAdmin: 0})
	require.NoError(t, err)

	adminsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "admins"})
	require.NoError(t, err)
	analystsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{Name: "analysts"})
	require.NoError(t, err)

	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{GroupID: analystsGroup.ID, MemberType: "user", MemberID: analyst.ID})
	require.NoError(t, err)

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: "catalog", SecurableID: 0, Privilege: "ALL_PRIVILEGES",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "schema", SecurableID: 0, Privilege: "USAGE",
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: analystsGroup.ID, PrincipalType: "group",
		SecurableType: "table", SecurableID: 1, Privilege: "SELECT",
	})
	require.NoError(t, err)

	return testRBACData{
		adminUser:     adminUser,
		analyst:       analyst,
		noAccessUser:  noAccessUser,
		adminsGroup:   adminsGroup,
		analystsGroup: analystsGroup,
	}
}

// testRepos holds all repository instances for test wiring.
type testRepos struct {
	audit         domain.AuditRepository
	principal     domain.PrincipalRepository
	group         domain.GroupRepository
	grant         domain.GrantRepository
	rowFilter     domain.RowFilterRepository
	columnMask    domain.ColumnMaskRepository
	introspection domain.IntrospectionRepository
	lineage       domain.LineageRepository
	queryHistory  domain.QueryHistoryRepository
	search        domain.SearchRepository
	tag           domain.TagRepository
	view          domain.ViewRepository
}

// buildTestRepos constructs all repository instances from a metaDB.
func buildTestRepos(metaDB *sql.DB) testRepos {
	return testRepos{
		audit:         repository.NewAuditRepo(metaDB),
		principal:     repository.NewPrincipalRepo(metaDB),
		group:         repository.NewGroupRepo(metaDB),
		grant:         repository.NewGrantRepo(metaDB),
		rowFilter:     repository.NewRowFilterRepo(metaDB),
		columnMask:    repository.NewColumnMaskRepo(metaDB),
		introspection: repository.NewIntrospectionRepo(metaDB),
		lineage:       repository.NewLineageRepo(metaDB),
		queryHistory:  repository.NewQueryHistoryRepo(metaDB),
		search:        repository.NewSearchRepo(metaDB),
		tag:           repository.NewTagRepo(metaDB),
		view:          repository.NewViewRepo(metaDB),
	}
}

// testServices holds all service instances for test wiring.
type testServices struct {
	query        *query.QueryService
	principal    *security.PrincipalService
	group        *security.GroupService
	grant        *security.GrantService
	rowFilter    *security.RowFilterService
	columnMask   *security.ColumnMaskService
	audit        *governance.AuditService
	catalog      *catalog.CatalogService
	queryHistory *governance.QueryHistoryService
	lineage      *governance.LineageService
	search       *catalog.SearchService
	tag          *governance.TagService
	view         *catalog.ViewService
}

// buildTestServices constructs all service instances from repos.
// duckDB and catalogRepo are passed separately because they differ across setup functions.
func buildTestServices(repos testRepos, duckDB *sql.DB, catalogRepo domain.CatalogRepository) testServices {
	cat := security.NewAuthorizationService(repos.principal, repos.group, repos.grant, repos.rowFilter, repos.columnMask, repos.introspection, nil)

	var eng *engine.SecureEngine
	var querySvc *query.QueryService
	if duckDB != nil {
		eng = engine.NewSecureEngine(duckDB, cat, nil, nil, slog.New(slog.DiscardHandler))
		querySvc = query.NewQueryService(eng, repos.audit, repos.lineage)
	}

	catalogSvc := catalog.NewCatalogService(catalogRepo, cat, repos.audit, repos.tag, nil, nil)
	viewSvc := catalog.NewViewService(repos.view, catalogRepo, cat, repos.audit)

	return testServices{
		query:        querySvc,
		principal:    security.NewPrincipalService(repos.principal, repos.audit),
		group:        security.NewGroupService(repos.group, repos.audit),
		grant:        security.NewGrantService(repos.grant, repos.audit),
		rowFilter:    security.NewRowFilterService(repos.rowFilter, repos.audit),
		columnMask:   security.NewColumnMaskService(repos.columnMask, repos.audit),
		audit:        governance.NewAuditService(repos.audit),
		catalog:      catalogSvc,
		queryHistory: governance.NewQueryHistoryService(repos.queryHistory),
		lineage:      governance.NewLineageService(repos.lineage),
		search:       catalog.NewSearchService(repos.search),
		tag:          governance.NewTagService(repos.tag, repos.audit),
		view:         viewSvc,
	}
}

// mountTestHandler creates a chi router with fixed-principal middleware and returns an httptest.Server.
func mountTestHandler(t *testing.T, handler StrictServerInterface, principalName string, isAdmin bool) *httptest.Server {
	t.Helper()
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

// openTestDuckDB opens a DuckDB connection and creates the titanic table from parquet.
// It skips the test if titanic.parquet is not found.
func openTestDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	if _, err := os.Stat("../../titanic.parquet"); os.IsNotExist(err) {
		t.Skip("titanic.parquet not found, skipping integration test")
	}

	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = duckDB.Close() })

	if _, err := duckDB.ExecContext(ctx, "CREATE TABLE titanic AS SELECT * FROM '../../titanic.parquet'"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return duckDB
}

// setupTestServer creates a fully wired test HTTP server with real DuckDB + SQLite.
func setupTestServer(t *testing.T, principalName string) *httptest.Server {
	t.Helper()

	duckDB := openTestDuckDB(t)
	metaDB, _ := internaldb.OpenTestSQLite(t)
	createMockDuckLakeTables(t, metaDB)

	rbac := seedTestRBAC(t, metaDB)

	// Row filter for analysts (specific to setupTestServer)
	q := dbstore.New(metaDB)
	filter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{TableID: 1, FilterSql: `"Pclass" = 1`})
	require.NoError(t, err)
	err = q.BindRowFilter(ctx, dbstore.BindRowFilterParams{RowFilterID: filter.ID, PrincipalID: rbac.analystsGroup.ID, PrincipalType: "group"})
	require.NoError(t, err)

	repos := buildTestRepos(metaDB)
	catalogRepo := repository.NewCatalogRepo(metaDB, duckDB, nil, slog.New(slog.DiscardHandler))
	svcs := buildTestServices(repos, duckDB, catalogRepo)

	handler := NewHandler(svcs.query, svcs.principal, svcs.group, svcs.grant, svcs.rowFilter, svcs.columnMask, svcs.audit, nil, svcs.catalog, svcs.queryHistory, svcs.lineage, svcs.search, svcs.tag, svcs.view, nil, nil, nil, nil, nil, nil)

	// Lookup principal to get admin status for context injection
	p, err := repos.principal.GetByName(context.Background(), principalName)
	isAdmin := false
	if err == nil {
		isAdmin = p.IsAdmin
	}

	return mountTestHandler(t, handler, principalName, isAdmin)
}

// === Request helpers ===

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

func itoa(i int64) string {
	return fmt.Sprintf("%d", i)
}

// === Basic tests ===

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
	req, err = http.NewRequestWithContext(ctx, "DELETE", srv.URL+"/principals/"+itoa(*p.Id), nil)
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
