package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/service/governance"
	"duck-demo/internal/service/security"
)

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
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, // computeEndpointSvc
		apiKeySvc,
	)

	return mountTestHandler(t, handler, principalName, isAdmin)
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
			body:   `{"principal_id":1,"principal_type":"user","privilege":"SELECT","securable_type":"table","securable_id":1}`,
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
	body := fmt.Sprintf(`{"principal_id":%d,"name":"test-key"}`, *p.Id)
	resp2 := doRequest(t, http.MethodPost, srv.URL+"/api-keys", body)
	require.Equal(t, http.StatusCreated, resp2.StatusCode)
	keyResp := decodeJSON[CreateAPIKeyResponse](t, resp2)
	require.NotNil(t, keyResp.Key, "raw key should be returned on create")
	require.NotEmpty(t, *keyResp.Key)
	require.NotNil(t, keyResp.Id)

	// List API keys.
	listURL := fmt.Sprintf("%s/api-keys?principal_id=%d", srv.URL, *p.Id)
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
	deleteURL := fmt.Sprintf("%s/api-keys/%d", srv.URL, *keyResp.Id)
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
		{"update table denied", "no_access_user", "PATCH", "/catalog/schemas/main/tables/users", `{"comment":"nope"}`, 403},
		{"update column denied", "no_access_user", "PATCH", "/catalog/schemas/main/tables/users/columns/id", `{"comment":"nope"}`, 403},
		{"update catalog denied", "no_access_user", "PATCH", "/catalog", `{"comment":"nope"}`, 403},
		{"admin update table", "admin_user", "PATCH", "/catalog/schemas/main/tables/users", `{"comment":"ok"}`, 200},
		{"admin update catalog", "admin_user", "PATCH", "/catalog", `{"comment":"ok"}`, 200},
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
