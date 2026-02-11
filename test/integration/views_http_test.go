//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_ViewCRUD exercises the full view lifecycle: create, list, get, drop,
// and verifies audit log entries are produced.
func TestHTTP_ViewCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	var viewName = "v_test"
	base := env.Server.URL + "/v1/catalog/schemas/main/views"

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_view", func(t *testing.T) {
			resp := doRequest(t, "POST", base, env.Keys.Admin, map[string]interface{}{
				"name":            viewName,
				"view_definition": "SELECT 1 AS val",
				"comment":         "test view",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, viewName, result["name"])
			assert.Equal(t, "main", result["schema_name"])
			assert.Equal(t, "SELECT 1 AS val", result["view_definition"])
			assert.Equal(t, "test view", result["comment"])
			assert.NotNil(t, result["id"])
			assert.NotEmpty(t, result["owner"])
		}},

		{"create_duplicate_409", func(t *testing.T) {
			resp := doRequest(t, "POST", base, env.Keys.Admin, map[string]interface{}{
				"name":            viewName,
				"view_definition": "SELECT 2",
			})
			defer resp.Body.Close()
			require.Equal(t, 409, resp.StatusCode)
		}},

		{"list_views", func(t *testing.T) {
			resp := doRequest(t, "GET", base, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 1)

			found := false
			for _, item := range data {
				v := item.(map[string]interface{})
				if v["name"] == viewName {
					found = true
					break
				}
			}
			assert.True(t, found, "expected to find view %q in list", viewName)
		}},

		{"get_view", func(t *testing.T) {
			resp := doRequest(t, "GET", base+"/"+viewName, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, viewName, result["name"])
			assert.Equal(t, "SELECT 1 AS val", result["view_definition"])
			assert.Equal(t, "test view", result["comment"])
		}},

		{"get_view_not_found", func(t *testing.T) {
			resp := doRequest(t, "GET", base+"/nonexistent", env.Keys.Admin, nil)
			defer resp.Body.Close()
			require.Equal(t, 404, resp.StatusCode)
		}},

		{"drop_view", func(t *testing.T) {
			resp := doRequest(t, "DELETE", base+"/"+viewName, env.Keys.Admin, nil)
			defer resp.Body.Close()
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"drop_view_idempotent", func(t *testing.T) {
			// Dropping an already-dropped view returns 204 (idempotent — the SQL
			// DELETE succeeds with 0 rows affected and no error).
			resp := doRequest(t, "DELETE", base+"/"+viewName, env.Keys.Admin, nil)
			defer resp.Body.Close()
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"verify_audit_logs", func(t *testing.T) {
			logs := fetchAuditLogs(t, env.Server.URL, env.Keys.Admin)

			expectedActions := map[string]bool{
				"CREATE_VIEW": false,
				"DROP_VIEW":   false,
			}
			for _, entry := range logs {
				action, _ := entry["action"].(string)
				if _, ok := expectedActions[action]; ok {
					expectedActions[action] = true
				}
			}
			for action, found := range expectedActions {
				assert.True(t, found, "expected audit entry with action %q", action)
			}
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_ViewAuthZ verifies that analysts (USAGE+SELECT only, no CREATE_TABLE
// on catalog) get 403 on create/drop but can list/get views.
func TestHTTP_ViewAuthZ(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	base := env.Server.URL + "/v1/catalog/schemas/main/views"
	viewName := "v_authz_test"

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"admin_creates_view", func(t *testing.T) {
			resp := doRequest(t, "POST", base, env.Keys.Admin, map[string]interface{}{
				"name":            viewName,
				"view_definition": "SELECT 1",
			})
			require.Equal(t, 201, resp.StatusCode)
			resp.Body.Close()
		}},

		{"analyst_create_403", func(t *testing.T) {
			resp := doRequest(t, "POST", base, env.Keys.Analyst, map[string]interface{}{
				"name":            "v_analyst_attempt",
				"view_definition": "SELECT 1",
			})
			defer resp.Body.Close()
			require.Equal(t, 403, resp.StatusCode)
		}},

		{"analyst_list_200", func(t *testing.T) {
			resp := doRequest(t, "GET", base, env.Keys.Analyst, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 1)
		}},

		{"analyst_get_200", func(t *testing.T) {
			resp := doRequest(t, "GET", base+"/"+viewName, env.Keys.Analyst, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, viewName, result["name"])
		}},

		{"analyst_drop_403", func(t *testing.T) {
			resp := doRequest(t, "DELETE", base+"/"+viewName, env.Keys.Analyst, nil)
			defer resp.Body.Close()
			require.Equal(t, 403, resp.StatusCode)
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_ViewSchemaNotFound verifies that requesting views for a nonexistent
// schema returns 404.
func TestHTTP_ViewSchemaNotFound(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	base := env.Server.URL + "/v1/catalog/schemas/nonexistent/views"

	t.Run("list_bad_schema_404", func(t *testing.T) {
		resp := doRequest(t, "GET", base, env.Keys.Admin, nil)
		defer resp.Body.Close()
		require.Equal(t, 404, resp.StatusCode)
	})

	t.Run("create_bad_schema", func(t *testing.T) {
		resp := doRequest(t, "POST", base, env.Keys.Admin, map[string]interface{}{
			"name":            "v_bad",
			"view_definition": "SELECT 1",
		})
		defer resp.Body.Close()
		// Schema not found should return 404 (or possibly 400)
		assert.Contains(t, []int{400, 404}, resp.StatusCode,
			fmt.Sprintf("expected 400 or 404, got %d", resp.StatusCode))
	})
}
