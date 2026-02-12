//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_AuditLogs_Operations verifies that CRUD operations produce audit entries.
func TestHTTP_AuditLogs_Operations(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	// Perform operations that should generate audit entries
	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create_principal_produces_audit", func(t *testing.T) {
			body := map[string]interface{}{"name": "audit-test-principal", "type": "user"}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()

			logs := fetchAuditLogs(t, env.Server.URL, env.Keys.Admin)
			found := false
			for _, entry := range logs {
				if action, ok := entry["action"].(string); ok && action == "CREATE_PRINCIPAL" {
					found = true
					// The service logs the created principal's name, not the caller
					assert.Equal(t, "audit-test-principal", entry["principal_name"])
					assert.Equal(t, "ALLOWED", entry["status"])
					break
				}
			}
			assert.True(t, found, "expected CREATE_PRINCIPAL audit entry")
		}},
		{"set_admin_produces_audit", func(t *testing.T) {
			// Get the principal we just created
			resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)
			var listResult map[string]interface{}
			decodeJSON(t, resp, &listResult)
			data := listResult["data"].([]interface{})

			var principalID int64
			for _, item := range data {
				p := item.(map[string]interface{})
				if p["name"] == "audit-test-principal" {
					principalID = int64(p["id"].(float64))
					break
				}
			}
			require.NotZero(t, principalID)

			url := fmt.Sprintf("%s/v1/principals/%d/admin", env.Server.URL, principalID)
			body := map[string]interface{}{"is_admin": true}
			resp2 := doRequest(t, "PUT", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp2.StatusCode)
			_ = resp2.Body.Close()

			logs := fetchAuditLogs(t, env.Server.URL, env.Keys.Admin)
			found := false
			for _, entry := range logs {
				if action, ok := entry["action"].(string); ok && action == "SET_ADMIN" {
					found = true
					break
				}
			}
			assert.True(t, found, "expected SET_ADMIN audit entry")
		}},
		{"delete_principal_produces_audit", func(t *testing.T) {
			// Find the principal
			resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)
			var listResult map[string]interface{}
			decodeJSON(t, resp, &listResult)
			data := listResult["data"].([]interface{})

			var principalID int64
			for _, item := range data {
				p := item.(map[string]interface{})
				if p["name"] == "audit-test-principal" {
					principalID = int64(p["id"].(float64))
					break
				}
			}
			require.NotZero(t, principalID)

			url := fmt.Sprintf("%s/v1/principals/%d", env.Server.URL, principalID)
			resp2 := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp2.StatusCode)
			_ = resp2.Body.Close()

			logs := fetchAuditLogs(t, env.Server.URL, env.Keys.Admin)
			found := false
			for _, entry := range logs {
				if action, ok := entry["action"].(string); ok && action == "DELETE_PRINCIPAL" {
					found = true
					break
				}
			}
			assert.True(t, found, "expected DELETE_PRINCIPAL audit entry")
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_AuditLogs_Filtering tests the audit log filtering parameters.
func TestHTTP_AuditLogs_Filtering(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	// Generate some audit entries by creating principals
	for i := 0; i < 3; i++ {
		body := map[string]interface{}{
			"name": fmt.Sprintf("audit-filter-user-%d", i),
			"type": "user",
		}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)
		_ = resp.Body.Close()
	}

	t.Run("filter_by_action", func(t *testing.T) {
		url := env.Server.URL + "/v1/audit-logs?action=CREATE_PRINCIPAL"
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.GreaterOrEqual(t, len(data), 3)
		for _, item := range data {
			entry := item.(map[string]interface{})
			assert.Equal(t, "CREATE_PRINCIPAL", entry["action"])
		}
	})

	t.Run("filter_by_principal_name", func(t *testing.T) {
		// The service logs the created principal's name as the audit principal_name
		// (not the authenticated caller). Filter by one of the created names.
		url := env.Server.URL + "/v1/audit-logs?principal_name=audit-filter-user-0"
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.GreaterOrEqual(t, len(data), 1)
		for _, item := range data {
			entry := item.(map[string]interface{})
			assert.Equal(t, "audit-filter-user-0", entry["principal_name"])
		}
	})

	t.Run("filter_by_status", func(t *testing.T) {
		url := env.Server.URL + "/v1/audit-logs?status=ALLOWED"
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.GreaterOrEqual(t, len(data), 1)
		for _, item := range data {
			entry := item.(map[string]interface{})
			assert.Equal(t, "ALLOWED", entry["status"])
		}
	})
}

// TestHTTP_AuditLogs_Pagination tests paginating through audit log entries.
func TestHTTP_AuditLogs_Pagination(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	// Create enough audit entries to require pagination
	for i := 0; i < 5; i++ {
		body := map[string]interface{}{
			"name": fmt.Sprintf("audit-page-user-%d", i),
			"type": "user",
		}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)
		_ = resp.Body.Close()
	}

	// Paginate with page_size=2
	var allEntries []map[string]interface{}
	pageToken := ""
	pages := 0
	for {
		url := env.Server.URL + "/v1/audit-logs?max_results=2"
		if pageToken != "" {
			url += "&page_token=" + pageToken
		}
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		for _, item := range data {
			allEntries = append(allEntries, item.(map[string]interface{}))
		}

		pages++
		if npt, ok := result["next_page_token"]; ok && npt != nil {
			pageToken = npt.(string)
		} else {
			break
		}

		// Safety limit
		if pages > 20 {
			t.Fatal("too many pages — possible infinite loop")
		}
	}

	assert.Greater(t, pages, 1, "expected pagination to require multiple pages")
	assert.GreaterOrEqual(t, len(allEntries), 5)
}
