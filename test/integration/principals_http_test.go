//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_PrincipalCRUD tests the full principal lifecycle through HTTP.
func TestHTTP_PrincipalCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	var createdID string

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create_user", func(t *testing.T) {
			body := map[string]interface{}{"name": "test-user-crud", "type": "user"}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			require.NotNil(t, result["id"])
			createdID = result["id"].(string)
			assert.Equal(t, "test-user-crud", result["name"])
			assert.Equal(t, "user", result["type"])
			assert.Equal(t, false, result["is_admin"])
		}},
		{"create_service_principal", func(t *testing.T) {
			body := map[string]interface{}{"name": "test-svc-crud", "type": "service_principal"}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "service_principal", result["type"])
		}},
		{"get_by_id", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/principals/%s", env.Server.URL, createdID)
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test-user-crud", result["name"])
		}},
		{"list_returns_created", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			// At least the 4 seeded principals + 2 we just created
			assert.GreaterOrEqual(t, len(data), 6)
		}},
		{"set_admin_true", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/principals/%s/admin", env.Server.URL, createdID)
			body := map[string]interface{}{"is_admin": true}
			resp := doRequest(t, "PUT", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()

			// Verify by fetching the principal
			getURL := fmt.Sprintf("%s/v1/principals/%s", env.Server.URL, createdID)
			resp2 := doRequest(t, "GET", getURL, env.Keys.Admin, nil)
			require.Equal(t, 200, resp2.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp2, &result)
			assert.Equal(t, true, result["is_admin"])
		}},
		{"set_admin_false", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/principals/%s/admin", env.Server.URL, createdID)
			body := map[string]interface{}{"is_admin": false}
			resp := doRequest(t, "PUT", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"delete", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/principals/%s", env.Server.URL, createdID)
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"get_after_delete_404", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/principals/%s", env.Server.URL, createdID)
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_PrincipalErrors tests error cases for principal endpoints.
func TestHTTP_PrincipalErrors(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	tests := []struct {
		name       string
		body       map[string]interface{}
		wantStatus int
	}{
		{"create_empty_name_400", map[string]interface{}{"name": "", "type": "user"}, 400},
		{"create_duplicate_name_409", map[string]interface{}{"name": "admin_user", "type": "user"}, 409},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, tc.body)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			_ = resp.Body.Close()
		})
	}

	t.Run("get_nonexistent_404", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/principals/99999", env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("delete_nonexistent_404", func(t *testing.T) {
		resp := doRequest(t, "DELETE", env.Server.URL+"/v1/principals/99999", env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		_ = resp.Body.Close()
	})
}

// TestHTTP_PrincipalPagination tests pagination of principals.
func TestHTTP_PrincipalPagination(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	// Create 5 additional principals
	for i := 0; i < 5; i++ {
		body := map[string]interface{}{"name": fmt.Sprintf("page-user-%d", i), "type": "user"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)
		_ = resp.Body.Close()
	}

	// Paginate with page_size=3
	var allNames []string
	pageToken := ""
	pages := 0
	for {
		url := env.Server.URL + "/v1/principals?max_results=3"
		if pageToken != "" {
			url += "&page_token=" + pageToken
		}
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		for _, item := range data {
			p := item.(map[string]interface{})
			allNames = append(allNames, p["name"].(string))
		}

		pages++
		if npt, ok := result["next_page_token"]; ok && npt != nil {
			pageToken = npt.(string)
		} else {
			break
		}
	}

	// Should have required multiple pages
	assert.Greater(t, pages, 1, "expected pagination to require multiple pages")
	// 4 seeded + 5 created = 9 total
	assert.GreaterOrEqual(t, len(allNames), 9)
}
