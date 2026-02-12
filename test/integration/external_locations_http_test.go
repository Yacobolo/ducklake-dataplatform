//go:build integration

package integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validLocationBody returns a valid create-external-location request body.
func validLocationBody(name, credName string) map[string]interface{} {
	return map[string]interface{}{
		"name":            name,
		"url":             fmt.Sprintf("s3://test-bucket/%s/", name),
		"credential_name": credName,
		"storage_type":    "S3",
	}
}

// createCredentialForLocation is a helper that creates a storage credential
// and returns its name. Fails the test on error.
func createCredentialForLocation(t *testing.T, serverURL, apiKey, name string) {
	t.Helper()
	body := validCredBody(name)
	resp := doRequest(t, "POST", serverURL+"/v1/storage-credentials", apiKey, body)
	require.Equal(t, 201, resp.StatusCode, "pre-create credential %s", name)
	_ = resp.Body.Close()
}

// TestHTTP_ExternalLocationCRUD tests the full CRUD lifecycle for external locations.
func TestHTTP_ExternalLocationCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true, CatalogAttached: true})

	credName := "loc-crud-cred"

	steps := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"setup_credential", func(t *testing.T) {
			createCredentialForLocation(t, env.Server.URL, env.Keys.Admin, credName)
		}},
		{"create", func(t *testing.T) {
			body := validLocationBody("test-loc", credName)
			body["comment"] = "integration test location"
			body["read_only"] = true

			resp := doRequest(t, "POST", env.Server.URL+"/v1/external-locations", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test-loc", result["name"])
			assert.Equal(t, fmt.Sprintf("s3://test-bucket/%s/", "test-loc"), result["url"])
			assert.Equal(t, credName, result["credential_name"])
			assert.Equal(t, "S3", result["storage_type"])
			assert.Equal(t, "integration test location", result["comment"])
			assert.Equal(t, true, result["read_only"])
			assert.NotNil(t, result["created_at"])
		}},
		{"get_by_name", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/external-locations/test-loc", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test-loc", result["name"])
			assert.Equal(t, credName, result["credential_name"])
			assert.Equal(t, "S3", result["storage_type"])
		}},
		{"list_returns_created", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/external-locations", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 1)

			names := make([]string, len(data))
			for i, item := range data {
				loc := item.(map[string]interface{})
				names[i] = loc["name"].(string)
			}
			assert.Contains(t, names, "test-loc")
		}},
		{"update_comment", func(t *testing.T) {
			body := map[string]interface{}{
				"comment": "updated location comment",
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/external-locations/test-loc", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "updated location comment", result["comment"])
		}},
		{"update_url", func(t *testing.T) {
			body := map[string]interface{}{
				"url": "s3://new-bucket/prefix/",
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/external-locations/test-loc", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "s3://new-bucket/prefix/", result["url"])
			// comment should be preserved
			assert.Equal(t, "updated location comment", result["comment"])
		}},
		{"update_read_only", func(t *testing.T) {
			body := map[string]interface{}{
				"read_only": false,
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/external-locations/test-loc", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, false, result["read_only"])
		}},
		{"delete", func(t *testing.T) {
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/external-locations/test-loc", env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"get_after_delete_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/external-locations/test-loc", env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}

	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_ExternalLocationAuthorization verifies RBAC enforcement.
func TestHTTP_ExternalLocationAuthorization(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true, CatalogAttached: true})

	// Pre-create a credential and location as admin for GET/update/delete tests.
	credName := "auth-loc-cred"
	createCredentialForLocation(t, env.Server.URL, env.Keys.Admin, credName)

	body := validLocationBody("auth-test-loc", credName)
	resp := doRequest(t, "POST", env.Server.URL+"/v1/external-locations", env.Keys.Admin, body)
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	// Need a second credential for the analyst create attempt
	credName2 := "auth-loc-cred2"
	createCredentialForLocation(t, env.Server.URL, env.Keys.Admin, credName2)

	tests := []struct {
		name       string
		method     string
		path       string
		apiKey     string
		body       interface{}
		wantStatus int
	}{
		// Write operations require ALL_PRIVILEGES
		{"admin_create_201", "POST", "/v1/external-locations", env.Keys.Admin, validLocationBody("admin-loc", credName2), 201},
		{"analyst_create_403", "POST", "/v1/external-locations", env.Keys.Analyst, validLocationBody("analyst-loc", credName), 403},
		{"noaccess_create_403", "POST", "/v1/external-locations", env.Keys.NoAccess, validLocationBody("noaccess-loc", credName), 403},
		{"analyst_update_403", "PATCH", "/v1/external-locations/auth-test-loc", env.Keys.Analyst, map[string]interface{}{"comment": "x"}, 403},
		{"analyst_delete_403", "DELETE", "/v1/external-locations/auth-test-loc", env.Keys.Analyst, nil, 403},

		// Read operations are open to all authenticated users
		{"analyst_list_200", "GET", "/v1/external-locations", env.Keys.Analyst, nil, 200},
		{"analyst_get_200", "GET", "/v1/external-locations/auth-test-loc", env.Keys.Analyst, nil, 200},
		{"noaccess_list_200", "GET", "/v1/external-locations", env.Keys.NoAccess, nil, 200},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, tc.method, env.Server.URL+tc.path, tc.apiKey, tc.body)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			_ = resp.Body.Close()
		})
	}
}

// TestHTTP_ExternalLocationValidation verifies validation error responses.
func TestHTTP_ExternalLocationValidation(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true, CatalogAttached: true})

	// Pre-create a credential for valid bodies
	credName := "val-loc-cred"
	createCredentialForLocation(t, env.Server.URL, env.Keys.Admin, credName)

	base := validLocationBody("valid-loc", credName)

	tests := []struct {
		name       string
		method     string
		path       string
		body       interface{}
		wantStatus int
	}{
		{"empty_name_400", "POST", "/v1/external-locations", withField(base, "name", ""), 400},
		{"name_too_long_400", "POST", "/v1/external-locations", withField(base, "name", strings.Repeat("x", 129)), 400},
		{"missing_url_400", "POST", "/v1/external-locations", withField(base, "url", ""), 400},
		{"missing_credential_name_400", "POST", "/v1/external-locations", withField(base, "credential_name", ""), 400},
		{"invalid_storage_type_400", "POST", "/v1/external-locations", withField(base, "storage_type", "AZURE"), 400},
		{"nonexistent_credential_400", "POST", "/v1/external-locations", validLocationBody("nc-loc", "nonexistent-cred"), 400},
		{"get_nonexistent_404", "GET", "/v1/external-locations/nonexistent", nil, 404},
		{"update_nonexistent_404", "PATCH", "/v1/external-locations/nonexistent", map[string]interface{}{"comment": "x"}, 404},
		{"delete_nonexistent_404", "DELETE", "/v1/external-locations/nonexistent", nil, 404},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, tc.method, env.Server.URL+tc.path, env.Keys.Admin, tc.body)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			_ = resp.Body.Close()
		})
	}
}

// TestHTTP_ExternalLocationDuplicate verifies duplicate name returns 409.
func TestHTTP_ExternalLocationDuplicate(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true, CatalogAttached: true})

	credName := "dup-loc-cred"
	createCredentialForLocation(t, env.Server.URL, env.Keys.Admin, credName)

	body := validLocationBody("dup-loc", credName)

	resp := doRequest(t, "POST", env.Server.URL+"/v1/external-locations", env.Keys.Admin, body)
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	resp = doRequest(t, "POST", env.Server.URL+"/v1/external-locations", env.Keys.Admin, body)
	assert.Equal(t, 409, resp.StatusCode)
	_ = resp.Body.Close()
}

// TestHTTP_ExternalLocationPagination verifies pagination works correctly.
func TestHTTP_ExternalLocationPagination(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true, CatalogAttached: true})

	// Create 5 credentials (one per location, since each location references a credential)
	locNames := []string{"alpha-loc", "bravo-loc", "charlie-loc", "delta-loc", "echo-loc"}
	for i, name := range locNames {
		credName := fmt.Sprintf("page-cred-%d", i)
		createCredentialForLocation(t, env.Server.URL, env.Keys.Admin, credName)

		body := validLocationBody(name, credName)
		resp := doRequest(t, "POST", env.Server.URL+"/v1/external-locations", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode, "create location %s", name)
		_ = resp.Body.Close()
	}

	var allNames []string
	pageToken := ""
	pages := 0

	for {
		url := env.Server.URL + "/v1/external-locations?max_results=2"
		if pageToken != "" {
			url += "&page_token=" + pageToken
		}
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)

		data := result["data"].([]interface{})
		for _, item := range data {
			loc := item.(map[string]interface{})
			allNames = append(allNames, loc["name"].(string))
		}
		pages++

		if npt, ok := result["next_page_token"]; ok && npt != nil {
			pageToken = npt.(string)
		} else {
			break
		}
	}

	assert.Len(t, allNames, 5, "should collect all 5 locations")
	assert.GreaterOrEqual(t, pages, 3, "should take at least 3 pages with max_results=2")
}
