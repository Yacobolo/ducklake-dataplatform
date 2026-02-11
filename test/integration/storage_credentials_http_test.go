//go:build integration

package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validCredBody returns a valid create-credential request body.
func validCredBody(name string) map[string]interface{} {
	return map[string]interface{}{
		"name":            name,
		"credential_type": "S3",
		"key_id":          "AKIAIOSFODNN7EXAMPLE",
		"secret":          "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"endpoint":        "s3.us-east-1.example.com",
		"region":          "us-east-1",
	}
}

// TestHTTP_StorageCredentialCRUD tests the full CRUD lifecycle for storage credentials.
func TestHTTP_StorageCredentialCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true})

	steps := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"create", func(t *testing.T) {
			body := validCredBody("test-cred")
			body["comment"] = "integration test credential"
			body["url_style"] = "path"

			resp := doRequest(t, "POST", env.Server.URL+"/v1/storage-credentials", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test-cred", result["name"])
			assert.Equal(t, "S3", result["credential_type"])
			assert.Equal(t, "integration test credential", result["comment"])
			assert.Equal(t, "s3.us-east-1.example.com", result["endpoint"])
			assert.Equal(t, "us-east-1", result["region"])
			assert.Equal(t, "path", result["url_style"])
			assert.NotNil(t, result["created_at"])
		}},
		{"create_omits_secrets", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/storage-credentials/test-cred", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var raw map[string]interface{}
			decodeJSON(t, resp, &raw)
			_, hasKeyID := raw["key_id"]
			_, hasSecret := raw["secret"]
			assert.False(t, hasKeyID, "response must not contain key_id")
			assert.False(t, hasSecret, "response must not contain secret")
		}},
		{"get_by_name", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/storage-credentials/test-cred", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test-cred", result["name"])
			assert.Equal(t, "S3", result["credential_type"])
			assert.Equal(t, "s3.us-east-1.example.com", result["endpoint"])
		}},
		{"list_returns_created", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/storage-credentials", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 1)

			names := make([]string, len(data))
			for i, item := range data {
				c := item.(map[string]interface{})
				names[i] = c["name"].(string)
			}
			assert.Contains(t, names, "test-cred")
		}},
		{"list_omits_secrets", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/storage-credentials", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			for _, item := range data {
				c := item.(map[string]interface{})
				_, hasKeyID := c["key_id"]
				_, hasSecret := c["secret"]
				assert.False(t, hasKeyID, "list items must not contain key_id")
				assert.False(t, hasSecret, "list items must not contain secret")
			}
		}},
		{"update_comment", func(t *testing.T) {
			body := map[string]interface{}{
				"comment": "updated comment",
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/storage-credentials/test-cred", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "updated comment", result["comment"])
		}},
		{"update_endpoint_and_region", func(t *testing.T) {
			body := map[string]interface{}{
				"endpoint": "s3.eu-west-1.example.com",
				"region":   "eu-west-1",
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/storage-credentials/test-cred", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "s3.eu-west-1.example.com", result["endpoint"])
			assert.Equal(t, "eu-west-1", result["region"])
		}},
		{"update_preserves_unset_fields", func(t *testing.T) {
			// Patch only comment, verify endpoint/region unchanged
			body := map[string]interface{}{
				"comment": "only comment changed",
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/storage-credentials/test-cred", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "only comment changed", result["comment"])
			assert.Equal(t, "s3.eu-west-1.example.com", result["endpoint"])
			assert.Equal(t, "eu-west-1", result["region"])
		}},
		{"delete", func(t *testing.T) {
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/storage-credentials/test-cred", env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			resp.Body.Close()
		}},
		{"get_after_delete_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/storage-credentials/test-cred", env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			resp.Body.Close()
		}},
	}

	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_StorageCredentialAuthorization verifies RBAC enforcement.
func TestHTTP_StorageCredentialAuthorization(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true})

	// Pre-create a credential as admin for GET/update/delete tests
	body := validCredBody("auth-test-cred")
	resp := doRequest(t, "POST", env.Server.URL+"/v1/storage-credentials", env.Keys.Admin, body)
	require.Equal(t, 201, resp.StatusCode)
	resp.Body.Close()

	createBody := validCredBody("new-cred")

	tests := []struct {
		name       string
		method     string
		path       string
		apiKey     string
		body       interface{}
		wantStatus int
	}{
		// Write operations require ALL_PRIVILEGES
		{"admin_create_201", "POST", "/v1/storage-credentials", env.Keys.Admin, validCredBody("admin-cred"), 201},
		{"analyst_create_403", "POST", "/v1/storage-credentials", env.Keys.Analyst, createBody, 403},
		{"noaccess_create_403", "POST", "/v1/storage-credentials", env.Keys.NoAccess, createBody, 403},
		{"analyst_update_403", "PATCH", "/v1/storage-credentials/auth-test-cred", env.Keys.Analyst, map[string]interface{}{"comment": "x"}, 403},
		{"analyst_delete_403", "DELETE", "/v1/storage-credentials/auth-test-cred", env.Keys.Analyst, nil, 403},

		// Read operations are open to all authenticated users
		{"analyst_list_200", "GET", "/v1/storage-credentials", env.Keys.Analyst, nil, 200},
		{"analyst_get_200", "GET", "/v1/storage-credentials/auth-test-cred", env.Keys.Analyst, nil, 200},
		{"noaccess_list_200", "GET", "/v1/storage-credentials", env.Keys.NoAccess, nil, 200},
		{"noaccess_get_200", "GET", "/v1/storage-credentials/auth-test-cred", env.Keys.NoAccess, nil, 200},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, tc.method, env.Server.URL+tc.path, tc.apiKey, tc.body)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			resp.Body.Close()
		})
	}
}

// TestHTTP_StorageCredentialValidation verifies validation error responses.
func TestHTTP_StorageCredentialValidation(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true})

	base := validCredBody("valid")

	tests := []struct {
		name       string
		method     string
		path       string
		body       interface{}
		wantStatus int
	}{
		{"empty_name_400", "POST", "/v1/storage-credentials", withField(base, "name", ""), 400},
		{"name_too_long_400", "POST", "/v1/storage-credentials", withField(base, "name", strings.Repeat("x", 129)), 400},
		{"invalid_credential_type_400", "POST", "/v1/storage-credentials", withField(base, "credential_type", "AZURE"), 400},
		{"missing_key_id_400", "POST", "/v1/storage-credentials", withField(base, "key_id", ""), 400},
		{"missing_secret_400", "POST", "/v1/storage-credentials", withField(base, "secret", ""), 400},
		{"missing_endpoint_400", "POST", "/v1/storage-credentials", withField(base, "endpoint", ""), 400},
		{"missing_region_400", "POST", "/v1/storage-credentials", withField(base, "region", ""), 400},
		{"get_nonexistent_404", "GET", "/v1/storage-credentials/nonexistent", nil, 404},
		{"update_nonexistent_404", "PATCH", "/v1/storage-credentials/nonexistent", map[string]interface{}{"comment": "x"}, 404},
		{"delete_nonexistent_404", "DELETE", "/v1/storage-credentials/nonexistent", nil, 404},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRequest(t, tc.method, env.Server.URL+tc.path, env.Keys.Admin, tc.body)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			resp.Body.Close()
		})
	}
}

// TestHTTP_StorageCredentialDuplicate verifies duplicate name returns 409.
func TestHTTP_StorageCredentialDuplicate(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true})

	body := validCredBody("dup-cred")

	resp := doRequest(t, "POST", env.Server.URL+"/v1/storage-credentials", env.Keys.Admin, body)
	require.Equal(t, 201, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, "POST", env.Server.URL+"/v1/storage-credentials", env.Keys.Admin, body)
	assert.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()
}

// TestHTTP_StorageCredentialPagination verifies pagination works correctly.
func TestHTTP_StorageCredentialPagination(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithStorageCredentials: true})

	names := []string{"alpha-cred", "bravo-cred", "charlie-cred", "delta-cred", "echo-cred"}
	for _, name := range names {
		resp := doRequest(t, "POST", env.Server.URL+"/v1/storage-credentials", env.Keys.Admin, validCredBody(name))
		require.Equal(t, 201, resp.StatusCode)
		resp.Body.Close()
	}

	var allNames []string
	pageToken := ""
	pages := 0

	for {
		url := env.Server.URL + "/v1/storage-credentials?max_results=2"
		if pageToken != "" {
			url += "&page_token=" + pageToken
		}
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)

		data := result["data"].([]interface{})
		for _, item := range data {
			c := item.(map[string]interface{})
			allNames = append(allNames, c["name"].(string))
		}
		pages++

		if npt, ok := result["next_page_token"]; ok && npt != nil {
			pageToken = npt.(string)
		} else {
			break
		}
	}

	assert.Equal(t, 5, len(allNames), "should collect all 5 credentials")
	assert.GreaterOrEqual(t, pages, 3, "should take at least 3 pages with max_results=2")
}

// withField returns a copy of the base map with one field overridden.
func withField(base map[string]interface{}, key string, value interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range base {
		out[k] = v
	}
	out[key] = value
	return out
}
