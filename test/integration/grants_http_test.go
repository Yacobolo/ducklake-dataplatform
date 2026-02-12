//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_GrantRevoke tests the full grant lifecycle through HTTP.
func TestHTTP_GrantRevoke(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	// Create a fresh principal and group to grant to
	pResp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin,
		map[string]interface{}{"name": "grant-test-user", "type": "user"})
	require.Equal(t, 201, pResp.StatusCode)
	var pResult map[string]interface{}
	decodeJSON(t, pResp, &pResult)
	userID := int64(pResult["id"].(float64))

	gResp := doRequest(t, "POST", env.Server.URL+"/v1/groups", env.Keys.Admin,
		map[string]interface{}{"name": "grant-test-group"})
	require.Equal(t, 201, gResp.StatusCode)
	var gResult map[string]interface{}
	decodeJSON(t, gResp, &gResult)
	groupID := int64(gResult["id"].(float64))

	var grantID float64

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"grant_to_user", func(t *testing.T) {
			body := map[string]interface{}{
				"principal_id":   userID,
				"principal_type": "user",
				"securable_type": "table",
				"securable_id":   1, // titanic table
				"privilege":      "SELECT",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/grants", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			require.NotNil(t, result["id"])
			grantID = result["id"].(float64)
			assert.Equal(t, "SELECT", result["privilege"])
		}},
		{"grant_to_group", func(t *testing.T) {
			body := map[string]interface{}{
				"principal_id":   groupID,
				"principal_type": "group",
				"securable_type": "schema",
				"securable_id":   0, // main schema
				"privilege":      "USAGE",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/grants", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"list_by_principal", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/grants?principal_id=%d&principal_type=user",
				env.Server.URL, userID)
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 1)
		}},
		{"list_by_securable", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/grants?securable_type=table&securable_id=1",
				env.Server.URL)
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			// At least the user grant + the seeded analyst/researcher grants
			assert.GreaterOrEqual(t, len(data), 1)
		}},
		{"revoke", func(t *testing.T) {
			body := map[string]interface{}{
				"principal_id":   userID,
				"principal_type": "user",
				"securable_type": "table",
				"securable_id":   1,
				"privilege":      "SELECT",
			}
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/grants", env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"list_after_revoke", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/grants?principal_id=%d&principal_type=user",
				env.Server.URL, userID)
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			// User should have 0 grants left
			assert.Empty(t, data)
		}},
	}
	_ = grantID // used indirectly via the grant_to_user step
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_GrantAllPrivileges tests granting ALL_PRIVILEGES on catalog.
func TestHTTP_GrantAllPrivileges(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	// Create a principal
	pResp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin,
		map[string]interface{}{"name": "allpriv-user", "type": "user"})
	require.Equal(t, 201, pResp.StatusCode)
	var pResult map[string]interface{}
	decodeJSON(t, pResp, &pResult)
	userID := int64(pResult["id"].(float64))

	// Grant ALL_PRIVILEGES on catalog (securable_id=0)
	body := map[string]interface{}{
		"principal_id":   userID,
		"principal_type": "user",
		"securable_type": "catalog",
		"securable_id":   0,
		"privilege":      "ALL_PRIVILEGES",
	}
	resp := doRequest(t, "POST", env.Server.URL+"/v1/grants", env.Keys.Admin, body)
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	// Verify the grant appears in list
	url := fmt.Sprintf("%s/v1/grants?principal_id=%d&principal_type=user", env.Server.URL, userID)
	resp2 := doRequest(t, "GET", url, env.Keys.Admin, nil)
	require.Equal(t, 200, resp2.StatusCode)
	var result map[string]interface{}
	decodeJSON(t, resp2, &result)
	data := result["data"].([]interface{})
	require.Len(t, data, 1)
	grant := data[0].(map[string]interface{})
	assert.Equal(t, "ALL_PRIVILEGES", grant["privilege"])
	assert.Equal(t, "catalog", grant["securable_type"])
}
