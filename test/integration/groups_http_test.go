//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_GroupCRUD tests the full group lifecycle through HTTP.
func TestHTTP_GroupCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	var groupID float64

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create", func(t *testing.T) {
			body := map[string]interface{}{
				"name":        "test-group-crud",
				"description": "a test group",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/groups", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			require.NotNil(t, result["id"])
			groupID = result["id"].(float64)
			assert.Equal(t, "test-group-crud", result["name"])
			assert.Equal(t, "a test group", result["description"])
		}},
		{"get_by_id", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d", env.Server.URL, int64(groupID))
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test-group-crud", result["name"])
		}},
		{"list", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/groups", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			// 3 seeded groups + 1 created
			assert.GreaterOrEqual(t, len(data), 4)
		}},
		{"delete", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d", env.Server.URL, int64(groupID))
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			resp.Body.Close()
		}},
		{"get_after_delete_404", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d", env.Server.URL, int64(groupID))
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			resp.Body.Close()
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_GroupMembership tests add/remove/list group members through HTTP.
func TestHTTP_GroupMembership(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	// Create a group
	groupBody := map[string]interface{}{"name": "membership-test-group"}
	resp := doRequest(t, "POST", env.Server.URL+"/v1/groups", env.Keys.Admin, groupBody)
	require.Equal(t, 201, resp.StatusCode)
	var groupResult map[string]interface{}
	decodeJSON(t, resp, &groupResult)
	groupID := int64(groupResult["id"].(float64))

	// Create two principals to add as members
	var memberIDs [2]int64
	for i := 0; i < 2; i++ {
		body := map[string]interface{}{"name": fmt.Sprintf("member-%d", i), "type": "user"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)
		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		memberIDs[i] = int64(result["id"].(float64))
	}

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"add_first_member", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
			body := map[string]interface{}{
				"member_type": "user",
				"member_id":   memberIDs[0],
			}
			resp := doRequest(t, "POST", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			resp.Body.Close()
		}},
		{"add_second_member", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
			body := map[string]interface{}{
				"member_type": "user",
				"member_id":   memberIDs[1],
			}
			resp := doRequest(t, "POST", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			resp.Body.Close()
		}},
		{"list_members_both", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Len(t, data, 2)
		}},
		{"remove_member", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
			body := map[string]interface{}{
				"member_type": "user",
				"member_id":   memberIDs[0],
			}
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			resp.Body.Close()
		}},
		{"list_members_one", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Len(t, data, 1)
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_GroupErrors tests error cases for group endpoints.
func TestHTTP_GroupErrors(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	t.Run("get_nonexistent_404", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/groups/99999", env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		resp.Body.Close()
	})
}
