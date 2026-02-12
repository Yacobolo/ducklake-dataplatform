//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_TagCRUD exercises the full tag lifecycle: create, list, assign,
// unassign, delete, and verifies audit log entries are produced.
func TestHTTP_TagCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	var tagID int64
	var tagNoValueID int64
	var assignmentID int64
	var columnAssignmentID int64

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_tag_with_value", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tags", env.Keys.Admin, map[string]interface{}{
				"key":   "env",
				"value": "production",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "env", result["key"])
			assert.Equal(t, "production", result["value"])
			assert.NotEmpty(t, result["created_by"])
			assert.NotNil(t, result["id"])
			tagID = int64(result["id"].(float64))
		}},

		{"create_tag_no_value", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tags", env.Keys.Admin, map[string]interface{}{
				"key": "pii",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "pii", result["key"])
			assert.Nil(t, result["value"])
			tagNoValueID = int64(result["id"].(float64))
			_ = tagNoValueID // used later if needed
		}},

		{"create_tag_duplicate_409", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tags", env.Keys.Admin, map[string]interface{}{
				"key":   "env",
				"value": "production",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 409, resp.StatusCode)
		}},

		{"list_tags", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/tags", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 2)
		}},

		{"assign_tag_to_table", func(t *testing.T) {
			resp := doRequest(t, "POST",
				fmt.Sprintf("%s/v1/tags/%d/assignments", env.Server.URL, tagID),
				env.Keys.Admin, map[string]interface{}{
					"securable_type": "table",
					"securable_id":   1,
				})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.InDelta(t, float64(tagID), result["tag_id"], 0)
			assert.Equal(t, "table", result["securable_type"])
			assert.InDelta(t, float64(1), result["securable_id"], 0)
			assignmentID = int64(result["id"].(float64))
		}},

		{"assign_tag_to_column", func(t *testing.T) {
			resp := doRequest(t, "POST",
				fmt.Sprintf("%s/v1/tags/%d/assignments", env.Server.URL, tagID),
				env.Keys.Admin, map[string]interface{}{
					"securable_type": "table",
					"securable_id":   1,
					"column_name":    "Name",
				})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "Name", result["column_name"])
			columnAssignmentID = int64(result["id"].(float64))
			_ = columnAssignmentID
		}},

		{"assign_tag_to_column_duplicate_409", func(t *testing.T) {
			// Duplicate the column-level assignment (column_name="Name") — should conflict
			resp := doRequest(t, "POST",
				fmt.Sprintf("%s/v1/tags/%d/assignments", env.Server.URL, tagID),
				env.Keys.Admin, map[string]interface{}{
					"securable_type": "table",
					"securable_id":   1,
					"column_name":    "Name",
				})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 409, resp.StatusCode)
		}},

		{"unassign_tag", func(t *testing.T) {
			resp := doRequest(t, "DELETE",
				fmt.Sprintf("%s/v1/tag-assignments/%d", env.Server.URL, assignmentID),
				env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"delete_tag", func(t *testing.T) {
			resp := doRequest(t, "DELETE",
				fmt.Sprintf("%s/v1/tags/%d", env.Server.URL, tagID),
				env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"delete_tag_idempotent", func(t *testing.T) {
			// Deleting an already-deleted tag returns 204 (idempotent — the SQL
			// DELETE succeeds with 0 rows affected and no error).
			resp := doRequest(t, "DELETE",
				fmt.Sprintf("%s/v1/tags/%d", env.Server.URL, tagID),
				env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"verify_audit_logs", func(t *testing.T) {
			logs := fetchAuditLogs(t, env.Server.URL, env.Keys.Admin)

			expectedActions := map[string]bool{
				"CREATE_TAG":   false,
				"ASSIGN_TAG":   false,
				"UNASSIGN_TAG": false,
				"DELETE_TAG":   false,
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
			t.FailNow() // stop on first failure — steps depend on each other
		}
	}
}

// TestHTTP_ListClassifications tests GET /v1/classifications.
func TestHTTP_ListClassifications(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	t.Run("list_returns_seeded_classifications", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/classifications", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.GreaterOrEqual(t, len(data), 8,
			"expected at least 8 seeded classification/sensitivity tags")

		// Verify at least one classification and one sensitivity tag exist
		hasClassification := false
		hasSensitivity := false
		for _, item := range data {
			tag := item.(map[string]interface{})
			key := tag["key"].(string)
			if key == "classification" {
				hasClassification = true
			}
			if key == "sensitivity" {
				hasSensitivity = true
			}
		}
		assert.True(t, hasClassification, "expected at least one 'classification' tag")
		assert.True(t, hasSensitivity, "expected at least one 'sensitivity' tag")
	})

	t.Run("any_user_can_list", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/classifications", env.Keys.Analyst, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.GreaterOrEqual(t, len(data), 1)
	})
}

// TestHTTP_TagAnyUserCanManage verifies that non-admin users can create and
// delete tags (no privilege checks enforced).
func TestHTTP_TagAnyUserCanManage(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	var tagID int64

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"analyst_creates_tag", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tags", env.Keys.Analyst, map[string]interface{}{
				"key":   "department",
				"value": "engineering",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			tagID = int64(result["id"].(float64))
		}},

		{"analyst_deletes_tag", func(t *testing.T) {
			resp := doRequest(t, "DELETE",
				fmt.Sprintf("%s/v1/tags/%d", env.Server.URL, tagID),
				env.Keys.Analyst, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 204, resp.StatusCode)
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}
