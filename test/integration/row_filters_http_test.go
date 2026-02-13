//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	dbstore "duck-demo/internal/db/dbstore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_RowFilterCRUD tests the full row filter lifecycle through HTTP.
func TestHTTP_RowFilterCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	// We need a principal ID to bind to. Use the analyst (seeded).
	q := dbstore.New(env.MetaDB)
	analyst, err := q.GetPrincipalByName(context.Background(), "analyst1")
	require.NoError(t, err)

	var filterID string

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create", func(t *testing.T) {
			body := map[string]interface{}{
				"filter_sql":  `"Age" > 30`,
				"description": "age filter",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tables/1/row-filters", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			require.NotNil(t, result["id"])
			filterID = result["id"].(string)
			assert.Equal(t, `"Age" > 30`, result["filter_sql"])
			assert.Equal(t, "age filter", result["description"])
		}},
		{"list_for_table", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/tables/1/row-filters", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			// At least the seeded filter + the one we created
			assert.GreaterOrEqual(t, len(data), 2)
		}},
		{"bind_to_user", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/row-filters/%s/bindings", env.Server.URL, filterID)
			body := map[string]interface{}{
				"principal_id":   analyst.ID,
				"principal_type": "user",
			}
			resp := doRequest(t, "POST", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"unbind", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/row-filters/%s/bindings?principal_id=%s&principal_type=user",
				env.Server.URL, filterID, analyst.ID)
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"delete", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/row-filters/%s", env.Server.URL, filterID)
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"list_after_delete", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/tables/1/row-filters", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			// Only the seeded filter should remain
			found := false
			for _, item := range data {
				f := item.(map[string]interface{})
				if f["id"].(string) == filterID {
					found = true
				}
			}
			assert.False(t, found, "deleted filter should not appear in list")
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_RowFilterMultiple tests creating multiple row filters for a table.
func TestHTTP_RowFilterMultiple(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	// Create two filters
	for i, sql := range []string{`"Survived" = 1`, `"Sex" = 'female'`} {
		body := map[string]interface{}{
			"filter_sql":  sql,
			"description": fmt.Sprintf("filter %d", i),
		}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/tables/1/row-filters", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode, "creating filter %d", i)
		_ = resp.Body.Close()
	}

	// List all filters for the table
	resp := doRequest(t, "GET", env.Server.URL+"/v1/tables/1/row-filters", env.Keys.Admin, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	data := result["data"].([]interface{})
	// 1 seeded + 2 new = at least 3
	assert.GreaterOrEqual(t, len(data), 3)
}
