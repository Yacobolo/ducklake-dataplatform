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

// TestHTTP_ColumnMaskCRUD tests the full column mask lifecycle through HTTP.
func TestHTTP_ColumnMaskCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	q := dbstore.New(env.MetaDB)
	analyst, err := q.GetPrincipalByName(context.Background(), "analyst1")
	require.NoError(t, err)
	researcher, err := q.GetPrincipalByName(context.Background(), "researcher1")
	require.NoError(t, err)

	var maskID float64

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create", func(t *testing.T) {
			body := map[string]interface{}{
				"column_name":     "Ticket",
				"mask_expression": "'REDACTED'",
				"description":     "ticket mask",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tables/1/column-masks", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			require.NotNil(t, result["id"])
			maskID = result["id"].(float64)
			assert.Equal(t, "Ticket", result["column_name"])
			assert.Equal(t, "'REDACTED'", result["mask_expression"])
		}},
		{"list_for_table", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/tables/1/column-masks", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			// At least the seeded Name mask + our new Ticket mask
			assert.GreaterOrEqual(t, len(data), 2)
		}},
		{"bind_see_original_false", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/column-masks/%d/bindings", env.Server.URL, int64(maskID))
			body := map[string]interface{}{
				"principal_id":   analyst.ID,
				"principal_type": "user",
				"see_original":   false,
			}
			resp := doRequest(t, "POST", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"bind_see_original_true", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/column-masks/%d/bindings", env.Server.URL, int64(maskID))
			body := map[string]interface{}{
				"principal_id":   researcher.ID,
				"principal_type": "user",
				"see_original":   true,
			}
			resp := doRequest(t, "POST", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"unbind_analyst", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/column-masks/%d/bindings?principal_id=%d&principal_type=user",
				env.Server.URL, int64(maskID), analyst.ID)
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"unbind_researcher", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/column-masks/%d/bindings?principal_id=%d&principal_type=user",
				env.Server.URL, int64(maskID), researcher.ID)
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"delete", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/column-masks/%d", env.Server.URL, int64(maskID))
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"list_after_delete", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/tables/1/column-masks", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			// Verify our mask is gone
			for _, item := range data {
				m := item.(map[string]interface{})
				assert.NotEqual(t, maskID, m["id"].(float64), "deleted mask should not appear")
			}
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}
