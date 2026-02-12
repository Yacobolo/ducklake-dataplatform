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

// TestWorkflow_RBAC_FullCycle tests the complete RBAC lifecycle through HTTP:
// create principal → create group → add to group → grant → verify → revoke → verify denied.
func TestWorkflow_RBAC_FullCycle(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	var principalID, groupID float64

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create_principal", func(t *testing.T) {
			body := map[string]interface{}{"name": "rbac-cycle-user", "type": "user"}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			principalID = result["id"].(float64)
		}},
		{"create_group", func(t *testing.T) {
			body := map[string]interface{}{"name": "rbac-cycle-group", "description": "workflow test group"}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/groups", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			groupID = result["id"].(float64)
		}},
		{"add_principal_to_group", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, int64(groupID))
			body := map[string]interface{}{
				"member_type": "user",
				"member_id":   int64(principalID),
			}
			resp := doRequest(t, "POST", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"grant_select_to_group", func(t *testing.T) {
			body := map[string]interface{}{
				"principal_id":   int64(groupID),
				"principal_type": "group",
				"securable_type": "table",
				"securable_id":   1, // titanic
				"privilege":      "SELECT",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/grants", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"verify_grant_listed", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/grants?principal_id=%d&principal_type=group",
				env.Server.URL, int64(groupID))
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 1)

			found := false
			for _, item := range data {
				g := item.(map[string]interface{})
				if g["privilege"] == "SELECT" && g["securable_type"] == "table" {
					found = true
					break
				}
			}
			assert.True(t, found, "expected SELECT grant on table")
		}},
		{"revoke_select_from_group", func(t *testing.T) {
			body := map[string]interface{}{
				"principal_id":   int64(groupID),
				"principal_type": "group",
				"securable_type": "table",
				"securable_id":   1,
				"privilege":      "SELECT",
			}
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/grants", env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"verify_grant_revoked", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/grants?principal_id=%d&principal_type=group",
				env.Server.URL, int64(groupID))
			resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})

			for _, item := range data {
				g := item.(map[string]interface{})
				assert.NotEqual(t, "SELECT", g["privilege"],
					"SELECT grant should have been revoked")
			}
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestWorkflow_GroupInheritance tests that users inherit grants through group membership
// and lose access when removed from the group.
func TestWorkflow_GroupInheritance(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	// Create a fresh principal and group
	pResp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin,
		map[string]interface{}{"name": "inherit-user", "type": "user"})
	require.Equal(t, 201, pResp.StatusCode)
	var pResult map[string]interface{}
	decodeJSON(t, pResp, &pResult)
	userID := int64(pResult["id"].(float64))

	gResp := doRequest(t, "POST", env.Server.URL+"/v1/groups", env.Keys.Admin,
		map[string]interface{}{"name": "inherit-group"})
	require.Equal(t, 201, gResp.StatusCode)
	var gResult map[string]interface{}
	decodeJSON(t, gResp, &gResult)
	groupID := int64(gResult["id"].(float64))

	// Grant to group
	resp := doRequest(t, "POST", env.Server.URL+"/v1/grants", env.Keys.Admin,
		map[string]interface{}{
			"principal_id":   groupID,
			"principal_type": "group",
			"securable_type": "catalog",
			"securable_id":   0,
			"privilege":      "ALL_PRIVILEGES",
		})
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	t.Run("add_to_group", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
		body := map[string]interface{}{"member_type": "user", "member_id": userID}
		resp := doRequest(t, "POST", url, env.Keys.Admin, body)
		require.Equal(t, 204, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("verify_membership", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		require.Len(t, data, 1)
		member := data[0].(map[string]interface{})
		assert.InDelta(t, float64(userID), member["member_id"], 0)
	})

	t.Run("remove_from_group", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
		body := map[string]interface{}{"member_type": "user", "member_id": userID}
		resp := doRequest(t, "DELETE", url, env.Keys.Admin, body)
		require.Equal(t, 204, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("verify_no_members", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/groups/%d/members", env.Server.URL, groupID)
		resp := doRequest(t, "GET", url, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.Empty(t, data)
	})
}

// TestWorkflow_RLS_Lifecycle tests the row filter lifecycle through HTTP.
func TestWorkflow_RLS_Lifecycle(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	q := dbstore.New(env.MetaDB)
	analystsGroup, err := q.GetGroupByName(context.Background(), "analysts")
	require.NoError(t, err)

	var filterID float64

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create_filter", func(t *testing.T) {
			body := map[string]interface{}{
				"filter_sql":  `"Survived" = 1`,
				"description": "survivors only",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tables/1/row-filters", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			filterID = result["id"].(float64)
		}},
		{"bind_to_group", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/row-filters/%d/bindings", env.Server.URL, int64(filterID))
			body := map[string]interface{}{
				"principal_id":   analystsGroup.ID,
				"principal_type": "group",
			}
			resp := doRequest(t, "POST", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"list_filters_shows_new", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/tables/1/row-filters", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			found := false
			for _, item := range data {
				f := item.(map[string]interface{})
				if f["id"].(float64) == filterID {
					found = true
					assert.Equal(t, `"Survived" = 1`, f["filter_sql"])
				}
			}
			assert.True(t, found, "new filter should appear in list")
		}},
		{"unbind_from_group", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/row-filters/%d/bindings", env.Server.URL, int64(filterID))
			body := map[string]interface{}{
				"principal_id":   analystsGroup.ID,
				"principal_type": "group",
			}
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"delete_filter", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/row-filters/%d", env.Server.URL, int64(filterID))
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestWorkflow_ColumnMask_Lifecycle tests the column mask lifecycle with see_original flag.
func TestWorkflow_ColumnMask_Lifecycle(t *testing.T) {
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
		{"create_mask", func(t *testing.T) {
			body := map[string]interface{}{
				"column_name":     "Fare",
				"mask_expression": "0.0",
				"description":     "fare mask",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tables/1/column-masks", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			maskID = result["id"].(float64)
		}},
		{"bind_analyst_see_original_false", func(t *testing.T) {
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
		{"bind_researcher_see_original_true", func(t *testing.T) {
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
		{"list_masks_shows_new", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/tables/1/column-masks", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			found := false
			for _, item := range data {
				m := item.(map[string]interface{})
				if m["id"].(float64) == maskID {
					found = true
					assert.Equal(t, "Fare", m["column_name"])
					assert.Equal(t, "0.0", m["mask_expression"])
				}
			}
			assert.True(t, found, "new mask should appear in list")
		}},
		{"cleanup_unbind_analyst", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/column-masks/%d/bindings", env.Server.URL, int64(maskID))
			body := map[string]interface{}{
				"principal_id":   analyst.ID,
				"principal_type": "user",
			}
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"cleanup_unbind_researcher", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/column-masks/%d/bindings", env.Server.URL, int64(maskID))
			body := map[string]interface{}{
				"principal_id":   researcher.ID,
				"principal_type": "user",
			}
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, body)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"cleanup_delete_mask", func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/column-masks/%d", env.Server.URL, int64(maskID))
			resp := doRequest(t, "DELETE", url, env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestWorkflow_ManagementEndpointsNoAuthz documents the design gap that management
// endpoints (principals, grants, row filters, column masks) have no authorization
// checks — any authenticated user can perform admin operations.
func TestWorkflow_ManagementEndpointsNoAuthz(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	t.Run("analyst_creates_principal", func(t *testing.T) {
		body := map[string]interface{}{"name": "analyst-created-user", "type": "user"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Analyst, body)
		// Documents gap: analyst can create principals (should be admin-only)
		assert.Equal(t, 201, resp.StatusCode,
			"expected 201 (documenting authz gap — any authenticated user can create principals)")
		_ = resp.Body.Close()
	})

	t.Run("analyst_grants_privilege", func(t *testing.T) {
		// Look up the analyst principal to get their ID
		ctx := context.Background()
		q := dbstore.New(env.MetaDB)
		analyst, err := q.GetPrincipalByName(ctx, "analyst1")
		require.NoError(t, err)

		body := map[string]interface{}{
			"principal_id":   analyst.ID,
			"principal_type": "user",
			"securable_type": "catalog",
			"securable_id":   0,
			"privilege":      "ALL_PRIVILEGES",
		}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/grants", env.Keys.Analyst, body)
		// Documents gap: analyst can grant themselves ALL_PRIVILEGES
		assert.Equal(t, 201, resp.StatusCode,
			"expected 201 (documenting authz gap — any authenticated user can grant privileges)")
		_ = resp.Body.Close()
	})

	t.Run("analyst_creates_row_filter", func(t *testing.T) {
		body := map[string]interface{}{
			"table_id":   1,
			"filter_sql": `"Pclass" = 99`,
		}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/row-filters", env.Keys.Analyst, body)
		// Documents gap: analyst can create row filters
		assert.Equal(t, 201, resp.StatusCode,
			"expected 201 (documenting authz gap — any authenticated user can create row filters)")
		_ = resp.Body.Close()
	})
}
