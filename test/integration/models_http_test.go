//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Model CRUD
// ---------------------------------------------------------------------------

// TestHTTP_ModelCRUD exercises the full model lifecycle via the HTTP API:
// create → get → list → update → delete, with error cases interspersed.
func TestHTTP_ModelCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_model", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name":    "analytics",
				"name":            "stg_orders",
				"sql":             "SELECT 1 AS id, 'test' AS name",
				"materialization": "VIEW",
				"description":     "Staging orders model",
				"tags":            []string{"staging", "orders"},
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "analytics", result["project_name"])
			assert.Equal(t, "stg_orders", result["name"])
			assert.Equal(t, "SELECT 1 AS id, 'test' AS name", result["sql"])
			assert.Equal(t, "VIEW", result["materialization"])
			assert.Equal(t, "Staging orders model", result["description"])
			assert.NotEmpty(t, result["id"])
			assert.NotNil(t, result["created_at"])
		}},

		{"create_model_duplicate_409", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name": "analytics",
				"name":         "stg_orders",
				"sql":          "SELECT 1",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 409, resp.StatusCode)
		}},

		{"create_model_missing_name_400", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name": "analytics",
				"sql":          "SELECT 1",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 400, resp.StatusCode)
		}},

		{"create_model_missing_sql_400", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name": "analytics",
				"name":         "empty_sql_model",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 400, resp.StatusCode)
		}},

		{"get_model", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models/analytics/stg_orders", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "analytics", result["project_name"])
			assert.Equal(t, "stg_orders", result["name"])
			assert.Equal(t, "VIEW", result["materialization"])
		}},

		{"get_model_not_found_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models/analytics/nonexistent", env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},

		{"list_models", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 1)
		}},

		{"list_models_by_project", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models?project_name=analytics", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 1)
			// All returned models should be in the analytics project
			for _, item := range data {
				m := item.(map[string]interface{})
				assert.Equal(t, "analytics", m["project_name"])
			}
		}},

		{"update_model", func(t *testing.T) {
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/models/analytics/stg_orders", env.Keys.Admin, map[string]interface{}{
				"sql":         "SELECT 1 AS id, 'updated' AS name",
				"description": "Updated staging orders",
			})
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "SELECT 1 AS id, 'updated' AS name", result["sql"])
			assert.Equal(t, "Updated staging orders", result["description"])
		}},

		{"update_model_not_found_404", func(t *testing.T) {
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/models/analytics/nonexistent", env.Keys.Admin, map[string]interface{}{
				"description": "nope",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},

		{"delete_model", func(t *testing.T) {
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/models/analytics/stg_orders", env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"delete_model_not_found_404", func(t *testing.T) {
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/models/analytics/stg_orders", env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},

		{"get_deleted_model_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models/analytics/stg_orders", env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// ---------------------------------------------------------------------------
// Model Dependencies + DAG
// ---------------------------------------------------------------------------

// TestHTTP_ModelDependenciesAndDAG verifies that auto-dependency extraction
// populates depends_on and that the DAG endpoint returns correct tiers.
func TestHTTP_ModelDependenciesAndDAG(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_base_model", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name":    "warehouse",
				"name":            "raw_events",
				"sql":             "SELECT 1 AS event_id, 'click' AS event_type",
				"materialization": "TABLE",
			})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"create_dependent_model", func(t *testing.T) {
			// This model references warehouse.raw_events — dependency should be auto-extracted.
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name":    "warehouse",
				"name":            "stg_events",
				"sql":             "SELECT event_id, event_type FROM warehouse.raw_events WHERE event_type = 'click'",
				"materialization": "VIEW",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			// Verify depends_on was auto-populated
			deps, ok := result["depends_on"].([]interface{})
			if assert.True(t, ok, "depends_on should be present") {
				assert.Contains(t, deps, "warehouse.raw_events")
			}
		}},

		{"create_downstream_model", func(t *testing.T) {
			// References stg_events (same project — resolved as warehouse.stg_events)
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name":    "warehouse",
				"name":            "mart_events",
				"sql":             "SELECT event_id FROM warehouse.stg_events",
				"materialization": "TABLE",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			deps, ok := result["depends_on"].([]interface{})
			if assert.True(t, ok, "depends_on should be present") {
				assert.Contains(t, deps, "warehouse.stg_events")
			}
		}},

		{"get_dag", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models/dag", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			tiers, ok := result["tiers"].([]interface{})
			require.True(t, ok, "DAG should have tiers")
			// With 3 models in a chain: raw_events → stg_events → mart_events
			// We expect at least 2 tiers (tier 0 = root, tier 1+ = downstream)
			assert.GreaterOrEqual(t, len(tiers), 2, "DAG should have at least 2 tiers")

			// Verify tier 0 contains raw_events (the root)
			tier0 := tiers[0].(map[string]interface{})
			nodes0 := tier0["nodes"].([]interface{})
			foundRoot := false
			for _, n := range nodes0 {
				node := n.(map[string]interface{})
				if node["model_name"] == "raw_events" {
					foundRoot = true
				}
			}
			assert.True(t, foundRoot, "tier 0 should contain raw_events")
		}},

		{"get_dag_filtered_by_project", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models/dag?project_name=warehouse", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			tiers, ok := result["tiers"].([]interface{})
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(tiers), 1)
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// ---------------------------------------------------------------------------
// Model Materialization Variants
// ---------------------------------------------------------------------------

// TestHTTP_ModelMaterializations verifies that different materialization types
// can be created and have correct default behavior.
func TestHTTP_ModelMaterializations(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	tests := []struct {
		name string
		mat  string
	}{
		{"view", "VIEW"},
		{"table", "TABLE"},
		{"incremental", "INCREMENTAL"},
		{"ephemeral", "EPHEMERAL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]interface{}{
				"project_name":    "mattest",
				"name":            "model_" + tc.name,
				"sql":             "SELECT 1 AS id",
				"materialization": tc.mat,
			}
			if tc.mat == "INCREMENTAL" {
				body["config"] = map[string]interface{}{
					"unique_key":           []string{"id"},
					"incremental_strategy": "merge",
				}
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, tc.mat, result["materialization"])
		})
	}
}

// ---------------------------------------------------------------------------
// Model Tests CRUD
// ---------------------------------------------------------------------------

// TestHTTP_ModelTestCRUD exercises model test lifecycle: create test → list → delete.
func TestHTTP_ModelTestCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	var testID string

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_model_for_tests", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name":    "testing",
				"name":            "users",
				"sql":             "SELECT 1 AS user_id, 'Alice' AS name, 'active' AS status",
				"materialization": "TABLE",
			})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"create_not_null_test", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/models/testing/users/tests",
				env.Keys.Admin, map[string]interface{}{
					"name":      "user_id_not_null",
					"test_type": "not_null",
					"column":    "user_id",
				})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "user_id_not_null", result["name"])
			assert.Equal(t, "not_null", result["test_type"])
			assert.Equal(t, "user_id", result["column"])
			assert.NotEmpty(t, result["id"])
			testID = result["id"].(string)
		}},

		{"create_unique_test", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/models/testing/users/tests",
				env.Keys.Admin, map[string]interface{}{
					"name":      "user_id_unique",
					"test_type": "unique",
					"column":    "user_id",
				})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"create_accepted_values_test", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/models/testing/users/tests",
				env.Keys.Admin, map[string]interface{}{
					"name":      "status_values",
					"test_type": "accepted_values",
					"column":    "status",
					"config": map[string]interface{}{
						"values": []string{"active", "inactive", "suspended"},
					},
				})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"create_test_missing_column_400", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/models/testing/users/tests",
				env.Keys.Admin, map[string]interface{}{
					"name":      "bad_test",
					"test_type": "not_null",
					// missing "column"
				})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 400, resp.StatusCode)
		}},

		{"create_test_model_not_found_404", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/models/testing/nonexistent/tests",
				env.Keys.Admin, map[string]interface{}{
					"name":      "test_on_missing",
					"test_type": "not_null",
					"column":    "id",
				})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},

		{"list_model_tests", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/models/testing/users/tests",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Equal(t, 3, len(data), "should have 3 tests")
		}},

		{"delete_model_test", func(t *testing.T) {
			resp := doRequest(t, "DELETE",
				fmt.Sprintf("%s/v1/models/testing/users/tests/%s", env.Server.URL, testID),
				env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"list_after_delete", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/models/testing/users/tests",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Equal(t, 2, len(data), "should have 2 tests after deletion")
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// ---------------------------------------------------------------------------
// Macro CRUD
// ---------------------------------------------------------------------------

// TestHTTP_MacroCRUD exercises the full macro lifecycle via the HTTP API.
func TestHTTP_MacroCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_scalar_macro", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/macros", env.Keys.Admin, map[string]interface{}{
				"name":        "double_val",
				"macro_type":  "SCALAR",
				"parameters":  []string{"x"},
				"body":        "x * 2",
				"description": "Doubles the input value",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "double_val", result["name"])
			assert.Equal(t, "SCALAR", result["macro_type"])
			assert.Equal(t, "x * 2", result["body"])
			assert.Equal(t, "Doubles the input value", result["description"])
			assert.NotEmpty(t, result["id"])
			assert.NotNil(t, result["created_at"])
		}},

		{"create_table_macro", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/macros", env.Keys.Admin, map[string]interface{}{
				"name":       "generate_series_custom",
				"macro_type": "TABLE",
				"parameters": []string{"n"},
				"body":       "SELECT * FROM generate_series(1, n)",
			})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"create_macro_duplicate_409", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/macros", env.Keys.Admin, map[string]interface{}{
				"name": "double_val",
				"body": "x * 3",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 409, resp.StatusCode)
		}},

		{"create_macro_missing_name_400", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/macros", env.Keys.Admin, map[string]interface{}{
				"body": "1 + 1",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 400, resp.StatusCode)
		}},

		{"create_macro_missing_body_400", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/macros", env.Keys.Admin, map[string]interface{}{
				"name": "empty_body_macro",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 400, resp.StatusCode)
		}},

		{"get_macro", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/macros/double_val", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "double_val", result["name"])
			assert.Equal(t, "x * 2", result["body"])
		}},

		{"get_macro_not_found_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/macros/nonexistent", env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},

		{"list_macros", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/macros", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 2, "should have at least 2 macros")
		}},

		{"update_macro", func(t *testing.T) {
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/macros/double_val", env.Keys.Admin, map[string]interface{}{
				"body":        "x * 3",
				"description": "Triples the input value",
			})
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "x * 3", result["body"])
			assert.Equal(t, "Triples the input value", result["description"])
		}},

		{"update_macro_not_found_404", func(t *testing.T) {
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/macros/nonexistent", env.Keys.Admin, map[string]interface{}{
				"body": "1",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},

		{"delete_macro", func(t *testing.T) {
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/macros/double_val", env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"delete_macro_idempotent_204", func(t *testing.T) {
			// Deleting an already-deleted macro returns 204 (idempotent delete).
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/macros/double_val", env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 204, resp.StatusCode)
		}},

		{"get_deleted_macro_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/macros/double_val", env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// ---------------------------------------------------------------------------
// Freshness Check
// ---------------------------------------------------------------------------

// TestHTTP_ModelFreshness verifies the freshness check endpoint.
func TestHTTP_ModelFreshness(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_model_with_freshness", func(t *testing.T) {
			// Create a model first (freshness policy is set via update since
			// create doesn't expose freshness in the OpenAPI schema body)
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name":    "freshtest",
				"name":            "stale_model",
				"sql":             "SELECT 1 AS id",
				"materialization": "TABLE",
			})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"check_freshness", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/models/freshtest/stale_model/freshness",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			// Without a freshness policy, should still return a valid status
			assert.NotNil(t, result["is_fresh"])
			assert.NotNil(t, result["max_lag_seconds"])
		}},

		{"check_freshness_not_found_404", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/models/freshtest/nonexistent/freshness",
				env.Keys.Admin, nil)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, 404, resp.StatusCode)
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_SourceFreshness verifies source freshness endpoint behavior.
func TestHTTP_SourceFreshness(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})
	require.NotNil(t, env.DuckDB)

	_, err := env.DuckDB.Exec(`CREATE SCHEMA IF NOT EXISTS raw`)
	require.NoError(t, err)
	_, err = env.DuckDB.Exec(`CREATE TABLE raw.orders (id INTEGER, updated_at TIMESTAMP)`)
	require.NoError(t, err)
	_, err = env.DuckDB.Exec(`INSERT INTO raw.orders VALUES (1, CURRENT_TIMESTAMP)`)
	require.NoError(t, err)

	t.Run("check_source_freshness_default_params", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/sources/raw/orders/freshness", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		assert.Equal(t, true, result["is_fresh"])
		assert.Equal(t, "raw", result["source_schema"])
		assert.Equal(t, "orders", result["source_table"])
		assert.Equal(t, "updated_at", result["timestamp_column"])
		assert.EqualValues(t, 3600, int(result["max_lag_seconds"].(float64)))
		assert.NotNil(t, result["last_loaded_at"])
	})

	t.Run("check_source_freshness_with_query_params", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/sources/raw/orders/freshness?timestamp_column=updated_at&max_lag_seconds=1", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		assert.Equal(t, "updated_at", result["timestamp_column"])
		assert.EqualValues(t, 1, int(result["max_lag_seconds"].(float64)))
	})

	t.Run("check_source_freshness_missing_timestamp_column_returns_400", func(t *testing.T) {
		_, err := env.DuckDB.Exec(`CREATE TABLE raw.no_ts (id INTEGER)`)
		require.NoError(t, err)

		resp := doRequest(t, "GET", env.Server.URL+"/v1/sources/raw/no_ts/freshness", env.Keys.Admin, nil)
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, 400, resp.StatusCode)
	})
}

// ---------------------------------------------------------------------------
// Model Runs (listing only — triggering requires DuckDB)
// ---------------------------------------------------------------------------

// TestHTTP_ModelRunEndpoints verifies model run list endpoints return valid
// responses. Triggering actual runs requires a DuckDB engine, so we only test
// the list/get paths here.
func TestHTTP_ModelRunEndpoints(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	seedRunID := "11111111-1111-1111-1111-111111111111"
	seedManifest := `{"version":1,"models":[{"model_name":"analytics.stg_orders","compiled_hash":"sha256:abc"}]}`
	seedDiagnostics := `{"warnings":["source registry is empty; source() references are rendered without strict existence checks"],"errors":[]}`

	t.Run("list_runs_empty", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/model-runs", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.Equal(t, 0, len(data), "no runs should exist initially")
	})

	t.Run("seeded_run_exposes_compile_artifacts", func(t *testing.T) {
		_, err := env.MetaDB.Exec(
			`INSERT INTO model_runs (id, status, trigger_type, triggered_by, target_catalog, target_schema, model_selector, variables, full_refresh, compile_manifest, compile_diagnostics)
			 VALUES (?, 'SUCCESS', 'MANUAL', 'admin', 'memory', 'analytics', '', '{}', 0, ?, ?)`,
			seedRunID,
			seedManifest,
			seedDiagnostics,
		)
		require.NoError(t, err)

		listResp := doRequest(t, "GET", env.Server.URL+"/v1/model-runs", env.Keys.Admin, nil)
		require.Equal(t, 200, listResp.StatusCode)

		var listResult map[string]interface{}
		decodeJSON(t, listResp, &listResult)
		data := listResult["data"].([]interface{})
		require.Len(t, data, 1)

		run := data[0].(map[string]interface{})
		assert.Equal(t, seedRunID, run["id"])
		assert.Equal(t, seedManifest, run["compile_manifest"])
		diagnostics, ok := run["compile_diagnostics"].(map[string]interface{})
		require.True(t, ok)
		warnings := diagnostics["warnings"].([]interface{})
		require.Len(t, warnings, 1)
		assert.Equal(t, "source registry is empty; source() references are rendered without strict existence checks", warnings[0])

		getResp := doRequest(t, "GET", env.Server.URL+"/v1/model-runs/"+seedRunID, env.Keys.Admin, nil)
		require.Equal(t, 200, getResp.StatusCode)

		var getResult map[string]interface{}
		decodeJSON(t, getResp, &getResult)
		assert.Equal(t, seedManifest, getResult["compile_manifest"])
		_, ok = getResult["compile_diagnostics"].(map[string]interface{})
		assert.True(t, ok)
	})

	t.Run("seeded_run_empty_compile_diagnostics_keeps_stable_shape", func(t *testing.T) {
		emptyDiagnosticsRunID := "22222222-2222-2222-2222-222222222222"
		_, err := env.MetaDB.Exec(
			`INSERT INTO model_runs (id, status, trigger_type, triggered_by, target_catalog, target_schema, model_selector, variables, full_refresh, compile_manifest, compile_diagnostics)
			 VALUES (?, 'SUCCESS', 'MANUAL', 'admin', 'memory', 'analytics', '', '{}', 0, ?, ?)`,
			emptyDiagnosticsRunID,
			`{"version":1,"models":[]}`,
			`{}`,
		)
		require.NoError(t, err)

		resp := doRequest(t, "GET", env.Server.URL+"/v1/model-runs/"+emptyDiagnosticsRunID, env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)

		diagnostics, ok := result["compile_diagnostics"].(map[string]interface{})
		require.True(t, ok)
		warnings, ok := diagnostics["warnings"].([]interface{})
		require.True(t, ok)
		assert.Len(t, warnings, 0)
		errors, ok := diagnostics["errors"].([]interface{})
		require.True(t, ok)
		assert.Len(t, errors, 0)
	})

	t.Run("get_run_not_found_404", func(t *testing.T) {
		resp := doRequest(t, "GET",
			env.Server.URL+"/v1/model-runs/00000000-0000-0000-0000-000000000000",
			env.Keys.Admin, nil)
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, 404, resp.StatusCode)
	})

	t.Run("list_run_steps_empty_for_nonexistent_run", func(t *testing.T) {
		resp := doRequest(t, "GET",
			env.Server.URL+"/v1/model-runs/00000000-0000-0000-0000-000000000000/steps",
			env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		data := result["data"].([]interface{})
		assert.Equal(t, 0, len(data), "steps should be empty for nonexistent run")
	})
}

// ---------------------------------------------------------------------------
// Model with Config (incremental)
// ---------------------------------------------------------------------------

// TestHTTP_ModelIncrementalConfig verifies that incremental models with config
// are correctly stored and returned.
func TestHTTP_ModelIncrementalConfig(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_incremental_model", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name":    "incr",
				"name":            "orders_incremental",
				"sql":             "SELECT order_id, amount FROM raw_orders",
				"materialization": "INCREMENTAL",
				"config": map[string]interface{}{
					"unique_key":           []string{"order_id"},
					"incremental_strategy": "merge",
				},
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "INCREMENTAL", result["materialization"])

			cfg, ok := result["config"].(map[string]interface{})
			if assert.True(t, ok, "config should be present") {
				assert.Equal(t, "merge", cfg["incremental_strategy"])
				uniqueKey, ok := cfg["unique_key"].([]interface{})
				if assert.True(t, ok) {
					assert.Contains(t, uniqueKey, "order_id")
				}
			}
		}},

		{"update_incremental_config", func(t *testing.T) {
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/models/incr/orders_incremental", env.Keys.Admin, map[string]interface{}{
				"config": map[string]interface{}{
					"unique_key":           []string{"order_id", "line_id"},
					"incremental_strategy": "merge",
				},
			})
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			cfg := result["config"].(map[string]interface{})
			uniqueKey := cfg["unique_key"].([]interface{})
			assert.Len(t, uniqueKey, 2)
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// ---------------------------------------------------------------------------
// Custom SQL Test Type
// ---------------------------------------------------------------------------

// TestHTTP_ModelCustomSQLTest verifies custom_sql test type creation.
func TestHTTP_ModelCustomSQLTest(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_model", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
				"project_name":    "sqltest",
				"name":            "orders",
				"sql":             "SELECT 1 AS order_id, 100.0 AS amount",
				"materialization": "TABLE",
			})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"create_custom_sql_test", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/models/sqltest/orders/tests",
				env.Keys.Admin, map[string]interface{}{
					"name":      "no_negative_amounts",
					"test_type": "custom_sql",
					"config": map[string]interface{}{
						"custom_sql": "SELECT * FROM {{ model }} WHERE amount < 0",
					},
				})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "custom_sql", result["test_type"])
			cfg := result["config"].(map[string]interface{})
			assert.Contains(t, cfg["custom_sql"], "amount < 0")
		}},

		{"create_relationships_test", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/models/sqltest/orders/tests",
				env.Keys.Admin, map[string]interface{}{
					"name":      "order_customer_fk",
					"test_type": "relationships",
					"column":    "order_id",
					"config": map[string]interface{}{
						"to_model":  "customers",
						"to_column": "id",
					},
				})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "relationships", result["test_type"])
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// ---------------------------------------------------------------------------
// Non-admin access
// ---------------------------------------------------------------------------

// TestHTTP_ModelNonAdminAccess verifies that non-admin users can also perform
// model operations (models don't require admin privileges for CRUD).
func TestHTTP_ModelNonAdminAccess(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"analyst_creates_model", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Analyst, map[string]interface{}{
				"project_name": "analyst_project",
				"name":         "analyst_model",
				"sql":          "SELECT 1 AS id",
			})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"analyst_lists_models", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models", env.Keys.Analyst, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 1)
		}},

		{"analyst_gets_model", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/models/analyst_project/analyst_model", env.Keys.Analyst, nil)
			require.Equal(t, 200, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"analyst_creates_macro", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/macros", env.Keys.Analyst, map[string]interface{}{
				"name": "analyst_macro",
				"body": "x + 1",
			})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// ---------------------------------------------------------------------------
// Macro with Default Type
// ---------------------------------------------------------------------------

// TestHTTP_MacroDefaultType verifies that omitting macro_type defaults to SCALAR.
func TestHTTP_MacroDefaultType(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	resp := doRequest(t, "POST", env.Server.URL+"/v1/macros", env.Keys.Admin, map[string]interface{}{
		"name": "default_type_macro",
		"body": "42",
	})
	require.Equal(t, 201, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	assert.Equal(t, "SCALAR", result["macro_type"], "default macro_type should be SCALAR")
}

// ---------------------------------------------------------------------------
// Model Default Materialization
// ---------------------------------------------------------------------------

// TestHTTP_ModelDefaultMaterialization verifies that omitting materialization
// defaults to VIEW.
func TestHTTP_ModelDefaultMaterialization(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})

	resp := doRequest(t, "POST", env.Server.URL+"/v1/models", env.Keys.Admin, map[string]interface{}{
		"project_name": "defaults",
		"name":         "default_mat_model",
		"sql":          "SELECT 1",
	})
	require.Equal(t, 201, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	assert.Equal(t, "VIEW", result["materialization"], "default materialization should be VIEW")
}
