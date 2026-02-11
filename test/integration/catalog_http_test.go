//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_CatalogInfo tests GET /v1/catalog.
func TestHTTP_CatalogInfo(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	resp := doRequest(t, "GET", env.Server.URL+"/v1/catalog", env.Keys.Admin, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	require.NotNil(t, result["name"])
	assert.Equal(t, "lake", result["name"])
}

// TestHTTP_SchemaCRUD tests the full schema lifecycle through HTTP.
func TestHTTP_SchemaCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create_201", func(t *testing.T) {
			body := map[string]interface{}{
				"name":    "test_schema",
				"comment": "integration test schema",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test_schema", result["name"])
			assert.Equal(t, "integration test schema", result["comment"])
		}},
		{"get_by_name_200", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/catalog/schemas/test_schema", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test_schema", result["name"])
		}},
		{"list_contains_created", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/catalog/schemas", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			names := make([]string, 0)
			for _, item := range data {
				s := item.(map[string]interface{})
				names = append(names, s["name"].(string))
			}
			assert.Contains(t, names, "test_schema")
			assert.Contains(t, names, "main")
		}},
		{"update_comment_200", func(t *testing.T) {
			body := map[string]interface{}{
				"comment": "updated comment",
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/catalog/schemas/test_schema", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "updated comment", result["comment"])
		}},
		{"update_properties_200", func(t *testing.T) {
			body := map[string]interface{}{
				"properties": map[string]string{"env": "test", "owner": "ci"},
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/catalog/schemas/test_schema", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			props := result["properties"].(map[string]interface{})
			assert.Equal(t, "test", props["env"])
			assert.Equal(t, "ci", props["owner"])
		}},
		{"delete_204", func(t *testing.T) {
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/catalog/schemas/test_schema", env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			resp.Body.Close()
		}},
		{"get_after_delete_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/catalog/schemas/test_schema", env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			resp.Body.Close()
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_SchemaErrors tests error cases for schema endpoints.
func TestHTTP_SchemaErrors(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	t.Run("get_nonexistent_404", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/catalog/schemas/nonexistent_schema", env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("create_duplicate_409", func(t *testing.T) {
		body := map[string]interface{}{"name": "dup_schema"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)
		resp.Body.Close()

		resp2 := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.Admin, body)
		assert.Equal(t, 409, resp2.StatusCode)
		resp2.Body.Close()
	})

	t.Run("delete_nonexistent_404", func(t *testing.T) {
		resp := doRequest(t, "DELETE", env.Server.URL+"/v1/catalog/schemas/nonexistent_schema", env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		resp.Body.Close()
	})
}

// TestHTTP_TableCRUD tests the full table lifecycle through HTTP.
func TestHTTP_TableCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	// Create a schema first
	schemaBody := map[string]interface{}{"name": "table_test_schema"}
	resp := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.Admin, schemaBody)
	require.Equal(t, 201, resp.StatusCode)
	resp.Body.Close()

	type step struct {
		name string
		fn   func(t *testing.T)
	}
	steps := []step{
		{"create_201", func(t *testing.T) {
			body := map[string]interface{}{
				"name": "test_table",
				"columns": []map[string]interface{}{
					{"name": "id", "type": "INTEGER"},
					{"name": "name", "type": "VARCHAR"},
					{"name": "score", "type": "DOUBLE"},
				},
				"comment": "test table",
			}
			resp := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas/table_test_schema/tables", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test_table", result["name"])
			assert.Equal(t, "table_test_schema", result["schema_name"])
		}},
		{"get_by_name_200", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalog/schemas/table_test_schema/tables/test_table",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test_table", result["name"])
			cols := result["columns"].([]interface{})
			assert.Len(t, cols, 3)
		}},
		{"list_tables", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalog/schemas/table_test_schema/tables",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 1)
		}},
		{"list_columns", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalog/schemas/table_test_schema/tables/test_table/columns",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Len(t, data, 3)

			names := make([]string, 0)
			for _, item := range data {
				c := item.(map[string]interface{})
				names = append(names, c["name"].(string))
			}
			assert.Contains(t, names, "id")
			assert.Contains(t, names, "name")
			assert.Contains(t, names, "score")
		}},
		{"drop_204", func(t *testing.T) {
			resp := doRequest(t, "DELETE",
				env.Server.URL+"/v1/catalog/schemas/table_test_schema/tables/test_table",
				env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			resp.Body.Close()
		}},
		{"get_after_drop_404", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalog/schemas/table_test_schema/tables/test_table",
				env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			resp.Body.Close()
		}},
	}
	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// TestHTTP_TableErrors tests error cases for table endpoints.
func TestHTTP_TableErrors(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	t.Run("create_in_nonexistent_schema_400", func(t *testing.T) {
		body := map[string]interface{}{
			"name": "bad_table",
			"columns": []map[string]interface{}{
				{"name": "id", "type": "INTEGER"},
			},
		}
		resp := doRequest(t, "POST",
			env.Server.URL+"/v1/catalog/schemas/does_not_exist/tables",
			env.Keys.Admin, body)
		assert.Equal(t, 400, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("drop_nonexistent_404", func(t *testing.T) {
		resp := doRequest(t, "DELETE",
			env.Server.URL+"/v1/catalog/schemas/main/tables/does_not_exist",
			env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		resp.Body.Close()
	})
}

// TestHTTP_MetastoreSummary tests GET /v1/metastore/summary.
func TestHTTP_MetastoreSummary(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	t.Run("initial_counts", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/metastore/summary", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		assert.Equal(t, "lake", result["catalog_name"])
		assert.GreaterOrEqual(t, result["schema_count"].(float64), float64(1))
	})

	t.Run("after_create_schema_and_table", func(t *testing.T) {
		// Get initial counts
		resp := doRequest(t, "GET", env.Server.URL+"/v1/metastore/summary", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)
		var before map[string]interface{}
		decodeJSON(t, resp, &before)
		beforeSchemas := before["schema_count"].(float64)
		beforeTables := before["table_count"].(float64)

		// Create a schema and table
		resp2 := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.Admin,
			map[string]interface{}{"name": "summary_test"})
		require.Equal(t, 201, resp2.StatusCode)
		resp2.Body.Close()

		resp3 := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas/summary_test/tables", env.Keys.Admin,
			map[string]interface{}{
				"name":    "summary_table",
				"columns": []map[string]interface{}{{"name": "x", "type": "INTEGER"}},
			})
		require.Equal(t, 201, resp3.StatusCode)
		resp3.Body.Close()

		// Check updated counts
		resp4 := doRequest(t, "GET", env.Server.URL+"/v1/metastore/summary", env.Keys.Admin, nil)
		require.Equal(t, 200, resp4.StatusCode)
		var after map[string]interface{}
		decodeJSON(t, resp4, &after)
		assert.Greater(t, after["schema_count"].(float64), beforeSchemas)
		assert.Greater(t, after["table_count"].(float64), beforeTables)
	})
}

// TestHTTP_CatalogAuthorization tests that catalog mutation endpoints enforce privileges.
func TestHTTP_CatalogAuthorization(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	// The no_access user has no grants — should be denied on all mutations.
	// The analyst has SELECT+USAGE but NOT CREATE_SCHEMA or CREATE_TABLE.

	t.Run("create_schema_denied_for_no_access", func(t *testing.T) {
		body := map[string]interface{}{"name": "unauthorized_schema"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.NoAccess, body)
		assert.Equal(t, 403, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("create_schema_denied_for_analyst", func(t *testing.T) {
		body := map[string]interface{}{"name": "unauthorized_schema_2"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.Analyst, body)
		assert.Equal(t, 403, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("admin_can_create_schema", func(t *testing.T) {
		body := map[string]interface{}{"name": "admin_allowed_schema"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("update_schema_denied_for_analyst", func(t *testing.T) {
		body := map[string]interface{}{"comment": "should fail"}
		resp := doRequest(t, "PATCH", env.Server.URL+"/v1/catalog/schemas/admin_allowed_schema", env.Keys.Analyst, body)
		assert.Equal(t, 403, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("delete_schema_denied_for_analyst", func(t *testing.T) {
		resp := doRequest(t, "DELETE", env.Server.URL+"/v1/catalog/schemas/admin_allowed_schema", env.Keys.Analyst, nil)
		assert.Equal(t, 403, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("create_table_denied_for_analyst", func(t *testing.T) {
		body := map[string]interface{}{
			"name": "unauthorized_table",
			"columns": []map[string]interface{}{
				{"name": "id", "type": "INTEGER"},
			},
		}
		resp := doRequest(t, "POST",
			env.Server.URL+"/v1/catalog/schemas/admin_allowed_schema/tables",
			env.Keys.Analyst, body)
		assert.Equal(t, 403, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("read_operations_allowed_for_all_authenticated", func(t *testing.T) {
		// Even the analyst should be able to read catalog data
		endpoints := []string{
			"/v1/catalog",
			"/v1/catalog/schemas",
			"/v1/catalog/schemas/main",
			"/v1/metastore/summary",
		}
		for _, ep := range endpoints {
			t.Run(ep, func(t *testing.T) {
				resp := doRequest(t, "GET", env.Server.URL+ep, env.Keys.Analyst, nil)
				assert.Equal(t, 200, resp.StatusCode, "endpoint %s should be readable", ep)
				resp.Body.Close()
			})
		}
	})
}

// TestHTTP_CatalogSchemaForceDelete tests cascade delete behavior through HTTP.
func TestHTTP_CatalogSchemaForceDelete(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	// Create a schema with a table
	resp := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas", env.Keys.Admin,
		map[string]interface{}{"name": "force_del_schema"})
	require.Equal(t, 201, resp.StatusCode)
	resp.Body.Close()

	resp2 := doRequest(t, "POST", env.Server.URL+"/v1/catalog/schemas/force_del_schema/tables", env.Keys.Admin,
		map[string]interface{}{
			"name":    "child_table",
			"columns": []map[string]interface{}{{"name": "id", "type": "INTEGER"}},
		})
	require.Equal(t, 201, resp2.StatusCode)
	resp2.Body.Close()

	t.Run("delete_non_empty_without_force_fails", func(t *testing.T) {
		resp := doRequest(t, "DELETE",
			env.Server.URL+"/v1/catalog/schemas/force_del_schema",
			env.Keys.Admin, nil)
		// Should fail because schema has children (returns 403 for ConflictError)
		assert.Equal(t, 403, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("delete_non_empty_with_force_succeeds", func(t *testing.T) {
		resp := doRequest(t, "DELETE",
			fmt.Sprintf("%s/v1/catalog/schemas/force_del_schema?force=true", env.Server.URL),
			env.Keys.Admin, nil)
		require.Equal(t, 204, resp.StatusCode)
		resp.Body.Close()

		// Verify schema is gone
		resp2 := doRequest(t, "GET",
			env.Server.URL+"/v1/catalog/schemas/force_del_schema",
			env.Keys.Admin, nil)
		assert.Equal(t, 404, resp2.StatusCode)
		resp2.Body.Close()
	})
}
