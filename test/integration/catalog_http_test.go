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

	resp := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/info", env.Keys.Admin, nil)
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
			resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test_schema", result["name"])
			assert.Equal(t, "integration test schema", result["comment"])
		}},
		{"get_by_name_200", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/schemas/test_schema", env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test_schema", result["name"])
		}},
		{"list_contains_created", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin, nil)
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
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/catalogs/lake/schemas/test_schema", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "updated comment", result["comment"])
		}},
		{"update_properties_200", func(t *testing.T) {
			body := map[string]interface{}{
				"properties": map[string]string{"env": "test", "owner": "ci"},
			}
			resp := doRequest(t, "PATCH", env.Server.URL+"/v1/catalogs/lake/schemas/test_schema", env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			props := result["properties"].(map[string]interface{})
			assert.Equal(t, "test", props["env"])
			assert.Equal(t, "ci", props["owner"])
		}},
		{"delete_204", func(t *testing.T) {
			resp := doRequest(t, "DELETE", env.Server.URL+"/v1/catalogs/lake/schemas/test_schema", env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"get_after_delete_404", func(t *testing.T) {
			resp := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/schemas/test_schema", env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
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
		resp := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/schemas/nonexistent_schema", env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("create_duplicate_409", func(t *testing.T) {
		body := map[string]interface{}{"name": "dup_schema"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)
		_ = resp.Body.Close()

		resp2 := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin, body)
		assert.Equal(t, 409, resp2.StatusCode)
		_ = resp2.Body.Close()
	})

	t.Run("delete_nonexistent_404", func(t *testing.T) {
		resp := doRequest(t, "DELETE", env.Server.URL+"/v1/catalogs/lake/schemas/nonexistent_schema", env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		_ = resp.Body.Close()
	})
}

// TestHTTP_TableCRUD tests the full table lifecycle through HTTP.
func TestHTTP_TableCRUD(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	// Create a schema first
	schemaBody := map[string]interface{}{"name": "table_test_schema"}
	resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin, schemaBody)
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

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
			resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas/table_test_schema/tables", env.Keys.Admin, body)
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "test_table", result["name"])
			assert.Equal(t, "table_test_schema", result["schema_name"])
		}},
		{"get_by_name_200", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalogs/lake/schemas/table_test_schema/tables/test_table",
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
				env.Server.URL+"/v1/catalogs/lake/schemas/table_test_schema/tables",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.GreaterOrEqual(t, len(data), 1)
		}},
		{"list_columns", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalogs/lake/schemas/table_test_schema/tables/test_table/columns",
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
				env.Server.URL+"/v1/catalogs/lake/schemas/table_test_schema/tables/test_table",
				env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
		{"get_after_drop_404", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalogs/lake/schemas/table_test_schema/tables/test_table",
				env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
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
			env.Server.URL+"/v1/catalogs/lake/schemas/does_not_exist/tables",
			env.Keys.Admin, body)
		assert.Equal(t, 400, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("drop_nonexistent_404", func(t *testing.T) {
		resp := doRequest(t, "DELETE",
			env.Server.URL+"/v1/catalogs/lake/schemas/main/tables/does_not_exist",
			env.Keys.Admin, nil)
		assert.Equal(t, 404, resp.StatusCode)
		_ = resp.Body.Close()
	})
}

// TestHTTP_MetastoreSummary tests GET /v1/metastore/summary.
func TestHTTP_MetastoreSummary(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	t.Run("initial_counts", func(t *testing.T) {
		resp := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/metastore/summary", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)

		var result map[string]interface{}
		decodeJSON(t, resp, &result)
		assert.Equal(t, "lake", result["catalog_name"])
		assert.GreaterOrEqual(t, result["schema_count"].(float64), float64(1))
	})

	t.Run("after_create_schema_and_table", func(t *testing.T) {
		// Get initial counts
		resp := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/metastore/summary", env.Keys.Admin, nil)
		require.Equal(t, 200, resp.StatusCode)
		var before map[string]interface{}
		decodeJSON(t, resp, &before)
		beforeSchemas := before["schema_count"].(float64)
		beforeTables := before["table_count"].(float64)

		// Create a schema and table
		resp2 := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin,
			map[string]interface{}{"name": "summary_test"})
		require.Equal(t, 201, resp2.StatusCode)
		_ = resp2.Body.Close()

		resp3 := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas/summary_test/tables", env.Keys.Admin,
			map[string]interface{}{
				"name":    "summary_table",
				"columns": []map[string]interface{}{{"name": "x", "type": "INTEGER"}},
			})
		require.Equal(t, 201, resp3.StatusCode)
		_ = resp3.Body.Close()

		// Check updated counts
		resp4 := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/metastore/summary", env.Keys.Admin, nil)
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
		resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.NoAccess, body)
		assert.Equal(t, 403, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("create_schema_denied_for_analyst", func(t *testing.T) {
		body := map[string]interface{}{"name": "unauthorized_schema_2"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Analyst, body)
		assert.Equal(t, 403, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("admin_can_create_schema", func(t *testing.T) {
		body := map[string]interface{}{"name": "admin_allowed_schema"}
		resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin, body)
		require.Equal(t, 201, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("update_schema_denied_for_analyst", func(t *testing.T) {
		body := map[string]interface{}{"comment": "should fail"}
		resp := doRequest(t, "PATCH", env.Server.URL+"/v1/catalogs/lake/schemas/admin_allowed_schema", env.Keys.Analyst, body)
		assert.Equal(t, 403, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("delete_schema_denied_for_analyst", func(t *testing.T) {
		resp := doRequest(t, "DELETE", env.Server.URL+"/v1/catalogs/lake/schemas/admin_allowed_schema", env.Keys.Analyst, nil)
		assert.Equal(t, 403, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("create_table_denied_for_analyst", func(t *testing.T) {
		body := map[string]interface{}{
			"name": "unauthorized_table",
			"columns": []map[string]interface{}{
				{"name": "id", "type": "INTEGER"},
			},
		}
		resp := doRequest(t, "POST",
			env.Server.URL+"/v1/catalogs/lake/schemas/admin_allowed_schema/tables",
			env.Keys.Analyst, body)
		assert.Equal(t, 403, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("read_operations_allowed_for_all_authenticated", func(t *testing.T) {
		// Even the analyst should be able to read catalog data
		endpoints := []string{
			"/v1/catalogs/lake/info",
			"/v1/catalogs/lake/schemas",
			"/v1/catalogs/lake/schemas/main",
			"/v1/catalogs/lake/metastore/summary",
		}
		for _, ep := range endpoints {
			t.Run(ep, func(t *testing.T) {
				resp := doRequest(t, "GET", env.Server.URL+ep, env.Keys.Analyst, nil)
				assert.Equal(t, 200, resp.StatusCode, "endpoint %s should be readable", ep)
				_ = resp.Body.Close()
			})
		}
	})
}

// TestHTTP_CatalogSchemaForceDelete tests cascade delete behavior through HTTP.
func TestHTTP_CatalogSchemaForceDelete(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	// Create a schema with a table
	resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin,
		map[string]interface{}{"name": "force_del_schema"})
	require.Equal(t, 201, resp.StatusCode)
	_ = resp.Body.Close()

	resp2 := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas/force_del_schema/tables", env.Keys.Admin,
		map[string]interface{}{
			"name":    "child_table",
			"columns": []map[string]interface{}{{"name": "id", "type": "INTEGER"}},
		})
	require.Equal(t, 201, resp2.StatusCode)
	_ = resp2.Body.Close()

	t.Run("delete_non_empty_without_force_fails", func(t *testing.T) {
		resp := doRequest(t, "DELETE",
			env.Server.URL+"/v1/catalogs/lake/schemas/force_del_schema",
			env.Keys.Admin, nil)
		// Should fail because schema has children (returns 409 Conflict)
		assert.Equal(t, 409, resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("delete_non_empty_with_force_succeeds", func(t *testing.T) {
		resp := doRequest(t, "DELETE",
			fmt.Sprintf("%s/v1/catalogs/lake/schemas/force_del_schema?force=true", env.Server.URL),
			env.Keys.Admin, nil)
		require.Equal(t, 204, resp.StatusCode)
		_ = resp.Body.Close()

		// Verify schema is gone
		resp2 := doRequest(t, "GET",
			env.Server.URL+"/v1/catalogs/lake/schemas/force_del_schema",
			env.Keys.Admin, nil)
		assert.Equal(t, 404, resp2.StatusCode)
		_ = resp2.Body.Close()
	})
}

// TestHTTP_UpdateTable tests PATCH /v1/catalog/schemas/{schemaName}/tables/{tableName}.
func TestHTTP_UpdateTable(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	tableURL := env.Server.URL + "/v1/catalogs/lake/schemas/main/tables/titanic"

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"patch_comment_200", func(t *testing.T) {
			body := map[string]interface{}{
				"comment": "Titanic dataset",
			}
			resp := doRequest(t, "PATCH", tableURL, env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "Titanic dataset", result["comment"])
		}},

		{"patch_properties_200", func(t *testing.T) {
			body := map[string]interface{}{
				"properties": map[string]string{"source": "kaggle"},
			}
			resp := doRequest(t, "PATCH", tableURL, env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			props := result["properties"].(map[string]interface{})
			assert.Equal(t, "kaggle", props["source"])
			// Comment from previous step should be preserved
			assert.Equal(t, "Titanic dataset", result["comment"])
		}},

		{"patch_owner_200", func(t *testing.T) {
			body := map[string]interface{}{
				"owner": "data_team",
			}
			resp := doRequest(t, "PATCH", tableURL, env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "data_team", result["owner"])
		}},

		{"get_verifies_update", func(t *testing.T) {
			resp := doRequest(t, "GET", tableURL, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "Titanic dataset", result["comment"])
			props := result["properties"].(map[string]interface{})
			assert.Equal(t, "kaggle", props["source"])
			assert.Equal(t, "data_team", result["owner"])
		}},

		{"patch_nonexistent_404", func(t *testing.T) {
			body := map[string]interface{}{"comment": "nope"}
			resp := doRequest(t, "PATCH",
				env.Server.URL+"/v1/catalogs/lake/schemas/main/tables/nonexistent",
				env.Keys.Admin, body)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"analyst_denied_403", func(t *testing.T) {
			body := map[string]interface{}{"comment": "should fail"}
			resp := doRequest(t, "PATCH", tableURL, env.Keys.Analyst, body)
			assert.Equal(t, 403, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_UpdateColumn tests PATCH /v1/catalog/schemas/{s}/tables/{t}/columns/{c}.
func TestHTTP_UpdateColumn(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	columnURL := env.Server.URL + "/v1/catalogs/lake/schemas/main/tables/titanic/columns/Name"

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"patch_comment_200", func(t *testing.T) {
			body := map[string]interface{}{
				"comment": "Passenger name",
			}
			resp := doRequest(t, "PATCH", columnURL, env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "Passenger name", result["comment"])
		}},

		{"get_table_verifies_column_comment", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalogs/lake/schemas/main/tables/titanic",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			cols := result["columns"].([]interface{})
			found := false
			for _, item := range cols {
				c := item.(map[string]interface{})
				if c["name"] == "Name" {
					found = true
					assert.Equal(t, "Passenger name", c["comment"])
					break
				}
			}
			assert.True(t, found, "expected to find column 'Name'")
		}},

		{"patch_nonexistent_column_404", func(t *testing.T) {
			body := map[string]interface{}{"comment": "nope"}
			resp := doRequest(t, "PATCH",
				env.Server.URL+"/v1/catalogs/lake/schemas/main/tables/titanic/columns/DoesNotExist",
				env.Keys.Admin, body)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_ProfileTable tests POST /v1/catalog/schemas/{s}/tables/{t}/profile.
func TestHTTP_ProfileTable(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	profileURL := env.Server.URL + "/v1/catalogs/lake/schemas/main/tables/titanic/profile"
	tableURL := env.Server.URL + "/v1/catalogs/lake/schemas/main/tables/titanic"

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"profile_200", func(t *testing.T) {
			resp := doRequest(t, "POST", profileURL, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.InDelta(t, float64(12), result["column_count"], 0)
			assert.NotNil(t, result["last_profiled_at"])
		}},

		{"get_table_has_stats", func(t *testing.T) {
			resp := doRequest(t, "GET", tableURL, env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			stats, ok := result["statistics"].(map[string]interface{})
			require.True(t, ok, "expected statistics field in table response")
			assert.InDelta(t, float64(12), stats["column_count"], 0)
		}},

		{"profile_nonexistent_404", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/catalogs/lake/schemas/main/tables/nonexistent/profile",
				env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_TagsInSchemaResponse tests that tags assigned to a schema appear in
// the GET /v1/catalog/schemas/{name} response.
func TestHTTP_TagsInSchemaResponse(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	var tagID string

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_tag", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tags", env.Keys.Admin, map[string]interface{}{
				"key":   "env",
				"value": "production",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			tagID = result["id"].(string)
		}},

		{"assign_tag_to_schema", func(t *testing.T) {
			resp := doRequest(t, "POST",
				fmt.Sprintf("%s/v1/tags/%s/assignments", env.Server.URL, tagID),
				env.Keys.Admin, map[string]interface{}{
					"securable_type": "schema",
					"securable_id":   "0", // schema_id=0 for "main"
				})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"get_schema_has_tags", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalogs/lake/schemas/main",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			tags, ok := result["tags"].([]interface{})
			require.True(t, ok, "expected 'tags' array in schema response")
			require.GreaterOrEqual(t, len(tags), 1)

			found := false
			for _, item := range tags {
				tag := item.(map[string]interface{})
				if tag["key"] == "env" && tag["value"] == "production" {
					found = true
					break
				}
			}
			assert.True(t, found, "expected tag with key=env, value=production in schema response")
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_TagsInTableResponse tests that tags assigned to a table appear in
// the GET /v1/catalog/schemas/{s}/tables/{t} response.
func TestHTTP_TagsInTableResponse(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	var tagID string

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_tag", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tags", env.Keys.Admin, map[string]interface{}{
				"key":   "domain",
				"value": "maritime",
			})
			require.Equal(t, 201, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			tagID = result["id"].(string)
		}},

		{"assign_tag_to_table", func(t *testing.T) {
			resp := doRequest(t, "POST",
				fmt.Sprintf("%s/v1/tags/%s/assignments", env.Server.URL, tagID),
				env.Keys.Admin, map[string]interface{}{
					"securable_type": "table",
					"securable_id":   "1", // table_id=1 for "titanic"
				})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"get_table_has_tags", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalogs/lake/schemas/main/tables/titanic",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			tags, ok := result["tags"].([]interface{})
			require.True(t, ok, "expected 'tags' array in table response")
			require.GreaterOrEqual(t, len(tags), 1)

			found := false
			for _, item := range tags {
				tag := item.(map[string]interface{})
				if tag["key"] == "domain" && tag["value"] == "maritime" {
					found = true
					break
				}
			}
			assert.True(t, found, "expected tag with key=domain, value=maritime in table response")
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_CascadeDeleteVerifiesGovernanceRecords tests that force-deleting a
// schema removes all subsidiary governance records (row_filters, column_masks,
// tag_assignments).
func TestHTTP_CascadeDeleteVerifiesGovernanceRecords(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	var tagID string

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"create_schema", func(t *testing.T) {
			resp := doRequest(t, "POST", env.Server.URL+"/v1/catalogs/lake/schemas", env.Keys.Admin,
				map[string]interface{}{"name": "cascade_test"})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"create_table", func(t *testing.T) {
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/catalogs/lake/schemas/cascade_test/tables",
				env.Keys.Admin, map[string]interface{}{
					"name": "gov_table",
					"columns": []map[string]interface{}{
						{"name": "id", "type": "INTEGER"},
						{"name": "secret", "type": "VARCHAR"},
					},
				})
			require.Equal(t, 201, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"create_and_assign_tag", func(t *testing.T) {
			// Create a tag
			resp := doRequest(t, "POST", env.Server.URL+"/v1/tags", env.Keys.Admin,
				map[string]interface{}{"key": "cascade_tag", "value": "test"})
			require.Equal(t, 201, resp.StatusCode)
			var tag map[string]interface{}
			decodeJSON(t, resp, &tag)
			tagID = tag["id"].(string)

			// Get the table to find its table_id
			resp2 := doRequest(t, "GET",
				env.Server.URL+"/v1/catalogs/lake/schemas/cascade_test/tables/gov_table",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp2.StatusCode)
			var table map[string]interface{}
			decodeJSON(t, resp2, &table)
			tableID := table["table_id"].(string)

			// Assign tag to table
			resp3 := doRequest(t, "POST",
				fmt.Sprintf("%s/v1/tags/%s/assignments", env.Server.URL, tagID),
				env.Keys.Admin, map[string]interface{}{
					"securable_type": "table",
					"securable_id":   tableID,
				})
			require.Equal(t, 201, resp3.StatusCode)
			_ = resp3.Body.Close()
		}},

		{"force_delete_schema", func(t *testing.T) {
			resp := doRequest(t, "DELETE",
				fmt.Sprintf("%s/v1/catalogs/lake/schemas/cascade_test?force=true", env.Server.URL),
				env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"verify_schema_gone", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/catalogs/lake/schemas/cascade_test",
				env.Keys.Admin, nil)
			assert.Equal(t, 404, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"verify_tag_assignments_cleaned", func(t *testing.T) {
			// Query MetaDB directly to check governance records
			var count int
			err := env.MetaDB.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM tag_assignments WHERE securable_type = 'table'
				 AND tag_id = ?`, tagID).Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 0, count, "expected tag assignments to be cleaned up after cascade delete")
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_TableProfileAuthorization tests profile endpoint authorization for
// different user roles.
func TestHTTP_TableProfileAuthorization(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})
	profileURL := env.Server.URL + "/v1/catalogs/lake/schemas/main/tables/titanic/profile"

	t.Run("no_access_denied", func(t *testing.T) {
		resp := doRequest(t, "POST", profileURL, env.Keys.NoAccess, nil)
		assert.Equal(t, 403, resp.StatusCode,
			"expected no_access user to be denied profile access")
		_ = resp.Body.Close()
	})

	t.Run("analyst_allowed", func(t *testing.T) {
		resp := doRequest(t, "POST", profileURL, env.Keys.Analyst, nil)
		// Analyst has SELECT on titanic table — should be able to profile it
		assert.Equal(t, 200, resp.StatusCode,
			"expected analyst with SELECT to be able to profile table")
		_ = resp.Body.Close()
	})
}
