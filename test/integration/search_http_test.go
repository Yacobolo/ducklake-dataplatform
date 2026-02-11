//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP_SearchCatalog tests the catalog search endpoint against seeded
// DuckLake metadata (schema "main", table "titanic", 12 columns).
func TestHTTP_SearchCatalog(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"search_table", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/search?query=titanic",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 1, "expected at least 1 search result for 'titanic'")

			found := false
			for _, item := range data {
				r := item.(map[string]interface{})
				if r["type"] == "table" && r["name"] == "titanic" {
					found = true
					assert.Equal(t, "main", r["schema_name"])
					break
				}
			}
			assert.True(t, found, "expected search result with type=table, name=titanic")
		}},

		{"search_column", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/search?query=Survived",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 1)

			found := false
			for _, item := range data {
				r := item.(map[string]interface{})
				if r["type"] == "column" && r["name"] == "Survived" {
					found = true
					break
				}
			}
			assert.True(t, found, "expected search result with type=column, name=Survived")
		}},

		{"search_type_filter", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/search?query=main&type=schema",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})

			for _, item := range data {
				r := item.(map[string]interface{})
				assert.Equal(t, "schema", r["type"], "expected all results to have type=schema when filtering")
			}
		}},

		{"search_no_results", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/search?query=zzzznonexistent",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Empty(t, data, "expected no search results for 'zzzznonexistent'")
		}},

		{"search_pagination", func(t *testing.T) {
			// "a" matches many columns (PassengerId, Age, Parch, Fare, Cabin, Embarked, Name, etc.)
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/search?query=a&max_results=2",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.LessOrEqual(t, len(data), 2, "expected at most 2 items with max_results=2")
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_SearchAnyUserCanSearch verifies that non-admin users can use the
// search endpoint (no privilege checks enforced).
func TestHTTP_SearchAnyUserCanSearch(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{SeedDuckLakeMetadata: true})

	resp := doRequest(t, "GET",
		env.Server.URL+"/v1/search?query=titanic",
		env.Keys.Analyst, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	data := result["data"].([]interface{})
	assert.GreaterOrEqual(t, len(data), 1)
}
