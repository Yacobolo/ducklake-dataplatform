//go:build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbstore "duck-demo/internal/db/dbstore"
)

// seedQueryHistoryEntries inserts audit_log rows with action='QUERY' to populate
// the query history endpoint.
func seedQueryHistoryEntries(t *testing.T, env *httpTestEnv) {
	t.Helper()
	ctx := context.Background()
	q := dbstore.New(env.MetaDB)

	entries := []dbstore.InsertAuditLogParams{
		{
			ID:             uuid.New().String(),
			PrincipalName:  "admin_user",
			Action:         "QUERY",
			StatementType:  sql.NullString{String: "SELECT", Valid: true},
			OriginalSql:    sql.NullString{String: "SELECT * FROM t1", Valid: true},
			RewrittenSql:   sql.NullString{String: "SELECT * FROM t1 WHERE 1=1", Valid: true},
			TablesAccessed: sql.NullString{String: "main.t1", Valid: true},
			Status:         "ALLOWED",
			DurationMs:     sql.NullInt64{Int64: 42, Valid: true},
			RowsReturned:   sql.NullInt64{Int64: 10, Valid: true},
		},
		{
			ID:             uuid.New().String(),
			PrincipalName:  "analyst1",
			Action:         "QUERY",
			StatementType:  sql.NullString{String: "SELECT", Valid: true},
			OriginalSql:    sql.NullString{String: "SELECT * FROM t2", Valid: true},
			TablesAccessed: sql.NullString{String: "main.t2", Valid: true},
			Status:         "ALLOWED",
			DurationMs:     sql.NullInt64{Int64: 15, Valid: true},
			RowsReturned:   sql.NullInt64{Int64: 5, Valid: true},
		},
		{
			ID:            uuid.New().String(),
			PrincipalName: "admin_user",
			Action:        "QUERY",
			StatementType: sql.NullString{String: "SELECT", Valid: true},
			OriginalSql:   sql.NullString{String: "SELECT * FROM secret", Valid: true},
			Status:        "DENIED",
			ErrorMessage:  sql.NullString{String: "access denied", Valid: true},
		},
	}

	for _, e := range entries {
		require.NoError(t, q.InsertAuditLog(ctx, e))
	}
}

// TestHTTP_QueryHistoryList tests listing query history with various filters
// and pagination.
func TestHTTP_QueryHistoryList(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"seed_entries", func(t *testing.T) {
			seedQueryHistoryEntries(t, env)
		}},

		{"list_all", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/query-history",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 3, "expected at least 3 query history entries")

			// Verify entry fields
			entry := data[0].(map[string]interface{})
			assert.NotEmpty(t, entry["principal_name"])
			assert.NotEmpty(t, entry["status"])
		}},

		{"filter_by_principal", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/query-history?principal_name=admin_user",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 2)

			for _, item := range data {
				entry := item.(map[string]interface{})
				assert.Equal(t, "admin_user", entry["principal_name"])
			}
		}},

		{"filter_by_status", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/query-history?status=DENIED",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			require.GreaterOrEqual(t, len(data), 1)

			for _, item := range data {
				entry := item.(map[string]interface{})
				assert.Equal(t, "DENIED", entry["status"])
			}
		}},

		{"pagination", func(t *testing.T) {
			// First page: max_results=1
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/query-history?max_results=1",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Len(t, data, 1, "expected exactly 1 item on first page")

			// Should have a next page token
			nextToken, ok := result["next_page_token"]
			require.True(t, ok && nextToken != nil, "expected next_page_token")

			// Second page
			resp2 := doRequest(t, "GET",
				env.Server.URL+"/v1/query-history?max_results=1&page_token="+nextToken.(string),
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp2.StatusCode)

			var result2 map[string]interface{}
			decodeJSON(t, resp2, &result2)
			data2 := result2["data"].([]interface{})
			assert.Len(t, data2, 1, "expected exactly 1 item on second page")
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_QueryHistoryRequiresAdmin verifies that non-admin users get 403
// when listing query history (admin privilege required).
func TestHTTP_QueryHistoryRequiresAdmin(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	seedQueryHistoryEntries(t, env)

	// Non-admin should get 403.
	resp := doRequest(t, "GET",
		env.Server.URL+"/v1/query-history",
		env.Keys.Analyst, nil)
	require.Equal(t, 403, resp.StatusCode)
	_ = resp.Body.Close()

	// Admin should get 200.
	resp2 := doRequest(t, "GET",
		env.Server.URL+"/v1/query-history",
		env.Keys.Admin, nil)
	require.Equal(t, 200, resp2.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp2, &result)
	data := result["data"].([]interface{})
	assert.GreaterOrEqual(t, len(data), 3)
}
