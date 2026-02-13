//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
)

// seedLineageEdges inserts test lineage edges directly via the repository.
// Graph:
//
//	main.orders ──READ──> main.revenue_summary ──READ──> main.monthly_report
//	main.customers ──READ──> main.revenue_summary
func seedLineageEdges(t *testing.T, env *httpTestEnv) {
	t.Helper()
	ctx := context.Background()
	repo := repository.NewLineageRepo(env.MetaDB)

	target := "main.revenue_summary"
	edges := []domain.LineageEdge{
		{SourceTable: "main.orders", TargetTable: &target, EdgeType: "READ", PrincipalName: "admin_user"},
		{SourceTable: "main.customers", TargetTable: &target, EdgeType: "READ", PrincipalName: "admin_user"},
	}
	for _, e := range edges {
		e := e
		require.NoError(t, repo.InsertEdge(ctx, &e))
	}

	// revenue_summary feeds into monthly_report
	target2 := "main.monthly_report"
	edge3 := domain.LineageEdge{
		SourceTable: "main.revenue_summary", TargetTable: &target2,
		EdgeType: "READ", PrincipalName: "analyst1",
	}
	require.NoError(t, repo.InsertEdge(ctx, &edge3))
}

// TestHTTP_LineageEndpoints tests full lineage, upstream, and downstream
// queries after seeding edges directly into the database.
func TestHTTP_LineageEndpoints(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"seed_edges", func(t *testing.T) {
			seedLineageEdges(t, env)
		}},

		{"full_lineage", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/lineage/tables/main/revenue_summary",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "main.revenue_summary", result["table_name"])

			upstream := result["upstream"].([]interface{})
			downstream := result["downstream"].([]interface{})
			assert.Len(t, upstream, 2, "expected 2 upstream edges")
			assert.Len(t, downstream, 1, "expected 1 downstream edge")
		}},

		{"upstream_only", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/lineage/tables/main/revenue_summary/upstream",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Len(t, data, 2)

			// Verify edge fields
			for _, item := range data {
				edge := item.(map[string]interface{})
				assert.NotEmpty(t, edge["source_table"])
				assert.Equal(t, "READ", edge["edge_type"])
				assert.NotEmpty(t, edge["principal_name"])
			}
		}},

		{"downstream_only", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/lineage/tables/main/revenue_summary/downstream",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Len(t, data, 1)

			edge := data[0].(map[string]interface{})
			assert.Equal(t, "main.revenue_summary", edge["source_table"])
		}},

		{"empty_lineage", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/lineage/tables/main/nonexistent",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			assert.Equal(t, "main.nonexistent", result["table_name"])

			// Upstream and downstream should be empty (or nil)
			if up, ok := result["upstream"]; ok && up != nil {
				assert.Empty(t, up.([]interface{}))
			}
			if down, ok := result["downstream"]; ok && down != nil {
				assert.Empty(t, down.([]interface{}))
			}
		}},

		{"leaf_node_no_downstream", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/lineage/tables/main/monthly_report/downstream",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			data := result["data"].([]interface{})
			assert.Empty(t, data, "leaf node should have no downstream edges")
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_DeleteLineageEdge tests DELETE /v1/lineage/edges/{edgeId}.
func TestHTTP_DeleteLineageEdge(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	var edgeID string

	steps := []step{
		{"seed_and_capture_edge_id", func(t *testing.T) {
			ctx := context.Background()
			repo := repository.NewLineageRepo(env.MetaDB)

			target := "main.del_target"
			e := domain.LineageEdge{
				SourceTable:   "main.del_source",
				TargetTable:   &target,
				EdgeType:      "READ",
				PrincipalName: "admin_user",
			}
			require.NoError(t, repo.InsertEdge(ctx, &e))

			// InsertEdge is :exec, so query the ID back
			err := env.MetaDB.QueryRowContext(ctx,
				`SELECT id FROM lineage_edges WHERE source_table = ? AND target_table = ?`,
				"main.del_source", "main.del_target",
			).Scan(&edgeID)
			require.NoError(t, err)
			require.NotEmpty(t, edgeID, "expected edge ID to be populated after insert")
		}},

		{"delete_edge_204", func(t *testing.T) {
			resp := doRequest(t, "DELETE",
				fmt.Sprintf("%s/v1/lineage/edges/%s", env.Server.URL, edgeID),
				env.Keys.Admin, nil)
			require.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},

		{"verify_edge_gone", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/lineage/tables/main/del_target",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			if up, ok := result["upstream"]; ok && up != nil {
				edges := up.([]interface{})
				for _, item := range edges {
					edge := item.(map[string]interface{})
					assert.NotEqual(t, edgeID, edge["id"],
						"deleted edge should not appear in upstream")
				}
			}
		}},

		{"delete_nonexistent_204_idempotent", func(t *testing.T) {
			// Deleting a nonexistent edge returns 204 (idempotent — the SQL
			// DELETE succeeds with 0 rows affected and no error).
			resp := doRequest(t, "DELETE",
				fmt.Sprintf("%s/v1/lineage/edges/%s", env.Server.URL, "nonexistent-id"),
				env.Keys.Admin, nil)
			assert.Equal(t, 204, resp.StatusCode)
			_ = resp.Body.Close()
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_PurgeLineage tests POST /v1/lineage/purge.
func TestHTTP_PurgeLineage(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{"seed_old_and_recent_edges", func(t *testing.T) {
			// Insert an old edge (100 days ago) directly via SQL
			_, err := env.MetaDB.ExecContext(ctx,
				`INSERT INTO lineage_edges (source_table, target_table, edge_type, principal_name, created_at)
				 VALUES (?, ?, ?, ?, datetime('now', '-100 days'))`,
				"main.old_source", "main.old_target", "READ", "admin_user",
			)
			require.NoError(t, err)

			// Insert a recent edge
			ctx := context.Background()
			repo := repository.NewLineageRepo(env.MetaDB)
			target := "main.recent_target"
			e := domain.LineageEdge{
				SourceTable:   "main.recent_source",
				TargetTable:   &target,
				EdgeType:      "READ",
				PrincipalName: "admin_user",
			}
			require.NoError(t, repo.InsertEdge(ctx, &e))
		}},

		{"purge_200", func(t *testing.T) {
			body := map[string]interface{}{
				"older_than_days": 30,
			}
			resp := doRequest(t, "POST",
				env.Server.URL+"/v1/lineage/purge",
				env.Keys.Admin, body)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			deletedCount := result["deleted_count"].(float64)
			assert.GreaterOrEqual(t, deletedCount, float64(1),
				"expected at least 1 old edge to be purged")
		}},

		{"verify_recent_survives", func(t *testing.T) {
			resp := doRequest(t, "GET",
				env.Server.URL+"/v1/lineage/tables/main/recent_target",
				env.Keys.Admin, nil)
			require.Equal(t, 200, resp.StatusCode)

			var result map[string]interface{}
			decodeJSON(t, resp, &result)
			upstream := result["upstream"].([]interface{})
			assert.GreaterOrEqual(t, len(upstream), 1,
				"recent edge should survive the purge")
		}},
	}

	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.FailNow()
		}
	}
}

// TestHTTP_LineageAnyUserCanQuery verifies that non-admin users can query
// lineage endpoints (no privilege checks enforced).
func TestHTTP_LineageAnyUserCanQuery(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	seedLineageEdges(t, env)

	resp := doRequest(t, "GET",
		env.Server.URL+"/v1/lineage/tables/main/revenue_summary",
		env.Keys.Analyst, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]interface{}
	decodeJSON(t, resp, &result)
	assert.Equal(t, "main.revenue_summary", result["table_name"])
}
