//go:build integration

package integration

import (
	"database/sql"
	"net/http"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DuckDB15_QueryExecutionSupportsNewSyntax(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true})

	ctx := t.Context()
	_, err := env.DuckDB.ExecContext(ctx, `CREATE OR REPLACE TABLE duckdb15_events AS
		SELECT {'name': 'Alice', 'scores': [1, 2, 3]}::VARIANT AS payload, ['vip', 'beta'] AS tags`)
	require.NoError(t, err)

	externalDBPath := filepath.Join(t.TempDir(), "numbers.duckdb")
	externalDB, err := sql.Open("duckdb", externalDBPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = externalDB.Close() })

	_, err = externalDB.ExecContext(ctx, `CREATE TABLE numbers AS SELECT 7 AS value`)
	require.NoError(t, err)

	t.Run("python_lambda", func(t *testing.T) {
		resp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/query-executions", env.Keys.Admin, map[string]any{
			"sql": `SELECT list_transform(tags, lambda x: upper(x)) AS transformed FROM main.duckdb15_events`,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode, responseBodyOnStatusMismatch(t, resp, http.StatusOK))

		var body struct {
			Rows []map[string]any `json:"rows"`
		}
		decodeJSON(t, resp, &body)
		require.Len(t, body.Rows, 1)
		assert.Equal(t, []any{"VIP", "BETA"}, body.Rows[0]["transformed"])
	})

	t.Run("variant_and_geometry", func(t *testing.T) {
		resp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/query-executions", env.Keys.Admin, map[string]any{
			"sql": `SELECT typeof(payload) AS payload_type, typeof(NULL::GEOMETRY) AS geometry_type, typeof(date_trunc('month', DATE '2026-03-27')) AS trunc_type FROM main.duckdb15_events`,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode, responseBodyOnStatusMismatch(t, resp, http.StatusOK))

		var body struct {
			Rows []map[string]any `json:"rows"`
		}
		decodeJSON(t, resp, &body)
		require.Len(t, body.Rows, 1)
		assert.Equal(t, "VARIANT", body.Rows[0]["payload_type"])
		assert.Equal(t, "GEOMETRY", body.Rows[0]["geometry_type"])
		assert.Equal(t, "TIMESTAMP", body.Rows[0]["trunc_type"])
	})

	t.Run("read_duckdb", func(t *testing.T) {
		resp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/query-executions", env.Keys.Admin, map[string]any{
			"sql": `SELECT min(value) AS min_value, max(value) AS max_value FROM read_duckdb('` + externalDBPath + `')`,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode, responseBodyOnStatusMismatch(t, resp, http.StatusOK))

		var body struct {
			Rows []map[string]any `json:"rows"`
		}
		decodeJSON(t, resp, &body)
		require.Len(t, body.Rows, 1)
		assert.EqualValues(t, 7, body.Rows[0]["min_value"])
		assert.EqualValues(t, 7, body.Rows[0]["max_value"])
	})
}
