package compute

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/duckdb/duckdb-go/v2"
)

func openTestDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestLocalExecutor_QueryContext(t *testing.T) {
	db := openTestDuckDB(t)
	exec := NewLocalExecutor(db)

	t.Run("simple_select", func(t *testing.T) {
		rows, err := exec.QueryContext(context.Background(), "SELECT 42 AS answer")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		require.True(t, rows.Next())
		var answer int
		require.NoError(t, rows.Scan(&answer))
		assert.Equal(t, 42, answer)
		assert.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})

	t.Run("multiple_rows", func(t *testing.T) {
		rows, err := exec.QueryContext(context.Background(), "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		cols, err := rows.Columns()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, cols)

		count := 0
		for rows.Next() {
			count++
			var id int
			var name string
			require.NoError(t, rows.Scan(&id, &name))
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 3, count)
	})

	t.Run("empty_result", func(t *testing.T) {
		rows, err := exec.QueryContext(context.Background(), "SELECT 1 WHERE false")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		assert.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})

	t.Run("invalid_sql", func(t *testing.T) {
		_, err := exec.QueryContext(context.Background(), "SELEKT invalid") //nolint:sqlclosecheck,rowserrcheck // error path, rows is nil
		require.Error(t, err)
	})
}
