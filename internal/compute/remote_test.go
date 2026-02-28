package compute_test

import (
	"context"
	"database/sql"
	"log/slog"
	"net"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"duck-demo/internal/agent"
	"duck-demo/internal/compute"
)

func TestRemoteExecutor_QueryContext(t *testing.T) {
	localDB := openTestDuckDB(t)

	t.Run("query_lifecycle_endpoints", func(t *testing.T) {
		exec := newGRPCRemoteExecutor(t, localDB, true)
		rows, err := exec.QueryContext(context.Background(), "SELECT 1 AS id UNION ALL SELECT 2 AS id")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		var got []string
		for rows.Next() {
			var id string
			require.NoError(t, rows.Scan(&id))
			got = append(got, id)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"1", "2"}, got)
	})

	t.Run("successful_query_execute_path", func(t *testing.T) {
		exec := newGRPCRemoteExecutor(t, localDB, false)
		rows, err := exec.QueryContext(context.Background(), "SELECT 1 AS id, 'Alice' AS name")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		cols, err := rows.Columns()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, cols)

		require.True(t, rows.Next())
		var id, name string
		require.NoError(t, rows.Scan(&id, &name))
		assert.Equal(t, "1", id)
		assert.Equal(t, "Alice", name)
		require.NoError(t, rows.Err())
	})

	t.Run("empty_result", func(t *testing.T) {
		exec := newGRPCRemoteExecutor(t, localDB, false)
		rows, err := exec.QueryContext(context.Background(), "SELECT 1 WHERE false")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		assert.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})

	t.Run("agent_error_response", func(t *testing.T) {
		exec := newGRPCRemoteExecutor(t, localDB, false)
		_, err := exec.QueryContext(context.Background(), "SELECT * FROM nonexistent") //nolint:sqlclosecheck,rowserrcheck // error path
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nonexistent")
	})

	t.Run("connection_refused", func(t *testing.T) {
		exec := compute.NewRemoteExecutor("grpc://127.0.0.1:1", "tok", localDB)
		_, err := exec.QueryContext(context.Background(), "SELECT 1") //nolint:sqlclosecheck,rowserrcheck // error path
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("null_values_in_response", func(t *testing.T) {
		exec := newGRPCRemoteExecutor(t, localDB, false)
		rows, err := exec.QueryContext(context.Background(), "SELECT 1 AS id, NULL AS name UNION ALL SELECT 2 AS id, 'Bob' AS name")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		var count int
		for rows.Next() {
			var id, name sql.NullString
			require.NoError(t, rows.Scan(&id, &name))
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 2, count)
	})
}

func TestRemoteExecutor_Ping(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		exec := newGRPCRemoteExecutor(t, openTestDuckDB(t), true)
		err := exec.Ping(context.Background())
		require.NoError(t, err)
	})

	t.Run("unreachable", func(t *testing.T) {
		exec := compute.NewRemoteExecutor("grpc://127.0.0.1:1", "tok", openTestDuckDB(t))
		err := exec.Ping(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})
}

func newGRPCRemoteExecutor(t *testing.T, localDB *sql.DB, cursorMode bool) *compute.RemoteExecutor {
	t.Helper()

	agentDB := openTestDuckDB(t)
	grpcAgent := agent.NewComputeGRPCServer(agent.HandlerConfig{
		DB:         agentDB,
		AgentToken: "tok",
		StartTime:  time.Now(),
		CursorMode: true,
		Logger:     slog.Default(),
	})

	compute.EnsureGRPCJSONCodec()
	grpcServer := grpc.NewServer()
	agent.RegisterComputeWorkerGRPCServer(grpcServer, grpcAgent)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = ln.Close()
	})
	go func() {
		_ = grpcServer.Serve(ln)
	}()

	return compute.NewRemoteExecutor("grpc://"+ln.Addr().String(), "tok", localDB, compute.RemoteExecutorOptions{
		CursorModeEnabled: cursorMode,
		InternalGRPC:      true,
	})
}

func openTestDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}
