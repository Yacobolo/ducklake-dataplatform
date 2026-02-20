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

func TestRemoteExecutor_QueryContext_GRPC(t *testing.T) {
	agentDB := openDuckDB(t)
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

	localDB := openDuckDB(t)
	exec := compute.NewRemoteExecutor("grpc://"+ln.Addr().String(), "tok", localDB, compute.RemoteExecutorOptions{
		CursorModeEnabled: true,
		InternalGRPC:      true,
	})

	rows, err := exec.QueryContext(context.Background(), "SELECT 42 AS answer")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	require.True(t, rows.Next())
	var answer string
	require.NoError(t, rows.Scan(&answer))
	assert.Equal(t, "42", answer)
	require.NoError(t, rows.Err())
}

func openDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}
