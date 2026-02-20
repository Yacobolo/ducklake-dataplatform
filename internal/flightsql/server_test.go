package flightsql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServer_StartAndShutdown(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil)
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	addr := srv.Addr()
	require.NotEmpty(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	healthClient := grpcHealthV1.NewHealthClient(conn)
	resp, err := healthClient.Check(ctx, &grpcHealthV1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpcHealthV1.HealthCheckResponse_SERVING, resp.GetStatus())

	require.NoError(t, srv.Shutdown(ctx))
}
