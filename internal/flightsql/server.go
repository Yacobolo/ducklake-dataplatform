package flightsql

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	arrowflight "github.com/apache/arrow-go/v18/arrow/flight"
	arrowflightsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"
)

// Server is a guarded Flight SQL listener placeholder.
// It provides feature-flagged isolation until full Flight SQL protocol handling lands.
type Server struct {
	addr   string
	logger *slog.Logger
	query  QueryExecutor

	mu         sync.Mutex
	ln         net.Listener
	grpcServer *grpc.Server
	health     *grpcHealth.Server
	wg         sync.WaitGroup
}

func NewServer(addr string, logger *slog.Logger, query QueryExecutor) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if query == nil {
		query = func(_ context.Context, _ string, _ string) (*QueryResult, error) {
			return nil, fmt.Errorf("flight sql query executor is not configured")
		}
	}
	return &Server{addr: addr, logger: logger, query: query}
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return fmt.Errorf("flight sql listener already started")
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen flight sql: %w", err)
	}
	grpcSrv := grpc.NewServer()
	arrowflight.RegisterFlightServiceServer(grpcSrv, arrowflightsql.NewFlightServer(newQueryServer(ln.Addr().String(), s.logger, s.query)))
	healthSrv := grpcHealth.NewServer()
	healthSrv.SetServingStatus("", grpcHealthV1.HealthCheckResponse_SERVING)
	grpcHealthV1.RegisterHealthServer(grpcSrv, healthSrv)

	s.ln = ln
	s.grpcServer = grpcSrv
	s.health = healthSrv
	s.wg.Add(1)
	go s.serveLoop()
	s.logger.Info("Flight SQL listener enabled", "addr", ln.Addr().String())
	return nil
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	ln := s.ln
	grpcSrv := s.grpcServer
	s.ln = nil
	s.grpcServer = nil
	s.health = nil
	s.mu.Unlock()
	if ln == nil {
		return nil
	}

	if grpcSrv != nil {
		stopped := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-ctx.Done():
			grpcSrv.Stop()
			return fmt.Errorf("flight sql shutdown: %w", ctx.Err())
		case <-time.After(5 * time.Second):
			grpcSrv.Stop()
		}
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("flight sql shutdown wait: %w", ctx.Err())
	}
}

func (s *Server) serveLoop() {
	defer s.wg.Done()

	s.mu.Lock()
	ln := s.ln
	grpcSrv := s.grpcServer
	s.mu.Unlock()

	if ln == nil || grpcSrv == nil {
		return
	}
	if err := grpcSrv.Serve(ln); err != nil {
		s.logger.Debug("flight sql gRPC server stopped", "error", err)
	}
}
