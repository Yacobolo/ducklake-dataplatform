package flightsql

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"
)

// Server is a guarded Flight SQL listener placeholder.
// It provides feature-flagged isolation until full Flight SQL protocol handling lands.
type Server struct {
	addr   string
	logger *slog.Logger

	mu         sync.Mutex
	ln         net.Listener
	grpcServer *grpc.Server
	health     *grpcHealth.Server
	wg         sync.WaitGroup
}

func NewServer(addr string, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{addr: addr, logger: logger}
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return fmt.Errorf("flight sql listener already started")
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen flight sql: %w", err)
	}
	grpcSrv := grpc.NewServer()
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
