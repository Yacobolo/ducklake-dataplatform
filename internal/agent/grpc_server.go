package agent

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"duck-demo/internal/compute"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type computeWorkerServer interface {
	Execute(context.Context, *compute.ExecuteRequest) (*compute.ExecuteResponse, error)
	SubmitQuery(context.Context, *compute.SubmitQueryRequest) (*compute.SubmitQueryResponse, error)
	GetQueryStatus(context.Context, *compute.GetQueryStatusRequest) (*compute.QueryStatusResponse, error)
	FetchQueryResults(context.Context, *compute.FetchQueryResultsRequest) (*compute.FetchQueryResultsResponse, error)
	CancelQuery(context.Context, *compute.CancelQueryRequest) (*compute.CancelQueryResponse, error)
	DeleteQuery(context.Context, *compute.DeleteQueryRequest) (*compute.CancelQueryResponse, error)
	Health(context.Context, *compute.HealthRequest) (*compute.HealthResponse, error)
}

type ComputeGRPCServer struct {
	cfg HandlerConfig

	activeQueries atomic.Int64
	jobs          *queryStore
}

func NewComputeGRPCServer(cfg HandlerConfig) *ComputeGRPCServer {
	jobs := newQueryStore(cfg.QueryResultTTL, cfg.CleanupInterval)
	jobs.db = cfg.DB
	return &ComputeGRPCServer{cfg: cfg, jobs: jobs}
}

func (s *ComputeGRPCServer) Metrics() (active, queued, running, completed, stored, cleaned int64) {
	queued, running, completed, stored, cleaned = s.jobs.metrics()
	return s.activeQueries.Load(), queued, running, completed, stored, cleaned
}

// RegisterComputeWorkerGRPCServer registers ComputeWorker gRPC methods.
func RegisterComputeWorkerGRPCServer(registrar grpc.ServiceRegistrar, server computeWorkerServer) {
	registrar.RegisterService(&computeWorkerServiceDesc, server)
}

func (s *ComputeGRPCServer) Execute(ctx context.Context, req *compute.ExecuteRequest) (*compute.ExecuteResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	result, err := runQuery(ctx, s.cfg.DB, req.SQL, &s.activeQueries)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	result.RequestID = req.RequestID
	return &result, nil
}

func (s *ComputeGRPCServer) SubmitQuery(ctx context.Context, req *compute.SubmitQueryRequest) (*compute.SubmitQueryResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if !s.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.SQL == "" {
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}

	requestID := req.RequestID
	s.jobs.maybeCleanup(time.Now())
	if existing, ok := s.jobs.getByRequestID(requestID); ok {
		state := existing.statusResponse()
		return &compute.SubmitQueryResponse{QueryID: existing.id, Status: state.Status, RequestID: requestID}, nil
	}

	job := &queryJob{
		id:        newQueryID(),
		requestID: requestID,
		status:    compute.QueryStatusQueued,
		createdAt: time.Now(),
	}
	s.jobs.set(job)

	go func(sqlQuery string) {
		jobCtx, cancel := context.WithCancel(context.Background())
		job.setRunning(cancel)

		columns, tableName, rowCount, err := runQueryToTable(jobCtx, s.cfg.DB, sqlQuery, &s.activeQueries, job.id)
		if err != nil {
			if jobCtx.Err() == context.Canceled {
				job.setCanceled()
				return
			}
			job.setFailed(err)
			return
		}
		job.setSucceeded(columns, tableName, rowCount)
	}(req.SQL)

	return &compute.SubmitQueryResponse{QueryID: job.id, Status: compute.QueryStatusQueued, RequestID: requestID}, nil
}

func (s *ComputeGRPCServer) GetQueryStatus(ctx context.Context, req *compute.GetQueryStatusRequest) (*compute.QueryStatusResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if !s.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.QueryID == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	s.jobs.maybeCleanup(time.Now())
	job, ok := s.jobs.get(req.QueryID)
	if !ok {
		return nil, status.Error(codes.NotFound, "query not found")
	}
	out := job.statusResponse()
	return &out, nil
}

func (s *ComputeGRPCServer) FetchQueryResults(ctx context.Context, req *compute.FetchQueryResultsRequest) (*compute.FetchQueryResultsResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if !s.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.QueryID == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	s.jobs.maybeCleanup(time.Now())
	job, ok := s.jobs.get(req.QueryID)
	if !ok {
		return nil, status.Error(codes.NotFound, "query not found")
	}

	state := job.statusResponse()
	switch state.Status {
	case compute.QueryStatusQueued, compute.QueryStatusRunning:
		return nil, status.Error(codes.FailedPrecondition, "query is not ready")
	case compute.QueryStatusFailed, compute.QueryStatusCanceled:
		return nil, status.Error(codes.FailedPrecondition, state.Error)
	}

	offset := compute.DecodePageToken(req.PageToken)
	limit := req.MaxResults
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > 5000 {
		limit = 5000
	}

	page, err := job.resultPage(ctx, s.cfg.DB, offset, limit)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &page, nil
}

func (s *ComputeGRPCServer) CancelQuery(ctx context.Context, req *compute.CancelQueryRequest) (*compute.CancelQueryResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if !s.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.QueryID == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	s.jobs.maybeCleanup(time.Now())
	job, ok := s.jobs.get(req.QueryID)
	if !ok {
		return nil, status.Error(codes.NotFound, "query not found")
	}
	job.cancelQuery()
	state := job.statusResponse()
	return &compute.CancelQueryResponse{QueryID: req.QueryID, Status: state.Status, RequestID: state.RequestID}, nil
}

func (s *ComputeGRPCServer) DeleteQuery(ctx context.Context, req *compute.DeleteQueryRequest) (*compute.CancelQueryResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if !s.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.QueryID == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	s.jobs.maybeCleanup(time.Now())
	job, ok := s.jobs.get(req.QueryID)
	if !ok {
		return nil, status.Error(codes.NotFound, "query not found")
	}
	job.cancelQuery()
	state := job.statusResponse()
	job.mu.RLock()
	tableName := job.tableName
	job.mu.RUnlock()
	if tableName != "" {
		_, _ = s.cfg.DB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %q", tableName)) //nolint:gosec
	}
	s.jobs.delete(req.QueryID)
	if state.Status == "" {
		state.Status = compute.QueryStatusCanceled
	}
	return &compute.CancelQueryResponse{QueryID: req.QueryID, Status: state.Status, RequestID: state.RequestID}, nil
}

func (s *ComputeGRPCServer) Health(ctx context.Context, _ *compute.HealthRequest) (*compute.HealthResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}

	s.jobs.maybeCleanup(time.Now())
	queuedJobs, runningJobs, completedJobs, storedJobs, cleanedJobs := s.jobs.metrics()
	return &compute.HealthResponse{
		Status:                "ok",
		UptimeSeconds:         int(time.Since(s.cfg.StartTime).Seconds()),
		ActiveQueries:         s.activeQueries.Load(),
		QueuedJobs:            queuedJobs,
		RunningJobs:           runningJobs,
		CompletedJobs:         completedJobs,
		StoredJobs:            storedJobs,
		CleanedJobs:           cleanedJobs,
		MaxMemoryGB:           s.cfg.MaxMemoryGB,
		QueryResultTTLSeconds: int(s.jobs.ttl.Seconds()),
	}, nil
}

func (s *ComputeGRPCServer) authorize(ctx context.Context) error {
	if s.cfg.AgentToken == "" {
		return nil
	}
	if metadataValue(ctx, "x-agent-token") == s.cfg.AgentToken {
		return nil
	}
	return status.Error(codes.Unauthenticated, "unauthorized")
}

func metadataValue(ctx context.Context, key string) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

var queryIDCounter atomic.Uint64

func newQueryID() string {
	return fmt.Sprintf("q-%d", queryIDCounter.Add(1))
}

var computeWorkerServiceDesc = grpc.ServiceDesc{
	ServiceName: "duckdemo.compute.v1.ComputeWorker",
	HandlerType: (*computeWorkerServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Execute", Handler: executeHandler},
		{MethodName: "SubmitQuery", Handler: submitQueryHandler},
		{MethodName: "GetQueryStatus", Handler: getQueryStatusHandler},
		{MethodName: "FetchQueryResults", Handler: fetchQueryResultsHandler},
		{MethodName: "CancelQuery", Handler: cancelQueryHandler},
		{MethodName: "DeleteQuery", Handler: deleteQueryHandler},
		{MethodName: "Health", Handler: healthHandler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "internal/compute/proto/compute_worker.proto",
}

func executeHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(compute.ExecuteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(computeWorkerServer).Execute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcMethodExecute}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(computeWorkerServer).Execute(ctx, req.(*compute.ExecuteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func submitQueryHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(compute.SubmitQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(computeWorkerServer).SubmitQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcMethodSubmitQuery}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(computeWorkerServer).SubmitQuery(ctx, req.(*compute.SubmitQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func getQueryStatusHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(compute.GetQueryStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(computeWorkerServer).GetQueryStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcMethodGetQueryStatus}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(computeWorkerServer).GetQueryStatus(ctx, req.(*compute.GetQueryStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func fetchQueryResultsHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(compute.FetchQueryResultsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(computeWorkerServer).FetchQueryResults(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcMethodFetchQueryResult}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(computeWorkerServer).FetchQueryResults(ctx, req.(*compute.FetchQueryResultsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func cancelQueryHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(compute.CancelQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(computeWorkerServer).CancelQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcMethodCancelQuery}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(computeWorkerServer).CancelQuery(ctx, req.(*compute.CancelQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func deleteQueryHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(compute.DeleteQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(computeWorkerServer).DeleteQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcMethodDeleteQuery}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(computeWorkerServer).DeleteQuery(ctx, req.(*compute.DeleteQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func healthHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(compute.HealthRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(computeWorkerServer).Health(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcMethodHealth}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(computeWorkerServer).Health(ctx, req.(*compute.HealthRequest))
	}
	return interceptor(ctx, in, info, handler)
}

const (
	grpcMethodExecute          = "/duckdemo.compute.v1.ComputeWorker/Execute"
	grpcMethodSubmitQuery      = "/duckdemo.compute.v1.ComputeWorker/SubmitQuery"
	grpcMethodGetQueryStatus   = "/duckdemo.compute.v1.ComputeWorker/GetQueryStatus"
	grpcMethodFetchQueryResult = "/duckdemo.compute.v1.ComputeWorker/FetchQueryResults"
	grpcMethodCancelQuery      = "/duckdemo.compute.v1.ComputeWorker/CancelQuery"
	grpcMethodDeleteQuery      = "/duckdemo.compute.v1.ComputeWorker/DeleteQuery"
	grpcMethodHealth           = "/duckdemo.compute.v1.ComputeWorker/Health"
)
