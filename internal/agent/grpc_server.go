package agent

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"duck-demo/internal/compute"
	computeproto "duck-demo/internal/compute/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ComputeGRPCServer struct {
	cfg HandlerConfig

	activeQueries atomic.Int64
	jobs          *queryStore
}

type computeWorkerAdapter struct {
	computeproto.UnimplementedComputeWorkerServer

	server *ComputeGRPCServer
}

var _ computeproto.ComputeWorkerServer = (*computeWorkerAdapter)(nil)

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
func RegisterComputeWorkerGRPCServer(registrar grpc.ServiceRegistrar, server *ComputeGRPCServer) {
	computeproto.RegisterComputeWorkerServer(registrar, &computeWorkerAdapter{server: server})
}

func (a *computeWorkerAdapter) Execute(ctx context.Context, req *computeproto.ExecuteRequest) (*computeproto.ExecuteResponse, error) {
	if err := a.server.authorize(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	requestID := requestIDFromContext(req.Context)
	result, err := runQuery(ctx, a.server.cfg.DB, req.Sql, &a.server.activeQueries)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	rows := make([]*computeproto.ResultRow, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, protoRow(row))
	}

	return &computeproto.ExecuteResponse{
		Columns:  append([]string(nil), result.Columns...),
		Rows:     rows,
		RowCount: int64(result.RowCount),
	}, withRequestIDHeader(ctx, requestID)
}

func (a *computeWorkerAdapter) SubmitQuery(ctx context.Context, req *computeproto.SubmitQueryRequest) (*computeproto.SubmitQueryResponse, error) {
	if err := a.server.authorize(ctx); err != nil {
		return nil, err
	}
	if !a.server.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.Sql == "" {
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}

	requestID := requestIDFromContext(req.Context)
	a.server.jobs.maybeCleanup(time.Now())
	if existing, ok := a.server.jobs.getByRequestID(requestID); ok {
		state := existing.statusResponse()
		return &computeproto.SubmitQueryResponse{QueryId: existing.id, Status: state.Status}, withRequestIDHeader(ctx, requestID)
	}

	job := &queryJob{
		id:        newQueryID(),
		requestID: requestID,
		status:    compute.QueryStatusQueued,
		createdAt: time.Now(),
	}
	a.server.jobs.set(job)

	go func(sqlQuery string) {
		jobCtx, cancel := context.WithCancel(context.Background())
		job.setRunning(cancel)

		columns, tableName, rowCount, err := runQueryToTable(jobCtx, a.server.cfg.DB, sqlQuery, &a.server.activeQueries, job.id)
		if err != nil {
			if jobCtx.Err() == context.Canceled {
				job.setCanceled()
				return
			}
			job.setFailed(err)
			return
		}
		job.setSucceeded(columns, tableName, rowCount)
	}(req.Sql)

	return &computeproto.SubmitQueryResponse{QueryId: job.id, Status: compute.QueryStatusQueued}, withRequestIDHeader(ctx, requestID)
}

func (a *computeWorkerAdapter) GetQueryStatus(ctx context.Context, req *computeproto.GetQueryStatusRequest) (*computeproto.QueryStatusResponse, error) {
	if err := a.server.authorize(ctx); err != nil {
		return nil, err
	}
	if !a.server.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.QueryId == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	a.server.jobs.maybeCleanup(time.Now())
	job, ok := a.server.jobs.get(req.QueryId)
	if !ok {
		return nil, status.Error(codes.NotFound, "query not found")
	}
	state := job.statusResponse()
	return &computeproto.QueryStatusResponse{
		QueryId:            state.QueryID,
		Status:             state.Status,
		Error:              state.Error,
		Columns:            append([]string(nil), state.Columns...),
		RowCount:           int64(state.RowCount),
		CompletedAtRfc3339: state.CompletedAt,
	}, withRequestIDHeader(ctx, state.RequestID)
}

func (a *computeWorkerAdapter) FetchQueryResults(ctx context.Context, req *computeproto.FetchQueryResultsRequest) (*computeproto.FetchQueryResultsResponse, error) {
	if err := a.server.authorize(ctx); err != nil {
		return nil, err
	}
	if !a.server.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.QueryId == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	a.server.jobs.maybeCleanup(time.Now())
	job, ok := a.server.jobs.get(req.QueryId)
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
	limit := int(req.MaxResults)
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > 5000 {
		limit = 5000
	}

	page, err := job.resultPage(ctx, a.server.cfg.DB, offset, limit)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	rows := make([]*computeproto.ResultRow, 0, len(page.Rows))
	for _, row := range page.Rows {
		rows = append(rows, protoRow(row))
	}

	return &computeproto.FetchQueryResultsResponse{
		QueryId:       page.QueryID,
		Columns:       append([]string(nil), page.Columns...),
		Rows:          rows,
		RowCount:      int64(page.RowCount),
		NextPageToken: page.NextPageToken,
	}, withRequestIDHeader(ctx, page.RequestID)
}

func (a *computeWorkerAdapter) CancelQuery(ctx context.Context, req *computeproto.CancelQueryRequest) (*computeproto.CancelQueryResponse, error) {
	if err := a.server.authorize(ctx); err != nil {
		return nil, err
	}
	if !a.server.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.QueryId == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	a.server.jobs.maybeCleanup(time.Now())
	job, ok := a.server.jobs.get(req.QueryId)
	if !ok {
		return nil, status.Error(codes.NotFound, "query not found")
	}
	job.cancelQuery()
	state := job.statusResponse()
	return &computeproto.CancelQueryResponse{QueryId: req.QueryId, Status: state.Status}, withRequestIDHeader(ctx, state.RequestID)
}

func (a *computeWorkerAdapter) DeleteQuery(ctx context.Context, req *computeproto.DeleteQueryRequest) (*computeproto.DeleteQueryResponse, error) {
	if err := a.server.authorize(ctx); err != nil {
		return nil, err
	}
	if !a.server.cfg.CursorMode {
		return nil, status.Error(codes.Unimplemented, "query lifecycle is disabled")
	}
	if req == nil || req.QueryId == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	a.server.jobs.maybeCleanup(time.Now())
	job, ok := a.server.jobs.get(req.QueryId)
	if !ok {
		return nil, status.Error(codes.NotFound, "query not found")
	}
	job.cancelQuery()
	state := job.statusResponse()
	job.mu.RLock()
	tableName := job.tableName
	job.mu.RUnlock()
	if tableName != "" {
		_, _ = a.server.cfg.DB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %q", tableName)) //nolint:gosec
	}
	a.server.jobs.delete(req.QueryId)
	if state.Status == "" {
		state.Status = compute.QueryStatusCanceled
	}
	return &computeproto.DeleteQueryResponse{QueryId: req.QueryId, Status: state.Status}, withRequestIDHeader(ctx, state.RequestID)
}

func (a *computeWorkerAdapter) Health(ctx context.Context, _ *computeproto.HealthRequest) (*computeproto.HealthResponse, error) {
	if err := a.server.authorize(ctx); err != nil {
		return nil, err
	}

	a.server.jobs.maybeCleanup(time.Now())
	queuedJobs, runningJobs, completedJobs, storedJobs, cleanedJobs := a.server.jobs.metrics()
	resp := &computeproto.HealthResponse{
		Status:        "ok",
		UptimeSeconds: int64(time.Since(a.server.cfg.StartTime).Seconds()),
		ActiveQueries: a.server.activeQueries.Load(),
		QueuedJobs:    queuedJobs,
		RunningJobs:   runningJobs,
		CompletedJobs: completedJobs,
		StoredJobs:    storedJobs,
		CleanedJobs:   cleanedJobs,
		MaxMemoryGb:   int32(a.server.cfg.MaxMemoryGB),
		ResultTtlSecs: int32(a.server.jobs.ttl.Seconds()),
	}
	return resp, nil
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

func requestIDFromContext(ctx *computeproto.RequestContext) string {
	if ctx == nil {
		return ""
	}
	return ctx.RequestId
}

func withRequestIDHeader(ctx context.Context, requestID string) error {
	if requestID == "" {
		return nil
	}
	return grpc.SetHeader(ctx, metadata.Pairs("x-request-id", requestID))
}

func protoRow(row []interface{}) *computeproto.ResultRow {
	values := make([]string, len(row))
	for i := range row {
		if row[i] == nil {
			values[i] = ""
			continue
		}
		values[i] = fmt.Sprintf("%v", row[i])
	}
	return &computeproto.ResultRow{Values: values}
}

var queryIDCounter atomic.Uint64

func newQueryID() string {
	return fmt.Sprintf("q-%d", queryIDCounter.Add(1))
}
