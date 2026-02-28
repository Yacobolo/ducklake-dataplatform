package compute

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	computeproto "duck-demo/internal/compute/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type grpcWorkerClient struct {
	conn      *grpc.ClientConn
	client    computeproto.ComputeWorkerClient
	authToken string
}

func newGRPCWorkerClient(endpointURL, authToken string) (*grpcWorkerClient, error) {
	EnsureGRPCJSONCodec()

	target, secure, err := grpcDialTarget(endpointURL)
	if err != nil {
		return nil, err
	}

	creds := insecure.NewCredentials()
	if secure {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(grpcJSONCodecName)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial grpc worker: %w", err)
	}

	return &grpcWorkerClient{conn: conn, client: computeproto.NewComputeWorkerClient(conn), authToken: authToken}, nil
}

func grpcDialTarget(endpointURL string) (target string, secure bool, err error) {
	u, parseErr := url.Parse(endpointURL)
	if parseErr != nil {
		return "", false, fmt.Errorf("parse endpoint url: %w", parseErr)
	}

	scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
	switch scheme {
	case "grpc", "grpcs":
		if u.Host == "" {
			return "", false, fmt.Errorf("grpc endpoint host is required")
		}
		return u.Host, scheme == "grpcs", nil
	default:
		return "", false, fmt.Errorf("grpc transport requires grpc:// or grpcs:// endpoint")
	}
}

func (c *grpcWorkerClient) withMetadata(ctx context.Context, requestID string) context.Context {
	pairs := []string{"x-agent-token", c.authToken}
	if requestID != "" {
		pairs = append(pairs, "x-request-id", requestID)
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...))
}

func (c *grpcWorkerClient) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, 30*time.Second)
}

func (c *grpcWorkerClient) execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	ctx = c.withMetadata(ctx, req.RequestID)
	out, err := c.client.Execute(ctx, &computeproto.ExecuteRequest{
		Sql: req.SQL,
		Context: &computeproto.RequestContext{
			RequestId: req.RequestID,
		},
	})
	if err != nil {
		return ExecuteResponse{}, err
	}
	return executeResponseFromProto(out), nil
}

func (c *grpcWorkerClient) submitQuery(ctx context.Context, req SubmitQueryRequest) (SubmitQueryResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	ctx = c.withMetadata(ctx, req.RequestID)
	out, err := c.client.SubmitQuery(ctx, &computeproto.SubmitQueryRequest{
		Sql: req.SQL,
		Context: &computeproto.RequestContext{
			RequestId: req.RequestID,
		},
	})
	if err != nil {
		return SubmitQueryResponse{}, err
	}
	return submitQueryResponseFromProto(out, req.RequestID), nil
}

func (c *grpcWorkerClient) getQueryStatus(ctx context.Context, req GetQueryStatusRequest, requestID string) (QueryStatusResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	ctx = c.withMetadata(ctx, requestID)
	out, err := c.client.GetQueryStatus(ctx, &computeproto.GetQueryStatusRequest{QueryId: req.QueryID})
	if err != nil {
		return QueryStatusResponse{}, err
	}
	return queryStatusResponseFromProto(out, requestID), nil
}

func (c *grpcWorkerClient) fetchQueryResults(ctx context.Context, req FetchQueryResultsRequest, requestID string) (FetchQueryResultsResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	ctx = c.withMetadata(ctx, requestID)
	maxResults := int32(0)
	if req.MaxResults > 0 {
		if req.MaxResults > math.MaxInt32 {
			maxResults = math.MaxInt32
		} else {
			maxResults = int32(req.MaxResults)
		}
	}
	out, err := c.client.FetchQueryResults(ctx, &computeproto.FetchQueryResultsRequest{
		QueryId:    req.QueryID,
		PageToken:  req.PageToken,
		MaxResults: maxResults,
	})
	if err != nil {
		return FetchQueryResultsResponse{}, err
	}
	return fetchQueryResultsResponseFromProto(out, requestID), nil
}

func (c *grpcWorkerClient) cancelQuery(ctx context.Context, req CancelQueryRequest, requestID string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	ctx = c.withMetadata(ctx, requestID)
	_, err := c.client.CancelQuery(ctx, &computeproto.CancelQueryRequest{QueryId: req.QueryID})
	return err
}

func (c *grpcWorkerClient) deleteQuery(ctx context.Context, req DeleteQueryRequest, requestID string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	ctx = c.withMetadata(ctx, requestID)
	_, err := c.client.DeleteQuery(ctx, &computeproto.DeleteQueryRequest{QueryId: req.QueryID})
	return err
}

func (c *grpcWorkerClient) health(ctx context.Context) (HealthResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	ctx = c.withMetadata(ctx, "")
	out, err := c.client.Health(ctx, &computeproto.HealthRequest{})
	if err != nil {
		return HealthResponse{}, err
	}
	return healthResponseFromProto(out), nil
}

func isGRPCUnavailable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unimplemented, codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func executeResponseFromProto(resp *computeproto.ExecuteResponse) ExecuteResponse {
	if resp == nil {
		return ExecuteResponse{}
	}
	out := ExecuteResponse{
		Columns:  append([]string(nil), resp.Columns...),
		Rows:     rowsFromProto(resp.Rows),
		RowCount: int(resp.RowCount),
	}
	return out
}

func submitQueryResponseFromProto(resp *computeproto.SubmitQueryResponse, requestID string) SubmitQueryResponse {
	if resp == nil {
		return SubmitQueryResponse{RequestID: requestID}
	}
	return SubmitQueryResponse{
		QueryID:   resp.QueryId,
		Status:    resp.Status,
		RequestID: requestID,
	}
}

func queryStatusResponseFromProto(resp *computeproto.QueryStatusResponse, requestID string) QueryStatusResponse {
	if resp == nil {
		return QueryStatusResponse{RequestID: requestID}
	}
	return QueryStatusResponse{
		QueryID:     resp.QueryId,
		Status:      resp.Status,
		Columns:     append([]string(nil), resp.Columns...),
		RowCount:    int(resp.RowCount),
		Error:       resp.Error,
		CompletedAt: resp.CompletedAtRfc3339,
		RequestID:   requestID,
	}
}

func fetchQueryResultsResponseFromProto(resp *computeproto.FetchQueryResultsResponse, requestID string) FetchQueryResultsResponse {
	if resp == nil {
		return FetchQueryResultsResponse{RequestID: requestID}
	}
	return FetchQueryResultsResponse{
		QueryID:       resp.QueryId,
		Columns:       append([]string(nil), resp.Columns...),
		Rows:          rowsFromProto(resp.Rows),
		RowCount:      int(resp.RowCount),
		NextPageToken: resp.NextPageToken,
		RequestID:     requestID,
	}
}

func healthResponseFromProto(resp *computeproto.HealthResponse) HealthResponse {
	if resp == nil {
		return HealthResponse{}
	}
	return HealthResponse{
		Status:                resp.Status,
		UptimeSeconds:         int(resp.UptimeSeconds),
		DuckDBVersion:         resp.DuckdbVersion,
		MemoryUsedMB:          resp.MemoryUsedMb,
		MaxMemoryGB:           int(resp.MaxMemoryGb),
		ActiveQueries:         resp.ActiveQueries,
		QueuedJobs:            resp.QueuedJobs,
		RunningJobs:           resp.RunningJobs,
		CompletedJobs:         resp.CompletedJobs,
		StoredJobs:            resp.StoredJobs,
		CleanedJobs:           resp.CleanedJobs,
		QueryResultTTLSeconds: int(resp.ResultTtlSecs),
	}
}

func rowsFromProto(rows []*computeproto.ResultRow) [][]interface{} {
	out := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			out = append(out, nil)
			continue
		}
		values := make([]interface{}, len(row.Values))
		for i := range row.Values {
			values[i] = row.Values[i]
		}
		out = append(out, values)
	}
	return out
}
