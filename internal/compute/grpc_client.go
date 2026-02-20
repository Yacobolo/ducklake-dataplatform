package compute

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	grpcMethodExecute          = "/duckdemo.compute.v1.ComputeWorker/Execute"
	grpcMethodSubmitQuery      = "/duckdemo.compute.v1.ComputeWorker/SubmitQuery"
	grpcMethodGetQueryStatus   = "/duckdemo.compute.v1.ComputeWorker/GetQueryStatus"
	grpcMethodFetchQueryResult = "/duckdemo.compute.v1.ComputeWorker/FetchQueryResults"
	grpcMethodCancelQuery      = "/duckdemo.compute.v1.ComputeWorker/CancelQuery"
	grpcMethodDeleteQuery      = "/duckdemo.compute.v1.ComputeWorker/DeleteQuery"
	grpcMethodHealth           = "/duckdemo.compute.v1.ComputeWorker/Health"
)

type grpcWorkerClient struct {
	conn      *grpc.ClientConn
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

	return &grpcWorkerClient{conn: conn, authToken: authToken}, nil
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

func (c *grpcWorkerClient) close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *grpcWorkerClient) withMetadata(ctx context.Context, requestID string) context.Context {
	pairs := []string{"x-agent-token", c.authToken}
	if requestID != "" {
		pairs = append(pairs, "x-request-id", requestID)
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...))
}

func (c *grpcWorkerClient) invoke(ctx context.Context, method string, req, resp interface{}, requestID string) error {
	ctxWithMD := c.withMetadata(ctx, requestID)
	if err := c.conn.Invoke(ctxWithMD, method, req, resp); err != nil {
		return fmt.Errorf("grpc invoke %s: %w", method, err)
	}
	return nil
}

func (c *grpcWorkerClient) execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	var out ExecuteResponse
	if err := c.invoke(ctx, grpcMethodExecute, &req, &out, req.RequestID); err != nil {
		return ExecuteResponse{}, err
	}
	return out, nil
}

func (c *grpcWorkerClient) submitQuery(ctx context.Context, req SubmitQueryRequest) (SubmitQueryResponse, error) {
	var out SubmitQueryResponse
	if err := c.invoke(ctx, grpcMethodSubmitQuery, &req, &out, req.RequestID); err != nil {
		return SubmitQueryResponse{}, err
	}
	return out, nil
}

func (c *grpcWorkerClient) getQueryStatus(ctx context.Context, req GetQueryStatusRequest, requestID string) (QueryStatusResponse, error) {
	var out QueryStatusResponse
	if err := c.invoke(ctx, grpcMethodGetQueryStatus, &req, &out, requestID); err != nil {
		return QueryStatusResponse{}, err
	}
	return out, nil
}

func (c *grpcWorkerClient) fetchQueryResults(ctx context.Context, req FetchQueryResultsRequest, requestID string) (FetchQueryResultsResponse, error) {
	var out FetchQueryResultsResponse
	if err := c.invoke(ctx, grpcMethodFetchQueryResult, &req, &out, requestID); err != nil {
		return FetchQueryResultsResponse{}, err
	}
	return out, nil
}

func (c *grpcWorkerClient) cancelQuery(ctx context.Context, req CancelQueryRequest, requestID string) error {
	var out CancelQueryResponse
	return c.invoke(ctx, grpcMethodCancelQuery, &req, &out, requestID)
}

func (c *grpcWorkerClient) deleteQuery(ctx context.Context, req DeleteQueryRequest, requestID string) error {
	var out CancelQueryResponse
	return c.invoke(ctx, grpcMethodDeleteQuery, &req, &out, requestID)
}

func (c *grpcWorkerClient) health(ctx context.Context) (HealthResponse, error) {
	var out HealthResponse
	if err := c.invoke(ctx, grpcMethodHealth, &HealthRequest{}, &out, ""); err != nil {
		return HealthResponse{}, err
	}
	return out, nil
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
