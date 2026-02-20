package compute

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"duck-demo/internal/domain"
)

var _ domain.ComputeExecutor = (*RemoteExecutor)(nil)

// RemoteExecutor sends pre-secured SQL to a remote compute agent via gRPC
// and materializes results into a local DuckDB temp table to return *sql.Rows.
type RemoteExecutor struct {
	endpointURL string
	authToken   string
	localDB     *sql.DB // for temp table materialization
	cursorMode  bool
	grpcClient  *grpcWorkerClient
	grpcMu      sync.Mutex
}

// NewRemoteExecutor creates a RemoteExecutor that sends queries to the given
// endpoint URL and materializes results into the local DuckDB instance.
func NewRemoteExecutor(endpointURL, authToken string, localDB *sql.DB, opts ...RemoteExecutorOptions) *RemoteExecutor {
	options := RemoteExecutorOptions{CursorModeEnabled: true, InternalGRPC: true}
	if len(opts) > 0 {
		options = opts[0]
	}

	return &RemoteExecutor{
		endpointURL: strings.TrimRight(endpointURL, "/"),
		authToken:   authToken,
		localDB:     localDB,
		cursorMode:  options.CursorModeEnabled,
	}
}

// QueryContext sends the query to the remote agent and materializes the result
// into a local DuckDB temp table, returning *sql.Rows from that table.
func (e *RemoteExecutor) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	requestID := uuid.New().String()
	client, err := e.ensureGRPCClient()
	if err != nil {
		return nil, err
	}

	if e.cursorMode {
		rows, lifecycleErr := e.queryViaGRPCLifecycleToRows(ctx, client, query, requestID)
		if lifecycleErr == nil {
			return rows, nil
		}
		if !isGRPCUnavailable(lifecycleErr) {
			return nil, lifecycleErr
		}
	}

	result, err := client.execute(ctx, ExecuteRequest{SQL: query, RequestID: requestID})
	if err != nil {
		return nil, fmt.Errorf("grpc execute: %w", err)
	}
	if result.Error != "" {
		return nil, fmt.Errorf("remote execution failed: %s", result.Error)
	}
	return e.materialize(ctx, result)
}

func (e *RemoteExecutor) queryViaGRPCLifecycleToRows(ctx context.Context, client *grpcWorkerClient, query, requestID string) (*sql.Rows, error) {
	submitResp, err := client.submitQuery(ctx, SubmitQueryRequest{SQL: query, RequestID: requestID})
	if err != nil {
		return nil, fmt.Errorf("submit grpc query lifecycle request: %w", err)
	}
	if submitResp.QueryID == "" {
		return nil, fmt.Errorf("submit grpc query lifecycle request failed: missing query id")
	}

	queryID := submitResp.QueryID
	defer func() {
		_ = client.deleteQuery(context.Background(), DeleteQueryRequest{QueryID: queryID}, requestID)
	}()

	statusResp, err := e.waitForQueryCompletionGRPC(ctx, client, queryID, requestID)
	if err != nil {
		return nil, err
	}
	if statusResp.Status == QueryStatusFailed || statusResp.Status == QueryStatusCanceled {
		errMsg := statusResp.Error
		if errMsg == "" {
			errMsg = "query did not complete successfully"
		}
		return nil, fmt.Errorf("remote execution failed: %s", errMsg)
	}

	result, err := e.fetchAllPagesGRPC(ctx, client, queryID, requestID)
	if err != nil {
		return nil, err
	}
	return e.materialize(ctx, result)
}

func (e *RemoteExecutor) waitForQueryCompletionGRPC(ctx context.Context, client *grpcWorkerClient, queryID, requestID string) (QueryStatusResponse, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		statusResp, err := client.getQueryStatus(ctx, GetQueryStatusRequest{QueryID: queryID}, requestID)
		if err != nil {
			return QueryStatusResponse{}, fmt.Errorf("query status failed: %w", err)
		}

		switch statusResp.Status {
		case QueryStatusSucceeded, QueryStatusFailed, QueryStatusCanceled:
			return statusResp, nil
		}

		select {
		case <-ctx.Done():
			_ = client.cancelQuery(context.Background(), CancelQueryRequest{QueryID: queryID}, requestID)
			return QueryStatusResponse{}, fmt.Errorf("wait for query completion: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (e *RemoteExecutor) fetchAllPagesGRPC(ctx context.Context, client *grpcWorkerClient, queryID, requestID string) (ExecuteResponse, error) {
	pageToken := ""
	result := ExecuteResponse{Columns: nil, Rows: make([][]interface{}, 0)}

	for {
		pageResp, err := client.fetchQueryResults(ctx, FetchQueryResultsRequest{
			QueryID:   queryID,
			PageToken: pageToken,
		}, requestID)
		if err != nil {
			return ExecuteResponse{}, fmt.Errorf("fetch query results failed: %w", err)
		}

		if result.Columns == nil {
			result.Columns = pageResp.Columns
		}
		result.Rows = append(result.Rows, pageResp.Rows...)
		result.RowCount = pageResp.RowCount
		result.RequestID = pageResp.RequestID

		if pageResp.NextPageToken == "" {
			if result.RowCount == 0 {
				result.RowCount = len(result.Rows)
			}
			return result, nil
		}
		pageToken = pageResp.NextPageToken
	}
}

func (e *RemoteExecutor) ensureGRPCClient() (*grpcWorkerClient, error) {
	e.grpcMu.Lock()
	defer e.grpcMu.Unlock()

	if e.grpcClient != nil {
		return e.grpcClient, nil
	}
	client, err := newGRPCWorkerClient(e.endpointURL, e.authToken)
	if err != nil {
		return nil, err
	}
	e.grpcClient = client
	return client, nil
}

// materialize creates a DuckDB temp table from the remote response and returns
// *sql.Rows over it. Uses a pinned connection for DDL and inserts, then closes
// it and queries the temp table from the pool, preventing connection leaks.
func (e *RemoteExecutor) materialize(ctx context.Context, result ExecuteResponse) (*sql.Rows, error) {
	if len(result.Columns) == 0 {
		return e.localDB.QueryContext(ctx, "SELECT 1 WHERE false")
	}

	suffix := randomSuffix()
	tableName := "_remote_result_" + suffix

	if err := e.populateTempTable(ctx, tableName, result); err != nil {
		return nil, err
	}

	selectSQL := fmt.Sprintf("SELECT * FROM %q", tableName) //nolint:gosec // tableName is generated internally, not user input
	rows, err := e.localDB.QueryContext(ctx, selectSQL)
	if err != nil {
		return nil, fmt.Errorf("select from temp: %w", err)
	}

	return rows, nil
}

func (e *RemoteExecutor) populateTempTable(ctx context.Context, tableName string, result ExecuteResponse) error {
	conn, err := e.localDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pin connection: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	if err := createResultTable(ctx, conn, tableName, result.Columns); err != nil {
		return err
	}
	if err := insertResultRows(ctx, conn, tableName, result.Rows, result.Columns); err != nil {
		return err
	}

	return nil
}

func createResultTable(ctx context.Context, conn *sql.Conn, tableName string, columns []string) error {
	if len(columns) == 0 {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %q (_empty VARCHAR)", tableName)); err != nil {
			return fmt.Errorf("create temp table: %w", err)
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %q DROP COLUMN _empty", tableName)); err != nil {
			return fmt.Errorf("prepare empty temp table: %w", err)
		}
		return nil
	}

	colDefs := make([]string, 0, len(columns))
	for _, col := range columns {
		colDefs = append(colDefs, fmt.Sprintf("%q VARCHAR", col))
	}
	createSQL := fmt.Sprintf("CREATE TABLE %q (%s)", tableName, strings.Join(colDefs, ", "))
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}
	return nil
}

func insertResultRows(ctx context.Context, conn *sql.Conn, tableName string, rows [][]interface{}, columns []string) error {
	if len(rows) == 0 || len(columns) == 0 {
		return nil
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO %q VALUES (%s)", tableName, strings.Join(placeholders, ", ")) //nolint:gosec // tableName is generated internally
	for _, row := range rows {
		args := make([]interface{}, len(columns))
		for i := range columns {
			if i < len(row) {
				if row[i] == nil {
					args[i] = nil
				} else {
					args[i] = fmt.Sprintf("%v", row[i])
				}
				continue
			}
			args[i] = nil
		}
		if _, err := conn.ExecContext(ctx, insertSQL, args...); err != nil {
			return fmt.Errorf("insert row: %w", err)
		}
	}
	return nil
}

// Ping performs a health check against the remote agent.
func (e *RemoteExecutor) Ping(ctx context.Context) error {
	if strings.HasPrefix(e.endpointURL, "http://") || strings.HasPrefix(e.endpointURL, "https://") {
		client := &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			},
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.endpointURL+"/health", nil)
		if err != nil {
			return fmt.Errorf("create health request: %w", err)
		}
		req.Header.Set("X-Agent-Token", e.authToken)

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("health check: %w", err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unhealthy: status %d", resp.StatusCode)
		}
		return nil
	}

	client, err := e.ensureGRPCClient()
	if err != nil {
		return err
	}
	if _, err := client.health(ctx); err != nil {
		return fmt.Errorf("grpc health check: %w", err)
	}
	return nil
}

// randomSuffix generates a cryptographically random hex suffix for temp table names.
func randomSuffix() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "fallback"
	}
	return hex.EncodeToString(b)
}
