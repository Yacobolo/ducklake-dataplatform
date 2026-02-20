package compute

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"duck-demo/internal/domain"
)

var _ domain.ComputeExecutor = (*RemoteExecutor)(nil)

// RemoteExecutor sends pre-secured SQL to a remote compute agent via HTTP
// and materializes results into a local DuckDB temp table to return *sql.Rows.
type RemoteExecutor struct {
	endpointURL string
	authToken   string
	localDB     *sql.DB // for temp table materialization
	httpClient  *http.Client
	cursorMode  bool
}

// NewRemoteExecutor creates a RemoteExecutor that sends queries to the given
// endpoint URL and materializes results into the local DuckDB instance.
func NewRemoteExecutor(endpointURL, authToken string, localDB *sql.DB, opts ...RemoteExecutorOptions) *RemoteExecutor {
	options := RemoteExecutorOptions{CursorModeEnabled: true}
	if len(opts) > 0 {
		options = opts[0]
	}

	return &RemoteExecutor{
		endpointURL: strings.TrimRight(endpointURL, "/"),
		authToken:   authToken,
		localDB:     localDB,
		cursorMode:  options.CursorModeEnabled,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			},
		},
	}
}

// QueryContext sends the query to the remote agent and materializes the result
// into a local DuckDB temp table, returning *sql.Rows from that table.
func (e *RemoteExecutor) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	requestID := uuid.New().String()
	if !e.cursorMode {
		result, err := e.queryViaLegacyExecute(ctx, query, requestID)
		if err != nil {
			return nil, err
		}
		return e.materialize(ctx, result) //nolint:sqlclosecheck // rows are returned to caller
	}

	rows, err := e.queryViaLifecycleToRows(ctx, query, requestID)
	if err == nil {
		return rows, nil
	}

	var unsupported *unsupportedLifecycleError
	if !errors.As(err, &unsupported) {
		return nil, err
	}

	result, err := e.queryViaLegacyExecute(ctx, query, requestID)
	if err != nil {
		return nil, err
	}

	return e.materialize(ctx, result) //nolint:sqlclosecheck // rows are returned to caller
}

type unsupportedLifecycleError struct {
	status int
}

func (e *unsupportedLifecycleError) Error() string {
	return fmt.Sprintf("query lifecycle endpoints are unsupported (status %d)", e.status)
}

func (e *RemoteExecutor) queryViaLifecycleToRows(ctx context.Context, query, requestID string) (*sql.Rows, error) {
	submit := SubmitQueryRequest{SQL: query, RequestID: requestID}
	var submitResp SubmitQueryResponse
	status, err := e.postJSON(ctx, "/queries", submit, &submitResp, requestID)
	if status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
		return nil, &unsupportedLifecycleError{status: status}
	}
	if err != nil {
		return nil, fmt.Errorf("submit query lifecycle request: %w", err)
	}
	if status != http.StatusAccepted && status != http.StatusOK {
		return nil, fmt.Errorf("submit query lifecycle request failed: status %d", status)
	}

	queryID := submitResp.QueryID
	if queryID == "" {
		return nil, fmt.Errorf("submit query lifecycle request failed: missing query id")
	}
	defer e.deleteQuery(context.Background(), queryID, requestID)

	statusResp, err := e.waitForQueryCompletion(ctx, queryID, requestID)
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

	return e.materializeFromLifecycle(ctx, queryID, requestID)
}

func (e *RemoteExecutor) queryViaLegacyExecute(ctx context.Context, query, requestID string) (ExecuteResponse, error) {

	reqBody := ExecuteRequest{SQL: query, RequestID: requestID}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return ExecuteResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		e.endpointURL+"/execute", bytes.NewReader(bodyBytes))
	if err != nil {
		return ExecuteResponse{}, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Request-ID", requestID)
	AttachSignedAgentHeaders(httpReq, e.authToken, bodyBytes, time.Now())

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return ExecuteResponse{}, fmt.Errorf("remote execute: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var result ExecuteResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ExecuteResponse{}, fmt.Errorf("decode response: %w", err)
	}

	if resp.StatusCode != http.StatusOK || result.Error != "" {
		errMsg := result.Error
		if errMsg == "" {
			errMsg = fmt.Sprintf("remote agent returned status %d", resp.StatusCode)
		}
		return ExecuteResponse{}, fmt.Errorf("remote execution failed: %s", errMsg)
	}

	return result, nil
}

func (e *RemoteExecutor) waitForQueryCompletion(ctx context.Context, queryID, requestID string) (QueryStatusResponse, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		statusResp, statusCode, err := e.getQueryStatus(ctx, queryID, requestID)
		if err != nil {
			return QueryStatusResponse{}, err
		}
		if statusCode != http.StatusOK {
			return QueryStatusResponse{}, fmt.Errorf("query status failed: status %d", statusCode)
		}

		switch statusResp.Status {
		case QueryStatusSucceeded, QueryStatusFailed, QueryStatusCanceled:
			return statusResp, nil
		}

		select {
		case <-ctx.Done():
			_ = e.cancelQuery(context.Background(), queryID, requestID)
			return QueryStatusResponse{}, fmt.Errorf("wait for query completion: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (e *RemoteExecutor) fetchAllPages(ctx context.Context, queryID, requestID string) (ExecuteResponse, error) {
	pageToken := ""
	result := ExecuteResponse{Columns: nil, Rows: make([][]interface{}, 0)}

	for {
		pageResp, statusCode, err := e.fetchQueryResultsPage(ctx, queryID, pageToken, requestID)
		if err != nil {
			return ExecuteResponse{}, err
		}
		if statusCode != http.StatusOK {
			return ExecuteResponse{}, fmt.Errorf("fetch query results failed: status %d", statusCode)
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

func (e *RemoteExecutor) materializeFromLifecycle(ctx context.Context, queryID, requestID string) (*sql.Rows, error) {
	tableName := "_remote_result_" + randomSuffix()

	conn, err := e.localDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("pin connection: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	pageToken := ""
	created := false
	for {
		pageResp, statusCode, err := e.fetchQueryResultsPage(ctx, queryID, pageToken, requestID)
		if err != nil {
			return nil, err
		}
		if statusCode != http.StatusOK {
			return nil, fmt.Errorf("fetch query results failed: status %d", statusCode)
		}

		if !created {
			if err := createResultTable(ctx, conn, tableName, pageResp.Columns); err != nil {
				return nil, err
			}
			created = true
		}
		if err := insertResultRows(ctx, conn, tableName, pageResp.Rows, pageResp.Columns); err != nil {
			return nil, err
		}

		if pageResp.NextPageToken == "" {
			break
		}
		pageToken = pageResp.NextPageToken
	}

	if !created {
		return e.localDB.QueryContext(ctx, "SELECT 1 WHERE false")
	}

	selectSQL := fmt.Sprintf("SELECT * FROM %q", tableName) //nolint:gosec // tableName is generated internally
	rows, err := e.localDB.QueryContext(ctx, selectSQL)
	if err != nil {
		return nil, fmt.Errorf("select from temp: %w", err)
	}
	return rows, nil
}

func (e *RemoteExecutor) postJSON(ctx context.Context, path string, payload interface{}, out interface{}, requestID string) (int, error) {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpointURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Token", e.authToken)
	req.Header.Set("X-Request-ID", requestID)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if err := decodeJSONOrError(resp, out); err != nil {
		return resp.StatusCode, err
	}
	return resp.StatusCode, nil
}

func (e *RemoteExecutor) getQueryStatus(ctx context.Context, queryID, requestID string) (QueryStatusResponse, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.endpointURL+"/queries/"+url.PathEscape(queryID), nil)
	if err != nil {
		return QueryStatusResponse{}, 0, fmt.Errorf("create status request: %w", err)
	}
	req.Header.Set("X-Agent-Token", e.authToken)
	req.Header.Set("X-Request-ID", requestID)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return QueryStatusResponse{}, 0, fmt.Errorf("query status request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var out QueryStatusResponse
	if err := decodeJSONOrError(resp, &out); err != nil {
		return QueryStatusResponse{}, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

func (e *RemoteExecutor) fetchQueryResultsPage(ctx context.Context, queryID, pageToken, requestID string) (FetchQueryResultsResponse, int, error) {
	u := e.endpointURL + "/queries/" + url.PathEscape(queryID) + "/results"
	if pageToken != "" {
		u += "?page_token=" + url.QueryEscape(pageToken)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return FetchQueryResultsResponse{}, 0, fmt.Errorf("create results request: %w", err)
	}
	req.Header.Set("X-Agent-Token", e.authToken)
	req.Header.Set("X-Request-ID", requestID)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return FetchQueryResultsResponse{}, 0, fmt.Errorf("query results request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var out FetchQueryResultsResponse
	if err := decodeJSONOrError(resp, &out); err != nil {
		return FetchQueryResultsResponse{}, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

func (e *RemoteExecutor) cancelQuery(ctx context.Context, queryID, requestID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpointURL+"/queries/"+url.PathEscape(queryID)+"/cancel", bytes.NewReader([]byte("{}")))
	if err != nil {
		return fmt.Errorf("create cancel request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Token", e.authToken)
	req.Header.Set("X-Request-ID", requestID)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cancel query request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("cancel query failed: status %d", resp.StatusCode)
	}
	return nil
}

func (e *RemoteExecutor) deleteQuery(ctx context.Context, queryID, requestID string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, e.endpointURL+"/queries/"+url.PathEscape(queryID), nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Agent-Token", e.authToken)
	req.Header.Set("X-Request-ID", requestID)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

func decodeJSONOrError(resp *http.Response, out interface{}) error {
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
			return fmt.Errorf("remote request failed: %s", errResp.Error)
		}
		if len(body) > 0 {
			return fmt.Errorf("remote request failed: status %d: %s", resp.StatusCode, string(body))
		}
		return fmt.Errorf("remote request failed: status %d", resp.StatusCode)
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// materialize creates a DuckDB temp table from the remote response and returns
// *sql.Rows over it. Uses a pinned connection for DDL and inserts, then closes
// it and queries the temp table from the pool, preventing connection leaks.
func (e *RemoteExecutor) materialize(ctx context.Context, result ExecuteResponse) (*sql.Rows, error) {
	if len(result.Columns) == 0 {
		// Return empty result set
		return e.localDB.QueryContext(ctx, "SELECT 1 WHERE false")
	}

	suffix := randomSuffix()
	tableName := "_remote_result_" + suffix

	// Use a pinned connection for temp table creation + inserts.
	if err := e.populateTempTable(ctx, tableName, result); err != nil {
		return nil, err
	}

	// Query the temp table from the pool. The pinned connection was closed
	// inside populateTempTable, preventing a connection leak.
	selectSQL := fmt.Sprintf("SELECT * FROM %q", tableName) //nolint:gosec // tableName is generated internally, not user input
	rows, err := e.localDB.QueryContext(ctx, selectSQL)
	if err != nil {
		return nil, fmt.Errorf("select from temp: %w", err)
	}

	return rows, nil
}

// populateTempTable creates and populates a temp table on a pinned connection,
// then closes the connection before returning.
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
	pingCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		pingCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(pingCtx, http.MethodGet, e.endpointURL+"/health", nil)
	if err != nil {
		return fmt.Errorf("create health request: %w", err)
	}
	AttachSignedAgentHeaders(req, e.authToken, nil, time.Now())

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: status %d", resp.StatusCode)
	}
	return nil
}

// randomSuffix generates a cryptographically random hex suffix for temp table names.
func randomSuffix() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		// Fallback to a fixed suffix (should never happen)
		return "fallback"
	}
	return hex.EncodeToString(b)
}
