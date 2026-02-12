package compute

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"duck-demo/internal/domain"
)

var _ domain.ComputeExecutor = (*RemoteExecutor)(nil)

type executeRequest struct {
	SQL       string `json:"sql"`
	RequestID string `json:"request_id"`
}

type executeResponse struct {
	Columns  []string        `json:"columns"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int             `json:"row_count"`
	Error    string          `json:"error,omitempty"`
	Code     string          `json:"code,omitempty"`
}

// RemoteExecutor sends pre-secured SQL to a remote compute agent via HTTP
// and materializes results into a local DuckDB temp table to return *sql.Rows.
type RemoteExecutor struct {
	endpointURL string
	authToken   string
	localDB     *sql.DB // for temp table materialization
	httpClient  *http.Client
}

// NewRemoteExecutor creates a RemoteExecutor that sends queries to the given
// endpoint URL and materializes results into the local DuckDB instance.
func NewRemoteExecutor(endpointURL, authToken string, localDB *sql.DB) *RemoteExecutor {
	return &RemoteExecutor{
		endpointURL: strings.TrimRight(endpointURL, "/"),
		authToken:   authToken,
		localDB:     localDB,
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

	// 1. Send to remote agent
	reqBody := executeRequest{SQL: query, RequestID: requestID}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		e.endpointURL+"/execute", strings.NewReader(string(bodyBytes)))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Agent-Token", e.authToken)
	httpReq.Header.Set("X-Request-ID", requestID)

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("remote execute: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var result executeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if resp.StatusCode != http.StatusOK || result.Error != "" {
		errMsg := result.Error
		if errMsg == "" {
			errMsg = fmt.Sprintf("remote agent returned status %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("remote execution failed: %s", errMsg)
	}

	// 2. Materialize into temp table for *sql.Rows return
	return e.materialize(ctx, result) //nolint:sqlclosecheck // rows are returned to caller
}

// materialize creates a DuckDB temp table from the remote response and returns
// *sql.Rows over it. Uses a pinned connection so the temp table is visible.
func (e *RemoteExecutor) materialize(ctx context.Context, result executeResponse) (*sql.Rows, error) {
	if len(result.Columns) == 0 {
		// Return empty result set
		return e.localDB.QueryContext(ctx, "SELECT 1 WHERE false")
	}

	suffix := randomSuffix()
	tableName := "_remote_result_" + suffix

	// Use a pinned connection for temp table visibility
	conn, err := e.localDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("pin connection: %w", err)
	}

	// Build CREATE TEMP TABLE with VARCHAR columns (type info not available from JSON)
	var colDefs []string
	for _, col := range result.Columns {
		colDefs = append(colDefs, fmt.Sprintf("%q VARCHAR", col))
	}
	createSQL := fmt.Sprintf("CREATE TEMP TABLE %q (%s)", tableName, strings.Join(colDefs, ", "))
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("create temp table: %w", err)
	}

	// Insert rows using parameterized queries
	if len(result.Rows) > 0 {
		placeholders := make([]string, len(result.Columns))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertSQL := fmt.Sprintf("INSERT INTO %q VALUES (%s)", tableName, strings.Join(placeholders, ", ")) //nolint:gosec // tableName is generated internally
		for _, row := range result.Rows {
			args := make([]interface{}, len(row))
			for i, v := range row {
				if v == nil {
					args[i] = nil
				} else {
					args[i] = fmt.Sprintf("%v", v)
				}
			}
			if _, err := conn.ExecContext(ctx, insertSQL, args...); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("insert row: %w", err)
			}
		}
	}

	// Query the temp table — rows.Close() will handle the connection lifecycle
	selectSQL := fmt.Sprintf("SELECT * FROM %q", tableName) //nolint:gosec // tableName is generated internally, not user input
	rows, err := conn.QueryContext(ctx, selectSQL)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("select from temp: %w", err)
	}

	return rows, nil
}

// Ping performs a health check against the remote agent.
func (e *RemoteExecutor) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.endpointURL+"/health", nil)
	if err != nil {
		return fmt.Errorf("create health request: %w", err)
	}
	req.Header.Set("X-Agent-Token", e.authToken)

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
