package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeLocalQueryExecutor struct {
	t      *testing.T
	called bool
	cfg    localQueryConfig
	sql    string
	result *localQueryResult
	err    error
}

func (f *fakeLocalQueryExecutor) Execute(_ context.Context, cfg localQueryConfig, sqlQuery string) (*localQueryResult, error) {
	f.t.Helper()
	f.called = true
	f.cfg = cfg
	f.sql = sqlQuery
	return f.result, f.err
}

func TestQueryOverride(t *testing.T) {
	type captured struct {
		method string
		path   string
		body   []byte
	}

	tests := []struct {
		name       string
		args       []string
		statusCode int
		response   string
		wantErr    bool
		errContain string
		checkReq   func(t *testing.T, c captured)
	}{
		{
			name:       "SQL from flag",
			args:       []string{"query", "execute", "--sql", "SELECT 1"},
			statusCode: http.StatusOK,
			response:   `{"columns":["1"],"rows":[[1]],"row_count":1}`,
			wantErr:    false,
			checkReq: func(t *testing.T, c captured) {
				t.Helper()
				assert.Equal(t, "POST", c.method)
				assert.Equal(t, "/v1/queries:execute", c.path)
				var body map[string]interface{}
				require.NoError(t, json.Unmarshal(c.body, &body))
				assert.Equal(t, "SELECT 1", body["sql"])
			},
		},
		{
			name:       "no SQL provided",
			args:       []string{"query", "execute"},
			statusCode: http.StatusOK,
			response:   `{}`,
			wantErr:    true,
			errContain: "provide SQL via --sql flag or stdin pipe",
		},
		{
			name:       "HTTP error",
			args:       []string{"query", "execute", "--sql", "SELECT 1"},
			statusCode: http.StatusForbidden,
			response:   `{"code":403,"message":"access denied"}`,
			wantErr:    true,
			errContain: "API error (HTTP 403)",
		},
		{
			name:       "SQL body content",
			args:       []string{"query", "execute", "--sql", "SELECT * FROM users"},
			statusCode: http.StatusOK,
			response:   `{"columns":["id","name"],"rows":[[1,"alice"]],"row_count":1}`,
			wantErr:    false,
			checkReq: func(t *testing.T, c captured) {
				t.Helper()
				var body map[string]interface{}
				require.NoError(t, json.Unmarshal(c.body, &body))
				assert.Equal(t, "SELECT * FROM users", body["sql"])
			},
		},
		{
			name:       "SQL from positional argument",
			args:       []string{"query", "execute", "SELECT 1"},
			statusCode: http.StatusOK,
			response:   `{"columns":["1"],"rows":[[1]],"row_count":1}`,
			wantErr:    false,
			checkReq: func(t *testing.T, c captured) {
				t.Helper()
				var body map[string]interface{}
				require.NoError(t, json.Unmarshal(c.body, &body))
				assert.Equal(t, "SELECT 1", body["sql"])
			},
		},
		{
			name:       "server returns nil values",
			args:       []string{"query", "execute", "--sql", "SELECT id, name FROM t"},
			statusCode: http.StatusOK,
			response:   `{"columns":["id","name"],"rows":[[1,null],[2,"bob"]],"row_count":2}`,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("HOME", dir)

			var (
				mu          sync.Mutex
				capturedReq captured
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				capturedReq.method = r.Method
				capturedReq.path = r.URL.Path
				if r.Body != nil {
					capturedReq.body, _ = io.ReadAll(r.Body)
					_ = r.Body.Close()
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.response))
			}))
			defer srv.Close()

			rootCmd := newRootCmd()
			rootArgs := append([]string{"--host", srv.URL, "--api-key", "test-key"}, tt.args...)
			if len(tt.args) >= 2 && tt.args[0] == "query" && tt.args[1] == "execute" {
				rootArgs = append(rootArgs, "--compute-mode", "SHARED_ENDPOINT", "--compute-endpoint", "warehouse-a")
			}
			rootCmd.SetArgs(rootArgs)

			err := rootCmd.Execute()

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContain != "" {
					assert.Contains(t, err.Error(), tt.errContain)
				}
				return
			}

			require.NoError(t, err)

			if tt.checkReq != nil {
				mu.Lock()
				c := capturedReq
				mu.Unlock()
				tt.checkReq(t, c)
			}
		})
	}
}

func TestQueryExecute_RejectsTooManyPositionalArgs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"query", "execute", "SELECT", "1"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accepts at most 1 arg(s)")
}

func TestQueryExecute_DefaultsToLocalBYOC(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	extensionPath := filepath.Join(dir, "duck_access.duckdb_extension")
	require.NoError(t, os.WriteFile(extensionPath, []byte("test extension"), 0o644))

	restoreStdout := captureStdout(t)

	fakeExec := &fakeLocalQueryExecutor{
		t: t,
		result: &localQueryResult{
			Columns:  []string{"answer"},
			Rows:     [][]interface{}{{float64(42)}},
			RowCount: 1,
		},
	}
	originalExecutor := cliLocalQueryExecutor
	cliLocalQueryExecutor = fakeExec
	t.Cleanup(func() { cliLocalQueryExecutor = originalExecutor })

	requestCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"--api-key", "test-key",
		"--duck-access-extension-path", extensionPath,
		"--output", "json",
		"query", "execute", "--sql", "SELECT 42 AS answer", "--compute-mode", "BYOC_LOCAL",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)
	assert.True(t, fakeExec.called)
	assert.Equal(t, "SELECT 42 AS answer", fakeExec.sql)
	assert.Equal(t, strings.TrimRight(srv.URL, "/")+"/v1", fakeExec.cfg.APIBaseURL)
	assert.Equal(t, "test-key", fakeExec.cfg.APIKey)
	assert.Equal(t, extensionPath, fakeExec.cfg.ExtensionPath)
	assert.Zero(t, requestCount)

	output := restoreStdout()
	assert.Contains(t, output, `"columns": [`)
	assert.Contains(t, output, `"answer"`)
}

func TestQueryExecute_ExplicitSharedEndpointUsesAPI(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	restoreStdout := captureStdout(t)

	fakeExec := &fakeLocalQueryExecutor{t: t}
	originalExecutor := cliLocalQueryExecutor
	cliLocalQueryExecutor = fakeExec
	t.Cleanup(func() { cliLocalQueryExecutor = originalExecutor })

	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/queries:execute", r.URL.Path)
		capturedBody, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"columns":["id"],"rows":[[1]],"row_count":1}`))
	}))
	defer srv.Close()

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"--api-key", "test-key",
		"--output", "json",
		"query", "execute", "--sql", "SELECT 1", "--compute-mode", "SHARED_ENDPOINT", "--compute-endpoint", "warehouse-a",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)
	assert.False(t, fakeExec.called)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(capturedBody, &body))
	assert.Equal(t, "SHARED_ENDPOINT", body["compute_mode"])
	assert.Equal(t, "warehouse-a", body["endpoint_name"])

	output := restoreStdout()
	assert.Contains(t, output, `"row_count": 1`)
}

func TestQueryExecute_LocalRequiresAPIKey(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	extensionPath := filepath.Join(dir, "duck_access.duckdb_extension")
	require.NoError(t, os.WriteFile(extensionPath, []byte("test extension"), 0o644))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--host", "http://example.com",
		"--token", "jwt-token",
		"--duck-access-extension-path", extensionPath,
		"query", "execute", "--sql", "SELECT 1", "--compute-mode", "BYOC_LOCAL",
	})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires an API key")
}

func TestQueryExecute_RemoteHostPrefersAPI(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	restoreStdout := captureStdout(t)

	fakeExec := &fakeLocalQueryExecutor{t: t}
	originalExecutor := cliLocalQueryExecutor
	cliLocalQueryExecutor = fakeExec
	t.Cleanup(func() { cliLocalQueryExecutor = originalExecutor })

	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/queries:execute", r.URL.Path)
		capturedBody, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"columns":["answer"],"rows":[[42]],"row_count":1}`))
	}))
	defer srv.Close()

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"--token", "jwt-token",
		"--output", "json",
		"query", "execute", "--sql", "SELECT 42 AS answer",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)
	assert.False(t, fakeExec.called)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(capturedBody, &body))
	assert.Equal(t, "SELECT 42 AS answer", body["sql"])

	output := restoreStdout()
	assert.Contains(t, output, `"row_count": 1`)
}

func TestQueryOverride_SubmitAndWaitResults(t *testing.T) {
	t.Run("submit sends SQL and request id", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("HOME", dir)

		var capturedBody []byte
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Path == "/v1/queries" {
				capturedBody, _ = io.ReadAll(r.Body)
				_ = r.Body.Close()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{"query_id":"q-1","status":"QUEUED"}`))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		rootCmd := newRootCmd()
		rootCmd.SetArgs([]string{"--host", srv.URL, "query", "submit", "--sql", "SELECT 1", "--request-id", "rid-1", "--output", "json"})

		err := rootCmd.Execute()
		require.NoError(t, err)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(capturedBody, &body))
		assert.Equal(t, "SELECT 1", body["sql"])
		assert.Equal(t, "rid-1", body["request_id"])
	})

	t.Run("submit wait results fetches status and rows", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("HOME", dir)

		var mu sync.Mutex
		paths := make([]string, 0)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.URL.Path)
			mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/v1/queries":
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{"query_id":"q-2","status":"QUEUED"}`))
			case r.Method == http.MethodGet && r.URL.Path == "/v1/queries/q-2":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"query_id":"q-2","status":"SUCCEEDED","row_count":2}`))
			case r.Method == http.MethodGet && r.URL.Path == "/v1/queries/q-2/results":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"columns":["id"],"rows":[[1],[2]],"row_count":2}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer srv.Close()

		rootCmd := newRootCmd()
		rootCmd.SetArgs([]string{"--host", srv.URL, "query", "submit", "--sql", "SELECT 1", "--wait", "--results", "--output", "json"})

		err := rootCmd.Execute()
		require.NoError(t, err)

		mu.Lock()
		joined := strings.Join(paths, ",")
		mu.Unlock()
		assert.Contains(t, joined, "/v1/queries")
		assert.Contains(t, joined, "/v1/queries/q-2")
		assert.Contains(t, joined, "/v1/queries/q-2/results")
	})
}

func TestQuerySubmit_RejectsBYOCLocal(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--host", "http://example.com", "query", "submit", "--sql", "SELECT 1", "--compute-mode", "BYOC_LOCAL"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BYOC_LOCAL is only supported for interactive execution")
}
