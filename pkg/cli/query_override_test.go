package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				assert.Equal(t, "/v1/query", c.path)
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
			rootCmd.SetArgs(append([]string{"--host", srv.URL}, tt.args...))

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
