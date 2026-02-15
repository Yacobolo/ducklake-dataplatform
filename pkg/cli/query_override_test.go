package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
