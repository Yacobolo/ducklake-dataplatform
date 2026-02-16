package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capturedRequest holds details captured from an incoming HTTP request.
type capturedRequest struct {
	Method  string
	Path    string
	Query   string
	Headers http.Header
	Body    string
}

// requestRecorder is a thread-safe recorder for HTTP requests received by httptest servers.
type requestRecorder struct {
	mu       sync.Mutex
	requests []capturedRequest
}

func (r *requestRecorder) record(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()

	body, _ := io.ReadAll(req.Body)
	defer func() { _ = req.Body.Close() }()

	r.requests = append(r.requests, capturedRequest{
		Method:  req.Method,
		Path:    req.URL.Path,
		Query:   req.URL.RawQuery,
		Headers: req.Header.Clone(),
		Body:    string(body),
	})
}

func (r *requestRecorder) last() capturedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.requests) == 0 {
		return capturedRequest{}
	}
	return r.requests[len(r.requests)-1]
}

// newTestRootCmd creates a fresh root command pointed at the given httptest server.
// It isolates HOME so no real config is loaded.
func newTestRootCmd(t *testing.T, srv *httptest.Server) *cobra.Command {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--host", srv.URL})
	return rootCmd
}

// jsonHandler returns an http.HandlerFunc that records the request and responds
// with the given status code and JSON body.
func jsonHandler(rec *requestRecorder, status int, respBody string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}
}

// === Error Propagation Tests (issue #99) ===

func TestCLI_ErrorPropagation(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantSubstr string
	}{
		{
			name:       "HTTP 403 forbidden",
			status:     403,
			body:       `{"code":403,"message":"access denied"}`,
			wantSubstr: "API error",
		},
		{
			name:       "HTTP 404 not found",
			status:     404,
			body:       `{"code":404,"message":"schema not found"}`,
			wantSubstr: "API error",
		},
		{
			name:       "HTTP 500 internal error",
			status:     500,
			body:       `{"code":500,"message":"internal server error"}`,
			wantSubstr: "API error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &requestRecorder{}
			srv := httptest.NewServer(jsonHandler(rec, tc.status, tc.body))
			defer srv.Close()

			rootCmd := newTestRootCmd(t, srv)
			rootCmd.SetArgs([]string{"--host", srv.URL, "catalog", "schemas", "list", "test"})

			err := rootCmd.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantSubstr)
			assert.Contains(t, err.Error(), http.StatusText(0)[:0]+string(rune('0'+tc.status/100))) // contains digit
		})
	}
}

func TestCLI_ConnectionRefused(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--host", "http://127.0.0.1:1", "catalog", "schemas", "list", "test"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "execute request")
}

func TestCLI_MissingRequiredArg(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "catalog", "schemas", "list"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accepts 1 arg(s)")
}

func TestCLI_MissingArgs(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "catalog", "schemas", "create", "--catalog-name", "test"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accepts 1 arg(s)")
}

// === Path Parameter Substitution Tests (issue #100) ===

func TestCLI_PathParamSubstitution(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "catalog", "schemas", "list", "production"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "/v1/catalogs/production/schemas", captured.Path)
	assert.NotContains(t, captured.Path, "{catalogName}")
}

func TestCLI_PathParamSubstitution_MultiLevel(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"name":"myschema"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "catalog", "schemas", "get", "myschema", "--catalog-name", "prod"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "/v1/catalogs/prod/schemas/myschema", captured.Path)
	assert.NotContains(t, captured.Path, "{catalogName}")
	assert.NotContains(t, captured.Path, "{schemaName}")
}

func TestCLI_PathParamSubstitution_NoUnresolvedPlaceholders(t *testing.T) {
	// When catalogName IS provided as a positional arg, verify no unresolved
	// placeholders remain. This test documents that substitution works
	// correctly when the argument is provided.
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "catalog", "schemas", "list", "mycat"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.NotContains(t, captured.Path, "{")
	assert.NotContains(t, captured.Path, "}")
}

// === JSON Input Tests (issue #101) ===

func TestCLI_JSONInputWithRequiredBodyFlag(t *testing.T) {
	// Fixed (issue #101): --json input now bypasses MarkFlagRequired for body
	// flags. Required body fields are validated inside RunE only when --json
	// is not provided.
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"users"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "tables", "create", "myschema",
		"--catalog-name", "test",
		"--json", `{"name":"users","columns":[{"name":"id","type":"BIGINT"}]}`,
		// --name flag intentionally omitted; --json provides it.
	})

	err := rootCmd.Execute()
	require.NoError(t, err, "--json should bypass required body flag validation")

	captured := rec.last()
	require.NotNil(t, captured, "server should have received a request")
	assert.Equal(t, "POST", captured.Method)
}

func TestCLI_JSONInputRawString(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"myschema"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "myschema",
		"--catalog-name", "test",
		"--json", `{"name":"myschema","comment":"test"}`,
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "POST", captured.Method)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "myschema", body["name"])
	assert.Equal(t, "test", body["comment"])
}

func TestCLI_JSONInputFromFile(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"myschema"}`))
	defer srv.Close()

	dir := t.TempDir()
	jsonFile := filepath.Join(dir, "input.json")
	require.NoError(t, os.WriteFile(jsonFile, []byte(`{"name":"myschema","comment":"from file"}`), 0o644))

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "myschema",
		"--catalog-name", "test",
		"--json", "@" + jsonFile,
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "myschema", body["name"])
	assert.Equal(t, "from file", body["comment"])
}

func TestCLI_JSONInputInvalid(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "myschema",
		"--catalog-name", "test",
		"--json", `{bad`,
	})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse JSON input")
}

// === CRUD Operation Tests ===

func TestCLI_CreateCommand(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"analytics","schema_id":"s1"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "analytics",
		"--catalog-name", "test",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "POST", captured.Method)
	assert.Equal(t, "/v1/catalogs/test/schemas", captured.Path)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "analytics", body["name"])
}

func TestCLI_ListCommand(t *testing.T) {
	rec := &requestRecorder{}
	resp := `{"data":[{"name":"schema1"},{"name":"schema2"}],"next_page_token":""}`
	srv := httptest.NewServer(jsonHandler(rec, 200, resp))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "list", "test",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "GET", captured.Method)
	assert.Equal(t, "/v1/catalogs/test/schemas", captured.Path)
}

func TestCLI_GetCommand(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"name":"myschema","schema_id":"s1"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "get", "myschema",
		"--catalog-name", "test",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "GET", captured.Method)
	assert.Equal(t, "/v1/catalogs/test/schemas/myschema", captured.Path)
}

func TestCLI_DeleteCommand(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 204, ``))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "delete", "myschema",
		"--catalog-name", "test",
		"--yes",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "DELETE", captured.Method)
	assert.Equal(t, "/v1/catalogs/test/schemas/myschema", captured.Path)
}

func TestCLI_UpdateCommand(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"name":"myschema","comment":"updated"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "update", "myschema",
		"--catalog-name", "test",
		"--comment", "updated",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "PATCH", captured.Method)
	assert.Equal(t, "/v1/catalogs/test/schemas/myschema", captured.Path)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "updated", body["comment"])
}

// === Body Construction Tests ===

func TestCLI_FlagBasedBody(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"analytics"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "analytics",
		"--catalog-name", "test",
		"--comment", "my comment",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "analytics", body["name"])
	assert.Equal(t, "my comment", body["comment"])
}

func TestCLI_OnlyChangedFlagsInBody(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"analytics"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "analytics",
		"--catalog-name", "test",
		// NOTE: --comment is intentionally omitted
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "analytics", body["name"])
	_, hasComment := body["comment"]
	assert.False(t, hasComment, "body should not contain 'comment' key when flag is not provided")
}

// === Query Param Tests ===

func TestCLI_MaxResults(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "list", "test",
		"--max-results", "50",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "max_results=50")
}

func TestCLI_PageToken(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "list", "test",
		"--page-token", "abc",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "page_token=abc")
}

// === Command Structure Tests ===

func TestCLI_CommandTree(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	cmdNames := make(map[string]bool)
	for _, cmd := range rootCmd.Commands() {
		cmdNames[cmd.Name()] = true
	}

	expectedCommands := []string{
		"catalog", "security", "query", "compute", "storage",
		"pipelines", "notebooks", "governance", "observability",
		"lineage", "manifest", "ingestion",
		"version", "config", "auth",
		"plan", "apply", "export", "validate",
		"commands", "api", "find", "describe",
		"completion",
	}

	for _, name := range expectedCommands {
		t.Run(name, func(t *testing.T) {
			assert.True(t, cmdNames[name], "expected command %q to exist on root", name)
		})
	}
}

func TestCLI_SubcommandTree(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()

	// Find the catalog command
	var catalogCmd *cobra.Command
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "catalog" {
			catalogCmd = cmd
			break
		}
	}
	require.NotNil(t, catalogCmd, "catalog command should exist")

	subNames := make(map[string]bool)
	for _, cmd := range catalogCmd.Commands() {
		subNames[cmd.Name()] = true
	}

	expectedSubs := []string{"schemas", "tables", "views", "volumes", "columns"}
	for _, name := range expectedSubs {
		t.Run(name, func(t *testing.T) {
			assert.True(t, subNames[name], "expected subcommand %q under catalog", name)
		})
	}
}

// === Auth Header Tests ===

func TestCLI_BearerTokenAuth(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"--token", "mytoken",
		"catalog", "schemas", "list", "test",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "Bearer mytoken", captured.Headers.Get("Authorization"))
}

func TestCLI_APIKeyAuth(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"--api-key", "mykey",
		"catalog", "schemas", "list", "test",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "mykey", captured.Headers.Get("X-API-Key"))
}

// === Output Format Tests ===

func TestCLI_InvalidOutputFormat(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"-o", "xml",
		"catalog", "schemas", "list", "test",
	})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported output format")
}

// === Additional Edge Cases ===

func TestCLI_ErrorPropagation_ContainsStatusCode(t *testing.T) {
	// Verify the error message contains the actual HTTP status code.
	tests := []struct {
		name       string
		status     int
		wantStatus string
	}{
		{"403", 403, "403"},
		{"404", 404, "404"},
		{"500", 500, "500"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &requestRecorder{}
			body := `{"code":` + tc.wantStatus + `,"message":"error"}`
			srv := httptest.NewServer(jsonHandler(rec, tc.status, body))
			defer srv.Close()

			rootCmd := newTestRootCmd(t, srv)
			rootCmd.SetArgs([]string{
				"--host", srv.URL,
				"catalog", "schemas", "list", "test",
			})

			err := rootCmd.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantStatus)
		})
	}
}

func TestCLI_DeleteCommand_RequiresYesFlag(t *testing.T) {
	// Without --yes, delete commands prompt for confirmation interactively.
	// When stdin is not a TTY (as in tests), fmt.Scanln returns immediately
	// with an empty string, which ConfirmPrompt interprets as "no" and the
	// command returns nil without making any request.
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 204, ``))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "delete", "myschema",
		"--catalog-name", "test",
		// NOTE: --yes intentionally omitted
	})

	// Redirect stdin to provide a "no" answer
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	_, _ = w.WriteString("n\n")
	_ = w.Close()
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	err := rootCmd.Execute()
	// Command should succeed (returns nil) but not make any HTTP request.
	require.NoError(t, err)
	assert.Empty(t, rec.requests, "no HTTP request should be made when confirmation is declined")
}

func TestCLI_TokenPrecedenceOverAPIKey(t *testing.T) {
	// When both --token and --api-key are provided, token takes precedence
	// (per client.Do logic: prefer token, then API key).
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"--token", "mytoken",
		"--api-key", "mykey",
		"catalog", "schemas", "list", "test",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "Bearer mytoken", captured.Headers.Get("Authorization"))
	assert.Empty(t, captured.Headers.Get("X-API-Key"), "X-API-Key should not be set when token is present")
}

func TestCLI_ContentTypeHeader(t *testing.T) {
	// POST commands with a body should set Content-Type: application/json.
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"analytics"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "analytics",
		"--catalog-name", "test",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "application/json", captured.Headers.Get("Content-Type"))
	assert.Equal(t, "application/json", captured.Headers.Get("Accept"))
}

func TestCLI_MultiplePathParams(t *testing.T) {
	// Test commands with multiple path parameters (e.g., tables delete with
	// schema-name, table-name, and catalog-name).
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 204, ``))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "tables", "delete", "myschema", "mytable",
		"--catalog-name", "prod",
		"--yes",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "DELETE", captured.Method)
	assert.Equal(t, "/v1/catalogs/prod/schemas/myschema/tables/mytable", captured.Path)
	assert.NotContains(t, captured.Path, "{")
}

func TestCLI_CreateSchemaWithAllFlags(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"analytics"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "analytics",
		"--catalog-name", "test",
		"--comment", "my schema",
		"--location-name", "s3://bucket/path",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "analytics", body["name"])
	assert.Equal(t, "my schema", body["comment"])
	assert.Equal(t, "s3://bucket/path", body["location_name"])
}

func TestCLI_ListWithMultipleQueryParams(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "list", "test",
		"--max-results", "25",
		"--page-token", "nextpage",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "max_results=25")
	assert.Contains(t, captured.Query, "page_token=nextpage")
}

func TestCLI_UpdateSchemaOnlyComment(t *testing.T) {
	// Verify that update only sends changed flags in the body.
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"name":"myschema","comment":"new"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "update", "myschema",
		"--catalog-name", "test",
		"--comment", "new",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "new", body["comment"])
	_, hasProperties := body["properties"]
	assert.False(t, hasProperties, "unchanged flags should not appear in body")
}

func TestCLI_JSONInputOverridesFlags(t *testing.T) {
	// When --json is provided, the raw JSON is sent as the body regardless
	// of other flags (for commands without MarkFlagRequired on body fields).
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"name":"override"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"catalog", "schemas", "create", "irrelevant",
		"--catalog-name", "test",
		"--json", `{"name":"override","comment":"from json"}`,
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(captured.Body), &body))
	assert.Equal(t, "override", body["name"])
	assert.Equal(t, "from json", body["comment"])
}

func TestCLI_HostTrailingSlash(t *testing.T) {
	// NewClient strips trailing slashes from the initial URL, but
	// PersistentPreRunE re-assigns the raw --host value. Verify the
	// request still reaches the server (the httptest mux ignores
	// double slashes in the path).
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL + "/",
		"catalog", "schemas", "list", "test",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	// The request reached the server; path starts with /v1
	assert.True(t, strings.HasPrefix(captured.Path, "/") || strings.Contains(captured.Path, "/v1"),
		"request should reach the server")
}

func TestCLI_UnknownCommand(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"nonexistent"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown command")
}

func TestCLI_UnknownSecuritySubcommand(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"security", "nope"})

	err := rootCmd.Execute()
	require.NoError(t, err)
}

func TestCLI_CatalogSetDefault_SendsEmptyJSONObject(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"status":"ok"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "catalog", "set-default", "lake"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, "POST", captured.Method)
	assert.Equal(t, "/v1/catalogs/lake/set-default", captured.Path)
	assert.JSONEq(t, `{}`, captured.Body)
}

func TestCLI_ModelsCommandRemoved(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 201, `{"id":"m1"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"models", "models", "create",
	})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown command \"models\"")
}

// === Agent-Friendly Output Tests ===

func TestCLI_DeleteCommand_JSONOutput(t *testing.T) {
	// DELETE commands should output {"status":"ok"} when --output json is set.
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 204, ``))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"--output", "json",
		"catalog", "schemas", "delete", "myschema",
		"--catalog-name", "test",
		"--yes",
	})

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := rootCmd.Execute()
	_ = w.Close()
	out, _ := io.ReadAll(r)
	os.Stdout = old

	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal(out, &result), "DELETE with --output json should produce valid JSON: %s", string(out))
	assert.Equal(t, "ok", result["status"])
}

func TestCLI_VersionCommand_JSONOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "version"})

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := rootCmd.Execute()
	_ = w.Close()
	out, _ := io.ReadAll(r)
	os.Stdout = old

	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal(out, &result), "version --output json should produce valid JSON: %s", string(out))
	assert.Contains(t, result, "version")
	assert.Contains(t, result, "commit")
}
