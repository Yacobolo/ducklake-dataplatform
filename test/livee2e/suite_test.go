//go:build livee2e

package livee2e

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"

	internalapi "duck-demo/internal/api"
	"duck-demo/pkg/cli/gen"
)

const (
	liveUsername      = "livee2e_admin"
	livePassword      = "LiveE2E#2026"
	livePrincipalName = "livee2e_admin"
)

type liveSuite struct {
	rootDir    string
	host       string
	cliPath    string
	token      string
	userToken  string
	runID      string
	httpClient *http.Client

	serverCmd *exec.Cmd
	logFile   *os.File
	logPath   string
	metaPath  string
	findingsPath string

	mu       sync.Mutex
	fixtures map[string]string
	findings []liveFinding
	doc      *openapi3.T
}

var suite *liveSuite

type liveFinding struct {
	Time        string `json:"time"`
	Category    string `json:"category"`
	Severity    string `json:"severity"`
	OperationID string `json:"operation_id,omitempty"`
	Method      string `json:"method,omitempty"`
	Path        string `json:"path,omitempty"`
	StatusCode  int    `json:"status_code,omitempty"`
	Message     string `json:"message"`
}

func TestMain(m *testing.M) {
	var err error
	suite, err = setupLiveSuite()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "setup livee2e suite: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	if teardownErr := suite.teardown(); teardownErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "teardown livee2e suite: %v\n", teardownErr)
		if code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}

func setupLiveSuite() (*liveSuite, error) {
	rootDir, err := repoRoot()
	if err != nil {
		return nil, err
	}

	s := &liveSuite{
		rootDir: rootDir,
		cliPath: filepath.Join(rootDir, "bin", "duck"),
		runID:   fmt.Sprintf("%d", time.Now().UTC().UnixNano()),
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
		fixtures: make(map[string]string),
	}
	if path := strings.TrimSpace(os.Getenv("E2E_LIVE_FINDINGS_PATH")); path != "" {
		s.findingsPath = path
	} else {
		s.findingsPath = filepath.Join(os.TempDir(), "livee2e-findings.json")
	}
	host, metaPath, err := deriveDevSettings(rootDir)
	if err != nil {
		return nil, err
	}
	s.host = host
	s.metaPath = metaPath

	doc, err := loadOpenAPISpec()
	if err != nil {
		return nil, err
	}
	s.doc = doc

	if reuse := strings.EqualFold(strings.TrimSpace(os.Getenv("E2E_LIVE_REUSE_SERVER")), "true"); reuse {
		s.host = strings.TrimRight(strings.TrimSpace(os.Getenv("E2E_LIVE_HOST")), "/")
		if s.host == "" {
			return nil, fmt.Errorf("E2E_LIVE_REUSE_SERVER=true requires E2E_LIVE_HOST")
		}
	} else if host := strings.TrimSpace(os.Getenv("E2E_LIVE_HOST")); host != "" {
		s.host = strings.TrimRight(host, "/")
	} else {
		if err := s.startManagedServer(); err != nil {
			return nil, err
		}
	}

	if err := s.waitForHealthy(); err != nil {
		return nil, err
	}
	if err := s.bootstrapAuth(); err != nil {
		return nil, err
	}
	if err := s.seedFixtures(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *liveSuite) teardown() error {
	if err := s.writeFindings(); err != nil {
		return err
	}
	if s.logFile != nil {
		defer s.logFile.Close() //nolint:errcheck
	}
	if s.serverCmd == nil || s.serverCmd.Process == nil {
		return nil
	}
	if err := s.serverCmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- s.serverCmd.Wait()
	}()

	select {
	case <-time.After(10 * time.Second):
		_ = s.serverCmd.Process.Kill()
		<-done
	case <-done:
	}
	return nil
}

func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("determine caller path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..")), nil
}

func loadOpenAPISpec() (*openapi3.T, error) {
	loader := openapi3.NewLoader()
	rootDir, err := repoRoot()
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(filepath.Join(rootDir, "api", "gen", "openapi.yaml"))
	if err != nil {
		return nil, fmt.Errorf("read canonical openapi yaml: %w", err)
	}
	return loader.LoadFromData(content)
}

func (s *liveSuite) startManagedServer() error {
	logFile, err := os.CreateTemp("", "livee2e-task-dev-*.log")
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}
	s.logFile = logFile
	s.logPath = logFile.Name()

	cmd := exec.Command("task", "dev")
	cmd.Dir = s.rootDir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start task dev: %w", err)
	}
	s.serverCmd = cmd

	hostCh := make(chan string, 1)
	scan := func(r io.Reader) {
		scanner := bufio.NewScanner(r)
		reHTTP := regexp.MustCompile(`HTTP:\s+(http://localhost:\d+)`)
		reMeta := regexp.MustCompile(`Meta DB:\s+(.+ducklake_meta_\d+\.sqlite)`)
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = fmt.Fprintln(logFile, line)
			if matches := reHTTP.FindStringSubmatch(line); len(matches) == 2 {
				select {
				case hostCh <- strings.TrimRight(matches[1], "/"):
				default:
				}
			}
			if matches := reMeta.FindStringSubmatch(line); len(matches) == 2 {
				metaPath := strings.TrimSpace(matches[1])
				if !filepath.IsAbs(metaPath) {
					metaPath = filepath.Join(s.rootDir, metaPath)
				}
				s.metaPath = metaPath
			}
		}
	}
	go scan(stdout)
	go scan(stderr)

	select {
	case <-hostCh:
		return nil
	case <-time.After(5 * time.Second):
		// task dev derives stable ports from the worktree path, so we already know
		// the host ahead of time. CI can spend longer compiling before it emits the
		// startup banner, so fall back to the derived host and let waitForHealthy
		// own readiness instead of failing on log timing.
		return nil
	}
}

func (s *liveSuite) waitForHealthy() error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := s.httpClient.Get(s.host + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s/healthz", s.host)
}

func (s *liveSuite) bootstrapAuth() error {
	if token := strings.TrimSpace(os.Getenv("E2E_LIVE_TOKEN")); token != "" {
		s.token = token
		return nil
	}

	body := map[string]string{
		"username":       liveUsername,
		"password":       livePassword,
		"principal_name": livePrincipalName,
	}
	resp, data, err := s.doJSON(context.Background(), http.MethodPost, "/v1/auth/bootstrap/complete", "", body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusCreated {
		token, err := tokenFromBody(data)
		if err != nil {
			return err
		}
		s.token = token
		return nil
	}

	loginResp, loginData, err := s.doJSON(context.Background(), http.MethodPost, "/v1/auth/local/login", "", map[string]string{
		"username": liveUsername,
		"password": livePassword,
	})
	if err != nil {
		return err
	}
	if loginResp.StatusCode != http.StatusCreated {
		if token, err := s.devAdminTokenFallback(); err == nil && token != "" {
			s.token = token
			return nil
		} else if err != nil {
			return fmt.Errorf("bootstrap/login failed: bootstrap=%d login=%d body=%s fallback=%v", resp.StatusCode, loginResp.StatusCode, string(loginData), err)
		}
		return fmt.Errorf("bootstrap/login failed: bootstrap=%d login=%d body=%s", resp.StatusCode, loginResp.StatusCode, string(loginData))
	}
	token, err := tokenFromBody(loginData)
	if err != nil {
		return err
	}
	s.token = token
	return nil
}

func (s *liveSuite) devAdminTokenFallback() (string, error) {
	if s.metaPath == "" {
		return "", fmt.Errorf("meta db path unavailable")
	}
	db, err := sql.Open("sqlite3", s.metaPath)
	if err != nil {
		return "", fmt.Errorf("open meta db: %w", err)
	}
	defer db.Close() //nolint:errcheck

	var principalName string
	err = db.QueryRowContext(context.Background(), `SELECT name FROM principals WHERE is_admin = 1 ORDER BY created_at LIMIT 1`).Scan(&principalName)
	if err != nil {
		return "", fmt.Errorf("query admin principal: %w", err)
	}

	secret, err := deriveDevJWTSecret(s.rootDir)
	if err != nil {
		return "", err
	}

	claims := jwt.MapClaims{
		"sub":   principalName,
		"email": principalName,
		"iat":   time.Now().Unix(),
		"exp":   time.Now().Add(2 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign fallback jwt: %w", err)
	}
	return signed, nil
}

func deriveDevJWTSecret(rootDir string) (string, error) {
	cmd := exec.Command("sh", "-c", `worktree_path="$(pwd -P)"; checksum="$(printf '%s' "$worktree_path" | cksum | awk '{print $1}')"; printf 'dev-jwt-%s' "$checksum"`)
	cmd.Dir = rootDir
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("derive dev jwt secret: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func deriveDevSettings(rootDir string) (host, metaPath string, err error) {
	cmd := exec.Command("sh", "-c", `worktree_path="$(pwd -P)"; checksum="$(printf '%s' "$worktree_path" | cksum | awk '{print $1}')"; offset=$((checksum % 1000)); printf 'http://localhost:%d\n.codex/dev/ducklake_meta_%d.sqlite\n' "$((8080 + offset))" "$offset"`)
	cmd.Dir = rootDir
	out, err := cmd.Output()
	if err != nil {
		return "", "", fmt.Errorf("derive dev settings: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		return "", "", fmt.Errorf("unexpected derived settings output: %q", string(out))
	}
	return strings.TrimSpace(lines[0]), filepath.Join(rootDir, strings.TrimSpace(lines[1])), nil
}

func tokenFromBody(data []byte) (string, error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("decode token payload: %w", err)
	}
	token, _ := payload["token"].(string)
	if token == "" {
		return "", fmt.Errorf("token missing from auth response")
	}
	return token, nil
}

func (s *liveSuite) seedFixtures() error {
	if err := s.seedAdminPrincipal(); err != nil {
		return err
	}
	if err := s.seedPrincipal(); err != nil {
		return err
	}
	if err := s.seedUserToken(); err != nil {
		return err
	}
	if err := s.seedGroup(); err != nil {
		return err
	}
	if err := s.seedTag(); err != nil {
		return err
	}
	if err := s.discoverCatalogFixtures(); err != nil {
		return err
	}
	return nil
}

func (s *liveSuite) seedAdminPrincipal() error {
	resp, data, err := s.doJSON(context.Background(), http.MethodGet, "/v1/principals", s.token, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("seed admin principal: unexpected status %d", resp.StatusCode)
	}
	var payload struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	for _, item := range payload.Data {
		isAdmin, _ := item["is_admin"].(bool)
		if !isAdmin {
			continue
		}
		id, _ := item["id"].(string)
		name, _ := item["name"].(string)
		if id == "" || name == "" {
			continue
		}
		s.fixtures["adminPrincipalId"] = id
		s.fixtures["adminPrincipalName"] = name
		return nil
	}
	return fmt.Errorf("seed admin principal: no admin principal found")
}

func (s *liveSuite) seedPrincipal() error {
	const name = "livee2e_user"
	resp, _, err := s.doJSON(context.Background(), http.MethodPost, "/v1/principals", s.token, map[string]any{
		"name": name,
		"type": "user",
	})
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("seed principal: unexpected status %d", resp.StatusCode)
	}
	id, err := s.lookupIDByName("/v1/principals", name)
	if err != nil {
		return err
	}
	s.fixtures["principalId"] = id
	s.fixtures["principalName"] = name
	return nil
}

func (s *liveSuite) seedUserToken() error {
	principalName := s.fixtures["principalName"]
	if principalName == "" {
		return fmt.Errorf("seed user token: missing principalName fixture")
	}
	token, err := s.signDevToken(principalName)
	if err != nil {
		return err
	}
	s.userToken = token
	return nil
}

func (s *liveSuite) seedGroup() error {
	const name = "livee2e_group"
	resp, _, err := s.doJSON(context.Background(), http.MethodPost, "/v1/groups", s.token, map[string]any{
		"name":        name,
		"description": "generated live e2e group",
	})
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("seed group: unexpected status %d", resp.StatusCode)
	}
	id, err := s.lookupIDByName("/v1/groups", name)
	if err != nil {
		return err
	}
	s.fixtures["groupId"] = id
	return nil
}

func (s *liveSuite) seedTag() error {
	resp, _, err := s.doJSON(context.Background(), http.MethodPost, "/v1/tags", s.token, map[string]any{
		"key":   "livee2e",
		"value": "generated",
	})
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("seed tag: unexpected status %d", resp.StatusCode)
	}
	id, err := s.lookupTagID("livee2e", "generated")
	if err != nil {
		return err
	}
	s.fixtures["tagId"] = id
	return nil
}

func (s *liveSuite) discoverCatalogFixtures() error {
	resp, data, err := s.doJSON(context.Background(), http.MethodGet, "/v1/catalogs", s.token, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	catalogName := firstDataField(data, "name")
	if catalogName == "" {
		return nil
	}
	s.fixtures["catalogName"] = catalogName
	if err := s.ensureDefaultCatalog(catalogName); err != nil {
		return err
	}

	path := fmt.Sprintf("/v1/catalogs/%s/schemas", catalogName)
	resp, data, err = s.doJSON(context.Background(), http.MethodGet, path, s.token, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	schemaName := firstDataField(data, "name")
	if schemaName == "" {
		return nil
	}
	s.fixtures["schemaName"] = schemaName

	path = fmt.Sprintf("/v1/catalogs/%s/schemas/%s/tables", catalogName, schemaName)
	resp, data, err = s.doJSON(context.Background(), http.MethodGet, path, s.token, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		if tableName := firstDataField(data, "name"); tableName != "" {
			s.fixtures["tableName"] = tableName
		}
	}

	path = fmt.Sprintf("/v1/catalogs/%s/schemas/%s/views", catalogName, schemaName)
	resp, data, err = s.doJSON(context.Background(), http.MethodGet, path, s.token, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		if viewName := firstDataField(data, "name"); viewName != "" {
			s.fixtures["viewName"] = viewName
		}
	}
	return nil
}

func (s *liveSuite) ensureDefaultCatalog(catalogName string) error {
	path := fmt.Sprintf("/v1/catalogs/%s/set-default", catalogName)
	resp, data, err := s.doJSON(context.Background(), http.MethodPost, path, s.token, map[string]any{})
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("set default catalog %q: status %d body=%s", catalogName, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}

func (s *liveSuite) lookupIDByName(path, name string) (string, error) {
	resp, data, err := s.doJSON(context.Background(), http.MethodGet, path, s.token, nil)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("lookup %s by name: status %d", path, resp.StatusCode)
	}
	var payload struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", err
	}
	for _, item := range payload.Data {
		if itemName, _ := item["name"].(string); itemName == name {
			id, _ := item["id"].(string)
			if id != "" {
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("name %q not found in %s", name, path)
}

func (s *liveSuite) lookupTagID(key, value string) (string, error) {
	resp, data, err := s.doJSON(context.Background(), http.MethodGet, "/v1/tags", s.token, nil)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("lookup tag: status %d", resp.StatusCode)
	}
	var payload struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", err
	}
	for _, item := range payload.Data {
		itemKey, _ := item["key"].(string)
		itemValue, _ := item["value"].(string)
		if itemKey == key && itemValue == value {
			id, _ := item["id"].(string)
			if id != "" {
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("tag %s=%s not found", key, value)
}

func firstDataField(data []byte, field string) string {
	var payload struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return ""
	}
	if len(payload.Data) == 0 {
		return ""
	}
	value, _ := payload.Data[0][field].(string)
	return value
}

func (s *liveSuite) doJSON(ctx context.Context, method, path, token string, body any) (*http.Response, []byte, error) {
	var bodyBytes []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, nil, err
		}
		bodyBytes = b
	}

	const maxAttempts = 5
	backoff := 100 * time.Millisecond
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var reader io.Reader
		if bodyBytes != nil {
			reader = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, s.host+path, reader)
		if err != nil {
			return nil, nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			return nil, nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, nil, readErr
		}
		resp.Body = io.NopCloser(bytes.NewReader(data))
		if resp.StatusCode != http.StatusTooManyRequests || attempt == maxAttempts {
			return resp, data, nil
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return nil, nil, fmt.Errorf("unreachable retry loop for %s %s", method, path)
}

func (s *liveSuite) signDevToken(principalName string) (string, error) {
	secret, err := deriveDevJWTSecret(s.rootDir)
	if err != nil {
		return "", err
	}
	claims := jwt.MapClaims{
		"sub":   principalName,
		"email": principalName,
		"iat":   time.Now().Unix(),
		"exp":   time.Now().Add(2 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign dev jwt for %s: %w", principalName, err)
	}
	return signed, nil
}

func (s *liveSuite) runCLI(t *testing.T, args ...string) []byte {
	t.Helper()
	require.FileExists(t, s.cliPath)

	cmd := exec.Command(s.cliPath, args...)
	cmd.Dir = s.rootDir
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "run cli %v: %s", args, string(out))
	return out
}

func (s *liveSuite) operationFilterAllowed(tags []string) bool {
	filter := strings.TrimSpace(os.Getenv("E2E_LIVE_TAGS"))
	if filter == "" {
		return true
	}
	allowed := make(map[string]struct{})
	for _, tag := range strings.Split(filter, ",") {
		tag = strings.TrimSpace(strings.ToLower(tag))
		if tag != "" {
			allowed[tag] = struct{}{}
		}
	}
	for _, tag := range tags {
		if _, ok := allowed[strings.ToLower(tag)]; ok {
			return true
		}
	}
	return false
}

func (s *liveSuite) operationLimit() int {
	raw := strings.TrimSpace(os.Getenv("E2E_LIVE_LIMIT"))
	if raw == "" {
		return 0
	}
	var n int
	_, _ = fmt.Sscanf(raw, "%d", &n)
	return n
}

type liveOperation struct {
	ref            gen.ReferenceOperation
	spec           *openapi3.Operation
	documentedCode map[int]struct{}
	contract       internalapi.GenOperationContract
}

func mergedOperations(t *testing.T) []liveOperation {
	t.Helper()
	opMap := make(map[string]*openapi3.Operation)
	for _, pathItem := range suite.doc.Paths.Map() {
		for _, operation := range pathItem.Operations() {
			opMap[operation.OperationID] = operation
		}
	}
	out := make([]liveOperation, 0, len(gen.CLIReferenceIndex.Operations))
	for _, ref := range gen.CLIReferenceIndex.Operations {
		spec := opMap[ref.OperationID]
		if spec == nil {
			continue
		}
		documented := make(map[int]struct{}, len(spec.Responses.Map()))
		for code := range spec.Responses.Map() {
			var n int
			if _, err := fmt.Sscanf(code, "%d", &n); err == nil {
				documented[n] = struct{}{}
			}
		}
		contract, _ := internalapi.GetAPIGenOperationContract(ref.OperationID)
		out = append(out, liveOperation{ref: ref, spec: spec, documentedCode: documented, contract: contract})
	}
	return out
}

func (s *liveSuite) resolvePath(op liveOperation) (string, bool) {
	path := op.ref.Path
	for _, param := range op.ref.Parameters {
		if param.In != "path" {
			continue
		}
		value := s.fixtures[param.Name]
		if value == "" {
			return "", false
		}
		path = strings.ReplaceAll(path, "{"+param.Name+"}", value)
	}
	return "/v1" + path, true
}

func (s *liveSuite) buildQueryPath(op liveOperation, extra url.Values) (string, bool) {
	path, ok := s.resolvePath(op)
	if !ok {
		return "", false
	}
	values, ok := s.inferQueryParams(op)
	if !ok {
		return "", false
	}
	for key, incoming := range extra {
		values.Del(key)
		for _, value := range incoming {
			values.Add(key, value)
		}
	}
	if len(values) == 0 {
		return path, true
	}
	return path + "?" + values.Encode(), true
}

func hasParam(op liveOperation, name string) bool {
	for _, param := range op.ref.Parameters {
		if param.Name == name {
			return true
		}
	}
	return false
}

func hasRequiredBody(op liveOperation) bool {
	for _, field := range op.ref.BodyFields {
		if field.Required {
			return true
		}
	}
	return false
}

func bodyStatusAllowed(op liveOperation, status int) bool {
	if _, ok := op.documentedCode[status]; ok {
		return true
	}
	return false
}

func (s *liveSuite) recordFinding(category, severity string, op liveOperation, path string, statusCode int, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.findings = append(s.findings, liveFinding{
		Time:        time.Now().UTC().Format(time.RFC3339),
		Category:    category,
		Severity:    severity,
		OperationID: op.ref.OperationID,
		Method:      op.ref.Method,
		Path:        path,
		StatusCode:  statusCode,
		Message:     message,
	})
}

func (s *liveSuite) writeFindings() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.findingsPath
	if path == "" {
		path = filepath.Join(os.TempDir(), "livee2e-findings.json")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir findings dir: %w", err)
	}
	payload := struct {
		GeneratedAt string        `json:"generated_at"`
		Host        string        `json:"host"`
		Findings    []liveFinding `json:"findings"`
	}{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Host:        s.host,
		Findings:    append([]liveFinding(nil), s.findings...),
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal findings: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write findings: %w", err)
	}
	s.findingsPath = path
	return nil
}

func (s *liveSuite) inferQueryParams(op liveOperation) (url.Values, bool) {
	values := url.Values{}
	for _, paramRef := range op.spec.Parameters {
		param := paramRef.Value
		if param == nil || param.In != "query" {
			continue
		}
		if !param.Required {
			continue
		}
		value, ok := s.inferScalarValue(param.Schema, param.Name)
		if !ok {
			return nil, false
		}
		values.Set(param.Name, fmt.Sprint(value))
	}
	return values, true
}

func (s *liveSuite) inferBodyPayload(op liveOperation) (map[string]any, []string, bool) {
	if op.spec.RequestBody == nil || op.spec.RequestBody.Value == nil {
		return nil, nil, false
	}
	content, ok := op.spec.RequestBody.Value.Content["application/json"]
	if !ok || content.Schema == nil {
		return nil, nil, false
	}
	value, ok := s.inferSchemaValue(content.Schema, "")
	if !ok {
		return nil, nil, false
	}
	body, ok := value.(map[string]any)
	if !ok {
		return nil, nil, false
	}
	required := requiredPropertyNames(content.Schema)
	if len(required) == 0 {
		return nil, nil, false
	}
	return body, required, true
}

func requiredPropertyNames(schemaRef *openapi3.SchemaRef) []string {
	if schemaRef == nil || schemaRef.Value == nil {
		return nil
	}
	schema := schemaRef.Value
	if len(schema.Required) > 0 {
		return append([]string(nil), schema.Required...)
	}
	for _, candidate := range append(schema.OneOf, schema.AnyOf...) {
		if names := requiredPropertyNames(candidate); len(names) > 0 {
			return names
		}
	}
	return nil
}

func (s *liveSuite) inferSchemaValue(schemaRef *openapi3.SchemaRef, name string) (any, bool) {
	if schemaRef == nil || schemaRef.Value == nil {
		return nil, false
	}
	schema := schemaRef.Value
	if len(schema.Enum) > 0 {
		if len(schema.Enum) > 0 {
			return schema.Enum[0], true
		}
	}
	if len(schema.OneOf) > 0 {
		return s.inferSchemaValue(schema.OneOf[0], name)
	}
	if len(schema.AnyOf) > 0 {
		return s.inferSchemaValue(schema.AnyOf[0], name)
	}
	if len(schema.AllOf) > 0 {
		merged := map[string]any{}
		for _, part := range schema.AllOf {
			value, ok := s.inferSchemaValue(part, name)
			if !ok {
				continue
			}
			obj, ok := value.(map[string]any)
			if !ok {
				return value, true
			}
			for key, item := range obj {
				merged[key] = item
			}
		}
		if len(merged) > 0 {
			return merged, true
		}
	}

	switch {
	case schema.Type != nil && schema.Type.Is("object"):
		if len(schema.Properties) == 0 {
			return map[string]any{}, true
		}
		payload := make(map[string]any)
		for _, property := range schema.Required {
			propSchema, ok := schema.Properties[property]
			if !ok {
				continue
			}
			value, ok := s.inferSchemaValue(propSchema, property)
			if !ok {
				return nil, false
			}
			payload[property] = value
		}
		return payload, true
	case schema.Type != nil && schema.Type.Is("array"):
		if schema.Items == nil {
			return []any{}, true
		}
		item, ok := s.inferSchemaValue(schema.Items, name)
		if !ok {
			return nil, false
		}
		return []any{item}, true
	default:
		return s.inferScalarValue(schemaRef, name)
	}
}

func (s *liveSuite) inferScalarValue(schemaRef *openapi3.SchemaRef, name string) (any, bool) {
	if schemaRef == nil || schemaRef.Value == nil {
		return nil, false
	}
	schema := schemaRef.Value
	if len(schema.Enum) > 0 {
		return schema.Enum[0], true
	}
	lowerName := strings.ToLower(name)
	switch {
	case schema.Type != nil && schema.Type.Is("string"):
		switch {
		case strings.Contains(lowerName, "principal") && strings.Contains(lowerName, "id"):
			return s.fixtures["principalId"], s.fixtures["principalId"] != ""
		case strings.Contains(lowerName, "group") && strings.Contains(lowerName, "id"):
			return s.fixtures["groupId"], s.fixtures["groupId"] != ""
		case strings.Contains(lowerName, "tag") && strings.Contains(lowerName, "id"):
			return s.fixtures["tagId"], s.fixtures["tagId"] != ""
		case strings.Contains(lowerName, "catalog") && strings.Contains(lowerName, "name"):
			return s.fixtures["catalogName"], s.fixtures["catalogName"] != ""
		case strings.Contains(lowerName, "schema") && strings.Contains(lowerName, "name"):
			return s.fixtures["schemaName"], s.fixtures["schemaName"] != ""
		case strings.Contains(lowerName, "table") && strings.Contains(lowerName, "name"):
			return s.fixtures["tableName"], s.fixtures["tableName"] != ""
		case strings.Contains(lowerName, "view") && strings.Contains(lowerName, "name"):
			return s.fixtures["viewName"], s.fixtures["viewName"] != ""
		case lowerName == "query":
			return "livee2e", true
		case lowerName == "key":
			return "livee2e", true
		case lowerName == "value":
			return "generated", true
		case lowerName == "name":
			return "livee2e-generated", true
		}
		switch schema.Format {
		case "date-time":
			return time.Now().UTC().Format(time.RFC3339), true
		case "date":
			return time.Now().UTC().Format("2006-01-02"), true
		case "uuid":
			return "00000000-0000-0000-0000-000000000001", true
		}
		if schema.MinLength > 1 {
			return strings.Repeat("x", int(schema.MinLength)), true
		}
		return "livee2e", true
	case schema.Type != nil && schema.Type.Is("integer"):
		return 1, true
	case schema.Type != nil && schema.Type.Is("number"):
		return 1, true
	case schema.Type != nil && schema.Type.Is("boolean"):
		return true, true
	}
	return nil, false
}

func parseAnyJSON(data []byte) any {
	var payload any
	_ = json.Unmarshal(data, &payload)
	return payload
}

func yamlToJSONBytes(t *testing.T, yamlBytes []byte) []byte {
	t.Helper()
	var payload any
	require.NoError(t, yaml.Unmarshal(yamlBytes, &payload))
	jsonBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	return jsonBytes
}
