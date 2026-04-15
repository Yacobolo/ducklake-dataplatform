package cli

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateConfigInitInputs(t *testing.T) {
	require.NoError(t, validateConfigInitInputs("hybrid", "development"))
	require.NoError(t, validateConfigInitInputs("oidc_only", "production"))
	require.Error(t, validateConfigInitInputs("wat", "development"))
	require.Error(t, validateConfigInitInputs("hybrid", "stage"))
}

func TestRenderEnvTemplate(t *testing.T) {
	content := renderEnvTemplate("hybrid", "production")
	assert.Contains(t, content, "ENV=production")
	assert.Contains(t, content, "AUTH_MODE=hybrid")
	assert.Contains(t, content, "TLS_CERT_FILE=")
	assert.Contains(t, content, "ENCRYPTION_KEY=replace-with-64-char-hex-key")

	content = renderEnvTemplate("local_only", "development")
	assert.Contains(t, content, "AUTH_MODE=local_only")
	assert.Contains(t, content, "JWT_SECRET=")
	assert.Contains(t, content, "CORS_ALLOWED_ORIGINS=*")
}

func TestConfigInitCommand_WritesFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()
	outPath := filepath.Join(dir, ".env.generated")

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "server", "env", "init", "--mode", "hybrid", "--env", "production", "--output", outPath})
	require.NoError(t, rootCmd.Execute())

	b, err := os.ReadFile(outPath)
	require.NoError(t, err)
	content := string(b)
	assert.Contains(t, content, "AUTH_MODE=hybrid")
	assert.Contains(t, content, "ENV=production")
}

func TestConfigInitCommand_RequiresForceToOverwrite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()
	outPath := filepath.Join(dir, ".env")
	require.NoError(t, os.WriteFile(outPath, []byte("existing"), 0o600))

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "server", "env", "init", "--output", outPath})
	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}
