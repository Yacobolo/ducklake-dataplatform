package apigen

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMainRepoCueParity(t *testing.T) {
	t.Helper()

	cwd, err := os.Getwd()
	require.NoError(t, err)

	repoRoot := repoRootFromWorkingDir(t, cwd)
	apigenRoot := filepath.Join(repoRoot, "pkg", "apigen")
	tempDir := t.TempDir()
	irPath := filepath.Join(tempDir, "json-ir.json")
	openAPIPath := filepath.Join(tempDir, "openapi.yaml")
	serverOut := filepath.Join(tempDir, "server.apigen.gen.go")
	requestModelsOut := filepath.Join(tempDir, "gen_request_models.gen.go")
	typesOut := filepath.Join(tempDir, "types.gen.go")
	cliOut := filepath.Join(tempDir, "apigen_registry.gen.go")
	manifestPath := filepath.Join(tempDir, "apigen.targets.yaml")

	require.NoError(t, os.WriteFile(manifestPath, []byte(fmt.Sprintf(`targets:
  - name: parity
    cue_dir: %s
    ir_out: %s
    openapi_out: %s
    server_out: %s
    server_package: api
    request_models_out: %s
    request_models_package: api
    compat_types_out: %s
    compat_types_package: api
    cli_out: %s
    cli_package: gen
    generate_cli: true
`, filepath.Join(repoRoot, "api", "v1", "cue"), irPath, openAPIPath, serverOut, requestModelsOut, typesOut, cliOut)), 0o644))

	runCommand(t, apigenRoot, "go", "run", "./cmd/apigen", "cue-compile", "-manifest", manifestPath, "-target", "parity")

	requireFileEquals(t, openAPIPath, filepath.Join(repoRoot, "internal", "api", "gen", "openapi.yaml"))

	runCommand(
		t,
		apigenRoot,
		"go",
		"run",
		"./cmd/apigen",
		"all",
		"-manifest", manifestPath,
		"-target", "parity",
	)

	requireFileEquals(t, requestModelsOut, filepath.Join(repoRoot, "internal", "api", "gen_request_models.gen.go"))
	requireFileEquals(t, serverOut, filepath.Join(repoRoot, "internal", "api", "server.apigen.gen.go"))
	requireFileEquals(t, typesOut, filepath.Join(repoRoot, "internal", "api", "types.gen.go"))
	requireFileEquals(t, cliOut, filepath.Join(repoRoot, "pkg", "cli", "gen", "apigen_registry.gen.go"))
}

func requireFileEquals(t *testing.T, gotPath string, wantPath string) {
	t.Helper()

	got := mustReadFile(t, gotPath)
	want := mustReadFile(t, wantPath)
	require.Equal(t, want, got, "file mismatch for %s", wantPath)
}
