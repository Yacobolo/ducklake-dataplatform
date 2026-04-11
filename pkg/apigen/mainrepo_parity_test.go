package apigen

import (
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
	tempDir := t.TempDir()
	irPath := filepath.Join(tempDir, "json-ir.json")
	openAPIPath := filepath.Join(tempDir, "openapi.yaml")

	runCommand(t, repoRoot, "go", "run", "./cmd/apigen", "cue-compile", "-cue-dir", filepath.Join(repoRoot, "api", "cue"), "-ir-out", irPath, "-openapi-out", openAPIPath)

	requireFileEquals(t, openAPIPath, filepath.Join(repoRoot, "api", "gen", "openapi.yaml"))

	serverOut := filepath.Join(tempDir, "server.apigen.gen.go")
	requestModelsOut := filepath.Join(tempDir, "gen_request_models.gen.go")
	typesOut := filepath.Join(tempDir, "types.gen.go")
	cliOut := filepath.Join(tempDir, "apigen_registry.gen.go")

	runCommand(
		t,
		repoRoot,
		"go",
		"run",
		"./cmd/apigen",
		"all",
		"-ir", irPath,
		"-canonical-openapi", openAPIPath,
		"-server-out", serverOut,
		"-server-package", "api",
		"-request-models-out", requestModelsOut,
		"-request-models-package", "api",
		"-compat-types-out", typesOut,
		"-compat-types-package", "api",
		"-cli-out", cliOut,
		"-cli-package", "gen",
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
