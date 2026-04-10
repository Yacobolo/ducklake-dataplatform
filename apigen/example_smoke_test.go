package apigen

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExampleConsumer_TypeSpecToGeneratedBuild(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm is required for the example TypeSpec smoke test")
	}

	cwd, err := os.Getwd()
	require.NoError(t, err)

	repoRoot := filepath.Dir(cwd)
	exampleRoot := filepath.Join(repoRoot, "examples", "apigen-consumer")
	specDir := filepath.Join(exampleRoot, "api", "spec")

	cleanupPaths := []string{
		filepath.Join(specDir, "node_modules"),
		filepath.Join(exampleRoot, "api", "gen"),
		filepath.Join(exampleRoot, "internal", "api", "server.apigen.gen.go"),
		filepath.Join(exampleRoot, "internal", "api", "gen_request_models.gen.go"),
		filepath.Join(exampleRoot, "internal", "api", "types.gen.go"),
		filepath.Join(exampleRoot, "cmd", "cli", "gen", "apigen_registry.gen.go"),
	}
	t.Cleanup(func() {
		for _, path := range cleanupPaths {
			_ = os.RemoveAll(path)
		}
	})

	runCommand(t, specDir, "npm", "install", "--no-fund", "--no-audit", "--package-lock=false")
	runCommand(t, specDir, "npm", "run", "compile")

	runCommand(
		t,
		repoRoot,
		"go",
		"run",
		"./cmd/apigen",
		"all",
		"-ir", filepath.Join(exampleRoot, "api", "gen", "json-ir.json"),
		"-canonical-openapi", filepath.Join(exampleRoot, "api", "gen", "openapi.yaml"),
		"-server-out", filepath.Join(exampleRoot, "internal", "api", "server.apigen.gen.go"),
		"-server-package", "api",
		"-request-models-out", filepath.Join(exampleRoot, "internal", "api", "gen_request_models.gen.go"),
		"-request-models-package", "api",
		"-compat-types-out", filepath.Join(exampleRoot, "internal", "api", "types.gen.go"),
		"-compat-types-package", "api",
		"-cli-out", filepath.Join(exampleRoot, "cmd", "cli", "gen", "apigen_registry.gen.go"),
		"-cli-package", "gen",
	)

	runCommand(t, exampleRoot, "go", "build", "./cmd/server")
	runCommand(t, exampleRoot, "go", "build", "./cmd/cli")

	serverGenerated := mustReadFile(t, filepath.Join(exampleRoot, "internal", "api", "server.apigen.gen.go"))
	require.Contains(t, serverGenerated, "APIGen Example API")

	cliGenerated := mustReadFile(t, filepath.Join(exampleRoot, "cmd", "cli", "gen", "apigen_registry.gen.go"))
	require.Contains(t, cliGenerated, `CLICommand: "widgets list"`)
	require.Contains(t, cliGenerated, `CLICommand: "widgets create"`)
	require.NotContains(t, cliGenerated, "deleteWidget")

	assertGeneratedImportsUsePublicSurfaces(t, filepath.Join(exampleRoot, "internal", "api", "server.apigen.gen.go"))
	assertGeneratedImportsUsePublicSurfaces(t, filepath.Join(exampleRoot, "internal", "api", "gen_request_models.gen.go"))
	assertGeneratedImportsUsePublicSurfaces(t, filepath.Join(exampleRoot, "internal", "api", "types.gen.go"))
	assertGeneratedImportsUsePublicSurfaces(t, filepath.Join(exampleRoot, "cmd", "cli", "gen", "apigen_registry.gen.go"))
}

func runCommand(t *testing.T, dir string, name string, args ...string) {
	t.Helper()

	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed:\n%s", name, strings.Join(args, " "), string(output))
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}

func assertGeneratedImportsUsePublicSurfaces(t *testing.T, path string) {
	t.Helper()

	content := mustReadFile(t, path)
	for _, line := range strings.Split(content, "\n") {
		if !strings.Contains(line, `"duck-demo/`) {
			continue
		}
		if strings.Contains(line, `"duck-demo/apigen/`) {
			continue
		}
		t.Fatalf("%s imports non-public repo package: %s", path, strings.TrimSpace(line))
	}
}
