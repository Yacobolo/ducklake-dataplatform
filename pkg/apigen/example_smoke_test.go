package apigen

import (
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExampleConsumer_CUEToGeneratedBuild(t *testing.T) {
	t.Helper()

	cwd, err := os.Getwd()
	require.NoError(t, err)

	repoRoot := repoRootFromWorkingDir(t, cwd)
	exampleRoot := prepareExampleWorkspace(t, repoRoot)
	cueDir := filepath.Join(exampleRoot, "api", "cue")

	cleanupPaths := []string{
		filepath.Join(exampleRoot, "api", "gen"),
		filepath.Join(exampleRoot, "internal", "api", "server.apigen.gen.go"),
		filepath.Join(exampleRoot, "internal", "api", "gen_request_models.gen.go"),
		filepath.Join(exampleRoot, "internal", "api", "types.gen.go"),
		filepath.Join(exampleRoot, "cmd", "cli", "gen", "apigen_registry.gen.go"),
		filepath.Join(exampleRoot, "server"),
		filepath.Join(exampleRoot, "cli"),
	}
	t.Cleanup(func() {
		for _, path := range cleanupPaths {
			_ = os.RemoveAll(path)
		}
	})
	for _, path := range cleanupPaths {
		_ = os.RemoveAll(path)
	}

	runCommand(
		t,
		repoRoot,
		"go",
		"run",
		"./cmd/apigen",
		"cue-compile",
		"-cue-dir", cueDir,
		"-ir-out", filepath.Join(exampleRoot, "api", "gen", "json-ir.json"),
		"-openapi-out", filepath.Join(exampleRoot, "api", "gen", "openapi.yaml"),
	)

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
		if strings.Contains(line, `"duck-demo/pkg/apigen/`) {
			continue
		}
		t.Fatalf("%s imports non-public repo package: %s", path, strings.TrimSpace(line))
	}
}

func repoRootFromWorkingDir(t *testing.T, cwd string) string {
	t.Helper()

	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate repo root from %s", cwd)
		}
		dir = parent
	}
}

func prepareExampleWorkspace(t *testing.T, repoRoot string) string {
	t.Helper()

	sourceRoot := filepath.Join(repoRoot, "pkg", "apigen", "testdata", "example_consumer")
	workspaceRoot := filepath.Join(t.TempDir(), "example-consumer")
	require.NoError(t, copyDir(sourceRoot, workspaceRoot))

	goModPath := filepath.Join(workspaceRoot, "go.mod")
	goModContent, err := os.ReadFile(goModPath)
	require.NoError(t, err)

	updatedGoMod := strings.ReplaceAll(
		string(goModContent),
		"replace duck-demo => ../..",
		"replace duck-demo => "+repoRoot,
	)
	require.NoError(t, os.WriteFile(goModPath, []byte(updatedGoMod), 0o644))

	return workspaceRoot
}

func copyDir(src string, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		targetPath := filepath.Join(dst, relPath)
		if d.IsDir() {
			return os.MkdirAll(targetPath, 0o755)
		}

		return copyFile(path, targetPath)
	})
}

func copyFile(src string, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		_ = srcFile.Close()
	}()

	info, err := srcFile.Stat()
	if err != nil {
		return err
	}

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	defer func() {
		_ = dstFile.Close()
	}()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return nil
}
