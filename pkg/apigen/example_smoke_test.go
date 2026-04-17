package apigen

import (
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExampleConsumer_CUEToGeneratedBuild(t *testing.T) {
	t.Helper()

	cwd, err := os.Getwd()
	require.NoError(t, err)

	repoRoot := repoRootFromWorkingDir(t, cwd)
	apigenRoot := filepath.Join(repoRoot, "pkg", "apigen")
	tests := []struct {
		name         string
		basePath     string
		expectedPath string
	}{
		{name: "root base path", basePath: "/", expectedPath: "/widgets"},
		{name: "versioned base path", basePath: "/v1", expectedPath: "/v1/widgets"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			exampleRoot := prepareExampleWorkspace(t, repoRoot)
			addr := allocateLoopbackAddr(t)
			baseURL := "http://" + addr
			cueDir := filepath.Join(exampleRoot, "api", "cue")

			configureExampleWorkspace(t, exampleRoot, baseURL, tt.basePath)

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
				apigenRoot,
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
				apigenRoot,
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

			serverBinary := filepath.Join(exampleRoot, "server")
			cliBinary := filepath.Join(exampleRoot, "cli")
			runCommand(t, exampleRoot, "go", "build", "-o", serverBinary, "./cmd/server")
			runCommand(t, exampleRoot, "go", "build", "-o", cliBinary, "./cmd/cli")

			serverGenerated := mustReadFile(t, filepath.Join(exampleRoot, "internal", "api", "server.apigen.gen.go"))
			require.Contains(t, serverGenerated, "APIGen Example API")
			require.Contains(t, serverGenerated, tt.expectedPath)

			cliGenerated := mustReadFile(t, filepath.Join(exampleRoot, "cmd", "cli", "gen", "apigen_registry.gen.go"))
			require.Contains(t, cliGenerated, `Command: []string{"widgets", "list"}`)
			require.Contains(t, cliGenerated, `Command: []string{"widgets", "create"}`)
			require.Contains(t, cliGenerated, `Path: "`+tt.expectedPath+`"`)
			require.NotContains(t, cliGenerated, "deleteWidget")

			assertGeneratedImportsUsePublicSurfaces(t, filepath.Join(exampleRoot, "internal", "api", "server.apigen.gen.go"))
			assertGeneratedImportsUsePublicSurfaces(t, filepath.Join(exampleRoot, "internal", "api", "gen_request_models.gen.go"))
			assertGeneratedImportsUsePublicSurfaces(t, filepath.Join(exampleRoot, "internal", "api", "types.gen.go"))
			assertGeneratedImportsUsePublicSurfaces(t, filepath.Join(exampleRoot, "cmd", "cli", "gen", "apigen_registry.gen.go"))

			serverCmd := exec.Command(serverBinary)
			serverCmd.Dir = exampleRoot
			serverOutput := &strings.Builder{}
			serverCmd.Stdout = serverOutput
			serverCmd.Stderr = serverOutput
			require.NoError(t, serverCmd.Start())
			t.Cleanup(func() {
				_ = serverCmd.Process.Kill()
				_ = serverCmd.Wait()
			})

			waitForHTTP(t, baseURL+tt.expectedPath)

			listOutput := runCommandOutput(t, exampleRoot, cliBinary, "widgets", "list")
			require.Contains(t, listOutput, "widget-1")
			require.Contains(t, listOutput, "first")

			createOutput := runCommandOutput(t, exampleRoot, cliBinary, "widgets", "create", "demo-widget")
			require.Contains(t, createOutput, "widget-2")
			require.Contains(t, createOutput, "created")
		})
	}
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

func runCommandOutput(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()

	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed:\n%s", name, strings.Join(args, " "), string(output))
	}
	return string(output)
}

func allocateLoopbackAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener.Close()
	}()
	return listener.Addr().String()
}

func configureExampleWorkspace(t *testing.T, workspaceRoot string, baseURL string, basePath string) {
	t.Helper()

	metadataPath := filepath.Join(workspaceRoot, "api", "cue", "metadata.cue")
	updateFile(t, metadataPath, `base_path: "/"`, `base_path: "`+basePath+`"`)
	updateFile(t, metadataPath, `url:         "http://127.0.0.1:8081"`, `url:         "`+baseURL+`"`)

	serverMainPath := filepath.Join(workspaceRoot, "cmd", "server", "main.go")
	updateFile(t, serverMainPath, `":8081"`, `"`+strings.TrimPrefix(baseURL, "http://")+`"`)

	cliMainPath := filepath.Join(workspaceRoot, "cmd", "cli", "main.go")
	updateFile(t, cliMainPath, `"http://127.0.0.1:8081"`, `"`+baseURL+`"`)
}

func updateFile(t *testing.T, path string, old string, new string) {
	t.Helper()
	if old == new {
		return
	}

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	updated := strings.Replace(string(content), old, new, 1)
	require.NotEqual(t, string(content), updated, "expected to update %s", path)
	require.NoError(t, os.WriteFile(path, []byte(updated), 0o644))
}

func waitForHTTP(t *testing.T, endpoint string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(endpoint) //nolint:noctx
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 500 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", endpoint)
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
		if !strings.Contains(line, `"github.com/Yacobolo/quackstack/`) {
			continue
		}
		if strings.Contains(line, `"github.com/Yacobolo/quackstack/pkg/apigen/`) {
			continue
		}
		t.Fatalf("%s imports non-public repo package: %s", path, strings.TrimSpace(line))
	}
}

func repoRootFromWorkingDir(t *testing.T, cwd string) string {
	t.Helper()

	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "AGENTS.md")); err == nil {
			return dir
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			parent := filepath.Dir(dir)
			if parent == dir {
				return dir
			}
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
		"replace github.com/Yacobolo/quackstack/pkg/apigen => ../..",
		"replace github.com/Yacobolo/quackstack/pkg/apigen => "+filepath.Join(repoRoot, "pkg", "apigen"),
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
