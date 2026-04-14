package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/declarative"
)

func TestResolveDeclarativeInitOptions(t *testing.T) {
	opts, err := resolveDeclarativeInitOptions(declarativeInitOptions{
		name:      "analytics",
		workspace: "main",
		owner:     "alice",
		template:  "analytics",
		outputDir: "./quackstack-config",
	})
	require.NoError(t, err)
	assert.Equal(t, "quackstack.local/quackstack-config", opts.module)

	_, err = resolveDeclarativeInitOptions(declarativeInitOptions{
		name:      "Bad Name",
		workspace: "main",
		owner:     "alice",
		template:  "analytics",
		outputDir: "./quackstack-config",
	})
	require.Error(t, err)

	_, err = resolveDeclarativeInitOptions(declarativeInitOptions{
		name:      "analytics",
		workspace: "main",
		owner:     "alice",
		template:  "wat",
		outputDir: "./quackstack-config",
	})
	require.Error(t, err)
}

func TestDeclarativeInitCommand_WritesMinimalScaffold(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	outDir := filepath.Join(t.TempDir(), "starter")

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"declarative", "init",
		"--template", "minimal",
		"--name", "sales",
		"--workspace", "main",
		"--owner", "alice",
		"--output-dir", outDir,
	})
	require.NoError(t, rootCmd.Execute())

	_, err := os.Stat(filepath.Join(outDir, "cue.mod", "module.cue"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(outDir, "workspaces", "main", "workspace.cue"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(outDir, "projects", "sales", "project.cue"))
	require.NoError(t, err)

	desired, err := declarative.LoadDirectory(outDir)
	require.NoError(t, err)
	assert.Empty(t, declarative.Validate(desired))
}

func TestDeclarativeInitCommand_WritesAnalyticsScaffold(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	outDir := filepath.Join(t.TempDir(), "analytics-config")

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"declarative", "init",
		"--template", "analytics",
		"--name", "revenue",
		"--workspace", "studio",
		"--owner", "alice",
		"--output-dir", outDir,
	})
	require.NoError(t, rootCmd.Execute())

	content, err := os.ReadFile(filepath.Join(outDir, "workspaces", "studio", "folders.cue"))
	require.NoError(t, err)
	assert.Contains(t, string(content), "Starter analytics notebook")

	desired, err := declarative.LoadDirectory(outDir)
	require.NoError(t, err)
	assert.Empty(t, declarative.Validate(desired))
}

func TestDeclarativeInitCommand_RequiresForceToOverwrite(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	outDir := filepath.Join(t.TempDir(), "starter")
	require.NoError(t, os.MkdirAll(outDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(outDir, "existing.txt"), []byte("existing"), 0o644))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"declarative", "init",
		"--name", "sales",
		"--workspace", "main",
		"--owner", "alice",
		"--output-dir", outDir,
	})
	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists and is not empty")
}

func TestDeclarativeInitCommand_JSONOutput(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	outDir := filepath.Join(t.TempDir(), "starter")

	rootCmd := newRootCmd()
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = oldStdout
	})

	rootCmd.SetArgs([]string{
		"--output", "json",
		"declarative", "init",
		"--template", "minimal",
		"--name", "sales",
		"--workspace", "main",
		"--owner", "alice",
		"--output-dir", outDir,
	})
	require.NoError(t, rootCmd.Execute())
	require.NoError(t, w.Close())

	var buf bytes.Buffer
	_, err = buf.ReadFrom(r)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "\"status\": \"ok\"")
}
