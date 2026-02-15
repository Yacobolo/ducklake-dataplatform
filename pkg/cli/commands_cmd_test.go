package cli

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommands_ListAll(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetArgs([]string{"commands"})

	err := rootCmd.Execute()
	require.NoError(t, err)
}

func TestCommands_JSONOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetArgs([]string{"--output", "json", "commands"})

	// Capture stdout
	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var entries []CommandEntry
	require.NoError(t, json.Unmarshal([]byte(output), &entries), "output should be valid JSON")
	assert.Greater(t, len(entries), 50, "should have many commands (generated + hand-written)")

	// Verify structure
	found := false
	for _, e := range entries {
		if e.Path != "" && e.Group != "" && e.Short != "" {
			found = true
			break
		}
	}
	assert.True(t, found, "entries should have path, group, and short fields")
}

func TestCommands_Filter(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "commands", "--filter", "row-filter"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var entries []CommandEntry
	require.NoError(t, json.Unmarshal([]byte(output), &entries))
	assert.NotEmpty(t, entries, "filter should match at least one command")
	for _, e := range entries {
		assert.True(t,
			containsIgnoreCase(e.Path, "row-filter") || containsIgnoreCase(e.Short, "row-filter") || containsIgnoreCase(e.Long, "row-filter"),
			"filtered entry should match query: %s", e.Path)
	}
}

func TestCommands_FilterGroup(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "commands", "--group", "security"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var entries []CommandEntry
	require.NoError(t, json.Unmarshal([]byte(output), &entries))
	assert.NotEmpty(t, entries, "security group should have commands")
	for _, e := range entries {
		assert.Equal(t, "security", e.Group, "all entries should be in security group")
	}
}

func TestCommands_HasFlags(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "commands", "--filter", "schemas create"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var entries []CommandEntry
	require.NoError(t, json.Unmarshal([]byte(output), &entries))
	require.NotEmpty(t, entries, "should find schemas create command")

	// The create schema command should have flags
	for _, e := range entries {
		if e.Path == "catalog schemas create" {
			assert.NotEmpty(t, e.Flags, "create command should have flags")
			return
		}
	}
}
