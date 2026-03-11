package cli

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscover_JSONOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "discover", "catalog schemas create"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload struct {
		Results []struct {
			Kind  string `json:"kind"`
			Title string `json:"title"`
		} `json:"results"`
	}
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	require.NotEmpty(t, payload.Results)

	var found bool
	for _, result := range payload.Results {
		if result.Kind == "command" && result.Title == "catalog schemas create" {
			found = true
			break
		}
	}
	assert.True(t, found)
}
