package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanCmd_InvalidOutputFormat(t *testing.T) {
	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--host", "http://127.0.0.1:1",
		"plan",
		"--config-dir", t.TempDir(),
		"--output", "yaml",
	})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported output format \"yaml\": use 'text' or 'json'")
}
