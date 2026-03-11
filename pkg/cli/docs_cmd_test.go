package cli

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocsSearch_JSONOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "docs", "search", "auth"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload struct {
		Results []struct {
			ID           string   `json:"id"`
			ProductAreas []string `json:"product_areas"`
		} `json:"results"`
	}
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	assert.NotEmpty(t, payload.Results)
	assert.NotEmpty(t, payload.Results[0].ProductAreas)
}

func TestDocsShow_JSONOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "docs", "show", "how-to/authentication"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload struct {
		Doc struct {
			ID           string   `json:"ID"`
			Title        string   `json:"Title"`
			Audiences    []string `json:"Audiences"`
			LastVerified string   `json:"LastVerified"`
		} `json:"doc"`
		RelatedDocs       []string `json:"related_docs"`
		RelatedOperations []string `json:"related_operations"`
	}
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	assert.Equal(t, "how-to/authentication", payload.Doc.ID)
	assert.NotEmpty(t, payload.Doc.Title)
	assert.NotEmpty(t, payload.Doc.Audiences)
	assert.NotEmpty(t, payload.Doc.LastVerified)
	assert.NotEmpty(t, payload.RelatedDocs)
}

func TestDocsList_JSONFilters(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "docs", "list", "--product-area", "auth", "--doc-kind", "task"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload []struct {
		ID string `json:"ID"`
	}
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	require.NotEmpty(t, payload)
	assert.Equal(t, "how-to/authentication", payload[0].ID)
}
