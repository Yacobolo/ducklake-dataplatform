package architecture_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCueSQLCutoverArtifactsRemainInExpectedState(t *testing.T) {
	t.Helper()

	root := repoRootDir()

	_, err := os.Stat(filepath.Join(root, "sqlc.yaml"))
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = os.Stat(filepath.Join(root, "internal/db/queries"))
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = os.Stat(filepath.Join(root, "internal/db/dbstore"))
	require.ErrorIs(t, err, os.ErrNotExist)

	entries, err := os.ReadDir(filepath.Join(root, "internal/db/querydefs"))
	require.NoError(t, err)
	assert.NotEmpty(t, entries)
}

func TestTaskGenerateUsesCueSQLOnly(t *testing.T) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(repoRootDir(), "Taskfile.yml"))
	require.NoError(t, err)
	content := string(body)

	assert.Contains(t, content, "cue-sql:")
	assert.Contains(t, content, "deps: [cue-sql, genduckdb, docs:generate]")
	assert.NotContains(t, content, "\n  sqlc:\n")
	assert.NotContains(t, content, "deps: [cue-sql, sqlc", "generate task should not depend on sqlc")
}
