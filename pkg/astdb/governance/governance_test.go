package governance_test

import (
	"context"
	"path/filepath"
	"testing"

	"duck-demo/pkg/astdb"
	"duck-demo/pkg/astdb/governance"

	"github.com/stretchr/testify/require"
)

func TestRunner_DefaultRulesAndAdhocQuery(t *testing.T) {
	t.Helper()

	duckPath := filepath.Join(t.TempDir(), "gov.duckdb")
	opts := astdb.DefaultOptions()
	opts.RepoRoot = filepath.Join("..", "..", "..")
	opts.Subdir = "internal"
	opts.MaxFiles = 250
	opts.Mode = "build"
	opts.QueryBench = false
	opts.DuckDBPath = duckPath
	opts.KeepOutputFiles = true
	opts.ForceRebuild = true

	_, err := astdb.Run(context.Background(), opts)
	require.NoError(t, err)

	runner := governance.NewRunner(duckPath)
	ctx := context.Background()

	rules, err := runner.ListRules(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, rules)

	violations, err := runner.Run(ctx, governance.RunOptions{})
	require.NoError(t, err)
	// Current repository should satisfy import boundary governance.
	require.Empty(t, violations)

	rows, err := runner.AdhocQuery(ctx, `SELECT COUNT(*) AS n FROM files`)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0]["n"])
}
