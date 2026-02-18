//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/declarative"
)

type exampleExpectation struct {
	MinModelCreates int
	MinMacroCreates int
}

var exampleExpectations = map[string]exampleExpectation{
	"showcase-movielens": {
		MinModelCreates: 8,
		MinMacroCreates: 1,
	},
}

func TestDeclarativeExamples_Lifecycle(t *testing.T) {
	repoRoot := findRepoRoot(t)
	examplesRoot := filepath.Join(repoRoot, "examples")

	entries, err := os.ReadDir(examplesRoot)
	require.NoError(t, err)

	exampleNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		configDir := filepath.Join(examplesRoot, entry.Name(), "config")
		if info, statErr := os.Stat(configDir); statErr == nil && info.IsDir() {
			exampleNames = append(exampleNames, entry.Name())
		}
	}

	require.NotEmpty(t, exampleNames, "expected at least one example config directory")
	sort.Strings(exampleNames)

	for _, exampleName := range exampleNames {
		exampleName := exampleName
		configDir := filepath.Join(examplesRoot, exampleName, "config")

		t.Run(exampleName, func(t *testing.T) {
			env := setupHTTPServer(t, httpTestOpts{WithModels: true})
			stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

			desired, loadErr := declarative.LoadDirectory(configDir)
			require.NoError(t, loadErr, "load example config %s", configDir)

			validationErrs := declarative.Validate(desired)
			require.Empty(t, validationErrs, "validate example config %s", configDir)

			actual, readErr := stateClient.ReadState(context.Background())
			require.NoError(t, readErr, "read actual state for %s", exampleName)

			plan := declarative.Diff(desired, actual)

			modelCreates := actionsOfKindAndOp(plan, declarative.KindModel, declarative.OpCreate)
			macroCreates := actionsOfKindAndOp(plan, declarative.KindMacro, declarative.OpCreate)

			expectation, hasExpectation := exampleExpectations[exampleName]
			if hasExpectation {
				assert.GreaterOrEqual(t, len(modelCreates), expectation.MinModelCreates, "expected model creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(macroCreates), expectation.MinMacroCreates, "expected macro creates for %s", exampleName)
			}

			actions := filterModelAndMacroActions(plan.Actions)
			require.NotEmpty(t, actions, "expected model/macro actions for %s", exampleName)
			executeExampleActions(t, stateClient, actions)

			actualAfterApply, readAfterErr := stateClient.ReadState(context.Background())
			require.NoError(t, readAfterErr, "read state after apply for %s", exampleName)

			replan := declarative.Diff(desired, actualAfterApply)
			assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindModel, declarative.OpCreate), "expected no model creates after apply for %s", exampleName)
			assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindModel, declarative.OpUpdate), "expected no model updates after apply for %s", exampleName)
			assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindMacro, declarative.OpCreate), "expected no macro creates after apply for %s", exampleName)
			assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindMacro, declarative.OpUpdate), "expected no macro updates after apply for %s", exampleName)
		})
	}
}

func filterModelAndMacroActions(actions []declarative.Action) []declarative.Action {
	filtered := make([]declarative.Action, 0, len(actions))
	for _, action := range actions {
		switch action.ResourceKind {
		case declarative.KindModel, declarative.KindMacro:
			filtered = append(filtered, action)
		}
	}
	return filtered
}

func executeExampleActions(t *testing.T, stateClient stateExecutor, actions []declarative.Action) {
	t.Helper()
	for _, action := range actions {
		require.NoError(t, stateClient.Execute(context.Background(), action), "execute %s %s", action.Operation, action.ResourceName)
	}
}

type stateExecutor interface {
	Execute(ctx context.Context, action declarative.Action) error
}

func findRepoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	cur := wd
	for {
		if _, statErr := os.Stat(filepath.Join(cur, "go.mod")); statErr == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			t.Fatalf("could not locate repository root from working directory %s", wd)
		}
		cur = parent
	}
}
