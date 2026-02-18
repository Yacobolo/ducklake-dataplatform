//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"duck-demo/internal/declarative"
)

type exampleExpectation struct {
	MinPrincipalCreates   int `yaml:"min_principal_creates"`
	MinGroupCreates       int `yaml:"min_group_creates"`
	MinGrantCreates       int `yaml:"min_grant_creates"`
	MinGroupMemberCreates int `yaml:"min_group_membership_creates"`
	MinModelCreates       int `yaml:"min_model_creates"`
	MinMacroCreates       int `yaml:"min_macro_creates"`
	MinNotebookCreates    int `yaml:"min_notebook_creates"`
	MinPipelineCreates    int `yaml:"min_pipeline_creates"`
	MinPipelineJobCreates int `yaml:"min_pipeline_job_creates"`
}

type exampleAssertionsDoc struct {
	Example      string             `yaml:"example"`
	Expectations exampleExpectation `yaml:"expectations"`
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
		assertionsPath := filepath.Join(examplesRoot, exampleName, "assertions.yaml")

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

			principalCreates := actionsOfKindAndOp(plan, declarative.KindPrincipal, declarative.OpCreate)
			groupCreates := actionsOfKindAndOp(plan, declarative.KindGroup, declarative.OpCreate)
			grantCreates := actionsOfKindAndOp(plan, declarative.KindPrivilegeGrant, declarative.OpCreate)
			groupMembershipCreates := actionsOfKindAndOp(plan, declarative.KindGroupMembership, declarative.OpCreate)
			modelCreates := actionsOfKindAndOp(plan, declarative.KindModel, declarative.OpCreate)
			macroCreates := actionsOfKindAndOp(plan, declarative.KindMacro, declarative.OpCreate)
			notebookCreates := actionsOfKindAndOp(plan, declarative.KindNotebook, declarative.OpCreate)
			pipelineCreates := actionsOfKindAndOp(plan, declarative.KindPipeline, declarative.OpCreate)
			pipelineJobCreates := actionsOfKindAndOp(plan, declarative.KindPipelineJob, declarative.OpCreate)

			expectation, hasExpectation, expErr := loadExampleExpectation(assertionsPath, exampleName)
			require.NoError(t, expErr)
			if hasExpectation {
				assert.GreaterOrEqual(t, len(principalCreates), expectation.MinPrincipalCreates, "expected principal creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(groupCreates), expectation.MinGroupCreates, "expected group creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(grantCreates), expectation.MinGrantCreates, "expected grant creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(groupMembershipCreates), expectation.MinGroupMemberCreates, "expected group membership creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(modelCreates), expectation.MinModelCreates, "expected model creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(macroCreates), expectation.MinMacroCreates, "expected macro creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(notebookCreates), expectation.MinNotebookCreates, "expected notebook creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(pipelineCreates), expectation.MinPipelineCreates, "expected pipeline creates for %s", exampleName)
				assert.GreaterOrEqual(t, len(pipelineJobCreates), expectation.MinPipelineJobCreates, "expected pipeline job creates for %s", exampleName)
			}

			actions := filterExampleApplyActions(plan.Actions)
			require.NotEmpty(t, actions, "expected showcase actions for %s", exampleName)
			executeExampleActions(t, stateClient, actions)

			actualAfterApply, readAfterErr := stateClient.ReadState(context.Background())
			require.NoError(t, readAfterErr, "read state after apply for %s", exampleName)

			replan := declarative.Diff(desired, actualAfterApply)
			assertNoCreateOrUpdate(t, replan, declarative.KindModel, "model", exampleName)
			assertNoCreateOrUpdate(t, replan, declarative.KindMacro, "macro", exampleName)
			assertNoNotebookDrift(t, replan, exampleName)
			assertNoCreateOrUpdate(t, replan, declarative.KindPipeline, "pipeline", exampleName)
			assertNoCreateOrUpdate(t, replan, declarative.KindPipelineJob, "pipeline job", exampleName)
		})
	}
}

func filterExampleApplyActions(actions []declarative.Action) []declarative.Action {
	filtered := make([]declarative.Action, 0, len(actions))
	for _, action := range actions {
		switch action.ResourceKind {
		case declarative.KindNotebook,
			declarative.KindPipeline,
			declarative.KindPipelineJob,
			declarative.KindModel,
			declarative.KindMacro:
			filtered = append(filtered, action)
		}
	}
	return filtered
}

func loadExampleExpectation(assertionsPath, exampleName string) (exampleExpectation, bool, error) {
	if _, err := os.Stat(assertionsPath); err != nil {
		if os.IsNotExist(err) {
			return exampleExpectation{}, false, nil
		}
		return exampleExpectation{}, false, err
	}

	content, err := os.ReadFile(assertionsPath)
	if err != nil {
		return exampleExpectation{}, false, err
	}

	var doc exampleAssertionsDoc
	if err := yaml.Unmarshal(content, &doc); err != nil {
		return exampleExpectation{}, false, err
	}

	if doc.Example != "" && doc.Example != exampleName {
		return exampleExpectation{}, false, fmt.Errorf("assertions example %q does not match directory name %q", doc.Example, exampleName)
	}

	return doc.Expectations, true, nil
}

func assertNoCreateOrUpdate(t *testing.T, plan *declarative.Plan, kind declarative.ResourceKind, label, exampleName string) {
	t.Helper()
	assert.Empty(t, actionsOfKindAndOp(plan, kind, declarative.OpCreate), "expected no %s creates after apply for %s", label, exampleName)
	assert.Empty(t, actionsOfKindAndOp(plan, kind, declarative.OpUpdate), "expected no %s updates after apply for %s", label, exampleName)
}

func assertNoNotebookDrift(t *testing.T, plan *declarative.Plan, exampleName string) {
	t.Helper()
	assert.Empty(t, actionsOfKindAndOp(plan, declarative.KindNotebook, declarative.OpCreate), "expected no notebook creates after apply for %s", exampleName)
	updates := actionsOfKindAndOp(plan, declarative.KindNotebook, declarative.OpUpdate)
	for _, action := range updates {
		for _, change := range action.Changes {
			assert.Equal(t, "owner", change.Field, "expected only notebook owner drift after apply for %s", exampleName)
		}
	}
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
