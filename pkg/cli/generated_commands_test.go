package cli

import (
	"strings"
	"testing"

	cobraruntime "duck-demo/pkg/apigen/runtime/cobra"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/pkg/cli/gen"
)

func TestBuildGeneratedCommandSpecsFromEndpoints_DetectsDuplicateCommands(t *testing.T) {
	t.Helper()

	root := &cobra.Command{Use: "duck"}
	err := cobraruntime.AddGeneratedCommands(root, nil, generatedRuntimeEndpoints([]gen.APIGenEndpoint{
		{OperationID: "listSchemas", CLICommand: "catalog schemas list", Path: "/catalogs/{catalog_name}/schemas"},
		{OperationID: "listTables", CLICommand: "catalog schemas list", Path: "/catalogs/{catalog_name}/schemas/{schema_name}/tables"},
	}))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate generated CLI command")
	assert.Contains(t, err.Error(), "listSchemas")
	assert.Contains(t, err.Error(), "listTables")
}

func TestBuildGeneratedCommandSpecsFromEndpoints_PromotesSingleSegmentRoots(t *testing.T) {
	t.Helper()

	root := &cobra.Command{Use: "duck"}
	err := cobraruntime.AddGeneratedCommands(root, nil, generatedRuntimeEndpoints([]gen.APIGenEndpoint{
		{OperationID: "executeQuery", CLICommand: "query", Path: "/query-executions"},
		{OperationID: "submitQuery", CLICommand: "query submit", Path: "/queries"},
	}))
	require.NoError(t, err)

	var paths []string
	for _, cmd := range root.Commands() {
		if cmd.Name() != "query" {
			continue
		}
		for _, sub := range cmd.Commands() {
			paths = append(paths, strings.Join([]string{cmd.Name(), sub.Name()}, " "))
		}
	}
	assert.Contains(t, paths, "query execute")
	assert.Contains(t, paths, "query submit")
}

func TestAllAPIEndpoints_AddsDashboardCLICommands(t *testing.T) {
	t.Helper()

	endpoints := allAPIEndpoints()
	commands := map[string]string{}
	for _, ep := range endpoints {
		switch ep.OperationID {
		case "createDashboard", "getRenderedDashboard", "createDashboardWidget":
			commands[ep.OperationID] = ep.CLICommand
		}
	}

	assert.Equal(t, "dashboards create", commands["createDashboard"])
	assert.Equal(t, "dashboards get-rendered", commands["getRenderedDashboard"])
	assert.Equal(t, "dashboards widgets create", commands["createDashboardWidget"])
}

func TestAllAPIEndpoints_AddsAssetFreshnessCLICommands(t *testing.T) {
	t.Helper()

	endpoints := allAPIEndpoints()
	commands := map[string]string{}
	for _, ep := range endpoints {
		switch ep.OperationID {
		case "getAssetFreshness", "explainAssetFreshness", "listAssetFreshnessRequirements", "listAssetFreshnessBlockers", "reconcileAssetFreshness":
			commands[ep.OperationID] = ep.CLICommand
		}
	}

	assert.Equal(t, "assets freshness get", commands["getAssetFreshness"])
	assert.Equal(t, "assets freshness explain", commands["explainAssetFreshness"])
	assert.Equal(t, "assets freshness requirements", commands["listAssetFreshnessRequirements"])
	assert.Equal(t, "assets freshness blockers", commands["listAssetFreshnessBlockers"])
	assert.Equal(t, "assets freshness reconcile", commands["reconcileAssetFreshness"])
}

func TestAllAPIEndpoints_AddsResourceCLICommands(t *testing.T) {
	t.Helper()

	endpoints := allAPIEndpoints()
	commands := map[string]string{}
	for _, ep := range endpoints {
		switch ep.OperationID {
		case "listRecentResources", "listSavedResources", "createSavedResource", "deleteSavedResource":
			commands[ep.OperationID] = ep.CLICommand
		}
	}

	assert.Equal(t, "me recent-resources list", commands["listRecentResources"])
	assert.Equal(t, "me saved-resources list", commands["listSavedResources"])
	assert.Equal(t, "me saved-resources create", commands["createSavedResource"])
	assert.Equal(t, "me saved-resources delete", commands["deleteSavedResource"])
}
