package cli

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/pkg/cli/gen"
)

func TestBuildGeneratedCommandSpecsFromEndpoints_DetectsDuplicateCommands(t *testing.T) {
	t.Helper()

	_, err := buildGeneratedCommandSpecsFromEndpoints([]gen.APIGenEndpoint{
		{OperationID: "listSchemas", CLICommand: "catalog schemas list", Path: "/catalogs/{catalogName}/schemas"},
		{OperationID: "listTables", CLICommand: "catalog schemas list", Path: "/catalogs/{catalogName}/schemas/{schemaName}/tables"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate generated CLI command")
	assert.Contains(t, err.Error(), "listSchemas")
	assert.Contains(t, err.Error(), "listTables")
}

func TestBuildGeneratedCommandSpecsFromEndpoints_PromotesSingleSegmentRoots(t *testing.T) {
	t.Helper()

	specs, err := buildGeneratedCommandSpecsFromEndpoints([]gen.APIGenEndpoint{
		{OperationID: "executeQuery", CLICommand: "query", Path: "/queries:execute"},
		{OperationID: "submitQuery", CLICommand: "query submit", Path: "/queries"},
	})
	require.NoError(t, err)
	require.Len(t, specs, 2)

	paths := []string{
		strings.Join(specs[0].CommandPath, " "),
		strings.Join(specs[1].CommandPath, " "),
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
		case "createDashboard", "getResolvedDashboard", "createDashboardWidget":
			commands[ep.OperationID] = ep.CLICommand
		}
	}

	assert.Equal(t, "dashboards create", commands["createDashboard"])
	assert.Equal(t, "dashboards get-resolved", commands["getResolvedDashboard"])
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

	assert.Equal(t, "resources recent list", commands["listRecentResources"])
	assert.Equal(t, "resources saved list", commands["listSavedResources"])
	assert.Equal(t, "resources saved create", commands["createSavedResource"])
	assert.Equal(t, "resources saved delete", commands["deleteSavedResource"])
}
