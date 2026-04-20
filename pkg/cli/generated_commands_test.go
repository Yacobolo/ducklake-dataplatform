package cli

import (
	"testing"

	cobraruntime "github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

)

func TestBuildGeneratedCommandSpecsFromEndpoints_DetectsDuplicateCommands(t *testing.T) {
	t.Helper()

	root := &cobra.Command{Use: "quack"}
	err := cobraruntime.AddGeneratedCommands(root, nil, []cobraruntime.CommandSpec{
		{OperationID: "listSchemas", Command: []string{"catalog", "schemas", "list"}, Path: "/catalogs/{catalog_name}/schemas"},
		{OperationID: "listTables", Command: []string{"catalog", "schemas", "list"}, Path: "/catalogs/{catalog_name}/schemas/{schema_name}/tables"},
	}, cobraruntime.RuntimeOptions{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate generated CLI command")
	assert.Contains(t, err.Error(), "listSchemas")
	assert.Contains(t, err.Error(), "listTables")
}

func TestBuildGeneratedCommandSpecsFromEndpoints_RejectsPrefixConflicts(t *testing.T) {
	t.Helper()

	root := &cobra.Command{Use: "quack"}
	err := cobraruntime.AddGeneratedCommands(root, nil, []cobraruntime.CommandSpec{
		{OperationID: "executeQuery", Command: []string{"query"}, Path: "/query-executions"},
		{OperationID: "submitQuery", Command: []string{"query", "submit"}, Path: "/queries"},
	}, cobraruntime.RuntimeOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicts")
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

func TestAllAPIEndpoints_AddsTransformationProjectCLICommands(t *testing.T) {
	t.Helper()

	endpoints := allAPIEndpoints()
	commands := map[string]string{}
	for _, ep := range endpoints {
		switch ep.OperationID {
		case "createWorkspaceProject", "listProjectEnvironments", "createProjectDependency", "getProjectSource", "updateProjectSeed", "createProjectBuild", "createProjectModelByID", "listProjectMacrosByID", "createProjectEnvironmentBuildRun", "getCatalogTableLineage":
			commands[ep.OperationID] = ep.CLICommand
		}
	}

	assert.Equal(t, "projects create", commands["createWorkspaceProject"])
	assert.Equal(t, "projects environments list", commands["listProjectEnvironments"])
	assert.Equal(t, "projects dependencies create", commands["createProjectDependency"])
	assert.Equal(t, "projects sources get", commands["getProjectSource"])
	assert.Equal(t, "projects seeds update", commands["updateProjectSeed"])
	assert.Equal(t, "projects builds create", commands["createProjectBuild"])
	assert.Equal(t, "projects models create", commands["createProjectModelByID"])
	assert.Equal(t, "projects macros list", commands["listProjectMacrosByID"])
	assert.Equal(t, "projects environments builds runs create", commands["createProjectEnvironmentBuildRun"])
	assert.Equal(t, "catalog lineage tables get", commands["getCatalogTableLineage"])
}
