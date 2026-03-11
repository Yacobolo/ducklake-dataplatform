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
		{OperationID: "executeQuery", CLICommand: "query", Path: "/query"},
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
