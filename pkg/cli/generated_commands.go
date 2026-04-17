package cli

import (
	"strings"

	"github.com/spf13/cobra"

	cobraruntime "github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
	"github.com/Yacobolo/quackstack/pkg/cli/gen"
)

var errGeneratedCommands error

func addRuntimeGeneratedCommands(rootCmd *cobra.Command, client *cobraruntime.Client) {
	if err := cobraruntime.AddGeneratedCommands(rootCmd, client, gen.APIGeneratedCommandSpecs, generatedRuntimeOptions()); err != nil {
		errGeneratedCommands = err
	}
}

func generatedRuntimeOptions() cobraruntime.RuntimeOptions {
	opts := cobraruntime.RuntimeOptions{
		RunOverrides:      map[string]func(*apiruntime.Client) func(*cobra.Command, []string) error{},
		CommandMutators:   map[string]func(*cobra.Command){},
		ResponseRenderers: map[string]func(*cobra.Command, []byte) error{},
		RootGroupResolver: func(commandPath []string) *cobraruntime.CommandGroup {
			if len(commandPath) == 0 {
				return nil
			}
			groupID, ok := generatedRootGroupIDs[commandPath[0]]
			if !ok {
				return nil
			}
			title, ok := generatedRootGroupTitles[groupID]
			if !ok {
				return nil
			}
			return &cobraruntime.CommandGroup{ID: groupID, Title: title}
		},
		GroupDescriptionResolver: func(commandPath []string) string {
			if len(commandPath) == 0 {
				return ""
			}
			return generatedGroupDescriptions[strings.Join(commandPath, " ")]
		},
	}

	addQueryRuntimeOptions(&opts)
	addPrincipalRuntimeOptions(&opts)

	return opts
}

var generatedRootGroupIDs = map[string]string{
	"catalog":    groupPlatform,
	"assets":     groupPlatform,
	"audit":      groupServer,
	"compute":    groupPlatform,
	"dashboards": groupPlatform,
	"governance": groupPlatform,
	"ingestion":  groupPlatform,
	"lineage":    groupExplore,
	"me":         groupExplore,
	"models":     groupPlatform,
	"notebooks":  groupPlatform,
	"pipelines":  groupPlatform,
	"projects":   groupPlatform,
	"query":      groupPlatform,
	"security":   groupServer,
	"semantic":   groupPlatform,
	"storage":    groupPlatform,
}

var generatedRootGroupTitles = map[string]string{
	groupExplore:  "Exploration",
	groupPlatform: "Platform Resources",
	groupServer:   "Server/Admin",
}

var generatedGroupDescriptions = map[string]string{
	"catalog":               "Manage catalogs, schemas, tables, and registrations",
	"assets":                "Manage assets, runs, materializations, and freshness",
	"audit":                 "Inspect audit entries and platform activity",
	"compute":               "Manage compute endpoints, assignments, and health",
	"dashboards":            "Manage dashboards and widgets",
	"governance":            "Manage governance resources such as tags and policies",
	"ingestion":             "Manage ingestion jobs and commits",
	"lineage":               "Inspect lineage relationships and impact",
	"me":                    "Inspect personal saved and recent resources",
	"models":                "Manage models, macros, tests, and related resources",
	"notebooks":             "Manage notebooks, sessions, and jobs",
	"pipelines":             "Manage pipelines and their runs",
	"projects":              "Manage workspace projects, environments, builds, and sources",
	"query":                 "Run queries and inspect query history",
	"security":              "Manage principals, groups, grants, and API keys",
	"semantic":              "Manage semantic models, metrics, and relationships",
	"storage":               "Manage storage credentials and locations",
	"catalog registrations": "Manage registered catalogs and defaults",
	"projects environments": "Manage project environments",
	"projects builds":       "Manage project builds",
	"projects dependencies": "Manage project dependencies",
	"projects sources":      "Manage project sources",
	"projects seeds":        "Manage project seeds",
}
