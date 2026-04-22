package cli

import (
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func newSummaryCmd() *cobra.Command {
	var (
		configDir          string
		allowUnknownFields bool
		loadFlags          declarativeLoadFlags
	)

	cmd := &cobra.Command{
		Use:   "summary",
		Short: "Summarize the declarative configuration tree",
		Long:  "Loads declarative CUE configuration and prints a compact operator summary of the resolved state.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			loadOptions, err := loadFlags.loadOptions(allowUnknownFields)
			if err != nil {
				return err
			}
			desired, err := declarative.LoadDirectoryWithOptions(configDir, loadOptions)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			payload := map[string]any{
				"config_dir": configDir,
				"counts":     declarativeResourceCounts(desired),
			}
			if desired.Resolution != nil {
				payload["target"] = desired.Resolution
			}

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}

			_, _ = fmt.Fprintf(os.Stdout, "Config directory: %s\n", configDir)
			if desired.Resolution != nil {
				_, _ = fmt.Fprintf(os.Stdout, "Target: %s\n", desired.Resolution.TargetRef)
				if desired.Resolution.TargetCatalog != "" || desired.Resolution.TargetSchema != "" {
					_, _ = fmt.Fprintf(os.Stdout, "Resolved schema: %s.%s\n", desired.Resolution.TargetCatalog, desired.Resolution.TargetSchema)
				}
				if len(desired.Resolution.Variables) > 0 {
					_, _ = fmt.Fprintln(os.Stdout, "Variables:")
					for _, key := range sortedStringKeys(desired.Resolution.Variables) {
						_, _ = fmt.Fprintf(os.Stdout, "  %s=%s\n", key, desired.Resolution.Variables[key])
					}
				}
			}

			_, _ = fmt.Fprintln(os.Stdout, "Resources:")
			for _, key := range sortedStringKeys(payload["counts"].(map[string]int)) {
				_, _ = fmt.Fprintf(os.Stdout, "  %s: %d\n", key, payload["counts"].(map[string]int)[key])
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&configDir, "config-dir", "./quackstack-config", "Path to the CUE configuration module")
	cmd.Flags().BoolVar(&allowUnknownFields, "allow-unknown-fields", false, "Deprecated no-op retained for compatibility with existing CLI wiring")
	addDeclarativeLoadFlags(cmd, &loadFlags)
	return cmd
}

func declarativeResourceCounts(state *declarative.DesiredState) map[string]int {
	return map[string]int{
		"api_keys":            len(state.APIKeys),
		"assets":              len(state.Assets),
		"catalogs":            len(state.Catalogs),
		"column_masks":        len(state.ColumnMasks),
		"compute_assignments": len(state.ComputeAssignments),
		"compute_endpoints":   len(state.ComputeEndpoints),
		"dashboards":          len(state.Dashboards),
		"environments":        len(state.Environments),
		"external_locations":  len(state.ExternalLocations),
		"folders":             len(state.Folders),
		"grants":              len(state.Grants),
		"groups":              len(state.Groups),
		"macros":              len(state.Macros),
		"models":              len(state.Models),
		"notebooks":           len(state.Notebooks),
		"principals":          len(state.Principals),
		"privilege_presets":   len(state.PrivilegePresets),
		"projects":            len(state.Projects),
		"row_filters":         len(state.RowFilters),
		"schemas":             len(state.Schemas),
		"semantic_models":     len(state.SemanticModels),
		"storage_credentials": len(state.StorageCredentials),
		"tables":              len(state.Tables),
		"tag_assignments":     len(state.TagAssignments),
		"tags":                len(state.Tags),
		"views":               len(state.Views),
		"volumes":             len(state.Volumes),
		"workspaces":          len(state.Workspaces),
	}
}

func sortedStringKeys[V any](items map[string]V) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
